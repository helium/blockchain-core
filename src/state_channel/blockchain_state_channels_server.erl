%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    nonce/1,
    packet/4,
    offer/4,
    gc_state_channels/1,
    state_channels/0,
    active_sc_id/0,
    active_sc/0,
    get_active_sc_count/0
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-define(SERVER, ?MODULE).
-define(STATE_CHANNELS, <<"blockchain_state_channels_server.STATE_CHANNELS">>). % also copied in sc_db_owner
-define(MAX_PAYLOAD_SIZE, 255). % lorawan max payload size is 255 bytes
%% https://hur.st/bloomfilter/?n=500000&p=1.0E-6&m=&k=20
-define(MAX_UNIQ_CLIENTS, 1000).
-define(BITMAP_SIZE, 15000000).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    scf :: rocksdb:cf_handle() | undefined,
    chain = undefined :: blockchain:blockchain() | undefined,
    swarm = undefined :: pid() | undefined,
    owner = undefined :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()} | undefined,
    state_channels = #{} :: state_channels(),
    active_sc_id = undefined :: undefined | blockchain_state_channel_v1:id(),
    streams = #{} :: streams(),
    dc_payload_size :: undefined | pos_integer(),
    sc_version = 0 :: non_neg_integer(), %% defaulting to 0 instead of undefined
    blooms = #{} :: blooms()
}).

-type state() :: #state{}.
-type sc_key() :: blockchain_state_channel_v1:id().
-type sc_value() :: {blockchain_state_channel_v1:state_channel(), skewed:skewed()}.
-type state_channels() :: #{sc_key() => sc_value()}.
-type bloom_value() :: {ClientBloom :: bloom_nif:bloom(), PacketBloom :: bloom_nif:bloom()}.
-type blooms() :: #{sc_key() => bloom_value()}.
-type streams() :: #{libp2p_crypto:pubkey_bin() => {pid(), reference()}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([insert_fake_sc_skewed/2]).

-spec insert_fake_sc_skewed(FakeSC :: blockchain_state_channel_v1:state_channel(),
                            FakeSkewed :: skewed:skewed()) -> ok.
insert_fake_sc_skewed(FakeSC, FakeSkewed) ->
    gen_server:call(?SERVER, {insert_fake_sc_skewed, FakeSC, FakeSkewed}, infinity).

-endif.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec nonce(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()} | {error, not_found}.
nonce(ID) ->
    gen_server:call(?SERVER, {nonce, ID}).

-spec packet(blockchain_state_channel_packet_v1:packet(), pos_integer(), atom(), pid()) -> ok.
packet(SCPacket, PacketTime, SCPacketHandler, HandlerPid) ->
    case SCPacketHandler:handle_packet(SCPacket, PacketTime, HandlerPid) of
        ok ->
            %% This is a valid hotspot on chain
            Packet = blockchain_state_channel_packet_v1:packet(SCPacket),
            ClientPubkeyBin = blockchain_state_channel_packet_v1:hotspot(SCPacket),
            gen_server:cast(?SERVER, {packet, ClientPubkeyBin, Packet, HandlerPid});
        {error, _Why} ->
            %% lager:warning("handle_packet failed: ~p", [Why])
            ok
    end.

-spec offer(blockchain_state_channel_offer_v1:offer(), blockchain_ledger_v1:ledger(), atom(), pid()) -> ok | reject.
offer(Offer, _Ledger, SCPacketHandler, HandlerPid) ->
    %% Get the client (i.e. the hotspot who received this packet)
    case blockchain_state_channel_offer_v1:validate(Offer) of
        {error, _Reason} ->
            lager:debug("offer failed to validate ~p ~p", [_Reason, Offer]),
            reject;
        true ->
            case SCPacketHandler:handle_offer(Offer, HandlerPid) of
                ok ->
                    gen_server:cast(?SERVER, {offer, Offer, HandlerPid});
                {error, _Why} ->
                    reject
            end
    end.

-spec gc_state_channels([ binary() ]) -> ok.
gc_state_channels([]) -> ok;
gc_state_channels(SCIDs) ->
    gen_server:cast(?SERVER, {gc_state_channels, SCIDs}).

-spec state_channels() -> state_channels().
state_channels() ->
    gen_server:call(?SERVER, state_channels, infinity).

-spec active_sc_id() -> undefined | blockchain_state_channel_v1:id().
active_sc_id() ->
    gen_server:call(?SERVER, active_sc_id, infinity).

-spec active_sc() -> undefined | blockchain_state_channel_v1:state_channel().
active_sc() ->
    gen_server:call(?SERVER, active_sc, infinity).

-spec get_active_sc_count() -> non_neg_integer().
get_active_sc_count() ->
    gen_server:call(?SERVER, get_active_sc_count, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = blockchain_state_channels_db_owner:db(),
    SCF = blockchain_state_channels_db_owner:sc_servers_cf(),
    ok = blockchain_event:add_handler(self()),
    {Owner, OwnerSigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    erlang:send_after(500, self(), post_init),
    {ok, #state{db=DB, scf=SCF, swarm=Swarm, owner={Owner, OwnerSigFun}}}.

handle_call({nonce, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
                undefined -> {error, not_found};
                {SC, _} -> {ok, blockchain_state_channel_v1:nonce(SC)}
            end,
    {reply, Reply, State};
handle_call({insert_fake_sc_skewed, FakeSC, FakeSkewed}, _From,
            #state{db=DB, state_channels=SCs, owner={_, OwnerSigFun}}=State) ->
    %% NOTE: This function is for testing, we should do something else probably
    FakeSCID = blockchain_state_channel_v1:id(FakeSC),
    SignedFakeSC = blockchain_state_channel_v1:sign(FakeSC, OwnerSigFun),
    ok = blockchain_state_channel_v1:save(DB, SignedFakeSC, FakeSkewed),
    SCMap = maps:update(FakeSCID, {SignedFakeSC, FakeSkewed}, SCs),
    {reply, ok, State#state{state_channels=SCMap}};
handle_call(state_channels, _From, #state{state_channels=SCs}=State) ->
    {reply, SCs, State};
handle_call(active_sc, _From, State) ->
    {reply, active_sc(State), State};
handle_call(active_sc_id, _From, #state{active_sc_id=ActiveSCID}=State) ->
    {reply, ActiveSCID, State};
handle_call(get_active_sc_count, _From, #state{active_sc_id=undefined}=State) ->
    {reply, 0, State};
handle_call(get_active_sc_count, _From, #state{state_channels=SCs}=State) ->
    Headroom = case application:get_env(blockchain, sc_headroom, 11) of
                   {ok, X} -> X;
                   X -> X
               end,
    Count = maps:fold(fun(_ID, {SC, _Skewed}, Acc) ->
                             SCState = blockchain_state_channel_v1:state(SC),
                             DCAmt = blockchain_state_channel_v1:amount(SC),
                             TtlDCs = blockchain_state_channel_v1:total_dcs(SC),
                             case SCState == open andalso DCAmt > TtlDCs + Headroom of
                               false -> Acc;
                               true -> Acc + 1
                             end
                      end, 0, SCs),
    {reply, Count, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({packet, _ClientPubkeyBin, SCPacket, _HandlerPid}, #state{active_sc_id=undefined}=State) ->
    lager:warning("Got packet: ~p when no sc is active", [SCPacket]),
    {noreply, State};
handle_cast({packet, ClientPubkeyBin, Packet, HandlerPid},
            #state{active_sc_id=ActiveSCID, state_channels=SCs}=State) ->
    %% ActiveSCID should always be in our state_channels map
    {SC, Skewed} = maps:get(ActiveSCID, SCs),
    NewState = process_packet(ClientPubkeyBin, Packet, SC,
                              Skewed, HandlerPid, State),
    {noreply, NewState};
handle_cast({offer, SCOffer, HandlerPid}, #state{active_sc_id=undefined}=State) ->
    erlang:spawn(
        fun() ->
            lager:warning("Got offer: ~p when no sc is active", [SCOffer]),
            %% Reject any offer if we don't have an active_sc as well, as a courtesy for the router
            ok = send_rejection(HandlerPid)
        end),
    {noreply, State};
handle_cast({offer, SCOffer, HandlerPid},
            #state{active_sc_id=ActiveSCID, state_channels=SCs, blooms=Blooms, owner={_Owner, OwnerSigFun}}=State) ->
    lager:debug("Got offer: ~p, active_sc_id: ~p", [SCOffer, ActiveSCID]),

    PayloadSize = blockchain_state_channel_offer_v1:payload_size(SCOffer),

    case PayloadSize =< ?MAX_PAYLOAD_SIZE of
        false ->
            lager:error("payload size (~p) exceeds maximum (~p). Sending rejection of offer ~p from ~p",
                        [PayloadSize, ?MAX_PAYLOAD_SIZE, SCOffer, HandlerPid]),
            ok = send_rejection(HandlerPid),
            {noreply, State};
        true ->
            Routing = blockchain_state_channel_offer_v1:routing(SCOffer),
            Region = blockchain_state_channel_offer_v1:region(SCOffer),
            Hotspot = blockchain_state_channel_offer_v1:hotspot(SCOffer),
            PacketHash = blockchain_state_channel_offer_v1:packet_hash(SCOffer),
            {ActiveSC, Skewed} = maps:get(ActiveSCID, SCs, undefined),
            {ClientBloom, _} = maps:get(ActiveSCID, Blooms),

            NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, State#state.dc_payload_size),
            TotalDCs = blockchain_state_channel_v1:total_dcs(ActiveSC),
            DCAmount = blockchain_state_channel_v1:amount(ActiveSC),
            case (TotalDCs + NumDCs) > DCAmount andalso
                 application:get_env(blockchain, prevent_sc_overspend, true) of
                true ->
                    SC1 = blockchain_state_channel_v1:sign(ActiveSC, OwnerSigFun),
                    ok = blockchain_state_channel_v1:save(State#state.db, SC1, Skewed),

                    %% will overspend so drop
                    %% TODO we should switch to the next state channel here
                    lager:warning("Dropping this packet because it will overspend DC ~p, (cost: ~p, total_dcs: ~p)",
                                [DCAmount, NumDCs, TotalDCs]),
                    lager:warning("overspend, SC1: ~p", [SC1]),
                    ok = send_rejection(HandlerPid),
                    %% NOTE: this function may return `undefined` if no SC is available
                    NewActiveID = maybe_get_new_active(maps:without([ActiveSCID], SCs), State),
                    lager:debug("Rolling to SC ID: ~p", [NewActiveID]),
                    %% switch over the active ID and save the closed one
                    NewState = State#state{active_sc_id=NewActiveID,
                                           state_channels=maps:put(ActiveSCID, {SC1, Skewed}, SCs)},
                    ok = maybe_broadcast_banner(active_sc(NewState), NewState),
                    {noreply, NewState};
                false ->
                    lager:debug("Routing: ~p, Hotspot: ~p", [Routing, Hotspot]),

                    {ok, NewSC} = send_purchase(ActiveSC, Hotspot, HandlerPid, PacketHash,
                                                PayloadSize, Region, State#state.dc_payload_size, OwnerSigFun, ClientBloom),

                    ok = blockchain_state_channel_v1:save(State#state.db, NewSC, Skewed),
                    NewState = maybe_add_stream(Hotspot, HandlerPid,
                                                State#state{state_channels=maps:put(ActiveSCID, {NewSC, Skewed}, SCs)}),
                    {noreply, NewState}
            end
    end;
handle_cast({gc_state_channels, SCIDs}, #state{state_channels=SCs}=State) ->
    %% let's make sure whatever IDs we are getting rid of here we also dump
    %% from pending writes... we don't want some ID that's been
    %% deleted from the DB to ressurrect like a zombie because it was
    %% a pending write.
    ok = blockchain_state_channels_db_owner:gc(SCIDs),
    {noreply, State#state{state_channels=maps:without(SCIDs, SCs)}};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(post_init, #state{chain=undefined}=State) ->
    case blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), post_init),
            {noreply, State};
        Chain ->
            DCPayloadSize = case blockchain_ledger_v1:config(?dc_payload_size, blockchain:ledger(Chain)) of
                                {ok, DCP} ->
                                    DCP;
                                _ ->
                                    0
                            end,
            SCVer = case blockchain_ledger_v1:config(?sc_version, blockchain:ledger(Chain)) of
                                {ok, SCV} ->
                                    SCV;
                                _ ->
                                    0
                            end,
            TempState = State#state{chain=Chain, dc_payload_size=DCPayloadSize, sc_version=SCVer},
            LoadState = update_state_with_ledger_channels(TempState),
            lager:info("loaded state channels: ~p", [LoadState#state.state_channels]),
            NewState = update_state_with_blooms(LoadState),
            {noreply, NewState}
    end;
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    {noreply, State#state{chain=NC}};
handle_info({blockchain_event, {add_block, _BlockHash, _Syncing, _Ledger}}, #state{chain=undefined}=State) ->
    erlang:send_after(500, self(), post_init),
    {noreply, State};
handle_info({blockchain_event, {add_block, BlockHash, _Syncing, Ledger}}, #state{chain=Chain}=State0) ->
    NewState = case blockchain:get_block(BlockHash, Chain) of
                   {error, Reason} ->
                       lager:error("Couldn't get block with hash: ~p, reason: ~p", [BlockHash, Reason]),
                       State0;
                   {ok, Block} ->
                       BlockHeight = blockchain_block:height(Block),
                       Txns = get_state_channels_txns_from_block(Chain, BlockHash, State0),
                       State1 = lists:foldl(
                                  fun(Txn, State) ->
                                          case blockchain_txn:type(Txn) of
                                              blockchain_txn_state_channel_open_v1 ->
                                                  update_state_sc_open(Txn, BlockHash, BlockHeight, State);
                                              blockchain_txn_state_channel_close_v1 ->
                                                  update_state_sc_close(Txn, State)
                                          end
                                  end,
                                  State0,
                                  Txns),
                       check_state_channel_expiration(BlockHeight, State1)
               end,

    DCPayloadSize = case blockchain_ledger_v1:config(?dc_payload_size, Ledger) of
                        {ok, DCP} ->
                            DCP;
                        _ ->
                            0
                    end,
    SCVer = case blockchain_ledger_v1:config(?sc_version, blockchain:ledger(Chain)) of
                {ok, SCV} ->
                    SCV;
                _ ->
                    0
            end,
    {noreply, NewState#state{dc_payload_size=DCPayloadSize, sc_version=SCVer}};
handle_info({'DOWN', _Ref, process, Pid, _}, State=#state{streams=Streams}) ->
    FilteredStreams = maps:filter(fun(_Name, {Stream, _}) ->
                                          Stream /= Pid
                                  end, Streams),
    {noreply, State#state{streams=FilteredStreams}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
-spec process_packet(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                     Packet :: blockchain_helium_packet_v1:packet(),
                     SC :: blockchain_state_channel_v1:state_channel(),
                     Skewed :: skewed:skewed(),
                     HandlerPid :: pid(),
                     State :: state()) -> NewState :: state().
process_packet(ClientPubkeyBin, Packet, SC, Skewed, HandlerPid,
               #state{db=DB, sc_version=SCVer, active_sc_id=ActiveSCID, state_channels=SCs,
                      blooms=Blooms, owner={_, OwnerSigFun}}=State) ->

    Payload = blockchain_helium_packet_v1:payload(Packet),

    {ClientBloom, PacketBloom} = maps:get(ActiveSCID, Blooms),

    case SCVer > 1 andalso bloom:check_and_set(PacketBloom, Payload) of
        true ->
            %% Don't add payload
            maybe_add_stream(ClientPubkeyBin, HandlerPid, State);
        false ->
            {SC1, Skewed1} = blockchain_state_channel_v1:add_payload(Payload, SC, Skewed),

            ExistingSCNonce = blockchain_state_channel_v1:nonce(SC1),
            SC2 = blockchain_state_channel_v1:nonce(ExistingSCNonce + 1, SC1),

            NewSC = case SCVer of
                        2 ->
                            %% we don't update the state channel summary here
                            %% it happens in `send_purchase` for v2 SCs
                            SC2;
                        _ ->
                            update_sc_summary(ClientPubkeyBin, byte_size(Payload), State#state.dc_payload_size, SC2, ClientBloom)
                    end,

            SignedSC = blockchain_state_channel_v1:sign(NewSC, OwnerSigFun),

            %% save it
            ok = blockchain_state_channel_v1:save(DB, SignedSC, Skewed1),

            %% Put new state_channel in our map
            TempState = State#state{state_channels=maps:update(ActiveSCID, {SignedSC, Skewed1}, SCs)},
            maybe_add_stream(ClientPubkeyBin, HandlerPid, TempState)
    end.

-spec maybe_add_stream(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                       Stream :: pid(),
                       State :: state()) -> state().
maybe_add_stream(ClientPubkeyBin, Stream, #state{streams=Streams}=State) ->
   State#state{streams =
               maps:update_with(ClientPubkeyBin, fun({_OldStream, OldRef}) ->
                                                         %% we have an existing stream, demonitor it
                                                         %% and monitor new one
                                                         erlang:demonitor(OldRef),
                                                         Ref = erlang:monitor(process, Stream),
                                                         {Stream, Ref}
                                                 end,
                                %% value if not present
                                {Stream, erlang:monitor(process, Stream)},
                                Streams)}.
-spec update_state_sc_open(
        Txn :: blockchain_txn_state_channel_open_v1:txn_state_channel_open(),
        BlockHash :: blockchain_block:hash(),
        BlockHeight :: pos_integer(),
        State :: state()) -> state().
update_state_sc_open(Txn,
                     BlockHash,
                     BlockHeight,
                     #state{owner={Owner, OwnerSigFun},
                            state_channels=SCs,
                            active_sc_id=ActiveSCID,
                            blooms=Blooms}=State) ->
    case blockchain_txn_state_channel_open_v1:owner(Txn) of
        %% Do the map put when we are the owner of the state_channel
        Owner ->
            ID = blockchain_txn_state_channel_open_v1:id(Txn),
            Amt = blockchain_txn_state_channel_open_v1:amount(Txn),
            ExpireWithin = blockchain_txn_state_channel_open_v1:expire_within(Txn),
            {SC, Skewed} = blockchain_state_channel_v1:new(ID,
                                                           Owner,
                                                           Amt,
                                                           BlockHash,
                                                           (BlockHeight + ExpireWithin)),

            SignedSC = blockchain_state_channel_v1:sign(SC, OwnerSigFun),

            {ok, ClientBloom} = bloom:new(?BITMAP_SIZE, ?MAX_UNIQ_CLIENTS),
            {ok, PacketBloom} = bloom:new(?BITMAP_SIZE, max(Amt, 1)),

            case ActiveSCID of
                undefined ->
                    %% Switching active sc, broadcast banner
                    lager:info("broadcasting banner scid: ~p",
                               [libp2p_crypto:bin_to_b58(blockchain_state_channel_v1:id(SignedSC))]),
                    ok = maybe_broadcast_banner(SignedSC, State),

                    %% Don't have any active state channel
                    %% Set this one to active
                    State#state{state_channels=maps:put(ID, {SignedSC, Skewed}, SCs),
                                active_sc_id=ID,
                                blooms=maps:put(ID, {ClientBloom, PacketBloom}, Blooms)};
                _A ->
                    State#state{state_channels=maps:put(ID, {SignedSC, Skewed}, SCs),
                                blooms=maps:put(ID, {ClientBloom, PacketBloom}, Blooms)}
            end;
        _ ->
            %% Don't do anything cuz we're not the owner
            State
    end.

-spec broadcast_banner(SC :: blockchain_state_channel_v1:state_channel(),
                       State :: state()) -> ok.
broadcast_banner(SC, #state{streams=Streams}) ->
    case maps:size(Streams) of
        0 -> ok;
        _ ->
            _Res = blockchain_utils:pmap(
                     fun({Stream, _Ref}) ->
                             catch send_banner(SC, Stream)
                     end, maps:values(Streams)),
            ok
    end.

-spec update_state_sc_close(
        Txn :: blockchain_txn_state_channel_close_v1:txn_state_channel_close(),
        State :: state()) -> state().
update_state_sc_close(Txn, #state{db=DB, scf=SCF, blooms=Blooms, state_channels=SCs, active_sc_id=ActiveSCID}=State) ->
    SC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
    ID = blockchain_state_channel_v1:id(SC),

    NewActiveSCID = case ActiveSCID of
                        undefined ->
                            %% No sc was active
                            undefined;
                        ID ->
                            %% Our active state channel got closed,
                            ExcludedActiveSCs = maps:without([ID], SCs),
                            maybe_get_new_active(ExcludedActiveSCs, State);
                        A ->
                            %% Some other sc was active, let it remain active
                            A
                    end,

    %% Delete closed state channel from sc database
    ok = delete_closed_sc(DB, SCF, ID),

    NewState = State#state{state_channels=maps:remove(ID, SCs),
                           blooms=maps:remove(ID, Blooms),
                           active_sc_id=NewActiveSCID},

    case NewActiveSCID /= ActiveSCID of
        true ->
            ok = maybe_broadcast_banner(active_sc(NewState), NewState);
        false ->
            ok
    end,

    NewState.

%%--------------------------------------------------------------------
%% @doc
%% Close expired state channels
%% @end
%%--------------------------------------------------------------------
-spec check_state_channel_expiration(BlockHeight :: pos_integer(),
                                     State :: state()) -> state().
check_state_channel_expiration(BlockHeight, #state{owner={Owner, OwnerSigFun},
                                                   active_sc_id=ActiveSCID,
                                                   state_channels=SCs}=State) ->
    NewStateChannels = maps:map(
                        fun(_ID, {SC, Skewed}) ->
                                ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
                                case ExpireAt =< BlockHeight andalso blockchain_state_channel_v1:state(SC) == open of
                                    false ->
                                        {SC, Skewed};
                                    true ->
                                        SC0 = blockchain_state_channel_v1:state(closed, SC),
                                        SC1 = blockchain_state_channel_v1:sign(SC0, OwnerSigFun),
                                        ok = close_state_channel(SC1, Owner, OwnerSigFun),
                                        {SC1, Skewed}
                                end
                        end,
                        SCs
                       ),

    NewActiveSCID = case ActiveSCID of
                        undefined ->
                            undefined;
                        _ ->
                            {ActiveSC, _ActiveSCSkewed} = maps:get(ActiveSCID, NewStateChannels),
                            case blockchain_state_channel_v1:state(ActiveSC) of
                                closed ->
                                    maybe_get_new_active(maps:without([ActiveSCID], NewStateChannels), State);
                                _ ->
                                    ActiveSCID
                            end
                    end,

    NewState = State#state{active_sc_id=NewActiveSCID, state_channels=NewStateChannels},
    ok = maybe_broadcast_banner(active_sc(NewState), NewState),

    NewState.


%%--------------------------------------------------------------------
%% @doc
%% Close state channel
%% @end
%%--------------------------------------------------------------------
-spec close_state_channel(SC :: blockchain_state_channel_v1:state_channel(),
                          Owner :: libp2p_crypto:pubkey_bin(),
                          OwnerSigFun :: function()) -> ok.
close_state_channel(SC, Owner, OwnerSigFun) ->
    SignedSC = blockchain_state_channel_v1:sign(SC, OwnerSigFun),
    Txn = blockchain_txn_state_channel_close_v1:new(SignedSC, Owner),
    SignedTxn = blockchain_txn_state_channel_close_v1:sign(Txn, OwnerSigFun),
    ok = blockchain_worker:submit_txn(SignedTxn),
    lager:info("closing state channel ~p: ~p",
               [libp2p_crypto:bin_to_b58(blockchain_state_channel_v1:id(SC)), SignedTxn]),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Get Block and open/close transactions
%% @end
%%--------------------------------------------------------------------
-spec get_state_channels_txns_from_block(
        Chain :: blockchain:blockchain(),
        BlockHash :: blockchain_block:hash(),
        State :: state()) -> [blockchain_txn_state_channel_open_v1:txn_state_channel_open() |
                              blockchain_txn_state_channel_close_v1:txn_state_channel_close()].
get_state_channels_txns_from_block(Chain, BlockHash, #state{state_channels=SCs, owner={Owner, _}}) ->
    case blockchain:get_block(BlockHash, Chain) of
        {error, _Reason} ->
            lager:error("failed to get block:~p ~p", [BlockHash, _Reason]),
            [];
        {ok, Block} ->
            lists:filter(
                fun(Txn) ->
                    case blockchain_txn:type(Txn) of
                        blockchain_txn_state_channel_open_v1 ->
                            not maps:is_key(blockchain_txn_state_channel_open_v1:id(Txn), SCs) andalso
                            blockchain_txn_state_channel_open_v1:owner(Txn) == Owner;
                        blockchain_txn_state_channel_close_v1 ->
                            SC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
                            maps:is_key(blockchain_state_channel_v1:id(SC), SCs) andalso
                            blockchain_state_channel_v1:owner(SC) == Owner;
                        _ -> false
                    end
                end,
                blockchain_block:transactions(Block)
            )
    end.

-spec update_state_with_ledger_channels(State :: state()) -> state().
update_state_with_ledger_channels(#state{db=DB}=State) ->
    ConvertedSCs = convert_to_state_channels(State),
    lager:info("state channels rehydrated from ledger: ~p", [ConvertedSCs]),
    ConvertedSCKeys = maps:keys(ConvertedSCs),
    DBSCs = lists:foldl(
              fun(ID, Acc) ->
                      case blockchain_state_channel_v1:fetch(DB, ID) of
                          {error, _Reason} ->
                              % TODO: Maybe cleanup not_found state channels from list
                              lager:warning("could not get state channel ~p: ~p",
                                            [libp2p_crypto:bin_to_b58(ID), _Reason]),
                              Acc;
                          {ok, {SC, Skewed}} ->
                              lager:info("updating state from scdb ID: ~p",
                                         [libp2p_crypto:bin_to_b58(ID), SC]),
                              maps:put(ID, {SC, Skewed}, Acc)
                      end
              end,
              #{}, ConvertedSCKeys),
    lager:info("fetched state channels from database writes: ~p", [DBSCs]),
    %% Merge DBSCs with ConvertedSCs with only matching IDs
    SCs = maps:merge(ConvertedSCs, maps:with(ConvertedSCKeys, DBSCs)),
    lager:info("scs after merge: ~p", [SCs]),

    %% These don't exist in the ledger but we have them in the sc db,
    %% presumably these have been closed
    ClosedSCIDs = maps:keys(maps:without(ConvertedSCKeys, DBSCs)),
    lager:debug("presumably closed sc ids: ~p", [ClosedSCIDs]),

    NewActiveSCID = maybe_get_new_active(SCs, State),
    lager:info("NewActiveSCID: ~p", [NewActiveSCID]),
    State#state{state_channels=SCs, active_sc_id=NewActiveSCID}.

-spec get_state_channels(DB :: rocksdb:db_handle(),
                         SCF :: rocksdb:cf_handle()) ->
   {ok, [blockchain_state_channel_v1:id()]} | {error, any()}.
get_state_channels(DB, SCF) ->
    case rocksdb:get(DB, SCF, ?STATE_CHANNELS, []) of
        {ok, Bin} ->
            L = binary_to_term(Bin),
            lager:debug("found sc ids from db: ~p", [L]),
            {ok, L};
        not_found ->
            lager:warning("no state_channel found in db"),
            {ok, []};
        Error ->
            lager:error("error: ~p", [Error]),
            Error
    end.

-spec delete_closed_sc(DB :: rocksdb:db_handle(),
                       SCF :: rocksdb:cf_handle(),
                       ID :: blockchain_state_channel_v1:id()) -> ok.
delete_closed_sc(DB, SCF, ID) ->
    case get_state_channels(DB, SCF) of
        {error, _} ->
            %% Can't delete anything
            ok;
        {ok, SCIDs} ->
            case lists:member(ID, SCIDs) of
                false ->
                    %% not in db
                    ok;
                true ->
                    ToInsert = erlang:term_to_binary(lists:delete(ID, SCIDs)),
                    rocksdb:put(DB, SCF, ?STATE_CHANNELS, ToInsert, [{sync, true}])
            end
    end.

-spec convert_to_state_channels(State :: state()) -> state_channels().
convert_to_state_channels(#state{chain=Chain, owner={Owner, OwnerSigFun}}) ->
    Ledger = blockchain:ledger(Chain),
    {ok, LedgerSCs} = blockchain_ledger_v1:find_scs_by_owner(Owner, Ledger),
    {ok, Head} = blockchain:head_block(Chain),
    maps:map(fun(ID, LedgerStateChannel) ->
                     SCMod = blockchain_ledger_v1:get_sc_mod(LedgerStateChannel, Ledger),
                     Owner = SCMod:owner(LedgerStateChannel),
                     ExpireAt = SCMod:expire_at_block(LedgerStateChannel),
                     Amount = case SCMod of
                                  blockchain_ledger_state_channel_v2 -> SCMod:original(LedgerStateChannel);
                                  _ -> 0
                              end,

                     SC0 = blockchain_state_channel_v1:new(ID, Owner, Amount),
                     Nonce = SCMod:nonce(LedgerStateChannel),
                     Filter = fun(T) -> blockchain_txn:type(T) == blockchain_txn_state_channel_open_v1 andalso
                                        blockchain_txn_state_channel_open_v1:id(T) == ID andalso
                                        blockchain_txn_state_channel_open_v1:nonce(T) == Nonce
                              end,
                     BlockHash = blockchain:fold_chain(fun(Block, undefined) ->
                                                               case blockchain_utils:find_txn(Block, Filter) of
                                                                   [_T] ->
                                                                       blockchain_block:hash_block(Block);
                                                                   _ ->
                                                                       undefined
                                                               end;
                                                          (_, _Hash) -> return
                                                       end, undefined, Head, Chain),
                     SC1 = blockchain_state_channel_v1:expire_at_block(ExpireAt, SC0),
                     SignedSC = blockchain_state_channel_v1:sign(SC1, OwnerSigFun),
                     Skewed = skewed:new(BlockHash),
                     {SignedSC, Skewed}
             end,
             LedgerSCs).

%%--------------------------------------------------------------------
%% @doc
%% Get a new active state channel based, based on their expiration
%% @end
%%-------------------------------------------------------------------
-spec maybe_get_new_active(SCs :: state_channels(),
                           State :: state()) -> undefined | blockchain_state_channel_v1:id().
maybe_get_new_active(SCs, State) ->
    {ok, BlockHeight} = blockchain:height(blockchain_worker:blockchain()),
    case maps:to_list(SCs) of
        [] ->
            %% Don't have any state channel in state
            undefined;
        L ->
            %% We want to pick the next active state channel which has a higher block expiration
            %% but lower nonce
            SCSortFun = fun({_ID1, {SC1, _}}, {_ID2, {SC2, _}}) ->
                                blockchain_state_channel_v1:expire_at_block(SC1) =< blockchain_state_channel_v1:expire_at_block(SC2)
                        end,
            SCSortFun2 = fun({_ID1, {SC1, _}}, {_ID2, {SC2, _}}) ->
                                 blockchain_state_channel_v1:nonce(SC1) >= blockchain_state_channel_v1:nonce(SC2)
                         end,

            Headroom = case application:get_env(blockchain, sc_headroom, 11) of
                           {ok, X} -> X;
                           X -> X
                       end,
            FilterFun = fun({_, {SC, _}}) ->
                                case State#state.sc_version of
                                    2 ->
                                        ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
                                        ExpireAt > BlockHeight andalso
                                        blockchain_state_channel_v1:state(SC) == open andalso
                                        blockchain_state_channel_v1:amount(SC) > (blockchain_state_channel_v1:total_dcs(SC) + Headroom);
                                    _ ->
                                        %% We are not on sc_version=2, just set this to true to include any state channel
                                        true
                                end
                        end,

            case lists:filter(FilterFun, lists:sort(SCSortFun2, lists:sort(SCSortFun, L))) of
                [] -> undefined;
                Y ->
                    [{ID, _}|_] = Y,
                    ID
            end
    end.

-spec send_purchase(SC :: blockchain_state_channel_v1:state_channel(),
                    Hotspot :: libp2p_crypto:pubkey_bin(),
                    Stream :: pid(),
                    PacketHash :: binary(),
                    PayloadSize :: pos_integer(),
                    Region :: atom(),
                    DCPayloadSize :: undefined | pos_integer(),
                    OwnerSigFun :: libp2p_crypto:sig_fun(),
                    ClientBloom :: bloom_nif:bloom()) -> {ok, blockchain_state_channel_v1:state_channel()}.
send_purchase(SC, Hotspot, Stream, PacketHash, PayloadSize, Region, DCPayloadSize, OwnerSigFun, ClientBloom) ->
    SCNonce = blockchain_state_channel_v1:nonce(SC),
    NewPurchaseSC0 = blockchain_state_channel_v1:nonce(SCNonce + 1, SC),
    NewPurchaseSC = update_sc_summary(Hotspot, PayloadSize, DCPayloadSize, NewPurchaseSC0, ClientBloom),
    SignedPurchaseSC = blockchain_state_channel_v1:sign(NewPurchaseSC, OwnerSigFun),
    %% make the handler do the purchase construction
    ok = blockchain_state_channel_common:send_purchase(Stream, SignedPurchaseSC, Hotspot, PacketHash, Region),
    {ok, SignedPurchaseSC}.

-spec active_sc(State :: state()) -> undefined | blockchain_state_channel_v1:state_channel().
active_sc(#state{active_sc_id=undefined}) ->
    undefined;
active_sc(#state{state_channels=SCs, active_sc_id=ActiveSCID}) ->
    {ActiveSC, _} = maps:get(ActiveSCID, SCs, undefined),
    ActiveSC.

-spec send_banner(SC :: blockchain_state_channel_v1:state_channel(),
                  Stream :: pid()) -> ok.
send_banner(SC, Stream) ->
    %% NOTE: The banner itself is not signed, however, the state channel
    %% it contains should be signed already
    BannerMsg1 = blockchain_state_channel_banner_v1:new(SC),
    blockchain_state_channel_common:send_banner(Stream, BannerMsg1).

-spec send_rejection(Stream :: pid()) -> ok.
send_rejection(Stream) ->
    RejectionMsg = blockchain_state_channel_rejection_v1:new(),
    blockchain_state_channel_common:send_rejection(Stream, RejectionMsg).

-spec update_sc_summary(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                        PayloadSize :: pos_integer(),
                        DCPayloadSize :: undefined | pos_integer(),
                        SC :: blockchain_state_channel_v1:state_channel(),
                        ClientBloom :: bloom_nif:bloom()) ->
    blockchain_state_channel_v1:state_channel().
update_sc_summary(ClientPubkeyBin, PayloadSize, DCPayloadSize, SC, ClientBloom) ->
    case blockchain_state_channel_v1:get_summary(ClientPubkeyBin, SC) of
        {error, not_found} ->
            NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, DCPayloadSize),
            NewSummary = blockchain_state_channel_summary_v1:new(ClientPubkeyBin, 1, NumDCs),
            %% Add this to summaries
            {NewSC, DidFit} = blockchain_state_channel_v1:update_summary_for(ClientPubkeyBin,
                                                                             NewSummary,
                                                                             SC,
                                                                             bloom:check(ClientBloom, ClientPubkeyBin)),
            ok = maybe_set_client_bloom(ClientPubkeyBin, ClientBloom, DidFit),
            NewSC;
        {ok, ExistingSummary} ->
            %% Update packet count for this client
            ExistingNumPackets = blockchain_state_channel_summary_v1:num_packets(ExistingSummary),
            %% Update DC count for this client
            NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, DCPayloadSize),
            ExistingNumDCs = blockchain_state_channel_summary_v1:num_dcs(ExistingSummary),
            NewSummary = blockchain_state_channel_summary_v1:update(ExistingNumDCs + NumDCs,
                                                                    ExistingNumPackets + 1,
                                                                    ExistingSummary),
            %% Update summaries
            {NewSC, DidFit} = blockchain_state_channel_v1:update_summary_for(ClientPubkeyBin,
                                                                             NewSummary,
                                                                             SC,
                                                                             bloom:check(ClientBloom, ClientPubkeyBin)),
            ok = maybe_set_client_bloom(ClientPubkeyBin, ClientBloom, DidFit),
            NewSC
    end.

-spec maybe_set_client_bloom(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                             ClientBloom :: bloom_nif:bloom(),
                             DidFit :: boolean()) -> ok.
maybe_set_client_bloom(_ClientPubkeyBin, _ClientBloom, false) -> ok;
maybe_set_client_bloom(ClientPubkeyBin, ClientBloom, true) ->
    bloom:set(ClientBloom, ClientPubkeyBin).

-spec maybe_broadcast_banner(SC :: undefined | blockchain_state_channel_v1:state_channel(),
                             State :: state()) -> ok.
maybe_broadcast_banner(_, #state{chain=undefined}) -> ok;
maybe_broadcast_banner(undefined, _State) -> ok;
maybe_broadcast_banner(SC, State) ->
    case State#state.sc_version of
        2 ->
            ok = broadcast_banner(SC, State);
        _ ->
            ok
    end.

-spec update_state_with_blooms(State :: state()) -> state().
update_state_with_blooms(#state{state_channels=SCs}=State) when map_size(SCs) == 0 ->
    State;
update_state_with_blooms(#state{state_channels=SCs}=State) ->
    Blooms = maps:map(fun(_, {SC, _}) ->
                              {ok, ClientBloom} = bloom:new(?BITMAP_SIZE, ?MAX_UNIQ_CLIENTS),
                              Amount = blockchain_state_channel_v1:amount(SC),
                              {ok, PacketBloom} = bloom:new(?BITMAP_SIZE, max(Amount, 1)),
                              {ClientBloom, PacketBloom}
                      end, SCs),
    State#state{blooms=Blooms}.