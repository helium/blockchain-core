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
    active_sc_ids/0,
    active_scs/0,
    get_active_sc_count/0,
    select_best_active_sc/3
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
-include("blockchain_utils.hrl").

-define(SERVER, ?MODULE).
-define(STATE_CHANNELS, <<"blockchain_state_channels_server.STATE_CHANNELS">>). % also copied in sc_db_owner
-define(MAX_PAYLOAD_SIZE, 255). % lorawan max payload size is 255 bytes
-define(FP_RATE, 0.99).
-define(ETS, blockchain_state_channels_server_ets).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    cf :: rocksdb:cf_handle() | undefined,
    chain = undefined :: blockchain:blockchain() | undefined,
    swarm = undefined :: pid() | undefined,
    owner = undefined :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()} | undefined,
    state_channels = #{} :: state_channels(),
    active_sc_ids = [] :: [blockchain_state_channel_v1:id()],
    streams = #{} :: streams(),
    dc_payload_size :: undefined | pos_integer(),
    sc_version = 0 :: non_neg_integer(), %% defaulting to 0 instead of undefined
    blooms = #{} :: blooms(),
    max_actors_allowed = ?SC_MAX_ACTORS :: pos_integer()
}).

-type state() :: #state{}.
-type sc_key() :: blockchain_state_channel_v1:id().
-type sc_value() :: {blockchain_state_channel_v1:state_channel(), skewed:skewed()}.
-type state_channels() :: #{sc_key() => sc_value()}.
-type blooms() :: #{sc_key() => bloom_nif:bloom()}.
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
            ClientPubKeyBin = blockchain_state_channel_packet_v1:hotspot(SCPacket),
            gen_server:cast(?SERVER, {packet, ClientPubKeyBin, Packet, HandlerPid});
        {error, _Why} ->
            lager:debug("handle_packet failed: ~p", [_Why]),
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

-spec gc_state_channels([binary()]) -> ok.
gc_state_channels([]) -> ok;
gc_state_channels(SCIDs) ->
    gen_server:cast(?SERVER, {gc_state_channels, SCIDs}).

-spec state_channels() -> state_channels().
state_channels() ->
    gen_server:call(?SERVER, state_channels, infinity).

-spec active_sc_ids() -> [blockchain_state_channel_v1:id()].
active_sc_ids() ->
    gen_server:call(?SERVER, active_sc_ids, infinity).

-spec active_scs() -> [blockchain_state_channel_v1:state_channel()].
active_scs() ->
    case ets:lookup(?ETS, active_scs) of
        [] -> [];
        [{active_scs, ActiveSCs}] -> ActiveSCs
    end.

-spec get_active_sc_count() -> non_neg_integer().
get_active_sc_count() ->
    gen_server:call(?SERVER, get_active_sc_count, infinity).

-spec select_best_active_sc(libp2p_crypto:pubkey_bin(),
                            [blockchain_state_channel_v1:state_channel()],
                            pos_integer()) ->
    {ok, blockchain_state_channel_v1:state_channel()} | {error, not_found}.
select_best_active_sc(PubKeyBin, ActiveSCs, Max) ->
    CanFitFilterFun = fun(ActiveSC) ->
        blockchain_state_channel_v1:can_fit(PubKeyBin, ActiveSC, Max)
    end,
    case lists:filter(CanFitFilterFun, ActiveSCs) of
        [] ->
            {error, not_found};
        FilteredActiveSCs ->
           InSumFilterFun = fun(ActiveSC) ->
                blockchain_state_channel_v1:get_summary(PubKeyBin, ActiveSC) =/= {error, not_found}
            end,
            case lists:filter(InSumFilterFun, ActiveSCs) of
                [] ->
                    [ActiveSC|_] = FilteredActiveSCs,
                    {ok, ActiveSC};
                [ActiveSC|_] ->
                    {ok, ActiveSC}
            end
    end.

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
    _ = ets:new(?ETS, [public, named_table, set]),
    erlang:send_after(500, self(), post_init),
    {ok, #state{db=DB, cf=SCF, swarm=Swarm, owner={Owner, OwnerSigFun}}}.

handle_call({nonce, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply =
        case maps:get(ID, SCs, undefined) of
            undefined -> {error, not_found};
            {SC, _} -> {ok, blockchain_state_channel_v1:nonce(SC)}
        end,
    {reply, Reply, State};
handle_call(state_channels, _From, #state{state_channels=SCs}=State) ->
    {reply, SCs, State};
handle_call(active_sc_ids, _From, #state{active_sc_ids=ActiveSCIDs}=State) ->
    {reply, ActiveSCIDs, State};
handle_call(get_active_sc_count, _From, State) ->
    {reply, get_active_sc_count(State), State};
%% NOTE: This function is for testing, we should do something else probably
handle_call({insert_fake_sc_skewed, FakeSC, FakeSkewed}, _From,
            #state{db=DB, state_channels=SCs, owner={_, OwnerSigFun}}=State) ->
    FakeSCID = blockchain_state_channel_v1:id(FakeSC),
    SignedFakeSC = blockchain_state_channel_v1:sign(FakeSC, OwnerSigFun),
    ok = blockchain_state_channel_v1:save(DB, SignedFakeSC, FakeSkewed),
    SCMap = maps:update(FakeSCID, {SignedFakeSC, FakeSkewed}, SCs),
    {reply, ok, State#state{state_channels=SCMap}};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({offer, SCOffer, HandlerPid}, #state{active_sc_ids=[]}=State) ->
    lager:warning("got offer: ~p from ~p when no sc is active", [SCOffer, HandlerPid]),
    ok = send_rejection(HandlerPid),
    {noreply, State};
handle_cast({offer, SCOffer, HandlerPid}, State0) ->
    lager:debug("got offer: ~p from ~p", [SCOffer, HandlerPid]),
    State1 = handle_offer(SCOffer, HandlerPid, State0),
    ok = update_active_scs_cache(State1),
    {noreply, State1};
handle_cast({packet, _ClientPubKeyBin, SCPacket, _HandlerPid}, #state{active_sc_ids=[]}=State) ->
    lager:warning("got packet: ~p from ~p/~p when no sc is active", [SCPacket, _ClientPubKeyBin, _HandlerPid]),
    {noreply, State};
handle_cast({packet, ClientPubKeyBin, Packet, HandlerPid}, State0) ->
    lager:debug("got packet: ~p from ~p/~p", [Packet, ClientPubKeyBin, HandlerPid]),
    State1 = handle_packet(ClientPubKeyBin, Packet, HandlerPid, State0),
    ok = update_active_scs_cache(State1),
    {noreply, State1};
handle_cast({gc_state_channels, SCIDs}, #state{state_channels=SCs}=State0) ->
    %% let's make sure whatever IDs we are getting rid of here we also dump
    %% from pending writes... we don't want some ID that's been
    %% deleted from the DB to ressurrect like a zombie because it was
    %% a pending write.
    ok = blockchain_state_channels_db_owner:gc(SCIDs),
    State1 = State0#state{state_channels=maps:without(SCIDs, SCs)},
    ok = update_active_scs_cache(State1),
    {noreply, State1};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(post_init, #state{chain=undefined}=State0) ->
    case blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), post_init),
            {noreply, State0};
        Chain ->
            Ledger = blockchain:ledger(Chain),
            DCPayloadSize =
                case blockchain_ledger_v1:config(?dc_payload_size, Ledger) of
                    {ok, DCP} ->
                        DCP;
                    _ ->
                        0
                end,
            SCVer =
                case blockchain_ledger_v1:config(?sc_version, Ledger) of
                    {ok, SCV} ->
                        SCV;
                    _ ->
                        0
                end,
            MaxActorsAllowed = blockchain_ledger_v1:get_sc_max_actors(Ledger),
            TempState = State0#state{chain=Chain, dc_payload_size=DCPayloadSize, sc_version=SCVer, max_actors_allowed=MaxActorsAllowed},
            LoadState = update_state_with_ledger_channels(TempState),
            lager:info("loaded state channels: ~p", [LoadState#state.state_channels]),
            State1 = update_state_with_blooms(LoadState),
            ok = update_active_scs_cache(State1),
            {noreply, State1}
    end;
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    {noreply, State#state{chain=NC}};
handle_info({blockchain_event, {add_block, _BlockHash, _Syncing, _Ledger}}, #state{chain=undefined}=State) ->
    erlang:send_after(500, self(), post_init),
    {noreply, State};
handle_info({blockchain_event, {add_block, BlockHash, _Syncing, Ledger}}, #state{chain=Chain}=State0) ->
    State1 = case blockchain:get_block(BlockHash, Chain) of
                   {error, Reason} ->
                       lager:error("Couldn't get block with hash: ~p, reason: ~p", [BlockHash, Reason]),
                       State0;
                   {ok, Block} ->
                       BlockHeight = blockchain_block:height(Block),
                       Txns = get_state_channels_txns_from_block(Chain, BlockHash, State0),
                       State = lists:foldl(
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
                       check_state_channel_expiration(BlockHeight, State)
               end,

    DCPayloadSize = case blockchain_ledger_v1:config(?dc_payload_size, Ledger) of
                        {ok, DCP} ->
                            DCP;
                        _ ->
                            0
                    end,
    SCVer = case blockchain_ledger_v1:config(?sc_version, Ledger) of
                {ok, SCV} ->
                    SCV;
                _ ->
                    0
            end,
    MaxActorsAllowed = blockchain_ledger_v1:get_sc_max_actors(Ledger),
    State2 = State1#state{dc_payload_size=DCPayloadSize, sc_version=SCVer, max_actors_allowed=MaxActorsAllowed},
    ok = update_active_scs_cache(State2),
    {noreply, State2};
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
-spec handle_packet(ClientPubKeyBin :: libp2p_crypto:pubkey_bin(),
                     Packet :: blockchain_helium_packet_v1:packet(),
                     HandlerPid :: pid(),
                     State :: state()) -> NewState :: state().
handle_packet(ClientPubKeyBin, Packet, HandlerPid,
              #state{db=DB, sc_version=SCVer,state_channels=SCs,
                     blooms=Blooms, owner={_, OwnerSigFun},
                     max_actors_allowed=MaxActorsAllowed}=State) ->
    Payload = blockchain_helium_packet_v1:payload(Packet),
    case select_best_active_sc(ClientPubKeyBin, State) of
        {error, _Reason} ->
            lager:warning("we failed to select a state channel ~p", [_Reason]),
            State;
        {ok, SC} ->
            ActiveSCID = blockchain_state_channel_v1:id(SC),
            PacketBloom = maps:get(ActiveSCID, Blooms),
            {_, Skewed} = maps:get(ActiveSCID, SCs),
            case SCVer > 1 andalso bloom:check_and_set(PacketBloom, Payload) of
                true ->
                    %% Don't add payload
                    maybe_add_stream(ClientPubKeyBin, HandlerPid, State);
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
                                    {SC3, _} =
                                        update_sc_summary(ClientPubKeyBin, byte_size(Payload),
                                                          State#state.dc_payload_size, SC2, MaxActorsAllowed),
                                    SC3
                            end,

                    SignedSC = blockchain_state_channel_v1:sign(NewSC, OwnerSigFun),
                    %% save it
                    ok = blockchain_state_channel_v1:save(DB, SignedSC, Skewed1),
                    %% Put new state_channel in our map  
                    TempState = State#state{state_channels=maps:update(ActiveSCID, {SignedSC, Skewed1}, SCs)},
                    maybe_add_stream(ClientPubKeyBin, HandlerPid, TempState)
            end
    end.

handle_offer(SCOffer, HandlerPid, #state{db=DB, dc_payload_size=DCPayloadSize, active_sc_ids=ActiveSCIDs,
                                         state_channels=SCs, owner={_Owner, OwnerSigFun},
                                         max_actors_allowed=MaxActorsAllowed}=State0) ->
    Hotspot = blockchain_state_channel_offer_v1:hotspot(SCOffer),
    HotspotName = blockchain_utils:addr2name(Hotspot),
    lager:debug("handling offer from ~p", [HotspotName]),
    PayloadSize = blockchain_state_channel_offer_v1:payload_size(SCOffer),
    case PayloadSize =< ?MAX_PAYLOAD_SIZE of
        false ->
            lager:error("payload size (~p) exceeds maximum (~p). Sending rejection of offer ~p from ~p",
                        [PayloadSize, ?MAX_PAYLOAD_SIZE, SCOffer, HandlerPid]),
            ok = send_rejection(HandlerPid),
            {noreply, State0};
        true ->
            case select_best_active_sc(Hotspot, State0) of
                {error, _Reason} ->
                    lager:debug("could not get a good active SC (~p)", [_Reason]),
                    case maybe_get_new_active(State0) of
                        undefined ->
                            lager:warning("could not get a new active SC (~p), rejecting", [_Reason]),
                            ok = send_rejection(HandlerPid),
                            State0;
                        NewActiveID ->
                            lager:info("adding new active SC ~p", [blockchain_utils:addr2name(NewActiveID)]),
                            State1 = State0#state{active_sc_ids=ActiveSCIDs ++ [NewActiveID]},
                            handle_offer(SCOffer, HandlerPid, State1)
                    end;
                {ok, ActiveSC} ->
                    ActiveSCID = blockchain_state_channel_v1:id(ActiveSC),
                    lager:debug("selected state channel ~p", [blockchain_utils:addr2name(ActiveSCID)]),
                    NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, State0#state.dc_payload_size),
                    TotalDCs = blockchain_state_channel_v1:total_dcs(ActiveSC),
                    DCAmount = blockchain_state_channel_v1:amount(ActiveSC),
                    PreventOverSpend = application:get_env(blockchain, prevent_sc_overspend, true),
                    case (TotalDCs + NumDCs) > DCAmount andalso PreventOverSpend of
                        true ->
                            lager:warning("dropping this packet because it will overspend DC ~p, (cost: ~p, total_dcs: ~p)",
                                          [DCAmount, NumDCs, TotalDCs]),
                            lager:warning("overspend, SC1: ~p", [ActiveSC]),
                            ok = send_rejection(HandlerPid),
                            State1 =
                                case maybe_get_new_active([ActiveSCID], State0) of
                                    undefined ->
                                        State0#state{active_sc_ids=lists:delete(ActiveSCID, ActiveSCIDs)};
                                    NewActiveSCID ->
                                        State0#state{active_sc_ids=lists:delete(ActiveSCID, ActiveSCIDs) ++ [NewActiveSCID]}
                                end,
                            ok = maybe_broadcast_banner([ActiveSC], State1),
                            State1;
                        false ->
                            Routing = blockchain_state_channel_offer_v1:routing(SCOffer),
                            lager:debug("routing: ~p, hotspot: ~p", [Routing, HotspotName]),
                            case
                                try_update_summary(ActiveSC, Hotspot, PayloadSize,
                                                   DCPayloadSize, MaxActorsAllowed)
                            of
                                {error, _Reason} ->
                                    lager:warning("dropping this packet because: ~p ~p",
                                                  [_Reason, lager:pr(ActiveSC, blockchain_state_channel_v1)]),
                                    ok = send_rejection(HandlerPid),
                                    State0;
                                {ok, PurchaseSC} ->
                                    lager:debug("purchasing offer from ~p", [HotspotName]),
                                    SignedPurchaseSC = blockchain_state_channel_v1:sign(PurchaseSC, OwnerSigFun),
                                    PacketHash = blockchain_state_channel_offer_v1:packet_hash(SCOffer),
                                    Region = blockchain_state_channel_offer_v1:region(SCOffer),
                                    ok = blockchain_state_channel_handler:send_purchase(HandlerPid,
                                                                                        SignedPurchaseSC,
                                                                                        Hotspot,
                                                                                        PacketHash,
                                                                                        Region),
                                    {_, Skewed} = maps:get(ActiveSCID, SCs),
                                    ok = blockchain_state_channel_v1:save(DB, SignedPurchaseSC, Skewed),
                                    maybe_add_stream(Hotspot,
                                                         HandlerPid,
                                                         State0#state{state_channels=maps:put(ActiveSCID, {SignedPurchaseSC, Skewed}, SCs)})
                            end
                            
                    end
            end
    end.

-spec select_best_active_sc(libp2p_crypto:pubkey_bin(), state()) ->
    {ok, blockchain_state_channel_v1:state_channel()} | {error, not_found}.
select_best_active_sc(PubKeyBin, #state{max_actors_allowed=MaxActorsAllowed}=State) ->
    ?MODULE:select_best_active_sc(PubKeyBin, active_scs(State), MaxActorsAllowed).

-spec maybe_add_stream(ClientPubKeyBin :: libp2p_crypto:pubkey_bin(),
                       Stream :: pid(),
                       State :: state()) -> state().
maybe_add_stream(ClientPubKeyBin, Stream, #state{streams=Streams}=State) ->
   State#state{streams =
               maps:update_with(ClientPubKeyBin, fun({_OldStream, OldRef}) ->
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
                            active_sc_ids=ActiveSCIDs,
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

            {ok, PacketBloom} = bloom:new_optimal(max(Amt, 1), ?FP_RATE),

            case ActiveSCIDs of
                [] ->
                    lager:info("no active state channel setting: ~p as active",
                               [blockchain_utils:addr2name(blockchain_state_channel_v1:id(SignedSC))]),
                    ok = maybe_broadcast_banner([SignedSC], State),
                    State#state{state_channels=maps:put(ID, {SignedSC, Skewed}, SCs),
                                active_sc_ids=[ID],
                                blooms=maps:put(ID, PacketBloom, Blooms)};
                _ ->
                    lager:debug("already got ~p active", [[blockchain_utils:addr2name(I) || I <- ActiveSCIDs]]),
                    State#state{state_channels=maps:put(ID, {SignedSC, Skewed}, SCs),
                                blooms=maps:put(ID, PacketBloom, Blooms)}
            end;
        _ ->
            %% Don't do anything cuz we're not the owner
            State
    end.

-spec update_state_sc_close(Txn :: blockchain_txn_state_channel_close_v1:txn_state_channel_close(),
                            State :: state()) -> state().
update_state_sc_close(Txn, #state{db=DB, cf=SCF, blooms=Blooms, state_channels=SCs0, active_sc_ids=ActiveSCIDs0}=State0) ->
    ClosedSC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
    ClosedID = blockchain_state_channel_v1:id(ClosedSC),
    ok = delete_closed_sc(DB, SCF, ClosedID),
    ActiveSCIDs1 = lists:delete(ClosedID, ActiveSCIDs0),
    SCs1 = maps:remove(ClosedID, SCs0),
    NewActiveSCIDs =
        case erlang:length(ActiveSCIDs1) < 2 andalso maps:size(SCs1) > 1 of
            true ->
                case maybe_get_new_active([ClosedID], State0) of
                    undefined ->
                        ActiveSCIDs1;
                    NewActiveSCID ->
                        ActiveSCIDs1 ++ [NewActiveSCID]
                end;
            false ->
                ActiveSCIDs1
        end,
    State1 =
        State0#state{state_channels=SCs1,
                     blooms=maps:remove(ClosedID, Blooms),
                     active_sc_ids=NewActiveSCIDs},
    case NewActiveSCIDs /= ActiveSCIDs1 of
        true ->
            ok = maybe_broadcast_banner(active_scs(State1), State1);
        false ->
            ok
    end,
    State1.

%%--------------------------------------------------------------------
%% @doc
%% Close expired state channels
%% @end
%%--------------------------------------------------------------------
-spec check_state_channel_expiration(BlockHeight :: pos_integer(),
                                     State :: state()) -> state().
check_state_channel_expiration(BlockHeight, #state{owner={Owner, OwnerSigFun},
                                                   active_sc_ids=ActiveSCIDs,
                                                   state_channels=SCs}=State0) ->
    lager:debug("check state channels expiration", []),
    NewStateChannels =
        maps:map(
            fun(ID, {SC, Skewed}) ->
                ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
                case ExpireAt =< BlockHeight andalso blockchain_state_channel_v1:state(SC) == open of
                    false ->
                        {SC, Skewed};
                    true ->
                        lager:info("closing ~p expired", [blockchain_utils:addr2name(ID)]),
                        SC0 = blockchain_state_channel_v1:state(closed, SC),
                        SC1 = blockchain_state_channel_v1:sign(SC0, OwnerSigFun),
                        ok = close_state_channel(SC1, Owner, OwnerSigFun),
                        {SC1, Skewed}
                end
            end,
            SCs
        ),
    NewActiveSCIDs =
        case ActiveSCIDs of
            [] ->
                [];
            _ ->
                ClosedSCIDs = lists:foldl(
                    fun(ActiveSCID, Acc) ->
                        {ActiveSC, _ActiveSCSkewed} = maps:get(ActiveSCID, NewStateChannels),
                        case blockchain_state_channel_v1:state(ActiveSC) of
                            closed ->
                                [ActiveSCID|Acc];
                            _ ->
                                Acc
                        end
                    end,
                    [],
                    ActiveSCIDs
                ),
                case ActiveSCIDs -- ClosedSCIDs of
                    [] ->
                        lager:info("no ActiveSCIDs, attempting to activate more"),
                        case maybe_get_new_active(ClosedSCIDs, State0) of
                            undefined -> ActiveSCIDs;
                            NewActiveSCID -> ActiveSCIDs ++ [NewActiveSCID]
                        end;
                    StillActiveIDs ->
                        lager:debug("we still have ~p, opened", [[blockchain_utils:addr2name(I) || I <- StillActiveIDs]]),
                        StillActiveIDs
                end
        end,
    lager:debug("we still have ~p, opened", [[blockchain_utils:addr2name(I) || I <- NewActiveSCIDs]]),
    State1 = State0#state{active_sc_ids=NewActiveSCIDs, state_channels=NewStateChannels},
    ok = maybe_broadcast_banner(active_scs(State1), State1),
    State1.


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
               [blockchain_utils:addr2name(blockchain_state_channel_v1:id(SC)), SignedTxn]),
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

-spec update_state_with_ledger_channels(State0 :: state()) -> state().
update_state_with_ledger_channels(#state{db=DB, chain=Chain}=State0) ->
    LedgerSCs = get_ledger_state_channels(State0),
    lager:info("state channels rehydrated from ledger: ~p", [LedgerSCs]),
    LedgerSCKeys = maps:keys(LedgerSCs),
    DBSCs = lists:foldl(
              fun(ID, Acc) ->
                      case blockchain_state_channel_v1:fetch(DB, ID) of
                          {error, _Reason} ->
                              % TODO: Maybe cleanup not_found state channels from list
                              lager:warning("could not get state channel ~p: ~p",
                                            [blockchain_utils:addr2name(ID), _Reason]),
                              Acc;
                          {ok, {SC, Skewed}} ->
                              lager:info("updating state from scdb ID: ~p ~p",
                                         [blockchain_utils:addr2name(ID), SC]),
                              maps:put(ID, {SC, Skewed}, Acc)
                      end
              end,
              #{}, LedgerSCKeys),
    lager:info("fetched state channels from database writes: ~p", [DBSCs]),
    %% Merge DBSCs with LedgerSCs with only matching IDs
    SCs = maps:merge(LedgerSCs, maps:with(LedgerSCKeys, DBSCs)),
    lager:info("scs after merge: ~p", [SCs]),

    %% These don't exist in the ledger but we have them in the sc db,
    %% presumably these have been closed
    ClosedSCIDs = maps:keys(maps:without(LedgerSCKeys, DBSCs)),
    lager:info("presumably closed sc ids: ~p", [[blockchain_utils:addr2name(I) || I <- ClosedSCIDs]]),

    {ok, BlockHeight} = blockchain:height(Chain),
    Headroom =
        case application:get_env(blockchain, sc_headroom, 11) of
            {ok, X} -> X;
            X -> X
        end,
    
    ActiveSCIDs =
        maps:fold(
            fun(ID, {SC, _}, Acc) ->
                ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
                case
                    ExpireAt > BlockHeight andalso
                    blockchain_state_channel_v1:state(SC) == open andalso
                    blockchain_state_channel_v1:amount(SC) > (blockchain_state_channel_v1:total_dcs(SC) + Headroom) andalso
                    erlang:length(blockchain_state_channel_v1:summaries(SC)) > 0
                of 
                    true ->
                        [ID|Acc];
                    false ->
                        Acc
                end
            end,
            [],
            SCs
        ),
    SortedActiveSCIDs =
        lists:sort(
            fun(IDA, IDB) ->
                {SCA, _} = maps:get(IDA, SCs),
                {SCB, _} = maps:get(IDB, SCs),
                erlang:length(blockchain_state_channel_v1:summaries(SCA)) >  erlang:length(blockchain_state_channel_v1:summaries(SCB))
            end,
            ActiveSCIDs
        ),
    State1 = State0#state{state_channels=SCs, active_sc_ids=SortedActiveSCIDs},
    case SortedActiveSCIDs of
        [] ->
            case maybe_get_new_active(State1) of
                undefined ->
                    State1;
                ID ->
                    State1#state{active_sc_ids=[ID]}
            end;
        _ ->
            State1
    end.

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

-spec get_ledger_state_channels(State :: state()) -> state_channels().
get_ledger_state_channels(#state{chain=Chain, owner={Owner, OwnerSigFun}}) ->
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
%% Get a new active state channel based on their expiration
%% @end
%%-------------------------------------------------------------------
-spec maybe_get_new_active(State :: state()) -> blockchain_state_channel_v1:id() | undefined.
maybe_get_new_active(State) ->
    maybe_get_new_active([], State).

-spec maybe_get_new_active(WithoutSCIDs :: [blockchain_state_channel_v1:id()], State :: state()) -> blockchain_state_channel_v1:id() | undefined.
maybe_get_new_active(WithoutSCIDs, #state{chain=Chain, sc_version=SCVersion, active_sc_ids=ActiveSCIDs, state_channels=SCs}) ->
    {ok, BlockHeight} = blockchain:height(Chain),
    case maps:to_list(maps:without(WithoutSCIDs, maps:without(ActiveSCIDs, SCs))) of
        [] ->
            %% Don't have any state channel in state
            undefined;
        L ->
            %% We want to pick the next active state channel which has a higher block expiration
            %% but lower nonce
            SCSortFun1 =
                fun({_ID1, {SC1, _}}, {_ID2, {SC2, _}}) ->
                    blockchain_state_channel_v1:expire_at_block(SC1) =< blockchain_state_channel_v1:expire_at_block(SC2)
                end,
            SCSortFun2 =
                fun({_ID1, {SC1, _}}, {_ID2, {SC2, _}}) ->
                    blockchain_state_channel_v1:nonce(SC1) >= blockchain_state_channel_v1:nonce(SC2)
                end,
            Headroom =
                case application:get_env(blockchain, sc_headroom, 11) of
                    {ok, X} -> X;
                    X -> X
                end,
            FilterFun =
                fun({_, {SC, _}}) ->
                        case SCVersion of
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
            case lists:filter(FilterFun, lists:sort(SCSortFun2, lists:sort(SCSortFun1, L))) of
                [] ->
                    undefined;
                Y ->
                    [{ID, _}|_] = Y,
                    ID
            end
    end.

-spec try_update_summary(SC :: blockchain_state_channel_v1:state_channel(),
                         Hotspot :: libp2p_crypto:pubkey_bin(),
                         PayloadSize :: pos_integer(),
                         DCPayloadSize :: undefined | pos_integer(),
                         MaxActorsAllowed :: non_neg_integer()) ->
    {ok, blockchain_state_channel_v1:state_channel()} | {error, does_not_fit}.
try_update_summary(SC, Hotspot, PayloadSize, DCPayloadSize, MaxActorsAllowed) ->
    SCNonce = blockchain_state_channel_v1:nonce(SC),
    NewPurchaseSC0 = blockchain_state_channel_v1:nonce(SCNonce + 1, SC),
    case update_sc_summary(Hotspot, PayloadSize, DCPayloadSize, NewPurchaseSC0, MaxActorsAllowed) of
        {NewPurchaseSC1, true} -> {ok, NewPurchaseSC1};
        {_SC, false} -> {error, does_not_fit}
    end.

-spec active_scs(State :: state()) -> [blockchain_state_channel_v1:state_channel()].
active_scs(#state{active_sc_ids=[]}) ->
    [];
active_scs(#state{state_channels=SCs, active_sc_ids=ActiveSCIDs}) ->
    F = fun(ID, Map) -> 
        {SC, _} = maps:get(ID, Map),
        SC
    end,
    [F(ID, SCs) || ID <- ActiveSCIDs].

-spec get_active_sc_count(State :: state()) -> non_neg_integer().
%% Only count open state channels; do not count state channels that are closed
get_active_sc_count(#state{active_sc_ids=[]}) -> 0;
get_active_sc_count(#state{active_sc_ids=ActiveIds, state_channels=SCMap, chain=Chain}) ->
   Ledger = blockchain:ledger(Chain),
   case blockchain_ledger_v1:config(?sc_only_count_open_active, Ledger) of
      {ok, true} ->
         lists:foldl(fun(Id, Acc) ->
                           {SC, _Skewed} = maps:get(Id, SCMap),
                           case blockchain_state_channel_v1:state(SC) of
                              open -> Acc + 1;
                              _ -> Acc
                           end
                     end, 0, ActiveIds);
      _ ->
         length(ActiveIds)
   end.


-spec send_rejection(Stream :: pid()) -> ok.
send_rejection(Stream) ->
    erlang:spawn(
        fun() ->
            RejectionMsg = blockchain_state_channel_rejection_v1:new(),
            ok = blockchain_state_channel_handler:send_rejection(Stream, RejectionMsg)
        end
    ),
    ok.

-spec update_sc_summary(ClientPubKeyBin :: libp2p_crypto:pubkey_bin(),
                        PayloadSize :: pos_integer(),
                        DCPayloadSize :: undefined | pos_integer(),
                        SC :: blockchain_state_channel_v1:state_channel(),
                        MaxActorsAllowed :: non_neg_integer()) ->
    {blockchain_state_channel_v1:state_channel(), boolean()}.
update_sc_summary(ClientPubKeyBin, PayloadSize, DCPayloadSize, SC, MaxActorsAllowed) ->
    case blockchain_state_channel_v1:get_summary(ClientPubKeyBin, SC) of
        {error, not_found} ->
            NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, DCPayloadSize),
            NewSummary = blockchain_state_channel_summary_v1:new(ClientPubKeyBin, 1, NumDCs),
            %% Add this to summaries
            {NewSC, DidFit} = blockchain_state_channel_v1:update_summary_for(ClientPubKeyBin,
                                                                             NewSummary,
                                                                             SC,
                                                                             MaxActorsAllowed),
            {NewSC, DidFit};
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
            {NewSC, DidFit} = blockchain_state_channel_v1:update_summary_for(ClientPubKeyBin,
                                                                             NewSummary,
                                                                             SC,
                                                                             MaxActorsAllowed),
            {NewSC, DidFit}
    end.

-spec maybe_broadcast_banner(SC :: [blockchain_state_channel_v1:state_channel()],
                             State :: state()) -> ok.
maybe_broadcast_banner(_, #state{chain=undefined}) -> ok;
maybe_broadcast_banner([], _State) -> ok;
maybe_broadcast_banner(SCs, #state{sc_version=SCVersion}=State) ->
    lists:foreach(
        fun(SC) ->
            case SCVersion of
                2 ->
                    ok = broadcast_banner(SC, State);
                _ ->
                    ok
            end
        end,
        SCs
    ).

-spec broadcast_banner(SC :: blockchain_state_channel_v1:state_channel(),
                       State :: state()) -> ok.
broadcast_banner(SC, #state{streams=Streams}) ->
    case maps:size(Streams) of
        0 ->
            ok;
        _ ->
            _ =
                blockchain_utils:pmap(
                    fun({Stream, _Ref}) ->
                        catch send_banner(SC, Stream)
                    end,
                    maps:values(Streams)
                ),
            ok
    end.

-spec send_banner(SC :: blockchain_state_channel_v1:state_channel(),
                  Stream :: pid()) -> ok.
send_banner(SC, Stream) ->
    %% NOTE: The banner itself is not signed, however, the state channel
    %% it contains should be signed already
    BannerMsg1 = blockchain_state_channel_banner_v1:new(SC),
    blockchain_state_channel_handler:send_banner(Stream, BannerMsg1).

-spec update_state_with_blooms(State :: state()) -> state().
update_state_with_blooms(#state{state_channels=SCs}=State) when map_size(SCs) == 0 ->
    State;
update_state_with_blooms(#state{state_channels=SCs}=State) ->
    Blooms = maps:map(fun(_, {SC, _}) ->
                              Amount = blockchain_state_channel_v1:amount(SC),
                              {ok, PacketBloom} = bloom:new_optimal(max(Amount, 1), ?FP_RATE),
                              PacketBloom
                      end, SCs),
    State#state{blooms=Blooms}.

-spec update_active_scs_cache(State :: state()) -> ok.
update_active_scs_cache(State) ->
    true = ets:insert(?ETS, {active_scs, active_scs(State)}),
    ok.