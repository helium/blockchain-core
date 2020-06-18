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
    packet/2,
    offer/2,
    state_channels/0,
    active_sc_id/0,
    active_sc/0
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).
-define(STATE_CHANNELS, <<"blockchain_state_channels_server.STATE_CHANNELS">>).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    chain = undefined :: blockchain:blockchain() | undefined,
    swarm = undefined :: pid() | undefined,
    owner = undefined :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()} | undefined,
    state_channels = #{} :: state_channels(),
    active_sc_id = undefined :: undefined | blockchain_state_channel_v1:id(),
    sc_packet_handler = undefined :: undefined | atom(),
    streams = #{} :: streams()
}).

-type state() :: #state{}.
-type state_channels() :: #{blockchain_state_channel_v1:id() => {blockchain_state_channel_v1:state_channel(),
                                                                 skewed:skewed()}}.
-type streams() :: #{libp2p_crypto:pubkey_bin() => pid()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec nonce(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()} | {error, not_found}.
nonce(ID) ->
    gen_server:call(?SERVER, {nonce, ID}).

-spec packet(blockchain_state_channel_packet_v1:packet(), pid()) -> ok.
packet(Packet, HandlerPid) ->
    spawn(fun() ->
                  case blockchain_state_channel_packet_v1:validate(Packet) of
                      {error, _Reason} ->
                          lager:warning("packet failed to validate ~p ~p", [_Reason, Packet]);
                      true ->
                          SCPacketHandler = application:get_env(blockchain, sc_packet_handler, undefined),
                          case SCPacketHandler:handle_packet(Packet, HandlerPid) of
                              ok ->
                                  gen_server:cast(?SERVER, {packet, Packet, HandlerPid});
                              {error, Why} ->
                                  lager:error("handle_packet failed: ~p", [Why])
                          end
                  end
          end),
    ok.

-spec offer(blockchain_state_channel_offer_v1:offer(), pid()) -> ok.
offer(Offer, HandlerPid) ->
    spawn(fun() ->
                  case blockchain_state_channel_offer_v1:validate(Offer) of
                      {error, _Reason} ->
                          lager:debug("offer failed to validate ~p ~p", [_Reason, Offer]);
                      true ->
                          SCPacketHandler = application:get_env(blockchain, sc_packet_handler, undefined),
                          case SCPacketHandler:handle_offer(Offer, HandlerPid) of
                              ok ->
                                  gen_server:cast(?SERVER, {offer, Offer, HandlerPid});
                              {error, Why} ->
                                  lager:error("handle_offer failed: ~p", [Why])
                          end
                  end
          end),
    ok.


-spec state_channels() -> state_channels().
state_channels() ->
    gen_server:call(?SERVER, state_channels, infinity).

-spec active_sc_id() -> undefined | blockchain_state_channel_v1:id().
active_sc_id() ->
    gen_server:call(?SERVER, active_sc_id, infinity).

-spec active_sc() -> undefined | blockchain_state_channel_v1:state_channel().
active_sc() ->
    gen_server:call(?SERVER, active_sc, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = blockchain_state_channels_db_owner:db(),
    SCPacketHandler = application:get_env(blockchain, sc_packet_handler, undefined),
    ok = blockchain_event:add_handler(self()),
    {Owner, OwnerSigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    erlang:send_after(500, self(), post_init),
    {ok, #state{db=DB, swarm=Swarm, owner={Owner, OwnerSigFun}, sc_packet_handler=SCPacketHandler}}.

handle_call({nonce, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
                undefined -> {error, not_found};
                {SC, _} -> {ok, blockchain_state_channel_v1:nonce(SC)}
            end,
    {reply, Reply, State};
handle_call(state_channels, _From, #state{state_channels=SCs}=State) ->
    {reply, SCs, State};
handle_call(active_sc, _From, State) ->
    {reply, active_sc(State), State};
handle_call(active_sc_id, _From, #state{active_sc_id=ActiveSCID}=State) ->
    {reply, ActiveSCID, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({packet, SCPacket, _HandlerPid}, #state{active_sc_id=undefined}=State) ->
    lager:warning("Got packet: ~p when no sc is active", [SCPacket]),
    {noreply, State};
handle_cast({packet, SCPacket, HandlerPid},
            #state{db=DB, active_sc_id=ActiveSCID, state_channels=SCs, chain=Chain, owner={_Owner, OwnerSigFun}}=State) ->
    Ledger = blockchain:ledger(Chain),

    %% Get the client (i.e. the hotspot who received this packet)
    ClientPubkeyBin = blockchain_state_channel_packet_v1:hotspot(SCPacket),

    case blockchain_gateway_cache:get(ClientPubkeyBin, Ledger) of
        {error, _} ->
            %% This client does not exist on chain, ignore
            {noreply, State};
        {ok, _} ->
            %% This is a valid hotspot on chain
            %% Get raw packet
            Packet = blockchain_state_channel_packet_v1:packet(SCPacket),
            %% ActiveSCID should always be in our state_channels map
            {SC, Skewed} = maps:get(ActiveSCID, SCs),
            %% Get payload from packet
            Payload = blockchain_helium_packet_v1:payload(Packet),
            %% Add this payload to state_channel's skewed merkle
            {SC1, Skewed1} = blockchain_state_channel_v1:add_payload(Payload, SC, Skewed),
            SC2 = update_sc_summary(ClientPubkeyBin, byte_size(Payload), Ledger, SC1, OwnerSigFun),
            ExistingSCNonce = blockchain_state_channel_v1:nonce(SC2),
            NewSC = blockchain_state_channel_v1:nonce(ExistingSCNonce + 1, SC2),

            %% Save state channel to db
            ok = blockchain_state_channel_v1:save(DB, NewSC, Skewed1),
            ok = store_active_sc_id(DB, ActiveSCID),

            %% Put new state_channel in our map
            lager:info("packet: ~p successfully validated, updating state",
                       [blockchain_utils:bin_to_hex(blockchain_helium_packet_v1:encode(Packet))]),

            %% Since we updated active sc, send new banner
            ok = send_banner(NewSC, HandlerPid),

            {noreply, State#state{state_channels=maps:update(ActiveSCID, {NewSC, Skewed1}, SCs)}}
    end;
handle_cast({offer, SCOffer, _Pid}, #state{active_sc_id=undefined}=State) ->
    lager:warning("Got offer: ~p when no sc is active", [SCOffer]),
    {noreply, State};
handle_cast({offer, SCOffer, HandlerPid},
            #state{active_sc_id=ActiveSCID, state_channels=SCs, owner={_Owner, OwnerSigFun}, chain=Chain}=State) ->
    %% TODO: handle offer
    lager:info("Got offer: ~p, active_sc_id: ~p", [SCOffer, ActiveSCID]),

    Ledger = blockchain:ledger(Chain),
    Routing = blockchain_state_channel_offer_v1:routing(SCOffer),
    Region = blockchain_state_channel_offer_v1:region(SCOffer),
    Hotspot = blockchain_state_channel_offer_v1:hotspot(SCOffer),
    PacketHash = blockchain_state_channel_offer_v1:packet_hash(SCOffer),
    PayloadSize = blockchain_state_channel_offer_v1:payload_size(SCOffer),
    {ActiveSC, _} = maps:get(ActiveSCID, SCs, undefined),
    lager:info("Routing: ~p, Hotspot: ~p", [Routing, Hotspot]),

    %% XXX: Accepting all offers for now
    ok = send_purchase(ActiveSC, Hotspot, HandlerPid, PacketHash, PayloadSize, Region, Ledger, OwnerSigFun),
    {noreply, State};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(post_init, #state{chain=undefined}=State) ->
    case blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), post_init),
            {noreply, State};
        Chain ->
            TempState = State#state{chain=Chain},
            LoadState = update_state_with_ledger_channels(TempState),
            lager:info("load state: ~p", [LoadState]),
            {noreply, LoadState}
    end;
handle_info({blockchain_event, {add_block, _BlockHash, _Syncing, _Ledger}}, #state{chain=undefined}=State) ->
    erlang:send_after(500, self(), post_init),
    {noreply, State};
handle_info({blockchain_event, {add_block, BlockHash, _Syncing, _Ledger}}, #state{chain=Chain}=State0) ->
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

    {noreply, NewState};
handle_info({'DOWN', _Ref, process, Pid, _}, State=#state{streams=Streams}) ->
    FilteredStreams = maps:filter(fun(_Name, Stream) ->
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
-spec update_state_sc_open(
        Txn :: blockchain_txn_state_channel_open_v1:txn_state_channel_open(),
        BlockHash :: blockchain_block:hash(),
        BlockHeight :: pos_integer(),
        State :: state()) -> state().
update_state_sc_open(Txn,
                     BlockHash,
                     BlockHeight,
                     #state{owner={Owner, OwnerSigFun}, state_channels=SCs, active_sc_id=ActiveSCID}=State) ->
    case blockchain_txn_state_channel_open_v1:owner(Txn) of
        %% Do the map put when we are the owner of the state_channel
        Owner ->
            ID = blockchain_txn_state_channel_open_v1:id(Txn),
            ExpireWithin = blockchain_txn_state_channel_open_v1:expire_within(Txn),
            {SC, Skewed} = blockchain_state_channel_v1:new(ID,
                                                           Owner,
                                                           BlockHash,
                                                           (BlockHeight + ExpireWithin)),

            SignedSC = blockchain_state_channel_v1:sign(SC, OwnerSigFun),

            case ActiveSCID of
                undefined ->
                    %% Switching active sc, broadcast banner
                    lager:info("broadcasting banner: ~p", [SignedSC]),
                    ok = broadcast_banner(SignedSC, State),

                    %% Don't have any active state channel
                    %% Set this one to active
                    State#state{state_channels=maps:put(ID, {SignedSC, Skewed}, SCs), active_sc_id=ID};
                _A ->
                    State#state{state_channels=maps:put(ID, {SignedSC, Skewed}, SCs)}
            end;
        _ ->
            %% Don't do anything cuz we're not the owner
            State
    end.

-spec broadcast_banner(SC :: undefined | blockchain_state_channel_v1:state_channel(),
                       State :: state()) -> ok.
broadcast_banner(undefined, _) -> ok;
broadcast_banner(SC, #state{streams=Streams}) ->
    case maps:size(Streams) of
        0 -> ok;
        _ ->
            _Res = blockchain_utils:pmap(
                     fun(Stream) ->
                             catch send_banner(SC, Stream)
                     end, maps:values(Streams)),
            ok
    end.

-spec update_state_sc_close(
        Txn :: blockchain_txn_state_channel_close_v1:txn_state_channel_close(),
        State :: state()) -> state().
update_state_sc_close(Txn, #state{db=DB, state_channels=SCs, active_sc_id=ActiveSCID}=State) ->
    SC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
    ID = blockchain_state_channel_v1:id(SC),

    NewActiveSCID = case ActiveSCID of
                        undefined ->
                            %% No sc was active
                            undefined;
                        ID ->
                            %% Our active state channel got closed,
                            ExcludedActiveSCs = maps:without([ID], SCs),
                            maybe_get_new_active(ExcludedActiveSCs);
                        A ->
                            %% Some other sc was active, let it remain active
                            A
                    end,

    %% Delete closed state channel from sc database
    ok = delete_closed_sc(DB, ID),

    NewState = State#state{state_channels=maps:remove(ID, SCs), active_sc_id=NewActiveSCID},

    %% Switching active sc, broadcast banner
    lager:info("broadcasting banner: ~p", [active_sc(NewState)]),
    ok = broadcast_banner(active_sc(NewState), NewState),

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
                                    maybe_get_new_active(maps:without([ActiveSCID], NewStateChannels));
                                _ ->
                                    ActiveSCID
                            end
                    end,

    NewState = State#state{active_sc_id=NewActiveSCID, state_channels=NewStateChannels},

    %% Switching active sc, broadcast banner
    ok = broadcast_banner(active_sc(NewState), NewState),

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
    Txn = blockchain_txn_state_channel_close_v1:new(SC, Owner),
    SignedTxn = blockchain_txn_state_channel_close_v1:sign(Txn, OwnerSigFun),
    ok = blockchain_worker:submit_txn(SignedTxn),
    lager:info("closing state channel ~p: ~p", [blockchain_state_channel_v1:id(SC), SignedTxn]),
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
    DBSCs = case get_state_channels(DB) of
                {error, _} ->
                    #{};
                {ok, SCIDs} ->
                    lists:foldl(
                      fun(ID, Acc) ->
                              case blockchain_state_channel_v1:fetch(DB, ID) of
                                  {error, _Reason} ->
                                      % TODO: Maybe cleanup not_found state channels from list
                                      lager:warning("could not get state channel ~p: ~p", [ID, _Reason]),
                                      Acc;
                                  {ok, {SC, Skewed}} ->
                                      lager:info("from scdb ID: ~p, SC: ~p", [ID, SC]),
                                      maps:put(ID, {SC, Skewed}, Acc)
                              end
                      end,
                      #{}, SCIDs)
            end,

    lager:info("ConvertedSCs: ~p, DBSCs: ~p", [ConvertedSCs, DBSCs]),
    ConvertedSCKeys = maps:keys(ConvertedSCs),
    %% Merge DBSCs with ConvertedSCs with only matching IDs
    SCs = maps:merge(ConvertedSCs, maps:with(ConvertedSCKeys, DBSCs)),
    %% These don't exist in the ledger but we have them in the sc db,
    %% presumably these have been closed
    ClosedSCIDs = maps:keys(maps:without(ConvertedSCKeys, DBSCs)),
    %% Delete these from sc db
    ok = lists:foreach(fun(CID) -> ok = delete_closed_sc(DB, CID) end, ClosedSCIDs),

    NewActiveSCID = maybe_get_new_active(SCs),
    lager:info("SCs: ~p, NewActiveSCID: ~p", [SCs, NewActiveSCID]),
    State#state{state_channels=SCs, active_sc_id=NewActiveSCID}.

-spec get_state_channels(DB :: rocksdb:db_handle()) -> {ok, [blockchain_state_channel_v1:id()]} | {error, any()}.
get_state_channels(DB) ->
    case rocksdb:get(DB, ?STATE_CHANNELS, [{sync, true}]) of
        {ok, Bin} ->
            lager:info("found sc: ~p, from db", [Bin]),
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            lager:warning("no state_channel found in db"),
            {ok, []};
        Error ->
            lager:error("error: ~p", [Error]),
            Error
    end.

-spec store_active_sc_id(DB :: rocksdb:db_handle(),
                         ID :: blockchain_state_channel_v1:id()) -> ok | {error, any()}.
store_active_sc_id(DB, ID) ->
    case get_state_channels(DB) of
        {error, _}=Error ->
            Error;
        {ok, SCIDs} ->
            case lists:member(ID, SCIDs) of
                true ->
                    ok;
                false ->
                    rocksdb:put(DB, ?STATE_CHANNELS, erlang:term_to_binary([ID|SCIDs]), [{sync, true}])
            end
    end.

-spec delete_closed_sc(DB :: rocksdb:db_handle(),
                       ID :: blockchain_state_channel_v1:id()) -> ok.
delete_closed_sc(DB, ID) ->
    case get_state_channels(DB) of
        {error, _} ->
            %% Can't delete anything
            ok;
        {ok, SCIDs} ->
            case lists:member(ID, SCIDs) of
                false ->
                    %% not in db
                    ok;
                true ->
                    rocksdb:put(DB, ?STATE_CHANNELS, erlang:term_to_binary(lists:delete(ID, SCIDs)), [{sync, true}])
            end
    end.

-spec convert_to_state_channels(State :: state()) -> state_channels().
convert_to_state_channels(#state{chain=Chain, owner={Owner, OwnerSigFun}}) ->
    Ledger = blockchain:ledger(Chain),
    {ok, LedgerSCs} = blockchain_ledger_v1:find_scs_by_owner(Owner, Ledger),
    {ok, Head} = blockchain:head_block(Chain),
    maps:map(fun(ID, LedgerStateChannel) ->
                     Owner = blockchain_ledger_state_channel_v1:owner(LedgerStateChannel),
                     ExpireAt = blockchain_ledger_state_channel_v1:expire_at_block(LedgerStateChannel),
                     SC0 = blockchain_state_channel_v1:new(ID, Owner),
                     Nonce = blockchain_ledger_state_channel_v1:nonce(LedgerStateChannel),
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
-spec maybe_get_new_active(state_channels()) -> undefined | blockchain_state_channel_v1:id().
maybe_get_new_active(SCs) ->
    case maps:to_list(SCs) of
        [] ->
            %% Don't have any state channel in state
            undefined;
        L ->
            SCSortFun = fun({_ID1, {SC1, _}}, {_ID2, {SC2, _}}) ->
                               blockchain_state_channel_v1:expire_at_block(SC1) =< blockchain_state_channel_v1:expire_at_block(SC2)
                        end,
            SCSortFun2 = fun({_ID1, {SC1, _}}, {_ID2, {SC2, _}}) ->
                               blockchain_state_channel_v1:nonce(SC1) >= blockchain_state_channel_v1:nonce(SC2)
                        end,

            {ID, _} = hd(lists:sort(SCSortFun2, lists:sort(SCSortFun, L))),
            ID
    end.

-spec send_purchase(SC :: blockchain_state_channel_v1:state_channel(),
                    Hotspot :: libp2p_crypto:pubkey_bin(),
                    Stream :: pid(),
                    PacketHash :: binary(),
                    PayloadSize :: pos_integer(),
                    Region :: atom(),
                    Ledger :: blockchain:ledger(),
                    OwnerSigFun :: libp2p_crypto:sig_fun()) -> ok.
send_purchase(SC, Hotspot, Stream, PacketHash, PayloadSize, Region, Ledger, OwnerSigFun) ->
    SCNonce = blockchain_state_channel_v1:nonce(SC),
    NewPurchaseSC0 = blockchain_state_channel_v1:nonce(SCNonce + 1, SC),
    NewPurchaseSC = update_sc_summary(Hotspot, PayloadSize, Ledger, NewPurchaseSC0, OwnerSigFun),
    %% NOTE: We're constructing the purchase with the hotspot obtained from offer here
    PurchaseMsg = blockchain_state_channel_purchase_v1:new(NewPurchaseSC, Hotspot, PacketHash, Region),
    lager:info("PurchaseMsg1: ~p, Stream: ~p", [PurchaseMsg, Stream]),
    blockchain_state_channel_handler:send_purchase(Stream, PurchaseMsg).

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
    blockchain_state_channel_handler:send_banner(Stream, BannerMsg1).

-spec update_sc_summary(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                        PayloadSize :: pos_integer(),
                        Ledger :: blockchain:ledger(),
                        SC :: blockchain_state_channel_v1:state_channel(),
                        OwnerSigFun :: libp2p_crypto:sig_fun()) ->
    blockchain_state_channel_v1:state_channel().
update_sc_summary(ClientPubkeyBin, PayloadSize, Ledger, SC, OwnerSigFun) ->
    NewSC = case blockchain_state_channel_v1:get_summary(ClientPubkeyBin, SC) of
                {error, not_found} ->
                    NumDCs = blockchain_utils:calculate_dc_amount(Ledger, PayloadSize),
                    NewSummary = blockchain_state_channel_summary_v1:new(ClientPubkeyBin, 1, NumDCs),
                    %% Add this to summaries
                    blockchain_state_channel_v1:update_summaries(ClientPubkeyBin, NewSummary, SC);
                {ok, ExistingSummary} ->
                    %% Update packet count for this client
                    ExistingNumPackets = blockchain_state_channel_summary_v1:num_packets(ExistingSummary),
                    %% Update DC count for this client
                    NumDCs = blockchain_utils:calculate_dc_amount(Ledger, PayloadSize),
                    ExistingNumDCs = blockchain_state_channel_summary_v1:num_dcs(ExistingSummary),
                    NewSummary = blockchain_state_channel_summary_v1:update(ExistingNumDCs + NumDCs,
                                                                            ExistingNumPackets + 1,
                                                                            ExistingSummary),
                    %% Update summaries
                    blockchain_state_channel_v1:update_summaries(ClientPubkeyBin, NewSummary, SC)
            end,
    blockchain_state_channel_v1:sign(NewSC, OwnerSigFun).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% TODO: add some eunits here...

-endif.
