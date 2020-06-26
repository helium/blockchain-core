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
    state_channels/0,
    active_sc_id/0
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).
-define(STATE_CHANNELS, <<"blockchain_state_channels_server.STATE_CHANNELS">>).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    chain = undefined :: blockchain:blockchain() | undefined,
    swarm = undefined :: pid() | undefined,
    owner = undefined :: {libp2p_crypto:pubkey_bin(), function()} | undefined,
    state_channels = #{} :: state_channels(),
    active_sc_id = undefined :: undefined | blockchain_state_channel_v1:id(),
    sc_packet_handler = undefined :: undefined | atom()
}).

-type state() :: #state{}.
-type state_channels() ::  #{blockchain_state_channel_v1:id() => {blockchain_state_channel_v1:state_channel(),
                                                                  skewed:skewed()}}.

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
                                  gen_server:cast(?SERVER, {packet, Packet});
                              {error, Why} ->
                                  lager:error("handle_packet failed: ~p", [Why])
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
handle_call(active_sc_id, _From, #state{active_sc_id=ActiveSCID}=State) ->
    {reply, ActiveSCID, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({packet, SCPacket}, #state{active_sc_id=undefined}=State) ->
    lager:warning("Got packet: ~p when no sc is active", [SCPacket]),
    {noreply, State};
handle_cast({packet, SCPacket},
            #state{db=DB, active_sc_id=ActiveSCID, state_channels=SCs, chain=Chain}=State) ->
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
            SC2 = case blockchain_state_channel_v1:get_summary(ClientPubkeyBin, SC1) of
                      {error, not_found} ->
                          NumDCs = blockchain_utils:calculate_dc_amount(Ledger, byte_size(Payload)),
                          NewSummary = blockchain_state_channel_summary_v1:new(ClientPubkeyBin, 1, NumDCs),
                          %% Add this to summaries
                          blockchain_state_channel_v1:update_summaries(ClientPubkeyBin, NewSummary, SC1);
                      {ok, ExistingSummary} ->
                          %% Update packet count for this client
                          ExistingNumPackets = blockchain_state_channel_summary_v1:num_packets(ExistingSummary),
                          %% Update DC count for this client
                          NumDCs = blockchain_utils:calculate_dc_amount(Ledger, byte_size(Payload)),
                          ExistingNumDCs = blockchain_state_channel_summary_v1:num_dcs(ExistingSummary),
                          NewSummary = blockchain_state_channel_summary_v1:update(ExistingNumDCs + NumDCs,
                                                                                  ExistingNumPackets + 1,
                                                                                  ExistingSummary),
                          %% Update summaries
                          blockchain_state_channel_v1:update_summaries(ClientPubkeyBin, NewSummary, SC1)
                  end,

            ExistingSCNonce = blockchain_state_channel_v1:nonce(SC2),
            NewSC = blockchain_state_channel_v1:nonce(ExistingSCNonce + 1, SC2),

            %% Save state channel to db
            ok = blockchain_state_channel_v1:save(DB, NewSC, Skewed1),
            ok = store_active_sc_id(DB, ActiveSCID),

            %% Put new state_channel in our map
            lager:info("packet: ~p successfully validated, updating state",
                       [blockchain_utils:bin_to_hex(blockchain_helium_packet_v1:encode(Packet))]),
            {noreply, State#state{state_channels=maps:update(ActiveSCID, {NewSC, Skewed1}, SCs)}}
    end;
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(post_init, #state{chain=undefined}=State) ->
    case blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), post_init),
            {noreply, State};
        Chain ->
            Ledger = blockchain:ledger(Chain),
            LoadState = load_state(Ledger, State#state{chain=Chain}),
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
                     #state{owner={Owner, _}, state_channels=SCs, active_sc_id=ActiveSCID}=State) ->
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

            case ActiveSCID of
                undefined ->
                    %% Don't have any active state channel
                    %% Set this one to active
                    State#state{state_channels=maps:put(ID, {SC, Skewed}, SCs), active_sc_id=ID};
                _A ->
                    State#state{state_channels=maps:put(ID, {SC, Skewed}, SCs)}
            end;
        _ ->
            %% Don't do anything cuz we're not the owner
            State
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

    State#state{state_channels=maps:remove(ID, SCs), active_sc_id=NewActiveSCID}.

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

    State#state{active_sc_id=NewActiveSCID, state_channels=NewStateChannels}.

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

-spec load_state(Ledger :: blockchain_ledger_v1:ledger(),
                 State :: state()) -> state().
load_state(Ledger, #state{db=DB, owner={Owner, _}, chain=Chain}=State) ->
    {ok, SCMap} = blockchain_ledger_v1:find_scs_by_owner(Owner, Ledger),
    SCMod = case blockchain_ledger_v1:config(?sc_version, Ledger) of
                {ok, 2} -> blockchain_ledger_state_channel_v2;
                _ -> blockchain_ledger_state_channel_v1
            end,
    ConvertedSCs = convert_to_state_channels(SCMap, SCMod, Chain),

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

-spec convert_to_state_channels(blockchain_ledger_v1:state_channel_map(), atom(), blockchain:blockchain()) -> state_channels().
convert_to_state_channels(LedgerSCs, SCMod, Chain) ->
    {ok, Head} = blockchain:head_block(Chain),
    maps:map(fun(ID, LedgerStateChannel) ->
                     Owner = SCMod:owner(LedgerStateChannel),
                     ExpireAt = SCMod:expire_at_block(LedgerStateChannel),
                     Amount = case SCMod of
                                  blockchain_ledger_state_channel_v2 -> SCMod:amount(LedgerStateChannel);
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
                     Skewed = skewed:new(BlockHash),
                     {SC1, Skewed}
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

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% TODO: add some eunits here...

-endif.
