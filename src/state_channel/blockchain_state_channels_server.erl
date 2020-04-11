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
    active/0
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
    owner = undefined :: {libp2p_crypto:pubkey_bin(), function()} | undefined,
    state_channels = #{} :: state_channels(),
    active = undefined :: undefined | blockchain_state_channel_v1:id(),
    clients = #{} :: clients(),
    sc_packet_handler = undefined :: undefined | atom()
}).

-type state() :: #state{}.
-type clients() :: #{blockchain_state_channel_v1:id() => [libp2p_crypto:pubkey_bin()]}.
-type state_channels() ::  #{blockchain_state_channel_v1:id() => blockchain_state_channel_v1:state_channel()}.

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
    gen_server:cast(?SERVER, {packet, Packet, HandlerPid}).

-spec state_channels() -> state_channels().
state_channels() ->
    gen_server:call(?SERVER, state_channels, infinity).

-spec active() -> undefined | blockchain_state_channel_v1:id().
active() ->
    gen_server:call(?SERVER, active, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = maps:get(db, Args),
    SCPacketHandler = application:get_env(blockchain, sc_packet_handler, undefined),
    ok = blockchain_event:add_handler(self()),
    {Owner, OwnerSigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    {ok, State} = load_state(DB),
    self() ! post_init,
    {ok, State#state{swarm=Swarm, owner={Owner, OwnerSigFun}, sc_packet_handler=SCPacketHandler}}.

handle_call({nonce, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel_v1:nonce(SC)}
    end,
    {reply, Reply, State};
handle_call(state_channels, _From, #state{state_channels=SCs}=State) ->
    {reply, SCs, State};
handle_call(active, _From, #state{active=Active}=State) ->
    {reply, Active, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({packet, SCPacket, HandlerPid}, #state{active=undefined, sc_packet_handler=SCPacketHandler}=State) ->
    lager:warning("Got packet: ~p when no sc is active", [SCPacket]),
    case blockchain_state_channel_packet_v1:validate(SCPacket) of
        {error, _Reason} ->
            lager:warning("packet failed to validate ~p ~p", [_Reason, SCPacket]);
        true ->
            %% we cannot record this packet in a state channel, but we can still deliver it
            SCPacketHandler:handle_packet(SCPacket, HandlerPid)
    end,
    {noreply, State};
handle_cast({packet, SCPacket, HandlerPid},
            #state{sc_packet_handler=SCPacketHandler, active=Active, state_channels=SCs}=State) ->
    NewState = case blockchain_state_channel_packet_v1:validate(SCPacket) of
                   {error, _Reason} ->
                       lager:warning("packet failed to validate ~p ~p", [_Reason, SCPacket]),
                       State;
                   true ->
                       case SCPacketHandler:handle_packet(SCPacket, HandlerPid) of
                           ok ->
                               %% Active should always be in our state_channels map
                               SC = maps:get(Active, SCs),
                               %% Get payload from packet
                               Packet = blockchain_state_channel_packet_v1:packet(SCPacket),
                               Payload = blockchain_helium_packet_v1:payload(Packet),
                               %% Add this payload to state_channel's skewed merkle
                               SC1 = blockchain_state_channel_v1:add_payload(Payload, SC),
                               %% Get the summary for this client
                               ClientPubkeyBin = blockchain_state_channel_packet_v1:hotspot(SCPacket),

                               SC2 = case blockchain_state_channel_v1:get_summary(ClientPubkeyBin, SC1) of
                                           {error, not_found} ->
                                               NumDCs = blockchain_state_channel_utils:calculate_dc_amount(byte_size(Payload)),
                                               NewSummary = blockchain_state_channel_summary_v1:new(ClientPubkeyBin, 1, NumDCs),
                                               %% Add this to summaries
                                               blockchain_state_channel_v1:update_summaries(ClientPubkeyBin, NewSummary, SC1);
                                           {ok, ExistingSummary} ->
                                               %% Update packet count for this client
                                               ExistingNumPackets = blockchain_state_channel_summary_v1:num_packets(ExistingSummary),
                                               %% Update DC count for this client
                                               NumDCs = blockchain_state_channel_utils:calculate_dc_amount(byte_size(Payload)),
                                               ExistingNumDCs = blockchain_state_channel_summary_v1:num_dcs(ExistingSummary),
                                               NewSummary = blockchain_state_channel_summary_v1:update(ExistingNumDCs + NumDCs,
                                                                                                       ExistingNumPackets + 1,
                                                                                                       ExistingSummary),
                                               %% Update summaries
                                               blockchain_state_channel_v1:update_summaries(ClientPubkeyBin, NewSummary, SC1)
                                       end,

                               ExistingSCNonce = blockchain_state_channel_v1:nonce(SC2),
                               NewSC = blockchain_state_channel_v1:nonce(ExistingSCNonce + 1, SC2),

                               %% Put new state_channel in our map
                               State#state{state_channels=maps:put(Active, NewSC, SCs)};
                           {error, Why} ->
                               lager:error("handle_packet failed: ~p", [Why]),
                               State
                       end
               end,
    {noreply, NewState};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(post_init, #state{chain=undefined, owner={Owner, _}, state_channels=SCs}=State) ->
    case blockchain_worker:blockchain() of
        undefined ->
            self() ! post_init,
            {noreply, State};
        Chain ->
            Ledger = blockchain:ledger(Chain),
            {ok, LedgerSCs} = blockchain_ledger_v1:find_scs_by_owner(Owner, Ledger),
            ConvertedSCs = convert_to_state_channels(LedgerSCs),
            {noreply, State#state{chain=Chain, state_channels=maps:merge(ConvertedSCs, SCs)}}
    end;
handle_info({blockchain_event, {add_block, BlockHash, _Syncing, _Ledger}},
            #state{chain=Chain, owner={Owner, _}, state_channels=SCs, active=Active}=State0) ->

    NewState = case blockchain:get_block(BlockHash, Chain) of
                   {error, Reason} ->
                       lager:error("Couldn't get block with hash: ~p, reason: ~p", [BlockHash, Reason]),
                       State0;
                   {ok, Block} ->
                       BlockHeight = blockchain_block:height(Block),
                       Txns = get_state_channels_txns_from_block(Chain, BlockHash, Owner, SCs),
                       State1 = lists:foldl(
                                  fun(Txn, #state{state_channels=SCs0, clients=Clients0}=State) ->
                                          case blockchain_txn:type(Txn) of
                                              blockchain_txn_state_channel_open_v1 ->
                                                  case blockchain_txn_state_channel_open_v1:owner(Txn) of
                                                      %% Do the map put when we are the owner of the state_channel
                                                      Owner ->
                                                          ID = blockchain_txn_state_channel_open_v1:id(Txn),
                                                          ExpireWithin = blockchain_txn_state_channel_open_v1:expire_within(Txn),
                                                          SC = blockchain_state_channel_v1:new(ID,
                                                                                               Owner,
                                                                                               BlockHash,
                                                                                               (BlockHeight + ExpireWithin)),

                                                          case Active of
                                                              undefined ->
                                                                  %% Don't have any active state channel
                                                                  %% Set this one to active
                                                                  State#state{state_channels=maps:put(ID, SC, SCs0), active=ID};
                                                              _A ->
                                                                  State#state{state_channels=maps:put(ID, SC, SCs0)}
                                                          end;
                                                      _ ->
                                                          %% Don't do anything cuz we're not the owner
                                                          State
                                                  end;
                                              blockchain_txn_state_channel_close_v1 ->
                                                  SC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
                                                  ID = blockchain_state_channel_v1:id(SC),

                                                  NewActive = case Active of
                                                                  undefined ->
                                                                      %% No sc was active
                                                                      undefined;
                                                                  ID ->
                                                                      %% Our active state channel got closed,
                                                                      %% set active to undefined
                                                                      ExcludedActiveSCs = maps:without([ID], SCs),
                                                                      maybe_get_new_active(ExcludedActiveSCs);
                                                                  A ->
                                                                      %% Some other sc was active, let it remain active
                                                                      A
                                                              end,

                                                  State#state{state_channels=maps:remove(ID, SCs0),
                                                              clients=maps:remove(ID, Clients0),
                                                              active=NewActive}
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

terminate(_Reason, _state) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Close expired state channels
%% @end
%%--------------------------------------------------------------------
-spec check_state_channel_expiration(BlockHeight :: pos_integer(),
                                     State :: state()) -> state().
check_state_channel_expiration(BlockHeight, #state{owner={Owner, OwnerSigFun},
                                                   active=Active,
                                                   state_channels=SCs}=State) ->
    NewStateChannels = maps:map(
                        fun(_ID, SC) ->
                                ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
                                case ExpireAt =< BlockHeight andalso blockchain_state_channel_v1:state(SC) == open of
                                    false ->
                                        SC;
                                    true ->
                                        SC0 = blockchain_state_channel_v1:state(closed, SC),
                                        SC1 = blockchain_state_channel_v1:skewed(undefined, SC0),
                                        SC2 = blockchain_state_channel_v1:sign(SC1, OwnerSigFun),
                                        ok = close_state_channel(SC2, Owner, OwnerSigFun),
                                        SC2
                                end
                        end,
                        SCs
                       ),

    NewActive = case Active of
                    undefined ->
                        undefined;
                    _ ->
                        ActiveSC = maps:get(Active, NewStateChannels),
                        case blockchain_state_channel_v1:state(ActiveSC) of
                            closed ->
                                maybe_get_new_active(maps:without([Active], NewStateChannels));
                            _ ->
                                Active
                        end
                end,

    State#state{active=NewActive, state_channels=NewStateChannels}.

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
-spec get_state_channels_txns_from_block(blockchain:blockchain(), binary(), binary(), map()) ->
    [blockchain_txn_state_channel_open_v1:txn_state_channel_open() |
     blockchain_txn_state_channel_close_v1:txn_state_channel_close()].
get_state_channels_txns_from_block(Chain, BlockHash, Owner, SCs) ->
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

-spec load_state(rocksdb:db_handle()) -> {ok, state()} | {error, any()}.
load_state(DB) ->
    % TODO: We should also check the ledger make sure we did not miss any new state channel
    case get_state_channels(DB) of
        {error, _}=Error ->
            Error;
        {ok, SCIDs} ->
            SCs = lists:foldl(
                fun(ID, Acc) ->
                    case blockchain_state_channel_v1:get(DB, ID) of
                        {error, _Reason} ->
                            % TODO: Maybe cleanup not_found state channels from list
                            lager:warning("could not get state channel ~p: ~p", [ID, _Reason]),
                            Acc;
                        {ok, SC} ->
                            maps:put(ID, SC, Acc)
                    end
                end,
                maps:new(),
                SCIDs
            ),
            SCClients = lists:foldl(fun(SC, Acc0) ->
                                            ID = blockchain_state_channel_v1:id(SC),
                                            Summaries = blockchain_state_channel_v1:summaries(SC),
                                            Clients = acc_clients(Summaries),
                                            maps:put(ID, Clients, Acc0)
                                    end,
                                    maps:new(),
                                    maps:values(SCs)),

            {ok, #state{db=DB, state_channels=SCs, clients=SCClients}}
    end.

-spec acc_clients(Summaries :: blockchain_state_channel_summary_v1:summaries()) -> [libp2p_crypto:pubkey_bin()].
acc_clients(Summaries) ->
    lists:foldl(fun(Summary, Acc) ->
                        ClientPubkeyBin = blockchain_state_channel_summary_v1:client_pubkeybin(Summary),
                        case lists:member(ClientPubkeyBin, Acc) of
                            false -> [ClientPubkeyBin | Acc];
                            true -> Acc
                        end
                end,
                [],
                Summaries).


-spec get_state_channels(rocksdb:db_handle()) -> {ok, [blockchain_state_channel_v1:id()]} | {error, any()}.
get_state_channels(DB) ->
    case rocksdb:get(DB, ?STATE_CHANNELS, [{sync, true}]) of
        {ok, Bin} -> {ok, erlang:binary_to_term(Bin)};
        not_found -> {ok, []};
        Error -> Error
    end.

-spec convert_to_state_channels(blockchain_ledger_v1:state_channel_map()) -> state_channels().
convert_to_state_channels(LedgerSCs) ->
    maps:map(fun(ID, LedgerStateChannel) ->
                     Owner = blockchain_ledger_state_channel_v1:owner(LedgerStateChannel),
                     ExpireAt = blockchain_ledger_state_channel_v1:expire_at_block(LedgerStateChannel),
                     Nonce = blockchain_ledger_state_channel_v1:nonce(LedgerStateChannel),
                     SC0 = blockchain_state_channel_v1:new(ID, Owner),
                     SC1 = blockchain_state_channel_v1:nonce(Nonce, SC0),
                     blockchain_state_channel_v1:expire_at_block(ExpireAt, SC1)
             end,
             LedgerSCs).

-spec maybe_get_new_active(state_channels()) -> undefined | blockchain_state_channel_v1:id().
maybe_get_new_active(SCs) ->
    case maps:keys(SCs) of
        [] ->
            undefined;
        L ->
            hd(blockchain_utils:shuffle(L))
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% TODO: add some eunits here...

-endif.
