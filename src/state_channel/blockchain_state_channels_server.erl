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
    credits/1, nonce/1,
    packet/1, packet/2,
    state_channels/0
]).

-export([
    burn/2,
    packet_forward/1
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
    clients = #{} :: clients(),
    payees_to_sc = #{} :: #{libp2p_crypto:pubkey_bin() => blockchain_state_channel_v1:id()},
    sc_server_mod = undefined :: undefined | atom()
}).

-type state() :: #state{}.
-type clients() :: #{blockchain_state_channel_v1:id() => [libp2p_crypto:pubkey_bin()]}.
-type state_channels() ::  #{blockchain_state_channel_v1:id() => blockchain_state_channel_v1:state_channel()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
credits(ID) ->
    gen_server:call(?SERVER, {credits, ID}).

-spec nonce(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
nonce(ID) ->
    gen_server:call(?SERVER, {nonce, ID}).

-spec packet(blockchain_state_channel_packet_v1:packet()) -> ok.
packet(Packet) ->
    gen_server:cast(?SERVER, {packet, Packet}).

-spec packet(blockchain_state_channel_packet_v1:packet(), pid()) -> ok.
packet(Packet, HandlerPid) ->
    gen_server:cast(?SERVER, {packet, Packet, HandlerPid}).

-spec state_channels() -> state_channels().
state_channels() ->
    gen_server:call(?SERVER, state_channels, infinity).

%% Helper function for tests (remove)
-spec burn(blockchain_state_channel_v1:id(), non_neg_integer()) -> ok.
burn(ID, Amount) ->
    gen_server:cast(?SERVER, {burn, ID, Amount}).

%% Helper function for tests (remove)
-spec packet_forward(pid() | undefined) -> ok.
packet_forward(Pid) ->
    gen_server:cast(?SERVER, {packet_forward, Pid}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = maps:get(db, Args),
    SCServerMod = maps:get(sc_server_mod, Args, undefined),
    ok = blockchain_event:add_handler(self()),
    {Owner, OwnerSigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    {ok, State} = load_state(DB),
    self() ! post_init,
    {ok, State#state{swarm=Swarm, owner={Owner, OwnerSigFun}, sc_server_mod=SCServerMod}}.

handle_call({credits, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel_v1:credits(SC)}
    end,
    {reply, Reply, State};
handle_call({nonce, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel_v1:nonce(SC)}
    end,
    {reply, Reply, State};
handle_call(state_channels, _From, #state{state_channels=SCs}=State) ->
    {reply, SCs, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

%% Helper function for tests (remove)
handle_cast({burn, ID, Amount}, #state{owner={Owner, _}, state_channels=SCs}=State) ->
    case maps:is_key(ID, SCs) of
        true ->
            {noreply, State};
        false ->
            SC0 = blockchain_state_channel_v1:new(ID, Owner),
            SC1 = blockchain_state_channel_v1:credits(Amount, SC0),
            {noreply, State#state{state_channels=maps:put(ID, SC1, SCs)}}
    end;
handle_cast({packet, Packet, HandlerPid}, #state{sc_server_mod=SCServerMod}=State) when is_pid(HandlerPid) ->
    case blockchain_state_channel_packet_v1:validate(Packet) of
        {error, _Reason} ->
            lager:warning("packet failed to validate ~p ~p", [_Reason, Packet]);
        true ->
            SCServerMod:handle_packet(Packet, HandlerPid)
    end,
    {noreply, State};
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
            #state{chain=Chain, owner={Owner, OwnerSigFun}, state_channels=SCs}=State0) ->
    {Block, Txns} = get_state_channels_txns_from_block(Chain, BlockHash, Owner, SCs),
    BlockHeight = blockchain_block:height(Block),
    State1 = lists:foldl(
        fun(Txn, #state{state_channels=SCs0, clients=Clients0, payees_to_sc=Payees0}=State) ->
                case blockchain_txn:type(Txn) of
                    blockchain_txn_state_channel_open_v1 ->
                        ID = blockchain_txn_state_channel_open_v1:id(Txn),
                        Owner = blockchain_txn_state_channel_open_v1:owner(Txn),
                        Amount = blockchain_txn_state_channel_open_v1:amount(Txn),
                        ExpireWithin = blockchain_txn_state_channel_open_v1:expire_within(Txn),
                        SC = blockchain_state_channel_v1:new(ID, Owner, Amount, BlockHeight + ExpireWithin),
                        State#state{state_channels=maps:put(ID, SC, SCs0)};
                    blockchain_txn_state_channel_close_v1 ->
                        SC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
                        ID = blockchain_state_channel_v1:id(SC),
                        Payees1 = maps:filter(fun(_, V) -> V =/= ID end, Payees0),
                        State#state{state_channels=maps:remove(ID, SCs0),
                                    clients=maps:remove(ID, Clients0),
                                    payees_to_sc=Payees1}
                end
        end,
        State0,
        Txns
    ),
    SCs1 = check_state_channel_expiration(BlockHeight, Owner, OwnerSigFun, State1#state.state_channels),
    {noreply, State1#state{state_channels=SCs1}};
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
                                     Owner :: libp2p_crypto:pubkey_bin(),
                                     OwnerSigFun :: function(),
                                     SCs :: state_channels()) -> state_channels().
check_state_channel_expiration(BlockHeight, Owner, OwnerSigFun, SCs) ->
    maps:map(
        fun(_ID, SC) ->
            ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
            case ExpireAt =< BlockHeight of
                false ->
                    SC;
                true ->
                    SC0 = blockchain_state_channel_v1:state(closed, SC),
                    SC1 = blockchain_state_channel_v1:sign(SC0, OwnerSigFun),
                    ok = close_state_channel(SC1, Owner, OwnerSigFun),
                    SC1
            end
        end,
        SCs
    ).

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
    {blockchain_block:block(), [blockchain_txn_state_channel_open_v1:txn_state_channel_open()
                                | blockchain_txn_state_channel_close_v1:txn_state_channel_close()]}.
get_state_channels_txns_from_block(Chain, BlockHash, Owner, SCs) ->
    case blockchain:get_block(BlockHash, Chain) of
        {error, _Reason} ->
            lager:error("failed to get block:~p ~p", [BlockHash, _Reason]),
            [];
        {ok, Block} ->
            {Block, lists:filter(
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
            )}
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
                     Amount = blockchain_ledger_state_channel_v1:amount(LedgerStateChannel),
                     ExpireAt = blockchain_ledger_state_channel_v1:expire_at_block(LedgerStateChannel),
                     Nonce = blockchain_ledger_state_channel_v1:nonce(LedgerStateChannel),
                     SC0 = blockchain_state_channel_v1:new(ID, Owner),
                     SC1 = blockchain_state_channel_v1:credits(Amount, SC0),
                     SC2 = blockchain_state_channel_v1:nonce(Nonce, SC1),
                     blockchain_state_channel_v1:expire_at_block(ExpireAt, SC2)
             end,
             LedgerSCs).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% TODO: add some eunits here...

-endif.
