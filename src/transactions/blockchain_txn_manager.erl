%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Manager ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_manager).

-behavior(gen_server).

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
         start_link/1,
         submit/5,
         status/1
        ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
         init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3
        ]).

-record(state, {
          txn_map = #{} :: #{blockchain_transactions:transaction() => ok | {error, any()}}
         }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Transaction :: blockchain_transactions:transaction(),
             Receivers :: [libp2p_crypto:address()],
             Handler :: atom(),
             Retries :: non_neg_integer(),
             CallbackFun :: fun()) -> ok.
submit(Transaction, Receivers, Handler, Retries, CallbackFun) ->
    gen_server:cast(?MODULE, {submit, Transaction, Receivers, Handler, Retries, CallbackFun}).

-spec status(Transaction :: blockchain_transactions:transaction()) -> ok | {error, any()}.
status(Transaction) ->
    gen_server:call(?MODULE, {status, Transaction}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    schedule_prune(),
    {ok, #state{}}.

handle_cast({submit, Transaction, Receivers, Handler, Retries, _CallbackFun}, State=#state{txn_map=TxnMap}) ->
    DataToSend = erlang:term_to_binary({blockchain_transactions:type(Transaction), Transaction}),
    RandomConsensusAddress = lists:nth(rand:uniform(length(Receivers)), Receivers),
    P2PAddress = libp2p_crypto:address_to_p2p(RandomConsensusAddress),
    Swarm = blockchain_swarm:swarm(),
    NewState = case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, Handler, [self()]) of
                   {ok, Stream} ->
                       lager:info("dialed peer ~p via ~p~n", [RandomConsensusAddress, ?TX_PROTOCOL]),
                       libp2p_framed_stream:send(Stream, DataToSend),
                       libp2p_framed_stream:close(Stream),
                       State#state{txn_map=maps:put(TxnMap, Transaction, ok)};
                   Other ->
                       lager:notice("Failed to dial ~p service on ~p : ~p", [?TX_PROTOCOL, RandomConsensusAddress, Other]),
                       case Retries > 0 of
                           true ->
                               erlang:send_after(1000, self(), {submit, Transaction, Receivers, Handler, Retries - 1, _CallbackFun}),
                               State;
                           false ->
                               State#state{txn_map=maps:put(TxnMap, Transaction, {error, no_retries_and_failed})}
                       end
               end,
    {noreply, NewState};
handle_cast({submit, Transaction, _, _, 0, _}, State=#state{txn_map=TxnMap}) ->
    NewState = State#state{txn_map=maps:put(TxnMap, Transaction, {error, no_retries})},
    {noreply, NewState};
handle_cast(_, State) ->
    {noreply, State}.

handle_call({status, Transaction}, _From, State=#state{txn_map=TxnMap}) ->
    Status = maps:get(TxnMap, Transaction, {error, not_found}),
    {reply, Status, State}.

handle_info(prune, State=#state{txn_map=TxnMap}) ->
    %% Remove processed transactions from State
    NewTxnMap = maps:filter(fun(_K, V) -> V /= ok end, TxnMap),
    {noreply, State#state{txn_map=NewTxnMap}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

schedule_prune() ->
    erlang:send_after(5000, self(), prune).
