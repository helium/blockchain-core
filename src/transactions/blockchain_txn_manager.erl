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
         submit/3,
         get_state/0
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
          txn_map = #{} :: txn_map()
         }).

-type txn_map() :: #{blockchain_transactions:transaction() => erlang:queue()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Txn :: blockchain_transactions:transaction(),
             ConsensusAddrs :: [libp2p_crypto:address()],
             Callback :: fun()) -> ok.
submit(Txn, ConsensusAddrs, Callback) ->
    gen_server:cast(?MODULE, {submit, Txn, ConsensusAddrs, Callback}).

-spec get_state() -> txn_map().
get_state() ->
    gen_server:call(?MODULE, get_state, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    %% ok = blockchain_event:add_handler(self()),
    {ok, #state{}}.

handle_cast({submit, Transaction, ConsensusAddrs, Callback}, State=#state{txn_map=TxnMap}) ->
    F = (length(ConsensusAddrs) - 1) div 3,
    RandomAddrs = random_n(F+1, ConsensusAddrs),
    lager:info("blockchain_txn_manager, F: ~p, RandomAddrs: ~p", [F, RandomAddrs]),
    NewTxnMap = maps:put(Transaction, queue:from_list(RandomAddrs), TxnMap),
    lager:info("blockchain_txn_manager, NewTxnMap: ~p", [NewTxnMap]),
    self() ! {process, Transaction, Callback},
    {noreply, State#state{txn_map=NewTxnMap}};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_manager got unknown cast: ~p", [_Msg]),
    {noreply, State}.

handle_call(get_state, _from, State) ->
    {reply, State#state.txn_map, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info({process, Txn, Callback}, State=#state{txn_map=TxnMap}) ->
    lager:info("blockchain_txn_manager, process txn: ~p, TxnMap: ~p", [Txn, TxnMap]),

    NewState = case maps:is_key(Txn, TxnMap) of
                   false ->
                       Callback({error, "Txn not found in State"}),
                       State;
                   true ->
                       TxnQueue = maps:get(Txn, TxnMap),
                       case queue:is_empty(TxnQueue) of
                           true ->
                               Callback(ok),
                               State#state{txn_map=maps:remove(Txn, TxnMap)};
                           false ->
                               {value, AddrToDial} = queue:peek(TxnQueue),
                               case dial(AddrToDial, Txn) of
                                   ok ->
                                       %% succesfful dial, pop txn from queue
                                       lager:info("blockchain_txn_manager, popping address: ~p for txn: ~p", [AddrToDial, Txn]),
                                       %% XXX: Get rid of this send_after
                                       erlang:send_after(100, self(), {process, Txn, Callback}),
                                       State#state{txn_map=maps:put(Txn, queue:drop(TxnQueue), TxnMap)};
                                   Other ->
                                       lager:error("Failed to dial txn: ~p, Error: ~p", [Txn, Other]),
                                       %% TODO: drop this address and put a new one at the end of queue
                                       %% XXX: Get rid of this send_after
                                       erlang:send_after(100, self(), {process, Txn, Callback}),
                                       %% Return same queue for now
                                       State#state{txn_map=maps:put(Txn, TxnQueue, TxnMap)}
                               end
                       end
               end,
    {noreply, NewState};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Helper functions
%% ------------------------------------------------------------------
random_n(N, List) ->
    lists:sublist(shuffle(List), N).

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- List])].

dial(Addr, Txn) ->
    DataToSend = erlang:term_to_binary({blockchain_transactions:type(Txn), Txn}),
    P2PAddress = libp2p_crypto:address_to_p2p(Addr),
    Swarm = blockchain_swarm:swarm(),
    case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, blockchain_txn_handler, [self()]) of
        {ok, Stream} ->
            lager:info("dialed peer ~p via ~p~n", [Addr, ?TX_PROTOCOL]),
            libp2p_framed_stream:send(Stream, DataToSend),
            libp2p_framed_stream:close(Stream),
            ok;
        Other ->
            lager:notice("Failed to dial ~p service on ~p : ~p", [?TX_PROTOCOL, Addr, Other]),
            Other
    end.
