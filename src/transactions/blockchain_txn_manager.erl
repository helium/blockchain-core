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
          txn_queue = [] :: txn_queue()
         }).

-type txn_queue() :: [{blockchain_transactions:transaction(), fun(), erlang:queue()}].

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

-spec get_state() -> txn_queue().
get_state() ->
    gen_server:call(?MODULE, get_state, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    %% ok = blockchain_event:add_handler(self()),
    {ok, #state{}}.

handle_cast({submit, Txn, ConsensusAddrs, Callback}, State=#state{txn_queue=TxnQueue}) ->
    self() ! {process, ConsensusAddrs},
    SortedTxnQueue = lists:sort(fun({TxnA, _, _}, {TxnB, _, _}) -> blockchain_transactions:sort(TxnA, TxnB) end, TxnQueue ++ [{Txn, Callback, queue:new()}]),
    lager:info("blockchain_txn_manager, got Txn: ~p, SortedTxnQueue: ~p", [Txn, SortedTxnQueue]),
    {noreply, State#state{txn_queue=SortedTxnQueue}};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_manager got unknown cast: ~p", [_Msg]),
    {noreply, State}.

handle_call(get_state, _from, State) ->
    {reply, State#state.txn_queue, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info({process, ConsensusAddrs}, State=#state{txn_queue=[{_Txn, _Callback, Queue0} | _Tail]=TxnQueue}) ->
    lager:info("blockchain_txn_manager, process TxnQueue: ~p", [TxnQueue]),
    %% F = (length(ConsensusAddrs) - 1) div 3,
    Swarm = blockchain_swarm:swarm(),
    SuccesfulDialAddrs = queue:to_list(Queue0),
    AddrsToSearch = ConsensusAddrs -- SuccesfulDialAddrs,
    RandomAddr = lists:nth(rand:uniform(length(AddrsToSearch)), AddrsToSearch),
    P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(RandomAddr),
    NewState = case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, blockchain_txn_handler, [self()]) of
                   {ok, Stream} ->
                       lager:info("blockchain_txn_manager, dialed peer ~p via ~p~n", [RandomAddr, ?TX_PROTOCOL]),
                       NewTxnQueue = lists:foldl(fun({Txn, Callback, Queue}, Acc) ->
                                                         case queue:member(RandomAddr, Queue) of
                                                             false ->
                                                                 DataToSend = erlang:term_to_binary({blockchain_transactions:type(Txn), Txn}),
                                                                 case libp2p_framed_stream:send(Stream, DataToSend) of
                                                                     {error, Reason} ->
                                                                         lager:error("blockchain_txn_manager, libp2p_framed_stream send failed: ~p", [Reason]),
                                                                         [{Txn, Callback, Queue} | Acc];
                                                                     _ ->
                                                                         lager:info("blockchain_txn_manager, successfully sent Txn: ~p to Stream: ~p", [Txn, Stream]),
                                                                         case queue:len(Queue) + 1 == length(ConsensusAddrs) of
                                                                             true ->
                                                                                 lager:info("blockchain_txn_manager, successfuly sent Txn: ~p to F+1 member", [Txn]),
                                                                                 Callback(ok),
                                                                                 Acc;
                                                                             false ->
                                                                                 [{Txn, Callback, queue:in(RandomAddr, Queue)} | Acc]
                                                                         end
                                                                 end;
                                                             true ->
                                                                 lager:info("blockchain_txn_manager, ignoring addr: ~p for txn: ~p", [RandomAddr, Txn]),
                                                                 [{Txn, Callback, Queue} | Acc]
                                                         end
                                                 end, [], TxnQueue),
                       libp2p_framed_stream:close(Stream),
                       case length(NewTxnQueue) > 0 of
                           true ->
                               self() ! {process, ConsensusAddrs};
                           false ->
                               ok
                       end,
                       State#state{txn_queue=lists:reverse(NewTxnQueue)};
                   Other ->
                       lager:notice("blockchain_txn_manager, Failed to dial ~p service on ~p : ~p", [?TX_PROTOCOL, RandomAddr, Other]),
                       self() ! {process, ConsensusAddrs},
                       State
               end,
    {noreply, NewState};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
