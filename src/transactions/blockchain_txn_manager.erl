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
          txn_queue = [] :: txn_queue(),
          chain
         }).

-type txn_queue() :: [{blockchain_transactions:transaction(), fun(), [libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()]}].

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
    ok = blockchain_event:add_handler(self()),
    Chain = blockchain_worker:blockchain(),
    {ok, #state{chain=Chain}}.

handle_cast({submit, Txn, ConsensusAddrs, Callback}, State=#state{txn_queue=TxnQueue}) ->
    self() ! {process, ConsensusAddrs},
    SortedTxnQueue = lists:sort(fun({TxnA, _, _, _}, {TxnB, _, _, _}) -> blockchain_transactions:sort(TxnA, TxnB) end, TxnQueue ++ [{Txn, Callback, [], []}]),
    lager:info("blockchain_txn_manager, got Txn: ~p, SortedTxnQueue: ~p, TxnQueueLen: ~p", [Txn, SortedTxnQueue, length(SortedTxnQueue)]),
    {noreply, State#state{txn_queue=SortedTxnQueue}};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_manager got unknown cast: ~p", [_Msg]),
    {noreply, State}.

handle_call(get_state, _from, State) ->
    {reply, State#state.txn_queue, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info({process, ConsensusAddrs}, State=#state{txn_queue=[{_Txn, _Callback, AcceptQueue0, _RejectQueue0} | _Tail]=TxnQueue}) ->
    lager:info("blockchain_txn_manager, process TxnQueue: ~p, TxnQueueLen: ~p", [TxnQueue, length(TxnQueue)]),
    F = (length(ConsensusAddrs) - 1) div 3,
    Swarm = blockchain_swarm:swarm(),
    AddrsToSearch = ConsensusAddrs -- AcceptQueue0,
    RandomAddr = lists:nth(rand:uniform(length(AddrsToSearch)), AddrsToSearch),
    P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(RandomAddr),
    Ref = make_ref(),
    NewState = case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, blockchain_txn_handler, [self(), Ref]) of
                   {ok, Stream} ->
                       lager:info("blockchain_txn_manager, dialed peer ~p via ~p~n, TxnQueueLen: ~p", [RandomAddr, ?TX_PROTOCOL, length(TxnQueue)]),
                       NewTxnQueue = lists:foldl(fun({Txn, Callback, AcceptQueue, RejectQueue}, Acc) ->
                                                         case lists:member(RandomAddr, AcceptQueue) of
                                                             false ->
                                                                 DataToSend = erlang:term_to_binary({blockchain_transactions:type(Txn), Txn}),
                                                                 case libp2p_framed_stream:send(Stream, DataToSend) of
                                                                     {error, Reason} ->
                                                                         lager:error("blockchain_txn_manager, libp2p_framed_stream send failed: ~p, TxnQueueLen: ~p", [Reason, length(TxnQueue)]),
                                                                         [{Txn, Callback, AcceptQueue, RejectQueue} | Acc];
                                                                     _ ->
                                                                         receive
                                                                             {Ref, ok} ->
                                                                                 lager:info("blockchain_txn_manager, successfully sent Txn: ~p to Stream: ~p, TxnQueueLen: ~p", [Txn, Stream, length(TxnQueue)]),
                                                                                 case length(AcceptQueue) > F of
                                                                                     true ->
                                                                                         lager:info("blockchain_txn_manager, successfuly sent Txn: ~p to F+1 members, TxnQueueLen: ~p", [Txn, length(TxnQueue)]),
                                                                                         Callback(ok),
                                                                                         Acc;
                                                                                     false ->
                                                                                         [{Txn, Callback, [RandomAddr | AcceptQueue], RejectQueue} | Acc]
                                                                                 end;
                                                                             {Ref, error} ->
                                                                                 case length(RejectQueue) > 2*F of
                                                                                     true ->
                                                                                         lager:error("blockchain_txn_manager, dropping txn: ~p rejected by 2F+1 members", [Txn]),
                                                                                         Acc;
                                                                                     false ->
                                                                                         lager:error("blockchain_txn_manager, txn: ~p reject by ~p, TxnQueueLen: ~p", [Txn, Stream, length(TxnQueue)]),
                                                                                         [{Txn, Callback, AcceptQueue, lists:usort([RandomAddr | RejectQueue])} | Acc]
                                                                                 end
                                                                         after
                                                                             30000 ->
                                                                                 lager:warning("blockchain_txn_manager, txn: ~p TIMEOUT, TxnQueueLen: ~p", [Txn, length(TxnQueue)]),
                                                                                 [{Txn, Callback, AcceptQueue, RejectQueue} | Acc]
                                                                         end
                                                                 end;
                                                             true ->
                                                                 lager:info("blockchain_txn_manager, ignoring addr: ~p for txn: ~p, TxnQueueLen: ~p", [RandomAddr, Txn, length(TxnQueue)]),
                                                                 [{Txn, Callback, AcceptQueue, RejectQueue} | Acc]
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
                       lager:notice("blockchain_txn_manager, Failed to dial ~p service on ~p : ~p, TxnQueueLen: ~p", [?TX_PROTOCOL, RandomAddr, Other, length(TxnQueue)]),
                       self() ! {process, ConsensusAddrs},
                       State
               end,
    {noreply, NewState};
handle_info({blockchain_event, {add_block, Hash, _Sync}}, State = #state{chain=Chain0}) ->
    Chain = case Chain0 of
                undefined ->
                    case blockchain_worker:blockchain() of
                undefined ->
                            %% uncaught throws count as a return value in gen_servers
                            throw({noreply, State});
                        C ->
                            C
                    end;
                C ->
                    C
            end,
    case blockchain:get_block(Hash, Chain) of
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            NewTxnQueue = [ {Txn, Callback, Accept, Reject} || {Txn, Callback, Accept, Reject} <- State#state.txn_queue, not lists:member(Txn, Txns) ],
            lager:info("removed ~p commited transactions from queue", [length(State#state.txn_queue -- NewTxnQueue)]),
            {_ValidTransactions, InvalidTransactions} = blockchain_transactions:validate([ Txn || {Txn, _, _, _} <- NewTxnQueue], blockchain:ledger(Chain)),
            NewerTxnQueue = [ {Txn, Callback, Accept, Reject} || {Txn, Callback, Accept, Reject} <- NewTxnQueue, not lists:member(Txn, InvalidTransactions) ],
            lager:info("removed ~p invalid transactions from queue", [length(NewTxnQueue -- NewerTxnQueue)]),
            {noreply, State#state{txn_queue=NewerTxnQueue}};
        _ ->
            %% this should not happen
            error(missing_block)
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
