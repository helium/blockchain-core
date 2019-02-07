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
         txn_queue/0
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
          chain,
          counter = 0
         }).

-define(TIMEOUT, 5000).

-type txn_queue() :: [{blockchain_txn:txn(), fun(), [libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()]}].

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Txn :: blockchain_txn:txn(),
             ConsensusAddrs :: [libp2p_crypto:address()],
             Callback :: fun()) -> ok.
submit(Txn, ConsensusAddrs, Callback) ->
    gen_server:cast(?MODULE, {submit, Txn, ConsensusAddrs, Callback}).

-spec txn_queue() -> txn_queue().
txn_queue() ->
    gen_server:call(?MODULE, txn_queue, 60000).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    ok = blockchain_event:add_handler(self()),
    Chain = blockchain_worker:blockchain(),
    {ok, #state{chain=Chain}}.

handle_cast({submit, Txn, _ConsensusAddrs, Callback}, State=#state{txn_queue=TxnQueue, counter=Counter}) ->
    SortedTxnQueue = lists:sort(fun({TxnA, _, _, _}, {TxnB, _, _, _}) -> blockchain_txn:sort(TxnA, TxnB) end, TxnQueue ++ [{Txn, Callback, [], []}]),
    case (Counter + 1) rem 50 == 0 of
        true ->
            %% force the txn queue to flush every 50 messages
            self() ! timeout;
        false ->
            ok
    end,
    {noreply, State#state{txn_queue=SortedTxnQueue, counter=Counter+1}, ?TIMEOUT};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_manager got unknown cast: ~p", [_Msg]),
    {noreply, State, ?TIMEOUT}.

handle_call(txn_queue, _From, State=#state{txn_queue=TxnQueue}) ->
    {reply, TxnQueue, State, ?TIMEOUT};
handle_call(_, _, State) ->
    {reply, ok, State, ?TIMEOUT}.

handle_info(timeout, State=#state{txn_queue=[{_Txn, _Callback, AcceptQueue0, _RejectQueue0} | _Tail]=TxnQueue, chain=Chain}) when Chain /= undefined ->
    Ledger = blockchain:ledger(Chain),
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
    F = (length(ConsensusAddrs) - 1) div 3,
    Swarm = blockchain_swarm:swarm(),
    AddrsToSearch = ConsensusAddrs -- AcceptQueue0,
    RandomAddr = lists:nth(rand:uniform(length(AddrsToSearch)), AddrsToSearch),
    P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(RandomAddr),
    Ref = make_ref(),
    NewState = case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, blockchain_txn_handler, [self(), Ref]) of
                   {ok, Stream} ->
                       NewTxnQueue = lists:foldl(fun({Txn, Callback, AcceptQueue, RejectQueue}, Acc) ->
                                                         case lists:member(RandomAddr, AcceptQueue) of
                                                             false ->
                                                                 DataToSend = blockchain_txn:serialize(Txn),
                                                                 case libp2p_framed_stream:send(Stream, DataToSend) of
                                                                     {error, Reason} ->
                                                                         lager:error("blockchain_txn_manager, libp2p_framed_stream send failed: ~p, TxnQueueLen: ~p", [Reason, length(TxnQueue)]),
                                                                         [{Txn, Callback, AcceptQueue, RejectQueue} | Acc];
                                                                     _ ->
                                                                         receive
                                                                             {Ref, ok} ->
                                                                                 case length(AcceptQueue) > F of
                                                                                     true ->
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
                                                                 [{Txn, Callback, AcceptQueue, RejectQueue} | Acc]
                                                         end
                                                 end, [], TxnQueue),
                       libp2p_framed_stream:close(Stream),
                       State#state{txn_queue=lists:reverse(NewTxnQueue)};
                   _Other ->
                       %% try to dial someone else ASAR
                       erlang:send_after(?TIMEOUT, self(), timeout),
                       State
               end,
    {noreply, NewState, ?TIMEOUT};
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
            {_ValidTransactions, InvalidTransactions} = blockchain_txn:validate([ Txn || {Txn, _, _, _} <- NewTxnQueue], blockchain:ledger(Chain)),
            NewerTxnQueue = [ {Txn, Callback, Accept, Reject} || {Txn, Callback, Accept, Reject} <- NewTxnQueue, not lists:member(Txn, InvalidTransactions) ],
            {noreply, State#state{txn_queue=NewerTxnQueue, chain=Chain}, ?TIMEOUT};
        _ ->
            %% this should not happen
            error(missing_block)
    end;
handle_info(_Msg, State) ->
    {noreply, State, ?TIMEOUT}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
