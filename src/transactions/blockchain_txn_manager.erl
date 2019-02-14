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

-record(entry, {
          txn :: blockchain_txn:txn(),
          acceptors :: pubkeys(),
          rejectors :: pubkeys(),
          callback :: fun()
         }).

-define(TIMEOUT, 5000).

-type entry() :: #entry{}.
-type txn_queue() :: [entry()].
-type pubkeys() :: [libp2p_crypto:pubkey_bin()].

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Txn :: blockchain_txn:txn(), ConsensusAddrs :: pubkeys(), Callback :: fun()) -> ok.
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
    SortedTxnQueue = lists:sort(fun(EntryA, EntryB) ->
                                        blockchain_txn:sort(txn(EntryA), txn(EntryB))
                                end, TxnQueue ++ [new_entry(Txn, Callback, [], [])]),

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

%% handle_info(timeout, State=#state{txn_queue=[{_Txn, _Callback, _CallbackInfo, AcceptQueue0, _RejectQueue0} | _Tail]=TxnQueue, chain=Chain}) when Chain /= undefined ->
handle_info(timeout, State=#state{txn_queue=[Head | _Tail]=TxnQueue, chain=Chain}) when Chain /= undefined ->
    Ledger = blockchain:ledger(Chain),
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
    F = (length(ConsensusAddrs) - 1) div 3,
    Swarm = blockchain_swarm:swarm(),
    AddrsToSearch = ConsensusAddrs -- acceptors(Head),
    RandomAddr = lists:nth(rand:uniform(length(AddrsToSearch)), AddrsToSearch),
    P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(RandomAddr),
    Ref = make_ref(),
    NewState = case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, blockchain_txn_handler, [self(), Ref]) of
                   {ok, Stream} ->
                       NewTxnQueue = lists:foldl(fun(#entry{txn=Txn, acceptors=Acceptors, rejectors=Rejectors, callback=Callback}=Entry, Acc) ->
                                                         case lists:member(RandomAddr, Acceptors) of
                                                             false ->
                                                                 DataToSend = blockchain_txn:serialize(Txn),
                                                                 case libp2p_framed_stream:send(Stream, DataToSend) of
                                                                     {error, Reason} ->
                                                                         lager:error("blockchain_txn_manager, libp2p_framed_stream send failed: ~p, TxnQueueLen: ~p", [Reason, length(TxnQueue)]),
                                                                         [Entry | Acc];
                                                                     _ ->
                                                                         receive
                                                                             {Ref, ok} ->
                                                                                 case length(Acceptors) > F of
                                                                                     true ->
                                                                                         invoke_callback(Callback, ok),
                                                                                         Acc;
                                                                                     false ->
                                                                                         NewEntry = new_entry(Txn, Callback, [RandomAddr | Acceptors], Rejectors),
                                                                                         [NewEntry | Acc]
                                                                                 end;
                                                                             {Ref, error} ->
                                                                                 case length(Rejectors) > 2*F of
                                                                                     true ->
                                                                                         lager:error("blockchain_txn_manager, dropping txn: ~p rejected by 2F+1 members", [Txn]),
                                                                                         invoke_callback(Callback, {error, rejected}),
                                                                                         Acc;
                                                                                     false ->
                                                                                         lager:error("blockchain_txn_manager, txn: ~p reject by ~p, TxnQueueLen: ~p", [Txn, Stream, length(TxnQueue)]),
                                                                                         NewEntry = new_entry(Txn, Callback, Acceptors, lists:usort([RandomAddr | Rejectors])),
                                                                                         [NewEntry | Acc]
                                                                                 end
                                                                         after
                                                                             30000 ->
                                                                                 lager:warning("blockchain_txn_manager, txn: ~p TIMEOUT, TxnQueueLen: ~p", [Txn, length(TxnQueue)]),
                                                                                 [Entry | Acc]
                                                                         end
                                                                 end;
                                                             true ->
                                                                 [Entry | Acc]
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
handle_info({blockchain_event, {add_block, Hash, _Sync}}, State = #state{chain=Chain0, txn_queue=TxnQueue}) ->
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
            {_ValidTransactions, InvalidTransactions} = blockchain_txn:validate([ txn(Entry) || Entry <- TxnQueue], blockchain:ledger(Chain)),
            NewTxnQueue = lists:foldl(fun(#entry{txn=Txn, callback=Callback}=Entry, Acc) ->
                                              case {lists:member(txn(Entry), Txns), lists:member(Txn, InvalidTransactions)} of
                                                  {true, _} ->
                                                      invoke_callback(Callback, ok),
                                                      Acc;
                                                  {_, true} ->
                                                      invoke_callback(Callback, {error, invalid}),
                                                      Acc;
                                                  _ ->
                                                      [Entry | Acc]
                                              end
                                      end, [], TxnQueue),

            {noreply, State#state{txn_queue=lists:reverse(NewTxnQueue), chain=Chain}, ?TIMEOUT};
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


%% ------------------------------------------------------------------
%% Helper functions
%% ------------------------------------------------------------------
-spec txn(entry()) -> blockchain_txn:txn().
txn(Entry) -> Entry#entry.txn.

-spec acceptors(entry()) -> pubkeys().
acceptors(Entry) -> Entry#entry.acceptors.

-spec new_entry(blockchain_txn:txn(), fun(), pubkeys(), pubkeys()) -> entry().
new_entry(Txn, Callback, Acceptors, Rejectors) ->
    #entry{txn=Txn, callback=Callback, acceptors=Acceptors, rejectors=Rejectors}.

invoke_callback(Callback, Msg) ->
    spawn(fun() -> Callback(Msg) end).
