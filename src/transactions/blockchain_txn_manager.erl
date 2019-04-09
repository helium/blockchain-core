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
         submit/2,
         txn_map/0,
         %% exports for the cli module
         acceptors/1,
         rejectors/1,
         txn/1
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
          txn_map = #{} :: txn_map(),
          chain :: undefined | blockchain:blockchain(),
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
-type txn_map() :: #{blockchain_txn:hash() => entry()}.
-type pubkeys() :: [libp2p_crypto:pubkey_bin()].

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Txn :: blockchain_txn:txn(), Callback :: fun()) -> ok.
submit(Txn, Callback) ->
    gen_server:cast(?MODULE, {submit, Txn, Callback}).

-spec txn_map() -> txn_map().
txn_map() ->
    gen_server:call(?MODULE, txn_map, 60000).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    ok = blockchain_event:add_handler(self()),
    Chain = blockchain_worker:blockchain(),
    {ok, #state{chain=Chain}}.

handle_cast({submit, Txn, Callback}, State=#state{txn_map=TxnMap, counter=Counter}) ->
    NewTxnMap = maps:put(blockchain_txn:hash(Txn), new_entry(Txn, Callback, [], []), TxnMap),

    case (Counter + 1) rem 50 == 0 of
        true ->
            %% force the txn queue to flush every 50 messages
            self() ! timeout;
        false ->
            ok
    end,
    {noreply, State#state{txn_map=NewTxnMap, counter=Counter+1}, ?TIMEOUT};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_manager got unknown cast: ~p", [_Msg]),
    {noreply, State, ?TIMEOUT}.

handle_call(txn_map, _From, State=#state{txn_map=TxnMap}) ->
    {reply, TxnMap, State, ?TIMEOUT};
handle_call(_, _, State) ->
    {reply, ok, State, ?TIMEOUT}.

handle_info(timeout, State=#state{txn_map=TxnMap, chain=Chain}) when Chain /= undefined ->
    Ledger = blockchain:ledger(Chain),
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
    Swarm = blockchain_swarm:swarm(),

    SortedTxns = lists:sort(fun(EntryA, EntryB) ->
                                    blockchain_txn:sort(txn(EntryA), txn(EntryB))
                            end, maps:values(TxnMap)),

    ok = maps:fold(fun(TxnHash, #entry{txn=Txn, acceptors=Acceptors}=_Entry, _Acc) ->
                            AddrsToSearch = ConsensusAddrs -- Acceptors,
                            RandomAddr = lists:nth(rand:uniform(length(AddrsToSearch)), AddrsToSearch),
                            P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(RandomAddr),

                            case lists:member(RandomAddr, Acceptors) of
                                false ->
                                    case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, blockchain_txn_handler, [self(), TxnHash, RandomAddr]) of
                                        {ok, Stream} ->
                                            DataToSend = blockchain_txn:serialize(Txn),
                                            case libp2p_framed_stream:send(Stream, DataToSend) of
                                                {error, Reason} ->
                                                    lager:error("blockchain_txn_manager, libp2p_framed_stream send failed: ~p, to: ~p, TxnHash: ~p", [Reason, P2PAddress, TxnHash]);
                                                _ ->
                                                    ok
                                            end;
                                        {error, Reason} ->
                                            %% try to dial someone else ASAR
                                            %% erlang:send_after(?TIMEOUT, self(), timeout)
                                            lager:error("blockchain_txn_manager, libp2p_framed_stream dial failed: ~p, to: ~p, TxnHash: ~p", [Reason, P2PAddress, TxnHash])
                                    end;
                                true ->
                                    ok
                            end
                    end,
                    ok,
                    SortedTxns),

    {noreply, State, ?TIMEOUT};
handle_info({blockchain_txn_response, {ok, TxnHash, AcceptedBy}}, State=#state{txn_map=TxnMap, chain=Chain}) ->
    Ledger = blockchain:ledger(Chain),
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
    F = (length(ConsensusAddrs) - 1) div 3,

    NewTxnMap = case maps:get(TxnHash, TxnMap, undefined) of
                    undefined ->
                        TxnMap;
                    #entry{acceptors=Acceptors, callback=Callback}=Entry ->
                        case (length(Acceptors) + 1) > F of
                            true ->
                                invoke_callback(Callback, ok),
                                maps:remove(TxnHash, TxnMap);
                            false ->
                                maps:put(TxnHash, Entry#entry{acceptors=[AcceptedBy | Acceptors]}, TxnMap)
                        end
                end,

    {noreply, State=#state{txn_map=NewTxnMap}, ?TIMEOUT};
handle_info({blockchain_txn_response, {error, TxnHash, RejectedBy}}, State=#state{chain=Chain, txn_map=TxnMap}) ->
    Ledger = blockchain:ledger(Chain),
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
    F = (length(ConsensusAddrs) - 1) div 3,

    NewTxnMap = case maps:get(TxnHash, TxnMap, undefined) of
                    undefined ->
                        TxnMap;
                    #entry{rejectors=Rejectors, callback=Callback}=Entry ->
                        case (length(Rejectors) + 1) > F of
                            true ->
                                invoke_callback(Callback, {error, rejected}),
                                maps:remove(TxnHash, TxnMap);
                            false ->
                                maps:put(TxnHash, Entry#entry{rejectors=[RejectedBy | Rejectors]}, TxnMap)
                        end
                end,
    {noreply, State#state{txn_map=NewTxnMap}, ?TIMEOUT};
handle_info({blockchain_event, {add_block, Hash, _Sync}}, State = #state{chain=Chain0, txn_map=TxnMap}) ->
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
            {_ValidTransactions, InvalidTransactions} = blockchain_txn:validate([txn(Entry) || Entry <- TxnQueue], Chain),
            NewTxnMap = maps:filter(fun(_TxnHash, #entry{txn=Txn, callback=Callback}) ->
                                            case {lists:member(Txn, Txns), lists:member(Txn, InvalidTransactions)} of
                                                {true, _} ->
                                                      invoke_callback(Callback, ok),
                                                      true;
                                                {_, true} ->
                                                      invoke_callback(Callback, {error, invalid}),
                                                      true;
                                                _ ->
                                                    false
                                            end
                                    end,
                                    TxnMap),

            {noreply, State#state{txn_map=NewTxnMap, chain=Chain}, ?TIMEOUT};
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

-spec rejectors(entry()) -> pubkeys().
rejectors(Entry) -> Entry#entry.rejectors.

-spec new_entry(blockchain_txn:txn(), fun(), pubkeys(), pubkeys()) -> entry().
new_entry(Txn, Callback, Acceptors, Rejectors) ->
    #entry{txn=Txn, callback=Callback, acceptors=Acceptors, rejectors=Rejectors}.

invoke_callback(Callback, Msg) ->
    spawn(fun() -> Callback(Msg) end).
