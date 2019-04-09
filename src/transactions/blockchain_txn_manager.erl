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
          chain :: undefined | blockchain:blockchain()
         }).

-record(entry, {
          txn :: blockchain_txn:txn(),
          acceptors :: pubkeys(),
          rejectors :: pubkeys(),
          callback :: fun()
         }).

-define(LIMIT, 10).

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

handle_cast({submit, Txn, Callback}, State=#state{txn_map=TxnMap}) ->
    TxnHash = blockchain_txn:hash(Txn),
    NewTxnMap = maps:put(TxnHash, new_entry(Txn, Callback, [], []), TxnMap),

    case maps:size(NewTxnMap) > ?LIMIT of
        true -> self() ! timeout;
        false -> ok
    end,

    lager:info("Got Txn: ~p, TxnMap: ~p", [TxnHash, NewTxnMap]),

    {noreply, State#state{txn_map=NewTxnMap}};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_manager got unknown cast: ~p", [_Msg]),
    {noreply, State}.

handle_call(txn_map, _From, State=#state{txn_map=TxnMap}) ->
    {reply, TxnMap, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(Msg, State = #state{chain=undefined}) ->
    %% Keep trying to get a running chain
    Chain = blockchain_worker:blockchain(),
    self() ! Msg,
    {noreply, State#state{chain=Chain}};
handle_info(timeout, State=#state{txn_map=TxnMap, chain=Chain}) ->
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    Swarm = blockchain_swarm:swarm(),

    SortedTxns = lists:sort(fun({_TxnHashA, EntryA}, {_TxnHashB, EntryB}) ->
                                    blockchain_txn:sort(txn(EntryA), txn(EntryB))
                            end, maps:to_list(TxnMap)),

    ok = lists:foreach(fun({TxnHash, #entry{txn=Txn, acceptors=Acceptors}=_Entry}) ->
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
                                                       lager:error("libp2p_framed_stream send failed. Reason: ~p, To: ~p, TxnHash: ~p", [Reason, P2PAddress, TxnHash]);
                                                   _ ->
                                                       ok
                                               end;
                                           {error, Reason} ->
                                               lager:error("libp2p_framed_stream dial failed. Reason: ~p, To: ~p, TxnHash: ~p", [Reason, P2PAddress, TxnHash])
                                       end;
                                   true ->
                                       lager:info("ConsensusMember: ~p already has txn: ~p", [P2PAddress, TxnHash]),
                                       ok
                               end
                       end,
                       SortedTxns),

    {noreply, State};
handle_info({blockchain_txn_response, {ok, TxnHash, AcceptedBy}}, State=#state{txn_map=TxnMap, chain=Chain}) ->
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    F = (length(ConsensusAddrs) - 1) div 3,

    NewTxnMap = case maps:get(TxnHash, TxnMap, undefined) of
                    undefined ->
                        TxnMap;
                    #entry{acceptors=Acceptors, callback=Callback}=Entry ->
                        case (length(Acceptors) + 1) > F of
                            true ->
                                lager:info("Reached acceptance threshold for txn: ~p, invoking ok callback", [TxnHash]),
                                invoke_callback(Callback, ok),
                                maps:remove(TxnHash, TxnMap);
                            false ->
                                maps:put(TxnHash, Entry#entry{acceptors=[AcceptedBy | Acceptors]}, TxnMap)
                        end
                end,

    {noreply, State#state{txn_map=NewTxnMap}};
handle_info({blockchain_txn_response, {error, TxnHash, RejectedBy}}, State=#state{chain=Chain, txn_map=TxnMap}) ->
    {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    F = (length(ConsensusAddrs) - 1) div 3,

    NewTxnMap = case maps:get(TxnHash, TxnMap, undefined) of
                    undefined ->
                        TxnMap;
                    #entry{rejectors=Rejectors, callback=Callback}=Entry ->
                        case (length(Rejectors) + 1) > F of
                            true ->
                                lager:info("Reached rejection threshold for txn: ~p, invoking error callback", [TxnHash]),
                                invoke_callback(Callback, {error, rejected}),
                                maps:remove(TxnHash, TxnMap);
                            false ->
                                maps:put(TxnHash, Entry#entry{rejectors=[RejectedBy | Rejectors]}, TxnMap)
                        end
                end,
    {noreply, State#state{txn_map=NewTxnMap}};
handle_info({blockchain_event, {add_block, Hash, _Sync}}, State = #state{chain=Chain, txn_map=TxnMap}) ->
    case blockchain:get_block(Hash, Chain) of
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            {_ValidTransactions, InvalidTransactions} = blockchain_txn:validate([txn(Entry) || Entry <- TxnQueue], Chain),
            NewTxnMap = maps:filter(fun(_TxnHash, #entry{txn=Txn, callback=Callback}) ->
                                            case {lists:member(Txn, Txns), lists:member(Txn, InvalidTransactions)} of
                                                {true, _} ->
                                                      invoke_callback(Callback, ok),
                                                      false;
                                                {_, true} ->
                                                      invoke_callback(Callback, {error, invalid}),
                                                      false;
                                                _ ->
                                                    true
                                            end
                                    end,
                                    TxnMap),

            self() ! timeout,

            {noreply, State#state{txn_map=NewTxnMap}};
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
