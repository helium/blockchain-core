%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Mgr ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_mgr).

-behavior(gen_server).

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
         start_link/1,
         submit/2,
         set_chain/1,
         txn_map/0
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
          resp_map = #{} :: resp_map(),
          chain :: undefined | blockchain:blockchain()
         }).

-type txn_map() :: #{blockchain_txn:txn() => {fun(), [pid()]}}.
-type resp_map() :: #{blockchain_txn:txn() => {pubkeys(), pubkeys()}}.
-type pubkeys() :: [libp2p_crypto:pubkey_bin()].

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Txn :: blockchain_txn:txn(), Callback :: fun()) -> ok.
submit(Txn, Callback) ->
    gen_server:cast(?MODULE, {submit, Txn, Callback}).

-spec set_chain(blockchain:blockchain()) -> ok.
set_chain(Chain) ->
    gen_server:cast(?MODULE, {set_chain, Chain}).

-spec txn_map() -> txn_map().
txn_map() ->
    gen_server:call(?MODULE, txn_map, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    ok = blockchain_event:add_handler(self()),
    Chain = blockchain_worker:blockchain(),
    {ok, #state{chain=Chain}}.

handle_cast({set_chain, Chain}, State=#state{chain=undefined}) ->
    {noreply, State#state{chain=Chain}};
handle_cast({submit, Txn, Callback}, State=#state{chain=undefined, txn_map=TxnMap}) ->
    %% Got txn when there is no chain
    %% Keep it in the txn_map and process when there is a chain
    NewTxnMap = maps:put(Txn, {Callback, []}, TxnMap),
    self() ! wait_for_chain,
    {noreply, State#state{txn_map=NewTxnMap}};
handle_cast({submit, Txn, Callback}, State=#state{chain=Chain, txn_map=TxnMap}) ->
    lager:info("blockchain_txn_mgr submit, Txn: ~p, Callback: ~p", [Txn, Callback]),
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    Dialers = blockchain_txn_mgr_sup:start_workers([self(), Txn, ConsensusMembers]),
    NewTxnMap = maps:put(Txn, {Callback, Dialers}, TxnMap),
    {noreply, State#state{txn_map=NewTxnMap}};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_mgr got unknown cast: ~p", [_Msg]),
    lager:warning("blockchain_txn_mgr, state: ~p", [State]),
    {noreply, State}.

handle_call(txn_map, _, State) ->
    {reply, State#state.txn_map, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(_, State=#state{chain=undefined}) ->
    self() ! wait_for_chain,
    {noreply, State};
handle_info(wait_for_chain, State=#state{chain=Chain}) ->
    case blockchain_worker:blockchain() of
        undefined ->
            %% check again after 1 second
            erlang:send_after(1000, self(), wait_for_chain),
            {noreply, State};
        Chain ->
            self() ! resubmit,
            {noreply, State#state{chain=Chain}}
    end;
handle_info(resubmit, State=#state{txn_map=TxnMap, chain=Chain}) ->
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),

    SortedTxns = lists:sort(fun({TxnA, _}, {TxnB, _}) ->
                                    blockchain_txn:sort(TxnA, TxnB)
                            end, maps:to_list(TxnMap)),

    NewTxnMap = lists:foldl(fun({Txn, {Callback, Dialers}}, Acc) ->
                                    case Dialers == [] of
                                        true ->
                                            maps:put(Txn,
                                                     {Callback, blockchain_txn_mgr_sup:start_workers([self(), Txn, ConsensusMembers])},
                                                     Acc);
                                        false ->
                                            Acc
                                    end
                            end,
                            #{},
                            SortedTxns),

    {noreply, State#state{txn_map=NewTxnMap}};
handle_info({accepted, {Dialer, Txn, Member}}, State=#state{chain=Chain, txn_map=TxnMap, resp_map=RespMap}) ->
    lager:info("blockchain_txn_mgr, txn: ~p, accepted_by: ~p", [Txn, Member]),

    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    F = (length(ConsensusMembers) - 1) div 3,

    case maps:get(Txn, RespMap, undefined) of
        undefined ->
            {noreply, State};
        {Accepts, Rejects} ->
            case length(Accepts) + 1 > F of
                true ->
                    %% Reached acceptance threshold
                    %% Fire callback
                    {Callback, Dialers} = maps:get(Txn, TxnMap),
                    %% Stop all dialers
                    lists:foreach(fun(D) ->
                                          blockchain_txn_mgr_sup:terminate_worker(D)
                                  end, Dialers),
                    invoke_callback(Callback, ok),
                    %% Remove this txn from state
                    NewTxnMap = maps:remove(Txn, TxnMap),
                    NewRespMap = maps:remove(Txn, RespMap),
                    {noreply, State#state{txn_map=NewTxnMap, resp_map=NewRespMap}};
                false ->
                    %% Stop this particular dialer
                    blockchain_txn_mgr_sup:terminate_worker(Dialer),
                    %% Update acceptance list for this txn
                    NewAccepts = [Member | Accepts],
                    NewRespMap = maps:put(Txn, {NewAccepts, Rejects}, RespMap),
                    {noreply, State#state{resp_map=NewRespMap}}
            end
    end;
handle_info({rejected, {Dialer, Txn, Member}}, State=#state{chain=Chain, txn_map=TxnMap, resp_map=RespMap}) ->
    lager:info("blockchain_txn_mgr, txn: ~p, rejected_by: ~p", [Txn, Member]),
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    F = (length(ConsensusMembers) - 1) div 3,

    case maps:get(Txn, RespMap, undefined) of
        undefined ->
            {noreply, State};
        {Accepts, Rejects} ->
            case length(Rejects) + 1 > 2*F of
                true ->
                    %% Reached rejection threshold
                    %% Fire callback
                    {Callback, Dialers} = maps:get(Txn, TxnMap),
                    lists:foreach(fun(D) ->
                                          blockchain_txn_mgr_sup:terminate_worker(D)
                                  end, Dialers),
                    invoke_callback(Callback, error),
                    %% Remove this txn from state
                    NewTxnMap = maps:remove(Txn, TxnMap),
                    NewRespMap = maps:remove(Txn, RespMap),
                    {noreply, State#state{txn_map=NewTxnMap, resp_map=NewRespMap}};
                false ->
                    %% Stop this particular dialer
                    blockchain_txn_mgr_sup:terminate_worker(Dialer),
                    %% Start a new one
                    blockchain_txn_mgr_sup:start_worker([self(), Txn, Member]),
                    %% Update rejection list
                    NewRejects = [Member | Rejects],
                    NewRespMap = maps:put(Txn, {Accepts, NewRejects}, RespMap),
                    {noreply, State#state{resp_map=NewRespMap}}
            end
    end;
handle_info({blockchain_event, {add_block, BlockHash, _Sync}},
            State=#state{chain=Chain, txn_map=TxnMap, resp_map=RespMap}) ->

    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),

    case blockchain:get_block(BlockHash, Chain) of
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            {_ValidTransactions, InvalidTransactions} = blockchain_txn:validate(maps:keys(TxnMap), Chain),

            SortedTxns = lists:sort(fun({TxnA, _}, {TxnB, _}) ->
                                            blockchain_txn:sort(TxnA, TxnB)
                                    end, maps:to_list(TxnMap)),

            NewTxnMap = lists:foldl(fun({Txn, {Callback, Dialers}}, Acc) ->
                                            case {lists:member(Txn, Txns),
                                                  lists:member(Txn, InvalidTransactions)} of
                                                {true, _} ->
                                                    invoke_callback(Callback, ok),
                                                    Acc;
                                                {_, true} ->
                                                    invoke_callback(Callback, {error, invalid}),
                                                    Acc;
                                                _ ->
                                                    %% Retry from scratch on a new block
                                                    %% Note that the response map is still intact
                                                    %% Ensures that we don't lose previously ingested responses
                                                    %% Still questionable I suppose
                                                    lists:foreach(fun(D) ->
                                                                          blockchain_txn_mgr_sup:terminate_worker(D)
                                                                  end, Dialers),
                                                    NewDialers = blockchain_txn_mgr_sup:start_workers([self(), Txn, ConsensusMembers]),
                                                    maps:put(Txn, {Callback, NewDialers}, Acc)
                                            end
                                    end,
                                    #{},
                                    SortedTxns),

            NewRespMap = lists:foldl(fun(Txn, Acc) ->
                                             case {lists:member(Txn, Txns),
                                                   lists:member(Txn, InvalidTransactions)} of
                                                 {true, _} ->
                                                     maps:remove(Txn, Acc);
                                                 {_, true} ->
                                                     maps:remove(Txn, Acc);
                                                 _ ->
                                                     Acc
                                             end
                                     end,
                                     RespMap,
                                     maps:keys(RespMap)),

            {noreply, State#state{txn_map=NewTxnMap, resp_map=NewRespMap}};
        _ ->
            lager:error("WTF happened!"),
            {noreply, State}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

invoke_callback(Callback, Msg) ->
    spawn(fun() -> Callback(Msg) end).
