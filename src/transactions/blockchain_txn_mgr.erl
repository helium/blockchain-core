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
          chain :: undefined | blockchain:blockchain()
         }).

-type txn_map() :: #{blockchain_txn:txn() => {fun(), [], undefined | pid()}}.

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
    NewTxnMap = maps:put(Txn, {Callback, [], undefined}, TxnMap),
    self() ! wait_for_chain,
    {noreply, State#state{txn_map=NewTxnMap}};
handle_cast({submit, Txn, Callback}, State=#state{chain=Chain, txn_map=TxnMap}) ->
    %% Get a random consensus member from the chain who signed the previous block
    {ok, RandMember} = signatory_rand_member([], Chain),
    {ok, Dialer} = blockchain_txn_mgr_sup:start_dialer([self(), Txn, RandMember]),
    ok = blockchain_txn_dialer:dial(Dialer),
    NewTxnMap = maps:put(Txn, {Callback, [], Dialer}, TxnMap),
    {noreply, State#state{txn_map=NewTxnMap}};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_mgr got unknown cast: ~p", [_Msg]),
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
    SortedTxns = lists:sort(fun({TxnA, _}, {TxnB, _}) ->
                                    blockchain_txn:sort(TxnA, TxnB)
                            end, maps:to_list(TxnMap)),

    NewTxnMap = lists:foldl(fun({Txn, {Callback, Rejections, Dialer}}, Acc) ->
                                    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
                                    {ok, RandMember} = signatory_rand_member(Rejections, Chain),
                                    {ok, NewDialer} = blockchain_txn_mgr_sup:start_dialer([self(), Txn, RandMember]),
                                    ok = blockchain_txn_dialer:dial(NewDialer),
                                    maps:put(Txn, {Callback, NewDialer}, Acc)
                            end,
                            TxnMap,
                            SortedTxns),

    {noreply, State#state{txn_map=NewTxnMap}};
handle_info({accepted, {Dialer, Txn, Member}}, State) ->
    lager:info("txn: ~p, accepted_by: ~p, Dialer: ~p", [Txn, Member, Dialer]),
    {noreply, State};
handle_info({dial_failed, {Dialer, Txn, Member}}, State) ->
    lager:debug("txn: ~p, dial_failed: ~p, Dialer: ~p", [Txn, Member, Dialer]),
    NewState = retry(Txn, State),
    {noreply, NewState};
handle_info({send_failed, {Dialer, Txn, Member}}, State) ->
    lager:debug("txn: ~p, send_failed: ~p, Dialer: ~p", [Txn, Member, Dialer]),
    NewState = retry(Txn, State),
    {noreply, NewState};
handle_info({rejected, {Dialer, Txn, Member}}, State) ->
    lager:debug("txn: ~p, rejected_by: ~p, Dialer: ~p", [Txn, Member, Dialer]),
    NewState = rejected(Txn, Member, State),
    {noreply, NewState};
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    {noreply, State#state{chain = NC}};
handle_info({blockchain_event, {add_block, BlockHash, _Sync, _Ledger}}, State=#state{chain=Chain, txn_map=TxnMap}) ->
    case blockchain:get_block(BlockHash, Chain) of
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            {_ValidTransactions, InvalidTransactions} =
                blockchain_txn:validate(lists:usort(fun blockchain_txn:sort/2,
                                                    maps:keys(TxnMap)), Chain),

            SortedTxns = lists:sort(fun({TxnA, _}, {TxnB, _}) ->
                                            blockchain_txn:sort(TxnA, TxnB)
                                    end, maps:to_list(TxnMap)),

            NewTxnMap = lists:foldl(fun({Txn, {Callback, Rejections, Dialer}}, Acc) ->
                                            case {lists:member(Txn, Txns), lists:member(Txn, InvalidTransactions)} of
                                                {true, _} ->
                                                    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
                                                    invoke_callback(Callback, ok),
                                                    maps:remove(Txn, Acc);
                                                {_, true} ->
                                                    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
                                                    invoke_callback(Callback, {error, invalid}),
                                                    maps:remove(Txn, Acc);
                                                _ ->
                                                    %% Stop this dialer
                                                    lager:info("Rescheduling txn: ~p, stopping Dialer: ~p", [blockchain_txn:hash(Txn), Dialer]),
                                                    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
                                                    {ok, RandMember} = signatory_rand_member(Rejections, Chain),
                                                    %% Retry with a new dialer
                                                    {ok, NewDialer} = blockchain_txn_mgr_sup:start_dialer([self(), Txn, RandMember]),
                                                    ok = blockchain_txn_dialer:dial(NewDialer),
                                                    maps:put(Txn, {Callback, Rejections, NewDialer}, Acc)
                                            end
                                    end,
                                    TxnMap,
                                    SortedTxns),

            {noreply, State#state{txn_map=NewTxnMap}};
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

%% ------------------------------------------------------------------
%% Helper functions
%% ------------------------------------------------------------------
invoke_callback(Callback, Msg) ->
    spawn(fun() -> Callback(Msg) end).

signatory_rand_member(Rejections, Chain) ->
    {ok, PrevBlock} = blockchain:head_block(Chain),
    Signatories = [Signer || {Signer, _} <- blockchain_block:signatures(PrevBlock), not lists:member(Signer, [blockchain_swarm:pubkey_bin()|Rejections])],
    case Signatories of
        [] ->
            %% rescue block! no signatures we can use
            %% so use a random consensus member
            Ledger = blockchain:ledger(Chain),
            {ok, Members0} = blockchain_ledger_v1:consensus_members(Ledger),
            Members = Members0 -- [blockchain_swarm:pubkey_bin()] -- Rejections,
            Index = rand:uniform(length(Members)),
            Member = lists:nth(Index, Members),
            {ok, Member};
        _ ->
            Index = rand:uniform(length(Signatories)),
            Signer= lists:nth(Index, Signatories),
            {ok, Signer}
    end.

retry(Txn, State=#state{txn_map=TxnMap, chain=Chain}) ->
    case maps:get(Txn, TxnMap, undefined) of
        undefined ->
            %% We no longer have this txn, do nothing
            State;
        {Callback, Rejections, _Dialer} ->
            %% Try a new one
            {ok, NewRandMember} = signatory_rand_member(Rejections, Chain),
            {ok, NewDialer} = blockchain_txn_mgr_sup:start_dialer([self(), Txn, NewRandMember]),
            ok = blockchain_txn_dialer:dial(NewDialer),
            NewTxnMap = maps:put(Txn, {Callback, Rejections, NewDialer}, TxnMap),
            State#state{txn_map=NewTxnMap}
    end.

rejected(Txn, Member, State=#state{txn_map=TxnMap, chain=Chain}) ->
    case maps:get(Txn, TxnMap, undefined) of
        undefined ->
            %% We no longer have this txn, do nothing
            State;
        {Callback, Rejections, Dialer} ->
            Ledger = blockchain:ledger(Chain),
            {ok, Members} = blockchain_ledger_v1:consensus_members(Ledger),
            N = length(Members),
            F = (N - 1) div 3,
            case length(lists:usort([Member|Rejections])) > F + 1 of
                true ->
                    %% too many rejections
                    State#state{txn_map=maps:remove(Txn, TxnMap)};
                false ->
                    retry(Txn, State#state{txn_map=maps:put(Txn, {Callback, [Member|Rejections], Dialer}, TxnMap)})
            end
    end.
