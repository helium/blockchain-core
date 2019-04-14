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
    Workers = blockchain_txn_mgr_sup:start_workers([self(), Txn, ConsensusMembers]),
    NewTxnMap = maps:put(Txn, {Callback, Workers}, TxnMap),
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
    NewTxnMap = maps:map(fun(Txn, {Callback, Workers}) ->
                                 case Workers == [] of
                                     true ->
                                         {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
                                         {Callback, blockchain_txn_mgr_sup:start_workers([self(), Txn, ConsensusMembers])};
                                    _ ->
                                         {Callback, Workers}
                                 end
                         end,
                         TxnMap),
    {noreply, State#state{txn_map=NewTxnMap}};
handle_info({accepted, {Dialer, Txn, Member}}, State) ->
    lager:info("blockchain_txn_mgr, txn: ~p, accepted_by: ~p", [Txn, Member]),

    %% The dialer did its job, no need to keep this one alive anymore
    Dialer ! terminate,

    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(State#state.chain)),
    F = (length(ConsensusMembers) - 1) div 3,

    case maps:get(Txn, State#state.resp_map, undefined) of
        undefined ->
            {noreply, State};
        {Accepts, Rejects} ->
            case length(Accepts) + 1 > F of
                true ->
                    {Callback, Workers} = maps:get(Txn, State#state.txn_map),
                    lists:foreach(fun(Worker) ->
                                          blockchain_txn_mgr_sup:terminate_worker(Worker)
                                  end, Workers),
                    invoke_callback(Callback, ok),
                    NewTxnMap = maps:remove(Txn, State#state.txn_map),
                    {noreply, State#state{txn_map=NewTxnMap}};
                false ->
                    NewAccepts = [Member | Accepts],
                    NewRespMap = maps:put(Txn, {NewAccepts, Rejects}, State#state.resp_map),
                    {noreply, State#state{resp_map=NewRespMap}}
            end
    end;
handle_info({rejected, {_Dialer, Txn, Member}}, State) ->
    lager:info("blockchain_txn_mgr, txn: ~p, rejected_by: ~p", [Txn, Member]),
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(State#state.chain)),
    F = (length(ConsensusMembers) - 1) div 3,

    case maps:get(Txn, State#state.resp_map, undefined) of
        undefined ->
            {noreply, State};
        {Accepts, Rejects} ->
            case length(Rejects) + 1 > 2*F of
                true ->
                    {Callback, Workers} = maps:get(Txn, State#state.txn_map),
                    lists:foreach(fun(Worker) ->
                                          blockchain_txn_mgr_sup:terminate_worker(Worker)
                                  end, Workers),
                    invoke_callback(Callback, error),
                    NewTxnMap = maps:remove(Txn, State#state.txn_map),
                    {noreply, State#state{txn_map=NewTxnMap}};
                false ->
                    NewRejects = [Member | Rejects],
                    NewRespMap = maps:put(Txn, {Accepts, NewRejects}, State#state.resp_map),
                    {noreply, State#state{resp_map=NewRespMap}}
            end
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

invoke_callback(Callback, Msg) ->
    spawn(fun() -> Callback(Msg) end).
