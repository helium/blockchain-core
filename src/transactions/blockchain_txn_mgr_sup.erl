%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Txn Mgr Sup ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_mgr_sup).

-behaviour(supervisor).

%% ------------------------------------------------------------------
%% API
%% ------------------------------------------------------------------
-export([start_link/1, start_workers/1, terminate_worker/1]).
-export([init/1]).

%% ------------------------------------------------------------------
%% API functions
%% ------------------------------------------------------------------
start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ------------------------------------------------------------------
%% Supervisor callbacks
%% ------------------------------------------------------------------
init(_Args) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [#{id => blockchain_txn_dialer,
                    start => {blockchain_txn_dialer, start_link, []},
                    type => worker,
                    restart => temporary,
                    shutdown => brutal_kill}],
    {ok, {SupFlags, ChildSpecs}}.

start_workers([Parent, Txn, ConsensusMembers]) ->
    lager:info("Parent: ~p, txn: ~p, ConsensusMembers: ~p", [Parent, Txn, ConsensusMembers]),
    lists:foldl(fun(Member, Acc) ->
                        {ok, Pid} = supervisor:start_child(?MODULE, [[Parent, Txn, Member]]),
                        Pid ! dial,
                        [Pid | Acc]
                end,
                [],
                ConsensusMembers).

terminate_worker(Pid) ->
    supervisor:terminate(?MODULE, Pid).
