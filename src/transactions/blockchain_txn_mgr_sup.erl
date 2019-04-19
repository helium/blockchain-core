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
-export([start_link/1,
         start_dialer/1,
         stop_dialer/1]).

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

start_dialer([Parent, Txn, ConsensusMember]) ->
    supervisor:start_child(?MODULE, [[Parent, Txn, ConsensusMember]]).

stop_dialer(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).
