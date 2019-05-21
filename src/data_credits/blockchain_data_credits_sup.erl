%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Sup ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(WORKER(I, Args), #{
    id => I,
    start => {I, start_link, Args},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [I]
}).
-define(FLAGS, #{
    strategy => rest_for_one,
    intensity => 1,
    period => 5
}).
-define(DB_FILE, "data_credits.db").

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API functions
%% ------------------------------------------------------------------

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ------------------------------------------------------------------
%% Supervisor callbacks
%% ------------------------------------------------------------------
init([BaseDir]) ->
    DBOpts = [BaseDir],
    ServersOpts = [],
    ClientsOpts = [],
    ChildSpecs = [
        ?WORKER(blockchain_data_credits_db, [DBOpts]),
        ?WORKER(blockchain_data_credits_servers_monitor, [ServersOpts]),
        ?WORKER(blockchain_data_credits_clients_monitor, [ClientsOpts])
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

