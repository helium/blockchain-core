%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Sup ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_sup).

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
    ChildSpecs = [
        ?WORKER(blockchain_dcs_db, [DBOpts]),
        ?WORKER(blockchain_dcs_channel_server_monitor, [ServersOpts])
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
