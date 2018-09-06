%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Core Sup ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SUP(I, Args), #{
    id => I
    ,start => {I, start_link, Args}
    ,restart => permanent
    ,shutdown => 5000
    ,type => supervisor
    ,modules => [I]
}).
-define(WORKER(I, Args), #{
    id => I
    ,start => {I, start_link, Args}
    ,restart => permanent
    ,shutdown => 5000
    ,type => worker
    ,modules => [I]
}).
-define(FLAGS, #{
    strategy => rest_for_one
    ,intensity => 1
    ,period => 5
}).

%% ------------------------------------------------------------------
%% API functions
%% ------------------------------------------------------------------

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ------------------------------------------------------------------
%% Supervisor callbacks
%% ------------------------------------------------------------------
init([PublicKey, SigFun]=_Args) ->
    application:ensure_all_started(ranch),
    application:ensure_all_started(lager),

    lager:info("~p init with ~p", [?MODULE, _Args]),
    % TODO: Add group gossip
    SwarmWorkerOpts = [{key, {PublicKey, SigFun}}],
    SwarmWorker = ?WORKER(blockchain_swarm, [SwarmWorkerOpts]),

    BlockchainWorker = ?WORKER(blockchain_worker, []),

    ChildSpecs = [
        SwarmWorker
        ,BlockchainWorker
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
