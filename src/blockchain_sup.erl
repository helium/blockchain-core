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

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API functions
%% ------------------------------------------------------------------

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ------------------------------------------------------------------
%% Supervisor callbacks
%% ------------------------------------------------------------------
init([PublicKey, SigFun, SeedNodes]=_Args) ->
    application:ensure_all_started(ranch),
    application:ensure_all_started(lager),

    lager:info("~p init with ~p", [?MODULE, _Args]),
    SwarmWorkerOpts = [
        {key, {PublicKey, SigFun}}
        ,{libp2p_group_gossip, [
            {stream_client, {?GOSSIP_PROTOCOL, {blockchain_gossip_handler, []}}}
            ,{seed_nodes, SeedNodes}
        ]}
    ],

    ChildSpecs = [
        ?WORKER(blockchain_swarm, [SwarmWorkerOpts])
        ,?WORKER(blockchain_worker, [])
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
