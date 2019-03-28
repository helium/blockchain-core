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
    id => I,
    start => {I, start_link, Args},
    restart => permanent,
    shutdown => 5000,
    type => supervisor,
    modules => [I]
}).

-define(WORKER(I, Args), #{
    id => I,
    start => {I, start_link, Args},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [I]
}).

-define(WORKER(I, Mod, Args), #{
    id => I,
    start => {Mod, start_link, Args},
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
init(Args) ->
    application:ensure_all_started(ranch),
    application:ensure_all_started(lager),
    application:ensure_all_started(clique),
    ok = blockchain_cli_registry:register_cli(),
    lager:info("~p init with ~p", [?MODULE, Args]),
    SwarmWorkerOpts = [
        {key, proplists:get_value(key, Args)},
        {base_dir, proplists:get_value(base_dir, Args, "data")},
        {libp2p_group_gossip, [
            {stream_client, {?GOSSIP_PROTOCOL, {blockchain_gossip_handler, []}}},
            {seed_nodes, proplists:get_value(seed_nodes, Args, [])},
            {inbound_connections, proplists:get_value(max_inbound_connections, Args, 10)}
        ]}
    ],
    BWorkerOpts = [
        {port, proplists:get_value(port, Args, 0)},
        {num_consensus_members, proplists:get_value(num_consensus_members, Args, 0)},
        {base_dir, proplists:get_value(base_dir, Args, "data")},
        {update_dir, proplists:get_value(update_dir, Args, undefined)}
    ],
    BEventOpts = [],
    BTxnManagerOpts = [],
    ChildSpecs = [
        ?WORKER(blockchain_lock, []),
        ?WORKER(blockchain_swarm, [SwarmWorkerOpts]),
        ?WORKER(?EVT_MGR, blockchain_event, [BEventOpts]),
        ?WORKER(blockchain_worker, [BWorkerOpts]),
        ?WORKER(blockchain_txn_manager, [BTxnManagerOpts])
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
