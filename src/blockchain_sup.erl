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
    SwarmWorkerOpts =
        [
         {key, proplists:get_value(key, Args)},
         {base_dir, proplists:get_value(base_dir, Args, "data")},
         {libp2p_proxy,
          [{limit, application:get_env(blockchain, relay_limit, 25)}]},
         {libp2p_peerbook,
          [{signed_metadata_fun, fun blockchain_worker:signed_metadata_fun/0}]},
         {libp2p_group_gossip,
          [
           {stream_client, {?GOSSIP_PROTOCOL, {blockchain_gossip_handler, []}}},
           {seed_nodes, proplists:get_value(seed_nodes, Args, [])},
           %% in should be ~2/3 out, otherwise nodes with good
           %% connections will hog all the gossip
           {peerbook_connections, proplists:get_value(outbound_gossip_connections, Args, 10)},
           {inbound_connections, proplists:get_value(max_inbound_connections, Args, 6)},
           {peer_cache_timeout, proplists:get_value(peer_cache_timeout, Args, 10 * 1000)}
          ]}
        ],
    BWorkerOpts = [
        {port, proplists:get_value(port, Args, 0)},
        {base_dir, proplists:get_value(base_dir, Args, "data")},
        {update_dir, proplists:get_value(update_dir, Args, undefined)}
    ],
    BEventOpts = [],
    BTxnManagerOpts = [],
    BTxnMgrSupOpts = [],
    ChildSpecs = [
        ?WORKER(blockchain_lock, []),
        ?WORKER(blockchain_swarm, [SwarmWorkerOpts]),
        ?WORKER(?EVT_MGR, blockchain_event, [BEventOpts]),
        ?WORKER(blockchain_worker, [BWorkerOpts]),
        ?WORKER(blockchain_txn_mgr, [BTxnManagerOpts]),
        ?SUP(blockchain_txn_mgr_sup, [BTxnMgrSupOpts])
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
