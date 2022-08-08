%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Core Sup ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_sup).

-behaviour(supervisor).

%% API
-export([
         start_link/1,
         cream_caches_init/0, % maybe test-only?
         cream_caches_clear/0 % maybe test-only?
        ]).

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
    application:ensure_all_started(telemetry),
    application:ensure_all_started(clique),
    application:ensure_all_started(throttle),
    %% start http client and ssl here
    %% currently used for s3 snapshot download
    ssl:start(),
    inets:start(httpc, [{profile, blockchain}]),
    ok = blockchain_cli_registry:register_cli(),
    lager:info("~p init with ~p", [?MODULE, Args]),
    GroupMgrArgs =
        case proplists:get_value(group_delete_predicate, Args, false) of
            false ->
                [];
            Pred ->
                [{group_delete_predicate, Pred}]
        end,
    BaseDir = proplists:get_value(base_dir, Args, "data"),

    %% allow the parent app to change this if it needs to.
    MetadataFun = application:get_env(blockchain, metadata_fun,
                                      fun blockchain_worker:signed_metadata_fun/0),

    %% cream inits
    cream_caches_init(),

    SwarmWorkerOpts =
        [
         {key, proplists:get_value(key, Args)},
         {base_dir, BaseDir},
         {libp2p_nat, [{enabled, application:get_env(blockchain, enable_nat, true)}]},
         {libp2p_proxy,
          [{limit, application:get_env(blockchain, relay_limit, 25)}]},
         {libp2p_peerbook,
          [{signed_metadata_fun, MetadataFun},
           {notify_peer_gossip_limit, application:get_env(blockchain, gossip_width, 100)},
           {notify_time, application:get_env(blockchain, peerbook_update_interval, timer:minutes(5))},
           {force_network_id, application:get_env(blockchain, force_network_id, undefined)},
           {allow_rfc1918, application:get_env(blockchain, peerbook_allow_rfc1918, false)}
          ]},
         {libp2p_group_gossip,
          [
           {stream_client, {?GOSSIP_PROTOCOL_V1, {blockchain_gossip_handler, []}}},   %% NOTE: this config is not used anywhere in bc core or libp2p
           {seed_nodes, proplists:get_value(seed_nodes, Args, [])},
           %% in should be ~2/3 out, otherwise nodes with good
           %% connections will hog all the gossip
           {peerbook_connections, application:get_env(blockchain, outbound_gossip_connections, 2)},
           {inbound_connections, application:get_env(blockchain, max_inbound_connections, 6)},
           {seednode_connections, application:get_env(blockchain, seednode_connections, undefined)},
           {peer_cache_timeout, application:get_env(blockchain, peer_cache_timeout, 10 * 1000)}
          ]}
        ] ++ GroupMgrArgs,

    BWorkerOpts = [
        {port, proplists:get_value(port, Args, 0)},
        {base_dir, BaseDir},
        {update_dir, proplists:get_value(update_dir, Args, undefined)},
        {ets_cache, blockchain_worker:make_ets_table()}
    ],

    BEventOpts = [],
    %% create the txn manager ets table under this supervisor and set ourselves as the heir
    %% we call `ets:give_away' every time we start_link the txn manager
    BTxnManagerOpts = #{ets => blockchain_txn_mgr:make_ets_tables()},
    BTxnMgrSupOpts = [],
    StateChannelSupOpts = [BaseDir],
    ChildSpecs =
        [
         ?WORKER(blockchain_lock, []),
         ?WORKER(blockchain_swarm, [SwarmWorkerOpts]),
         ?WORKER(?EVT_MGR, blockchain_event, [BEventOpts]),
         ?WORKER(?POC_EVT_MGR, blockchain_poc_event, [BEventOpts])]
        ++
        [?WORKER(blockchain_score_cache, []),
         ?WORKER(blockchain_worker, [BWorkerOpts]),
         ?WORKER(blockchain_gc, []),
         ?WORKER(blockchain_txn_mgr, [BTxnManagerOpts]),
         ?SUP(blockchain_txn_mgr_sup, [BTxnMgrSupOpts]),
         ?SUP(blockchain_state_channel_sup, [StateChannelSupOpts])
        ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

cream_caches_init() ->
    MaxItemCapacity = 10000,
    MB = 1024 * 1024,
    {ok, VarCache}
        = cream:new(MaxItemCapacity,
                    [{initial_capacity, 1000},
                     {seconds_to_idle, 60 * 60}]),
    persistent_term:put(?var_cache, VarCache),

    RegionMem = application:get_env(blockchain, region_cache_limit_mb, 100) * MB,
    {ok, RegionCache}
        = cream:new(RegionMem,
                    [{bounding, memory}]),
    persistent_term:put(?region_cache, RegionCache),

    GwMem = application:get_env(blockchain, gateway_cache_limit_mb, 100) * MB,
    {ok, GwCache}
        = cream:new(GwMem,
                    [{bounding, memory}]),
    persistent_term:put(?gw_cache, GwCache),

    {ok, ScoreCache}
        = cream:new(MaxItemCapacity,
                    [{initial_capacity, 1000},
                     {seconds_to_idle, 12 * 60 * 60}]),
    persistent_term:put(?score_cache, ScoreCache),

    {ok, FPCache}
        = cream:new(5,
                    [{initial_capacity, 1},
                     {seconds_to_idle, 12 * 60 * 60}]),
    persistent_term:put(?fp_cache, FPCache),

    {ok, SCCache}
        = cream:new(1000,
                    [{initial_capacity, 50},
                     {seconds_to_idle, 60}]),
    persistent_term:put(?sc_server_cache, SCCache),

    {ok, RoutingCache}
        = cream:new(1000,
                    [{initial_capacity, 50},
                     {seconds_to_idle, 6 * 60 * 60}]),
    persistent_term:put(?routing_cache, RoutingCache),

    {ok, WitnessCache}
        = cream:new(50000,
                    [{initial_capacity, 10000},
                     {seconds_to_idle, 60 * 60}]),
    persistent_term:put(?witness_cache, WitnessCache).


cream_caches_clear() ->
    cream:drain(persistent_term:get(?var_cache)),
    cream:drain(persistent_term:get(?region_cache)),
    cream:drain(persistent_term:get(?score_cache)),
    cream:drain(persistent_term:get(?fp_cache)),
    cream:drain(persistent_term:get(?sc_server_cache)),
    cream:drain(persistent_term:get(?routing_cache)),
    cream:drain(persistent_term:get(?witness_cache)).
