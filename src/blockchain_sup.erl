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

    blockchain_utils:init_var_cache(),

    %% allow the parent app to change this if it needs to.
    MetadataFun = application:get_env(blockchain, metadata_fun,
                                      fun blockchain_worker:signed_metadata_fun/0),
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
           {peer_cache_timeout, application:get_env(blockchain, peer_cache_timeout, 10 * 1000)}
          ]}
        ] ++ GroupMgrArgs,

    BWorkerOpts = [
        {port, proplists:get_value(port, Args, 0)},
        {base_dir, BaseDir},
        {update_dir, proplists:get_value(update_dir, Args, undefined)}
    ],

    BEventOpts = [],
    %% create the txn manager ets table under this supervisor and set ourselves as the heir
    %% we call `ets:give_away' every time we start_link the txn manager
    BTxnManagerOpts = #{ets => blockchain_txn_mgr:make_ets_table()},
    BTxnMgrSupOpts = [],
    StateChannelSupOpts = [BaseDir],
    ChildSpecs =
        [
         ?WORKER(blockchain_lock, []),
         ?WORKER(blockchain_swarm, [SwarmWorkerOpts]),
         ?WORKER(?EVT_MGR, blockchain_event, [BEventOpts])]
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
