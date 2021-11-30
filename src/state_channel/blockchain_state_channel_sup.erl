%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Sup ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_sup).

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
    ok = blockchain_state_channels_cache:init(),
    Swarm = blockchain_swarm:swarm(),
    ServerOpts = #{swarm => Swarm},
    ClientOpts = #{swarm => Swarm},
    DbOwnerOpts = #{
        base_dir => BaseDir,
        cfs => [
            "default",
            "sc_servers_cf",
            "sc_clients_cf"
        ]
    },
    ChildSpecs =
        case application:get_env(blockchain, sc_sup_type, undefined) of
            testing ->
                lager:info("starting as testing mode"),
                [
                    ?WORKER(blockchain_state_channels_db_owner, [DbOwnerOpts]),
                    ?WORKER(blockchain_state_channels_server, [ServerOpts]),
                    ?WORKER(blockchain_state_channels_client, [ClientOpts])
                ];
            server ->
                lager:info("starting as server mode"),
                [
                    ?WORKER(blockchain_state_channels_db_owner, [DbOwnerOpts]),
                    ?WORKER(blockchain_state_channels_server, [ServerOpts])
                ];
            _ ->
                lager:info("starting as client mode"),
                [
                    ?WORKER(blockchain_state_channels_db_owner, [DbOwnerOpts]),
                    ?WORKER(blockchain_state_channels_client, [ClientOpts])
                ]
        end,
    {ok, {?FLAGS, ChildSpecs}}.
