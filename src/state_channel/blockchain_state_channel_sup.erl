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
    Swarm = blockchain_swarm:swarm(),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?STATE_CHANNEL_PROTOCOL_V1,
        {libp2p_framed_stream, server, [blockchain_state_channel_handler]}
    ),
    ServerOpts = #{swarm => Swarm},
    ClientOpts = #{swarm => Swarm},
    DbOwnerOpts = #{base_dir => BaseDir,
                    cfs => ["default",
                            "sc_servers_cf",
                            "sc_client_banners_cf",
                            "sc_client_purchases_cf"]
                   },
    ChildSpecs = [
        ?WORKER(blockchain_state_channels_db_owner, [DbOwnerOpts]),
        ?WORKER(blockchain_state_channels_server, [ServerOpts]),
        ?WORKER(blockchain_state_channels_client, [ClientOpts])
    ],
    {ok, {?FLAGS, ChildSpecs}}.
