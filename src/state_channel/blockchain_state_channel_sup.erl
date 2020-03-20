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

-define(DB_FILE, "state_channels.db").

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
        ?STATE_CHANNEL_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_state_channel_handler]}
    ),
    {ok, DB} = open_db(BaseDir),
    ServerOpts = #{swarm => Swarm, db => DB},
    ClientOpts = #{swarm => Swarm, db => DB},
    ChildSpecs = [
        ?WORKER(blockchain_state_channels_server, [ServerOpts]),
        ?WORKER(blockchain_state_channels_client, [ClientOpts])
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    rocksdb:open(DBDir, DBOptions).
