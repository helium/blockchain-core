%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Sup ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_sup).

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
    strategy => one_for_one,
    intensity => 1,
    period => 5
}).
-define(DB_FILE, "data_credits.db").

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
    {ok, DB} = open_db(BaseDir),
    ServersOpts = [DB],
    ClientsOpts = [DB],
    ChildSpecs = [
        ?WORKER(blockchain_data_credits_servers_monitor, [ServersOpts]),
        ?WORKER(blockchain_data_credits_clients_monitor, [ClientsOpts])
    ],
    {ok, {?FLAGS, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle()} | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    DBOptions = [{create_if_missing, true}],
    rocksdb:open(DBDir, DBOptions).


