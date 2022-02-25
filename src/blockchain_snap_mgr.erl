%% @doc This module handles maintaining metadata state for snapshots -
%% currently available snapshots, what the local system has (or doesn't
%% have), etc.
%%
%% This is implemented as a gen_server so that we can periodically
%% poll upstream for new snapshot data.

-module(blockchain_snap_mgr).
-behavior(gen_server).

-record(state, {
          snapshot_timer :: reference()
         }).

%% API
-export([
         start_link/0,
         latest_snap_data/0,
         check_new_snap/0,
         have_latest_snap_locally/0
        ]).

%% gen server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2
        ]).

start_link() ->
    PollingIntervals = application:get_env(blockchain, snapshot_polling_interval, 60*1000),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [PollingIntervals], []).

%% @doc Return the latest snapshot height and internal hash; metadata about the snapshot
%% file hash and size shouldn't be needed outside of this gen_server.
latest_snap_data() ->
    gen_server:call(?MODULE, current_state, 5000).

%% @doc Force the server to check upstream for new snapshot information, even if the
%% timer isn't due to fire.
check_new_snap() ->
    gen_server:cast(?MODULE, check_new_snap, 5000).

%% @doc Returns true if the system has a snapshot file that matches the latest
%% snapshot metadata, otherwise false.
have_latest_snap_locally() ->
    gen_server:call(?MODULE, have_latest, 5000).


init([PollingInterval]) ->
    TRef = schedule_snapshot_timer(PollingInterval),
    {ok, #state{ snapshot_timer = TRef }}.

handle_cast(Cast, State) ->
    lager:error("Unexpected cast ~p", [Cast]),
    {noreply, State}.

handle_call(Call, From, State) ->
    lager:error("Unexpected call ~p from ~p", [Call, From]),
    {reply, diediediedie, State}.

handle_info(Info, State) ->
    lager:error("Unexpected info ~p", [Info]),
    {noreply, State}.

schedule_snapshot_timer(Interval) ->
    erlang:send_after(Interval, self(), snapshot_tick).
