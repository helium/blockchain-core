%%%-------------------------------------------------------------------
%% @doc
%% == State channels db owner and related functions ==
%%
%% * This process is started first in the state_channels supervision tree
%% * SC client/server will get the db reference from here when they init
%% * This process also traps exits and closes rocksdb (if need be)
%%
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_db_owner).

-behavior(gen_server).

%% api exports
-export([start_link/1,
         db/0]).

%% gen_server exports
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(DB_FILE, "state_channels.db").

-record(state, {
          db :: rocksdb:db_handle()
         }).

%% api functions
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec db() -> rocksdb:db_handle().
db() ->
    gen_server:call(?MODULE, db).

%% gen_server callbacks
init(Args) ->
    lager:info("~p init with ~p", [?MODULE, Args]),
    erlang:process_flag(trap_exit, true),
    BaseDir = maps:get(base_dir, Args),
    {ok, DB} = open_db(BaseDir),
    {ok, #state{db=DB}}.

handle_call(db, _From, #state{db=DB}=State) ->
    {reply, DB, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({'EXIT', _From, _Reason} , #state{db=DB}=State) ->
    lager:info("EXIT because: ~p, closing rocks: ~p", [_Reason, DB]),
    ok = rocksdb:close(DB),
    {stop, db_owner_exit, State};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{db=DB}) ->
    ok = rocksdb:close(DB),
    ok.

%% Helper functions
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    rocksdb:open(DBDir, DBOptions).
