%%%-------------------------------------------------------------------
%% @doc
%% == State channels db owner and related functions ==
%%
%% * This process is started first in the state_channels supervision tree
%% * SC client/server will get the db reference from here when they init
%% * This process also traps exits and closes rocksdb (if need be)
%% * This process is responsible for serializing SC updates to disk in a
%%   batch write each write interval (currently 1000 millis)
%%
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_db_owner).

-behavior(gen_server).

%% api exports
-export([start_link/1,
         db/0,
         sc_servers_cf/0,
         sc_clients_cf/0,
         write/2,
         gc/1
        ]).

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
-define(TICK, '__sc_write_tick').

-record(state, {
          db :: rocksdb:db_handle(),
          default :: rocksdb:cf_handle(),
          sc_servers_cf :: rocksdb:cf_handle(),
          sc_clients_cf :: rocksdb:cf_handle(),
          write_interval = 1000 :: pos_integer(),
          tref :: reference(),
          pending = #{} :: maps:map()
         }).

%% api functions
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec db() -> rocksdb:db_handle().
db() ->
    gen_server:call(?MODULE, db).

-spec sc_servers_cf() -> rocksdb:cf_handle().
sc_servers_cf() ->
    gen_server:call(?MODULE, sc_servers_cf).

-spec sc_clients_cf() -> rocksdb:cf_handle().
sc_clients_cf() ->
    gen_server:call(?MODULE, sc_clients_cf).

-spec write( SC :: blockchain_state_channel_v1:state_channel(),
             Skewed :: skewed:skewed()) -> ok.
write(SC, Skewed) ->
    gen_server:cast(?MODULE, {write, SC, Skewed}).

-spec gc( [ blockchain_state_channel_v1:id() ] ) -> ok.
gc(IDs) ->
    gen_server:call(?MODULE, {gc, IDs}, infinity).

%% gen_server callbacks
init(Args) ->
    lager:info("~p init with ~p", [?MODULE, Args]),
    erlang:process_flag(trap_exit, true),
    BaseDir = maps:get(base_dir, Args),
    CFs = maps:get(cfs, Args, ["default", "sc_servers_cf", "sc_clients_cf"]),
    {ok, DB, [DefaultCF, SCServersCF, SCClientsCF]} = open_db(BaseDir, CFs),
    WriteInterval = get_env(state_channel_write_interval, 1000),
    Tref = schedule_next_tick(WriteInterval),
    {ok, #state{db=DB, default=DefaultCF, sc_servers_cf=SCServersCF,
                sc_clients_cf=SCClientsCF, tref=Tref,
                write_interval=WriteInterval}}.

handle_call(db, _From, #state{db=DB}=State) ->
    {reply, DB, State};
handle_call(sc_clients_cf, _From, #state{sc_clients_cf=CF}=State) ->
    {reply, CF, State};
handle_call(sc_servers_cf, _From, #state{sc_servers_cf=CF}=State) ->
    {reply, CF, State};
handle_call({gc, IDs}, _From, #state{pending=P, db=DB}=State)->
    {ok, Batch} = rocksdb:batch(),
    ok = lists:foreach(fun(SCID) ->
                      ok = rocksdb:batch_delete(Batch, SCID)
              end, IDs),
    ok = rocksdb:write_batch(DB, Batch, []),
    ok = rocksdb:release_batch(Batch),
    {reply, ok, State#state{pending=maps:without(IDs, P)}};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({write, SC, Skewed}, #state{pending=P}=State) ->
    SCID = blockchain_state_channel_v1:id(SC),
    %% defer encoding until write time
    NewP = maps:put(SCID, {SC, Skewed}, P),
    {noreply, State#state{pending=NewP}};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({'EXIT', _From, _Reason} , #state{db=DB}=State) ->
    lager:info("EXIT because: ~p, closing rocks: ~p", [_Reason, DB]),
    ok = rocksdb:close(DB),
    {stop, db_owner_exit, State};
handle_info(?TICK, #state{pending=P, write_interval=W}=State) when map_size(P) == 0 ->
    Tref = schedule_next_tick(W),
    {noreply, State#state{tref=Tref}};
handle_info(?TICK, #state{pending=P, db=DB,
                          write_interval=W}=State) ->
    lager:info("~p pending writes this tick", [map_size(P)]),
    ok = handle_batch_write(DB, P),
    Tref = schedule_next_tick(W),
    {noreply, State#state{tref=Tref, pending=#{}}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{db=DB,
                          pending=P}) when map_size(P) == 0 ->
    ok = rocksdb:close(DB),
    ok;
terminate(_Reason, #state{db=DB,
                          pending=P}) ->
    ok = handle_batch_write(DB, P),
    ok = rocksdb:close(DB),
    ok.

%% Helper functions
-spec open_db(Dir::file:filename_all(),
              CFNames::[string()]) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} |
                                      {error, any()}.
open_db(Dir, CFNames) ->
    ok = filelib:ensure_dir(Dir),
    DBDir = filename:join(Dir, ?DB_FILE),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}, {atomic_flush, true}] ++ GlobalOpts,
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

    CFOpts = GlobalOpts,
    case rocksdb:open_with_cf(DBDir, DBOptions,  [{CF, CFOpts} || CF <- ExistingCFs]) of
        {error, _Reason}=Error ->
            Error;
        {ok, DB, OpenedCFs} ->
            L1 = lists:zip(ExistingCFs, OpenedCFs),
            L2 = lists:map(
                fun(CF) ->
                    {ok, CF1} = rocksdb:create_column_family(DB, CF, CFOpts),
                    {CF, CF1}
                end,
                CFNames -- ExistingCFs
            ),
            L3 = L1 ++ L2,
            {ok, DB, [proplists:get_value(X, L3) || X <- CFNames]}
    end.

schedule_next_tick(Interval) ->
    erlang:send_after(Interval, self(), ?TICK).

handle_batch_write(DB, P) ->
    {ok, Batch} = rocksdb:batch(),
    ok = maps:fold(fun(SCID, {SC, Skewed}, Acc) ->
                      Bin = term_to_binary({blockchain_state_channel_v1:encode(SC),
                                            Skewed}),
                      ok = rocksdb:batch_put(Batch, SCID, Bin),
                      Acc
              end, ok, P),
    Res = rocksdb:write_batch(DB, Batch, []),
    ok = rocksdb:release_batch(Batch),
    Res.

get_env(Key, Default) ->
    case application:get_env(blockchain, Key, Default) of
        {ok, X} -> X;
        Default -> Default
    end.
