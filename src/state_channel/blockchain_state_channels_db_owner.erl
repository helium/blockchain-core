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
         db/0,
         sc_servers_cf/0,
         sc_clients_cf/0
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

-record(state, {
          db :: rocksdb:db_handle(),
          default :: rocksdb:cf_handle(),
          sc_servers_cf :: rocksdb:cf_handle(),
          sc_clients_cf :: rocksdb:cf_handle()
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

%% gen_server callbacks
init(Args) ->
    lager:info("~p init with ~p", [?MODULE, Args]),
    erlang:process_flag(trap_exit, true),
    BaseDir = maps:get(base_dir, Args),
    CFs = maps:get(cfs, Args, ["default", "sc_servers_cf", "sc_clients_cf"]),
    case open_db(BaseDir, CFs) of
        {error, _Reason} ->
            %% Go into a tight loop
            init(Args);
        {ok, DB, [DefaultCF, SCServersCF, SCClientsCF]} ->
            State = #state{
                       db=DB,
                       default=DefaultCF,
                       sc_servers_cf=SCServersCF,
                       sc_clients_cf=SCClientsCF
                      },
            {ok, State}
    end.

handle_call(db, _From, #state{db=DB}=State) ->
    {reply, DB, State};
handle_call(sc_clients_cf, _From, #state{sc_clients_cf=CF}=State) ->
    {reply, CF, State};
handle_call(sc_servers_cf, _From, #state{sc_servers_cf=CF}=State) ->
    {reply, CF, State};
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
-spec open_db(Dir::file:filename_all(),
              CFNames::[string()]) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} |
                                      {error, any()}.
open_db(Dir, CFNames) ->
    ok = filelib:ensure_dir(Dir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}, {atomic_flush, true}] ++ GlobalOpts,
    ExistingCFs =
        case rocksdb:list_column_families(Dir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

    CFOpts = GlobalOpts,
    case rocksdb:open_with_cf(Dir, DBOptions,  [{CF, CFOpts} || CF <- ExistingCFs]) of
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
