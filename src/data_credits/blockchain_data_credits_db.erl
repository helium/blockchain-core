%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits DB ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_db).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    get_db/0,
    get_cf/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("blockchain.hrl").

-define(SERVER, ?MODULE).
-define(DB_FILE, "data_credits.db").

-record(state, {
    db :: rocksdb:db_handle(),
    cfs :: #{string() => rocksdb:cf_handle()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

get_db() ->
    gen_statem:call(?SERVER, get_db).

get_cf(PubKeyBin) ->
    gen_statem:call(?SERVER, {get_cf, PubKeyBin}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Dir]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, DB, CFs} = open_db(Dir),
    {ok, #state{
        db=DB,
        cfs=CFs
    }}.

handle_call(get_db, _From, #state{db=DB}=State) ->
    {reply, {ok, DB}, State};
handle_call({get_cf, PubKeyBin}, _From, #state{db=DB, cfs=CFs0}=State) ->
    CFName = erlang:binary_to_list(PubKeyBin),
    case maps:get(CFName, CFs0, undefined) of
        undefined ->
            {ok, CF} = rocksdb:create_column_family(DB, CFName, []),
            CFs1 = maps:put(CFName, CF, CFs0),
            {reply, {ok, CF}, State#state{cfs=CFs1}};
        CF ->
            {reply, {ok, CF}, State}
    end;
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), #{string() => rocksdb:cf_handle()}}
                                      | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

    CFOpts = GlobalOpts,
    {ok, DB, OpenedCFs} = rocksdb:open_with_cf(DBDir, DBOptions, [{CF, CFOpts} || CF <- ExistingCFs]),
    {ok, DB, maps:from_list(lists:zip(ExistingCFs, OpenedCFs))}.

