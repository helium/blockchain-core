%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits State Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    burn/2
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
-include("pb/blockchain_data_credits_handler_pb.hrl").

-define(SERVER, ?MODULE).
-define(DB_FILE, "data_credits.db").

-record(state, {
    dir :: file:filename_all(),
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    server :: rocksdb:cf_handle(),
    client :: rocksdb:cf_handle(),
    total = 0 :: non_neg_integer(),
    swarm :: undefined | pid()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

burn(Keys, Amount) ->
    gen_statem:cast(?SERVER, {burn, Keys, Amount}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Dir]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, DB, [DefaultCF, ServerCF, ClientCF]} = open_db(Dir),
    Swarm = blockchain_swarm:swarm(),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?DATA_CREDITS_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_data_credits_handler]}
    ),
    {ok, #state{
        dir=Dir,
        db=DB,
        default=DefaultCF,
        server=ServerCF,
        client=ClientCF,
        swarm=Swarm
    }}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({burn, #{public := PubKey}=Keys, _Amount}, #state{db=DB, default=DefaultCF}=State) ->
    KeysBin = libp2p_crypto:keys_to_bin(Keys),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    % TODO: Replace this by real channel
    _Pid = erlang:spawn_link(fun() -> receive _ -> ok end end),
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, DefaultCF, PubKeyBin, KeysBin),
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
    {noreply, State};
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
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    DBOptions = [{create_if_missing, true}],
    DefaultCFs = ["default", "server", "client"],
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

    {ok, DB, OpenedCFs} = rocksdb:open_with_cf(DBDir, DBOptions,  [{CF, []} || CF <- ExistingCFs]),

    L1 = lists:zip(ExistingCFs, OpenedCFs),
    L2 = lists:map(
        fun(CF) ->
            {ok, CF1} = rocksdb:create_column_family(DB, CF, []),
            {CF, CF1}
        end,
        DefaultCFs -- ExistingCFs
    ),
    L3 = L1 ++ L2,
    {ok, DB, [proplists:get_value(X, L3) || X <- DefaultCFs]}.
