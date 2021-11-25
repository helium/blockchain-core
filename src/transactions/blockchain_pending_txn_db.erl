-module(blockchain_pending_txn_db).

-export([open_db/2, open_db/3, clean_db/1]).
-export([get_state/1]).
-export([get_follower_height/2, put_follower_height/3, batch_put_follower_height/3]).

-spec open_db(Dir :: file:filename_all(), CFNames :: [string()]) ->
    {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir, CFNames) ->
    open_db(Dir, CFNames, []).
-spec open_db(Dir :: file:filename_all(), CFNames :: [string()], AdditionalCFOpts :: [term()]) ->
    {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir, CFNames, AdditionalCFOpts) ->
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

    CFOpts = GlobalOpts ++ AdditionalCFOpts,
    case rocksdb:open_with_cf(Dir, DBOptions, [{CF, CFOpts} || CF <- ExistingCFs]) of
        {error, _Reason} = Error ->
            Error;
        {ok, DB, OpenedCFs} ->
            L1 = lists:zip(ExistingCFs, OpenedCFs),
            L2 = lists:map(
                fun (CF) ->
                    {ok, CF1} = rocksdb:create_column_family(DB, CF, CFOpts),
                    {CF, CF1}
                end,
                CFNames -- ExistingCFs
            ),
            L3 = L1 ++ L2,
            {ok, DB, [proplists:get_value(X, L3) || X <- CFNames]}
    end.

clean_db(Dir) when is_list(Dir) ->
    ok = rocksdb:destroy(Dir, []).

-spec get_state(Module :: atom()) -> {ok, any()} | {error, term()}.
get_state(Module) ->
    case persistent_term:get(Module, false) of
        false ->
            {error, {no_database, Module}};
        State ->
            {ok, State}
    end.

-define(HEIGHT_KEY, <<"height">>).

-spec get_follower_height(rocksdb:db_handle(), rocksdb:cf_handle()) ->
    {ok, non_neg_integer()} | {error, term()}.
get_follower_height(DB, CF) ->
    case rocksdb:get(DB, CF, ?HEIGHT_KEY, []) of
        {ok, <<Height:64/integer-unsigned-little>>} ->
            {ok, blockchain:snapshot_height(Height)};
        not_found ->
            {ok, blockchain:snapshot_height(0)};
        {error, _} = Error ->
            Error
    end.

-spec put_follower_height(rocksdb:db_handle(), rocksdb:cf_handle(), non_neg_integer()) ->
    ok | {error, term()}.
put_follower_height(DB, CF, BlockHeight) ->
    rocksdb:put(DB, CF, ?HEIGHT_KEY, <<BlockHeight:64/integer-unsigned-little>>, []).

-spec batch_put_follower_height(
    rocksdb:batch_handle(),
    rocksdb:cf_handle(),
    non_neg_integer()
) ->
    ok | {error, term()}.
batch_put_follower_height(Batch, CF, BlockHeight) ->
    rocksdb:batch_put(Batch, CF, ?HEIGHT_KEY, <<BlockHeight:64/integer-unsigned-little>>).