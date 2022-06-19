-module(blockchain_rocks).

-export([
    fold/3,
    fold/4,
    foreach/2,
    stream/1,
    stream/2,
    sample/2,
    sample/3
]).

%% API ========================================================================

-spec fold(rocksdb:db_handle(), Acc, fun(({K :: binary(), V :: binary()}, Acc) -> Acc)) ->
    Acc.
fold(DB, Acc, F) ->
    data_stream:fold(stream(DB), Acc, F).

-spec fold(
    rocksdb:db_handle(),
    rocksdb:cf_handle(),
    Acc,
    fun(({K :: binary(), V :: binary()}, Acc) -> Acc)
) ->
    Acc.
fold(DB, CF, Acc, F) ->
    data_stream:fold(stream(DB, CF), Acc, F).

-spec foreach(rocksdb:db_handle(), fun(({K :: binary(), V :: binary()}) -> ok)) ->
    ok.
foreach(DB, F) ->
    data_stream:foreach(stream(DB), F).

-spec stream(rocksdb:db_handle()) ->
    data_stream:t({K :: binary(), V :: binary()}).
stream(DB) ->
    Opts = [], % rocksdb:read_options()
    stream_(fun () -> rocksdb:iterator(DB, Opts) end).

-spec stream(rocksdb:db_handle(), rocksdb:cf_handle()) ->
    data_stream:t({K :: binary(), V :: binary()}).
stream(DB, CF) ->
    Opts = [], % rocksdb:read_options()
    stream_(fun () -> rocksdb:iterator(DB, CF, Opts) end).

%% @doc Select K random records from database.
-spec sample(rocksdb:db_handle(), pos_integer()) ->
    [{K :: binary(), V :: binary()}].
sample(DB, K) ->
    Stream = stream(DB),
    data_stream:sample(Stream, K).

%% @doc Select K random records from CF.
-spec sample(rocksdb:db_handle(), rocksdb:cf_handle(), pos_integer()) ->
    [{K :: binary(), V :: binary()}].
sample(DB, CF, K) ->
    Stream = stream(DB, CF),
    data_stream:sample(Stream, K).

%% Internal ===================================================================

-spec stream_(fun(() -> {ok, rocksdb:itr_handle()} | {error, term()})) ->
    data_stream:t({K :: binary(), V :: binary()}).
stream_(IterOpen) ->
    case IterOpen() of
        {error, Reason} ->
            error({blockchain_rocks_iter_make, Reason});
        {ok, Iter} ->
            Move =
                fun Move_ (Target) ->
                    fun () ->
                        case rocksdb:iterator_move(Iter, Target) of
                            {ok, K, V} ->
                                {some, {{K, V}, Move_(next)}};
                            {error, invalid_iterator} ->
                                ok = rocksdb:iterator_close(Iter),
                                none;
                            Error ->
                                error({blockchain_rocks_iter_move, Target, Error})
                        end
                    end
                end,
            data_stream:from_fun(Move(first))
    end.

%% Test =======================================================================
%% See test/blockchain_rocks_SUITE.erl
