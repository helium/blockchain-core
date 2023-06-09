-module(blockchain_rocks).

-export([
    fold/4,
    fold/5,
    foreach/3,
    foreach/4,
    stream/2,
    stream/3,
    sample/3,
    sample/4
]).

%% API ========================================================================

-spec fold(
    rocksdb:db_handle(),
    rocksdb:read_options(),
    Acc,
    fun(({K :: binary(), V :: binary()}, Acc) -> Acc)
) ->
    Acc.
fold(DB, Opts, Acc, F) ->
    data_stream:fold(stream(DB, Opts), Acc, F).

-spec fold(
    rocksdb:db_handle(),
    rocksdb:cf_handle(),
    rocksdb:read_options(),
    Acc,
    fun(({K :: binary(), V :: binary()}, Acc) -> Acc)
) ->
    Acc.
fold(DB, CF, Opts, Acc, F) ->
    data_stream:fold(stream(DB, CF, Opts), Acc, F).

-spec foreach(
    rocksdb:db_handle(),
    rocksdb:read_options(),
    fun(({K :: binary(), V :: binary()}) -> ok)
) ->
    ok.
foreach(DB, Opts, F) ->
    data_stream:foreach(stream(DB, Opts), F).

-spec foreach(
    rocksdb:db_handle(),
    rocksdb:cf_handle(),
    rocksdb:read_options(),
    fun(({K :: binary(), V :: binary()}) -> ok)
) ->
    ok.
foreach(DB, CF, Opts, F) ->
    data_stream:foreach(stream(DB, CF, Opts), F).

-spec stream(rocksdb:db_handle(), rocksdb:read_options()) ->
    data_stream:t({K :: binary(), V :: binary()}).
stream(DB, Opts) ->
    stream_(fun () -> rocksdb:iterator(DB, Opts) end).

-spec stream(
    rocksdb:db_handle(),
    rocksdb:cf_handle(),
    rocksdb:read_options()
) ->
    data_stream:t({K :: binary(), V :: binary()}).
stream(DB, CF, Opts) ->
    stream_(fun () -> rocksdb:iterator(DB, CF, Opts) end).

%% @doc Select K random records from database.
-spec sample(rocksdb:db_handle(), rocksdb:read_options(), pos_integer()) ->
    [{K :: binary(), V :: binary()}].
sample(DB, Opts, K) ->
    Stream = stream(DB, Opts),
    data_stream:sample(Stream, K).

%% @doc Select K random records from CF.
-spec sample(
    rocksdb:db_handle(),
    rocksdb:cf_handle(),
    rocksdb:read_options(),
    pos_integer()
) ->
    [{K :: binary(), V :: binary()}].
sample(DB, CF, Opts, K) ->
    Stream = stream(DB, CF, Opts),
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
