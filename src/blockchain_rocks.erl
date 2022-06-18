-module(blockchain_rocks).

-export([
    %% TODO fold
    foreach/2,
    stream/2,
    stream/3,
    sample/3,
    sample/4
]).

-type stream() :: data_stream:t({K :: binary(), V :: binary()}).

%% API ========================================================================

-spec foreach(rocksdb:db_handle(), fun((K :: binary(), V :: binary()) -> ok)) ->
    ok.
foreach(DB, F) ->
    case rocksdb:iterator(DB, []) of
        {error, Reason} ->
            error({blockchain_rocks_iter_make, Reason});
        {ok, Iter} ->
            Move =
                fun Move_ (Target) ->
                    case rocksdb:iterator_move(Iter, Target) of
                        {ok, K, V} ->
                            F(K, V),
                            Move_(next);
                        {error, invalid_iterator} ->
                            ok = rocksdb:iterator_close(Iter);
                        Error ->
                            error({blockchain_rocks_iter_move, Target, Error})
                    end
                end,
            Move(first)
    end.

-spec stream(rocksdb:db_handle(), rocksdb:read_options()) ->
    stream().
stream(DB, Opts) ->
    stream_(fun () -> rocksdb:iterator(DB, Opts) end).

-spec stream(rocksdb:db_handle(), rocksdb:cf_handle(), rocksdb:read_options()) ->
    stream().
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
    stream().
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
