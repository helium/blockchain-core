-module(blockchain_rocks_SUITE).

%% CT
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases
-export([
    t_foreach_sanity_check/1,
    t_sample_sanity_check/1,
    t_sample/1,
    t_sample_filtered/1,
    t_stream_mapped_and_filtered/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


%% CT =========================================================================

all() ->
    [
        t_foreach_sanity_check,
        t_sample_sanity_check,
        t_sample,
        t_sample_filtered,
        t_stream_mapped_and_filtered
    ].

init_per_suite(Cfg) ->
    DB = db_init(?MODULE, Cfg, 1000),
    [{db, DB} | Cfg].

end_per_suite(_) ->
    ok.

%% Test cases =================================================================

t_foreach_sanity_check(Cfg) ->
    DB = ?config(db, Cfg),
    blockchain_rocks:foreach(
        DB,
        fun(K, V) ->
            ?assertMatch({<<"k", I/binary>>, <<"v", I/binary>>}, {K, V})
        end
    ).

t_sample_sanity_check(Cfg) ->
    DB = ?config(db, Cfg),
    Sample = blockchain_rocks:sample(DB, [], 1),
    ?assertMatch([{<<"k", V/binary>>, <<"v", V/binary>>}], Sample),
    DBEmpty = db_init(?FUNCTION_NAME, Cfg, 0),
    ?assertEqual([], blockchain_rocks:sample(DBEmpty, [], 1)),
    ?assertEqual([], blockchain_rocks:sample(DBEmpty, [], 5)),
    ?assertEqual([], blockchain_rocks:sample(DBEmpty, [], 10)).

t_sample(Cfg) ->
    DB = ?config(db, Cfg),
    K = 1,
    Trials = 100,
    Samples =
        [blockchain_rocks:sample(DB, [], K) || _ <- lists:duplicate(Trials, {})],

    %% The samples are roughly what we expected, not something weird.
    %% Technically this is sufficient at this level of abstraction, as
    %% randomness is tested at the data_stream level.
    lists:foreach(
        fun (Sample) ->
            lists:foreach(
                fun (Record) ->
                    ?assertMatch({<<"k", V/binary>>, <<"v", V/binary>>}, Record)
                end,
                Sample
            )
        end,
        Samples
    ),

    % TODO Somekind of a distribution test. Then maybe move to stream tests.
    Counts = [C || {_, C} <- count(Samples)],
    ct:pal(">>> Counts: ~p", [Counts]),

    NumUniqueSamples = length(lists:usort(Samples)),
    ProportionOfUnique = NumUniqueSamples / Trials,

    %% At least 1/2 the time a new record-set was sampled:
    ?assert(ProportionOfUnique >= 0.5),

    %% But some were picked more than once:
    ?assert(ProportionOfUnique =< 1.0).

t_sample_filtered(Cfg) ->
    DB = ?config(db, Cfg),
    S0 = blockchain_rocks:stream(DB, []),
    S1 = data_stream:lazy_filter(S0, fun kv_is_even/1),
    lists:foreach(
        fun (KV) ->
            ?assertMatch({<<"k", IBin/binary>>, <<"v", IBin/binary>>}, KV),
            {<<"k", IBin/binary>>, <<"v", IBin/binary>>} = KV,
            ?assertEqual(0, binary_to_integer(IBin) rem 2)
        end,
        data_stream:sample(S1, 100)
    ).

t_stream_mapped_and_filtered(Cfg) ->
    DB = ?config(db, Cfg),
    S0 = blockchain_rocks:stream(DB, []),
    S1 = data_stream:lazy_map(S0, fun kv_to_int/1),
    S2 = data_stream:lazy_filter(S1, fun (I) -> I rem 2 =:= 0 end),
    data_stream:iter(fun (I) -> ?assert(I rem 2 =:= 0) end, S2).

%% Internal ===================================================================

-spec db_init(atom(), [{atom(), term()}], non_neg_integer()) ->
    rocksdb:db_handle().
db_init(TestCase, Cfg, NumRecords) ->
    PrivDir = ?config(priv_dir, Cfg),
    DBFile = atom_to_list(TestCase) ++ ".db",
    DBPath = filename:join(PrivDir, DBFile),
    {ok, DB} = rocksdb:open(DBPath, [{create_if_missing, true}]),
    lists:foreach(
        fun ({K, V}) -> ok = rocksdb:put(DB, K, V, []) end,
        lists:map(fun int_to_kv/1, lists:seq(1, NumRecords))
    ),
    %% Sanity check that all keys and values are formatted as expected:
    blockchain_rocks:foreach(
        DB,
        fun(K, V) ->
            ?assertMatch({<<"k", I/binary>>, <<"v", I/binary>>}, {K, V})
        end
    ),
    DB.

-spec int_to_kv(integer()) -> {binary(), binary()}.
int_to_kv(I) ->
    K = <<"k", (integer_to_binary(I))/binary>>,
    V = <<"v", (integer_to_binary(I))/binary>>,
    {K, V}.

-spec kv_to_int({binary(), binary()}) -> integer().
kv_to_int({<<"k", I/binary>>, <<"v", I/binary>>}) ->
    binary_to_integer(I).

-spec kv_is_even({binary(), binary()}) -> boolean().
kv_is_even(KV) ->
    kv_to_int(KV) rem 2 =:= 0.

-spec count([A]) -> [{A, non_neg_integer()}].
count(Xs) ->
    maps:to_list(lists:foldl(
        fun (X, Counts) ->
            maps:update_with(X, fun(C) -> C + 1 end, 1, Counts)
        end,
        #{},
        Xs
    )).
