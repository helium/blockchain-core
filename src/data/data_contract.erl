%%% ===========================================================================
%%% Check that a piece of data satisfies a given contract (i.e. specification).
%%% ===========================================================================
-module(data_contract).

-export_type([
    key/0,
    val/0,
    measure/0,
    measure/1,
    txn_type/0,
    quantifier/0,
    forall/0,
    exists/0,
    either/0,
    failure/0,
    failure_bin/0,
    failure_int/0,
    failure_list/0,
    failure_txn/0,
    result/0,

    t/0
]).

-export([
     check/2,
     is_satisfied/2
]).

-type key() :: atom().
-type val() :: term().

-type measure(A) ::
      any
    | {exactly, A}
    | {range, Min :: A, Max :: A}
    | {min, A}
    | {max, A}
    .

-type measure() ::
    measure(integer()).

-type txn_type() ::
    any | {type, atom()}.

%% TODO Pick one of the alternative keyword sets:
-type forall() :: forall | '∀'  | all_of | 'and'.  % ALL contracts must be satisfied
-type exists() :: exists | '∃'  | any_of | 'or' .  % AT LEAST ONE contract must be satisfied
-type either() :: either | '∃!' | one_of | 'xor'.  % EXACTLY ONE contract must be satisfied
-type quantifier() :: forall() | exists() | either().

-type t() ::
      {quantifier(), [t()]}
    | {'not', t()}
    | any
    | defined
    | undefined
    | {string, measure()}
    | {iodata, measure()}
    | {binary, measure()}
    | {tuple, [t()]}
    | {list, measure(), t()}
    | {kvl, [{key(), t()}]}
    %% TODO Reconsider semantics - how can we spec a looser contract than every key?
    %%      Maybe:
    %%          {kvl, [{key(), t()}], Opt} when Opt :: fail_unknown_keys | pass_unknown_keys
    %%          {kvl, Required :: [{key(), t()}], Optional :: [{key(), t()}]}
    %%      Report:
    %%          - duplicated
    %%          - unsupported
    %%          - missing
    %%          - invalid contract
    %%      See hope_kv_list validations.
    %% TODO Better name than "kvl"? "pairs"?

    % TODO Reconsider name, since we only require element uniquness, not order.
    % ordset alternatives:
    % - ulist
    % - set_list
    % - list_set
    % - list_of_uniques
    | {ordset, measure(), t()}
    | {ordset, measure(), t(), Cmp :: fun((val(), val()) -> boolean())}

    | {number, measure(float() | integer())}
    | {float, measure(float())}
    | {integer, measure(integer())}
    % TODO Design integration of finer refinements, like is_power_of_2, etc.
    %       {integer, measure(), [refinement()]} ?
    %       {integer, measure(), [t()]} ? where refinement is a t variant
    %
    % Use-case in blockchain_txn_oui_v1.erl

    | {member, [any()]}
    | {address, libp2p}
    %% TODO Custom test fun should return result:empty/1 instead of boolean, so we can propagate the error.
    | {custom, fun((val()) -> boolean()), Label :: term()} % TODO Maybe rename "custom" to "test"
    | h3_string
    | {txn, txn_type()}
    | {val, val()}  % A concrete, given value.
    .
    %% TODO More contracts:
    %%  - [x] txn
    %%  - [x] tuple
    %%  - [ ] records as tuple with given head?
    %%      Can we automate mapping field names to positions?
    %%          Yes, and then record contracts don't seem that useful if we can
    %%          check them as kvls...
    %%  - [ ] atom. But, maybe not useful in light of {val, A}?
    %%  - [x] a concrete, given value, something like: -type() val(A) :: {val, A}.

-type failure_bin() ::
      {not_a_binary, val()}
    | {binary_wrong_size, Actual :: non_neg_integer(), Required :: measure()}
    .

-type failure_iodata() ::
      not_iodata
    | {iodata_wrong_size, Actual :: non_neg_integer(), Required :: measure()}
    .

-type failure_float() ::
      {not_a_float, val()}
    | {float_out_of_range, Actual :: float(), Required :: measure(float())}
    .

-type failure_int() ::
      {not_an_integer, val()}
    | {integer_out_of_range, Actual :: integer(), Required :: measure()}
    .

-type failure_list() ::
      {not_a_list, val()}
    | {list_wrong_size, Actual :: non_neg_integer(), Required :: measure()}
    | {list_contains_invalid_elements, [failure()]}
    .

-type failure_tuple() ::
      {not_a_tuple, val()}
    | {tuple_wrong_size, Actual :: non_neg_integer(), Required :: non_neg_integer()}
    | {tuple_contract_breaches_in, [{Pos :: pos_integer(), failure()}]}
    .

-type failure_kvl() ::
      {invalid_kv_pair, {key(), val()}} % TODO Integrate into failure_list(), where it occurs
    | {invalid_kvl, failure_list()}
    | {invalid_kvl_pairs, [{key(), failure()}]}
    | {kvl_keys_missing_a_contract, [key()]}
    | {kvl_keys_missing_a_value, [key()]}
    .

-type failure_txn() ::
      {not_a_txn, val()}
    | {txn_wrong_type, Actual :: atom(), Required :: atom()}
    | {txn_malformed, val()}
    .

-type failure_either() ::
      {zero_contracts_satisfied, [failure()]}
    | {multiple_contracts_satisfied, [t()]}
    .

-type failure() ::
      invalid_address
    | invalid_h3_string
    | negation_failed
    | {unexpected_val, given, val(), expected, val()}
    | {not_a_member_of, [val()]}
    | defined
    | undefined
    | failure_iodata()
    | failure_txn()
    | failure_bin()
    | failure_int()
    | failure_float()
    | failure_list()
    | failure_tuple()
    | failure_kvl()
    | {list_contains_duplicate_elements, [term()]}
    | {invalid_string, failure_list()}
    | failure_either()
    .

%% Externally presented
-type result() ::
    result:empty({contract_breach, failure()}).

%% Internally used
-type test_result() ::
    result:empty(failure()).

-define(CHAR_MIN, 0).
-define(CHAR_MAX, 255).

%% API ========================================================================

-spec is_satisfied(val(), t()) -> boolean().
is_satisfied(Val, Contract) ->
    case check(Val, Contract) of
        ok -> true;
        {error, _} -> false
    end.

-spec check(val(), t()) -> result().
check(Val, Contract) ->
    case test(Val, Contract) of
        ok -> ok;
        {error, Failure} -> {error, {contract_breach, Failure}}
    end.

%% Internal ===================================================================

-spec test(val(), t()) -> test_result().
test(_, any)                         -> ok;
test(V, {val, Expected})             -> test_val(V, Expected);
test(V, {'not', Contract})           -> test_not(V, Contract);
test(V, {custom, IsValid, Label})    -> test_custom(V, IsValid, Label);
test(V, defined)                     -> test_defined(V);
test(V, undefined)                   -> test_undefined(V);
test(V, {string, Measure})           -> test_string(V, Measure);
test(V, {iodata, Measure})           -> test_iodata(V, Measure);
test(V, {binary, Measure})           -> test_binary(V, Measure);
test(V, {tuple, Contracts})          -> test_tuple(V, Contracts);
test(V, {list, Measure, Contract})   -> test_list(V, Measure, Contract);
test(V, {ordset, Measure, C})        -> test_ordset(V, Measure, C, fun erlang:'=<'/2);
test(V, {ordset, Measure, C, F})     -> test_ordset(V, Measure, C, F);
test(V, {kvl, KeyContracts})         -> test_kvl(V, KeyContracts);
test(V, {integer, Measure})          -> test_int(V, Measure);
test(V, {float, Measure})            -> test_float(V, Measure);
test(V, {number, Measure})           -> test_number(V, Measure);
test(V, {member, Vs})                -> test_membership(V, Vs);
test(V, {address, libp2p})           -> test_address_libp2p(V);
test(V, h3_string)                   -> test_h3_string(V);
test(V, {txn, TxnType})              -> test_txn(V, TxnType);
test(V, {Q, Contracts}) when Q =:= forall; Q =:= '∀' ; Q =:= 'all_of'; Q =:= 'and' ->
    test_forall(V, Contracts);
test(V, {Q, Contracts}) when Q =:= exists; Q =:= '∃' ; Q =:= 'any_of'; Q =:= 'or'  ->
    test_exists(V, Contracts);
test(V, {Q, Contracts}) when Q =:= either; Q =:= '∃!'; Q =:= 'one_of'; Q =:= 'xor' ->
    test_either(V, Contracts);
test(_, BadContract) ->
    error({bad_contract, BadContract}).

-spec test_kvl(val(), [{key(), t()}]) -> test_result().
test_kvl(KeyValues, KeyContracts) ->
    IsPair = fun ({_, _}) -> true; (_) -> false end,
    case test_list(KeyValues, any, {custom, IsPair, invalid_kv_pair}) of
        {error, Failure} ->
            {error, {invalid_kvl, Failure}};
        ok ->
            KeyValueContracts =
                [{K, V, kvl_get(K, KeyContracts)} || {K, V} <- KeyValues],
            KeysMissingContracts =
                [K || {K, _, Opt} <- KeyValueContracts, Opt =:= none],
            case KeysMissingContracts of
                [_|_] ->
                    {error, {kvl_keys_missing_a_contract, KeysMissingContracts}};
                [] ->
                    case [K || {K, _} <- KeyContracts, kvl_get(K, KeyValues) =:= none] of
                        [_|_]=KeysMissingValues ->
                            {error, {kvl_keys_missing_a_value, KeysMissingValues}};
                        [] ->
                            KeyResults =
                                [{K, test(V, C)} || {K, V, {some, C}} <- KeyValueContracts],
                            KeyFailures =
                                lists:filtermap(
                                    fun ({_, ok}) ->
                                            false;
                                        ({K, {error, F}}) ->
                                            {true, {K, F}}
                                    end,
                                    KeyResults
                                ),
                            case KeyFailures of
                                [] ->
                                    ok;
                                [_|_] ->
                                    {error, {invalid_kvl_pairs, KeyFailures}}
                            end
                    end
            end
    end.

-spec test_not(val(), t()) -> test_result().
test_not(V, Contract) ->
    case test(V, Contract) of
        ok -> {error, negation_failed};
        {error, _} -> ok
    end.

-spec test_val(val(), val()) -> test_result().
test_val(V, V) -> ok;
test_val(G, E) -> {error, {unexpected_val, given, G, expected, E}}.

-spec test_forall(val(), [t()]) -> test_result().
test_forall(V, Contracts) ->
    lists:foldl(
        fun (C, ok) -> test(V, C);
            (_, {error, _}=Failed) -> Failed
        end,
        ok,
        Contracts
    ).

-spec test_exists(val(), [t()]) -> test_result().
test_exists(V, Contracts) ->
    %% TODO More-informative error, not simply the last failure.
    lists:foldl(
        fun (_, ok) -> ok;
            (C, {error, _}) -> test(V, C)
        end,
        case Contracts of
            [] ->
                ok;
            [_|_] ->
                %% XXX Init failure must never escape this foldl
                {error, {'BUG_IN', {?MODULE, 'test_exists', ?LINE}}}
        end,
        Contracts
    ).

-spec test_either(val(), [t()]) -> test_result().
test_either(V, Contracts) ->
    Results = [{C, test(V, C)} || C <- Contracts],
    case lists:partition(fun ({_, R}) -> R =:= ok end, Results) of
        {[]     , F} -> {error, {zero_contracts_satisfied, [R || {_, {error, R}} <- F]}};
        {[_]    , _} -> ok;
        {[_|_]=P, _} -> {error, {multiple_contracts_satisfied, [C || {C, ok} <- P]}}
    end.

-spec test_custom(val(), fun((val()) -> boolean()), term()) -> test_result().
test_custom(V, IsValid, Label) ->
    case IsValid(V) of
        true -> ok;
        false -> {error, {Label, V}}
    end.

-spec test_defined(val()) -> test_result().
test_defined(undefined) ->
    {error, undefined};
test_defined(_) ->
    ok.

-spec test_undefined(val()) -> test_result().
test_undefined(undefined) ->
    ok;
test_undefined(_) ->
    {error, defined}.

-spec test_iodata(val(), measure()) -> test_result().
test_iodata(V, Measure) ->
    try erlang:iolist_size(V) of
        Size ->
            res_of_bool(
                is_in_range(Size, Measure),
                {iodata_wrong_size, Size, Measure}
            )
    catch
        _:_ ->
            {error, not_iodata}
    end.

-spec test_binary(val(), measure()) -> test_result().
test_binary(V, Measure) ->
    case is_binary(V) of
        false ->
            {error, {not_a_binary, V}};
        true ->
            Size = byte_size(V),
            res_of_bool(
                is_in_range(Size, Measure),
                {binary_wrong_size, Size, Measure}
            )
    end.

-spec test_string(val(), measure()) -> test_result().
test_string(V, Measure) ->
    case test(V, {list, Measure, {integer, {range, ?CHAR_MIN, ?CHAR_MAX}}}) of
        ok ->
            ok;
        {error, Reason} ->
            {error, {invalid_string, Reason}}
    end.

-spec test_list_size(val(), measure()) -> test_result().
test_list_size(V, Measure) ->
    case is_list(V) of
        false ->
            {error, {not_a_list, V}};
        true ->
            Size = length(V),
            res_of_bool(
                is_in_range(Size, Measure),
                {list_wrong_size, Size, Measure}
            )
    end.

-spec test_list(val(), measure(), t()) -> test_result().
test_list(Xs, Measure, ElementContract) ->
    case test_list_size(Xs, Measure) of
        {error, _}=Fail ->
            Fail;
        ok ->
            Failures =
                lists:foldl(
                    fun (X, Failures) ->
                        case test(X, ElementContract) of
                            ok -> Failures;
                            {error, F} -> [F | Failures]
                        end
                    end,
                    [],
                    Xs
                ),
            case Failures of
                [] ->
                    ok;
                [_|_] ->
                    {error, {list_contains_invalid_elements, Failures}}
            end
    end.

-spec test_tuple(val(), [t()]) -> test_result().
test_tuple(V, Contracts) when is_tuple(V) ->
    NumElements = tuple_size(V),
    NumContracts = length(Contracts),
    case NumElements =:= NumContracts of
        false ->
            {error, {tuple_wrong_size, NumElements, NumContracts}};
        true ->
            Elements = tuple_to_list(V),
            Failures =
                lists:foldl(
                    fun ({I, E, C}, Failures) ->
                        case test(E, C) of
                            ok ->
                                Failures;
                            {error, F} ->
                                [{I, F} | Failures]
                        end
                    end,
                    [],
                    lists:zip3(lists:seq(1, NumElements), Elements, Contracts)
                ),
            case Failures of
                [] ->
                    ok;
                [_|_] ->
                    {error, {tuple_contract_breaches_in, Failures}}
            end
    end;
test_tuple(V, _) ->
    {error, {not_a_tuple, V}}.

-spec test_ordset(val(), measure(), t(), fun((val(), val()) -> boolean())) ->
    test_result().
test_ordset(Xs, Measure, ElementContract, Cmp) ->
    case test_list(Xs, Measure, ElementContract) of
        {error, _}=Fail ->
            Fail;
        ok ->
            case Xs -- lists:usort(Cmp, Xs) of
                [] ->
                    ok;
                [_|_]=Dups ->
                    {error, {list_contains_duplicate_elements, Dups}}
            end
    end.

-spec test_float(val(), measure(float())) -> test_result().
test_float(V, Range) ->
    test_num(V, Range, fun erlang:is_float/1, not_a_float, float_out_of_range).

-spec test_int(val(), measure()) -> test_result().
test_int(V, Range) ->
    test_num(V, Range, fun erlang:is_integer/1, not_an_integer, integer_out_of_range).

-spec test_number(val(), measure(integer() | float())) -> test_result().
test_number(V, Range) ->
    test(V,
        {either, [
            {float, Range},
            {integer, Range},
            {val, infinity},
            {val, '-infinity'},
            {val, nan}
        ]}
    ).

-spec test_num(val(), measure(Type), fun((val()) -> boolean()), atom(), atom()) ->
    test_result() when Type :: integer() | float().
test_num(V, Range, TypeTest, TypeFailureLabel, RangeFailureLabel) ->
    case TypeTest(V) of
        false ->
            {error, {TypeFailureLabel, V}};
        true ->
            res_of_bool(
                is_in_range(V, Range),
                {RangeFailureLabel, V, Range}
            )
    end.

-spec is_in_range(A, measure(A)) -> boolean().
is_in_range(_, any) -> true;
is_in_range(X, {exactly, Y}) -> X =:= Y;
is_in_range(X, {min, Min}) -> X >= Min;
is_in_range(X, {max, Max}) -> X =< Max;
is_in_range(X, {range, Min, Max}) ->
    is_in_range(X, {min, Min}) andalso
    is_in_range(X, {max, Max}).

-spec test_membership(val(), [val()]) -> test_result().
test_membership(V, Vs) ->
    res_of_bool(lists:member(V, Vs), {not_a_member_of, Vs}).

-spec test_address_libp2p(val()) -> test_result().
test_address_libp2p(V) ->
    try libp2p_crypto:bin_to_pubkey(V) of
        _ -> ok
    catch
        _:_ -> {error, invalid_address}
    end.

-spec test_h3_string(val()) -> test_result().
test_h3_string(V) ->
    try h3:from_string(V) of
        _ -> ok
    catch
        _:_ -> {error, invalid_h3_string}
    end.

-spec test_txn(val(), txn_type()) -> test_result().
test_txn(V, TxnType) ->
    case blockchain_txn:type_check(V) of
        {error, not_a_known_txn_value} ->
            {error, {not_a_txn, V}};
        {ok, TypeActual} ->
            TypeRequired =
                case TxnType of
                    any ->
                        TypeActual;
                    {type, Type} ->
                        Type
                end,
            case TypeActual =:= TypeRequired of
                true ->
                    case TypeActual:is_well_formed(V) of
                        ok ->
                            ok;
                        {error, _} ->
                            {error, {txn_malformed, V}}
                    end;
                false ->
                    {error, {txn_wrong_type, TypeActual, TypeRequired}}
            end
    end.

-spec res_of_bool(boolean(), failure()) -> test_result().
res_of_bool(true, _) -> ok;
res_of_bool(false, Failure) -> {error, Failure}.

-spec kvl_get(K, [{K, V}]) -> none | {some, V}.
kvl_get(K, KVL) ->
    case lists:keyfind(K, 1, KVL) of
        {_, V} -> {some, V};
        false -> none
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% Test cases =================================================================
logic_test_() ->
    [
        ?_assertEqual(ok, test(<<>>, {'∀', [defined, {binary, any}]})),
        ?_assertEqual(ok, test(<<>>, {forall, [defined, {binary, any}]})),
        ?_assertEqual(ok, test(<<>>, {all_of, [defined, {binary, any}]})),
        ?_assertEqual(ok, test(<<>>, {exists, [defined, {binary, any}]})),
        ?_assertEqual(ok, test(<<>>, {any_of, [defined, {binary, any}]})),
        ?_assertEqual(ok, test(<<>>, {'∃', [defined, {binary, {exactly, 5}}]})),
        ?_assertEqual(ok, test(<<>>, {exists, [defined, {binary, {exactly, 5}}]})),
        ?_assertEqual(
            ok,
            test(
                undefined,
                {exists, [
                    defined,
                    {binary, {exactly, 5}},
                    {custom, fun erlang:is_atom/1, is_atom}
                ]}
            )
        ),
        ?_assertMatch(
            {error, undefined},
            test(undefined, {forall, [defined, {binary, {exactly, 5}}]})
        ),
        ?_assertMatch(
            {error, {binary_wrong_size, 0, {exactly, 5}}},
            test(<<>>, {forall, [defined, {binary, {exactly, 5}}]})
        ),
        ?_assertEqual(ok, test(5, {one_of, [{integer, any}, {binary, any}]})),
        ?_assertEqual(ok, test(5, {either, [{integer, any}, {binary, any}]})),
        ?_assertEqual(ok, test(5, {'∃!', [{integer, any}, {binary, any}]})),
        ?_assertMatch(
            {error, {zero_contracts_satisfied, _}},
            test(5, {either, [{integer, {max, 1}}, {integer, {exactly, 10}}]})
        ),
        ?_assertMatch(
            {error, {multiple_contracts_satisfied, [{integer, any}, {integer, any}]}},
            test(5, {either, [{integer, any}, {integer, any}]})
        ),
        ?_assertMatch(
            {error, {multiple_contracts_satisfied, [{integer, any}, {integer, _}]}},
            test(5, {either, [{integer, any}, {integer, {range, 0, 10}}]})
        )
    ].

membership_test_() ->
    [
        ?_assertEqual(ok, test(x, {member, [x, y, x]})),
        ?_assertEqual({error, {not_a_member_of, []}}, test(x, {member, []}))
    ].

integer_test_() ->
    [
        ?_assertEqual(ok, test(1, {integer, any})),
        ?_assertEqual(ok, test(1, {integer, {exactly, 1}})),
        ?_assertEqual(
            {error, {integer_out_of_range, 2, {exactly, 1}}},
            test(2, {integer, {exactly, 1}})
        )
    ].

float_test_() ->
    [
        ?_assertEqual(ok, test(1.0, {float, any})),
        ?_assertEqual(ok, test(1.0, {float, {exactly, 1.0}})),
        ?_assertEqual(
            {error, {float_out_of_range, 2.0, {exactly, 1.0}}},
            test(2.0, {float, {exactly, 1.0}})
        )
    ].

custom_test_() ->
    BarContract = {custom, fun(X) -> X =:= bar end, not_bar},
    Key = foo,
    [
        ?_assertEqual(ok, test(bar, BarContract)),
        ?_assertEqual({error, {not_bar, baz}}, test(baz, BarContract)),
        ?_assertEqual(ok, check([{Key, bar}], {kvl, [{Key, BarContract}]})),

        ?_assertEqual(
            {error, {contract_breach, {invalid_kvl_pairs, [{Key, {not_bar, baz}}]}}},
            check([{Key, baz}], {kvl, [{Key, BarContract}]})
        )
    ].

defined_test_() ->
    Contract = defined,
    Key = foo,
    [
        ?_assertEqual(ok, test(bar, Contract)),
        ?_assertEqual({error, undefined}, test(undefined, Contract)),
        ?_assertEqual(ok, check([{Key, bar}], {kvl, [{Key, Contract}]})),
        ?_assertEqual(
            {error, {contract_breach, {invalid_kvl_pairs, [{Key, undefined}]}}},
            check([{Key, undefined}], {kvl, [{Key, Contract}]})
        )
    ].

binary_test_() ->
    Key = foo,
    [
        ?_assertEqual(ok, test(<<>>, {binary, any})),
        ?_assertEqual(ok, test(<<>>, {binary, {exactly, 0}})),
        ?_assertEqual(ok, test(<<>>, {binary, {range, 0, 1024}})),
        ?_assertEqual(
            {error, {binary_wrong_size, 0, {range, 1, 1024}}},
            test(<<>>, {binary, {range, 1, 1024}})
        ),
        ?_assertEqual(ok, test(<<"a">>, {binary, {range, 1, 1024}})),
        ?_assertEqual(ok, test(<<"bar">>, {binary, {range, 3, 1024}})),
        ?_assertEqual(ok, check([{Key, <<>>}], {kvl, [{Key, {binary, any}}]})),
        ?_assertEqual(ok, check([{Key, <<>>}], {kvl, [{Key, {binary, {exactly, 0}}}]})),
        ?_assertEqual(
            {error, {contract_breach, {invalid_kvl_pairs, [{Key, {binary_wrong_size, 0, {range, 8, 1024}}}]}}},
            check([{Key, <<>>}], {kvl, [{Key, {binary, {range, 8, 1024}}}]})
        )
    ].

list_test_() ->
    Key = foo,
    BadList = <<"trust me, i'm a list">>,
    [
        ?_assertEqual(ok, test([], {list, any, any})),
        ?_assertEqual(ok, test([], {list, {exactly, 0}, any})),
        ?_assertEqual(ok, test([], {list, {range, 0, 1024}, any})),
        ?_assertEqual(
            {error, {list_wrong_size, 0, {range, 1, 1024}}},
            test([], {list, {range, 1, 1024}, any})
        ),
        ?_assertEqual(ok, test([a], {list, {range, 1, 1024}, any})), % TODO atom contract
        ?_assertEqual(ok, test([a, b, c], {list, {range, 3, 1024}, any})), % TODO atom contract
        ?_assertEqual(ok, test([a, b, c, d, e, f], {list, {range, 3, 1024}, any})), % TODO atom contract
        ?_assertEqual(ok, check([{Key, []}], {kvl, [{Key, {list, any, any}}]})),
        ?_assertEqual(ok, check([{Key, []}], {kvl, [{Key, {list, {exactly, 0}, any}}]})),
        ?_assertEqual(
            {error, {contract_breach, {invalid_kvl_pairs, [{Key, {list_wrong_size, 0, {range, 8, 1024}}}]}}},
            check([{Key, []}], {kvl, [{Key, {list, {range, 8, 1024}, any}}]})
        ),
        ?_assertEqual(
            {error, {contract_breach, {invalid_kvl_pairs, [{Key, {not_a_list, BadList}}]}}},
            check(
                [{Key, BadList}],
                {kvl, [{Key, {list, {range, 8, 1024}, any}}]}
            )
        ),
        ?_assertEqual(ok, test([], {list, any, {integer, any}})),
        ?_assertEqual(ok, test([], {list, any, {integer, {range, 1, 5}}})),
        ?_assertEqual(ok, test([1, 2, 3], {list, any, {integer, any}})),
        ?_assertEqual(ok, test([1, 2, 3], {list, {exactly, 3}, {integer, any}})),
        ?_assertEqual(ok, test([1, 2, 3], {list, any, {integer, {range, 1, 5}}})),
        ?_assertEqual(
            {error, {list_contains_invalid_elements, [{integer_out_of_range, 30, {range, 1, 5}}]}},
            test([1, 2, 30], {list, any, {integer, {range, 1, 5}}})
        )
    ].

address_test_() ->
    Addr = addr_gen(),
    [
        ?_assertEqual(ok, test(Addr, {address, libp2p})),
        ?_assertEqual(
            {error, invalid_address},
            test(<<"eggplant", Addr/binary>>, {address, libp2p})
        ),
        ?_assertEqual(
            ok,
            test(
                Addr,
                {forall, [
                    defined,
                    {binary, any},
                    {binary, {range, 0, 1024}},
                    {binary, {exactly, 33}},
                    {address, libp2p}
                ]}
            )
        )
    ].

iodata_test_() ->
    CharMin = 0,
    CharMax = 255,
    IOData = ["foo", <<"baz">>],
    [
        ?_assertEqual(ok                                    , test_iodata(IOData, any)),
        ?_assertMatch({error, {iodata_wrong_size, _, {min, _}}}, test_iodata(IOData, {min, iolist_size(IOData) + 1})),
        ?_assertMatch({error, {iodata_wrong_size, _, {max, _}}}, test_iodata(IOData, {max, iolist_size(IOData) - 1})),

        ?_assertEqual({error, not_iodata}, test_iodata(undefined, any)),
        ?_assertEqual({error, not_iodata}, test_iodata([undefined], any)),
        ?_assertEqual({error, not_iodata}, test_iodata(["foo", bar, <<"baz">>], any)),
        ?_assertEqual(ok, test_iodata(["foo", [["123"], [[], ["qux"]]], <<"baz">>], any)),
        ?_assertEqual({error, not_iodata}, test_iodata(["foo", [["123"], [[hi], ["qux"]]], <<"baz">>], any)),
        ?_assertEqual({error, not_iodata}, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMin - 1, <<"baz">>], any)),
        ?_assertEqual({error, not_iodata}, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMax + 1, <<"baz">>], any)),
        ?_assertEqual(ok, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMin, <<"baz">>], any)),
        ?_assertEqual(ok, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMax, <<"baz">>], any)),
        ?_assertEqual(ok, test([[], [<<"1">>], "2", <<"3">>], {list, any, {iodata, any}})),

        ?_assertMatch(ok                                       , test("12345678", {list, any, {integer, any}})),
        ?_assertMatch({error, {list_contains_invalid_elements, _}}, test("12345678", {list, any, {iodata, any}})),
        ?_assertEqual(ok                                       , test("12345678", {iodata, any}))
    ].

string_test_() ->
    [
        ?_assertEqual(ok, test("foo", {string, any})),
        ?_assertEqual(
            {error, {invalid_string, {list_wrong_size, 3, {min, 4}}}},
            test("foo", {string, {min, 4}})
        ),
        ?_assertEqual(
            {error, {invalid_string, {not_a_list, <<"foo">>}}},
            test(<<"foo">>, {string, any})
        ),
        ?_assertEqual(
            {error, {invalid_string, {list_contains_invalid_elements, [{integer_out_of_range, ?CHAR_MIN - 1, {range, 0, 255}}]}}},
            test("foo" ++ [?CHAR_MIN - 1], {string, any})
        ),
        ?_assertEqual(
            {error, {invalid_string, {list_contains_invalid_elements, [{integer_out_of_range, ?CHAR_MAX + 1, {range, 0, 255}}]}}},
            test("foo" ++ [?CHAR_MAX + 1], {string, any})
        )
    ].

txn_test_() ->
    Addr = addr_gen(),
    Type = blockchain_txn_add_gateway_v1,
    Txn  = Type:new(Addr, Addr),
    TxnMalformed = Type:new(<<"not addr">>, Addr),
    [
        ?_assertEqual({error, {not_a_txn, trust_me_im_a_txn}}, test(trust_me_im_a_txn, {txn, any})),
        ?_assertEqual(ok, test(Txn, {txn, any})),
        ?_assertEqual(ok, test(Txn, {txn, {type, Type}})),
        ?_assertEqual(
            {error, {txn_wrong_type, Type, not_a_txn_type}},
            test(Txn, {txn, {type, not_a_txn_type}})
        ),
        ?_assertEqual(
            {error, {txn_malformed, TxnMalformed}},
            test(TxnMalformed, {txn, any})
        )
    ].

is_satisfied_test() ->
    ?assert(is_satisfied("foo", {forall, [{string, any}, {iodata, any}]})).

ordset_test_() ->
    [
        ?_assertMatch(ok, test([], {ordset, any, any})),
        ?_assertMatch(ok, test([a, b, c], {ordset, any, any})),

        % XXX Note that it isn't a strict ordset, since order is not enforced,
        % only uniquness:
        ?_assertMatch(ok, test([c, a, b], {ordset, any, any})),
        ?_assertMatch(ok, test([c, b, a], {ordset, any, any})),

        ?_assertMatch(
            {error, {list_contains_duplicate_elements, [c]}},
            test([c, b, a, c], {ordset, any, any})
        ),
        ?_assertMatch(
            {error, {list_contains_duplicate_elements, [c, c]}},
            test([c, b, a, c, c], {ordset, any, any})
        )
    ].

val_test_() ->
    [
        ?_assertEqual(ok, test(a, {val, a})),
        ?_assertEqual(
            {error, {unexpected_val, given, b, expected, a}},
            test(b, {val, a})
        ),
        ?_assertEqual(
            {error, negation_failed},
            test(a, {'not', {val, a}})
        ),
        ?_assertEqual(
            ok,
            test(b, {'not', {val, a}})
        )
    ].

kvl_test_() ->
    [
        ?_assertMatch(
           ok,
            test([{a, 1}], {kvl, [{a, defined}]})
        ),
        ?_assertMatch(
           {error, {kvl_keys_missing_a_contract, [a]}},
            test([{a, 1}], {kvl, [{b, defined}]})
        ),
        ?_assertMatch(
           {error, {kvl_keys_missing_a_value, [b]}},
            test([{a, 1}], {kvl, [{a, defined}, {b, defined}]})
        )
    ].

tuple_test_() ->
    [
        ?_assertMatch(ok, test({a}, {tuple, [{val, a}]})),
        ?_assertMatch(ok, test({a, 1}, {tuple, [{val, a}, {integer, {min, 0}}]})),
        ?_assertMatch(
            {error, {tuple_wrong_size, 1, 2}},
            test({a}, {tuple, [{val, a}, {integer, {min, 0}}]})
        ),
        ?_assertMatch(
            {error, {tuple_contract_breaches_in, [{2, {not_an_integer, b}}]}},
            test({a, b}, {tuple, [{val, a}, {integer, {min, 0}}]})
        )
    ].

%% Test helpers ===============================================================

-spec addr_gen() -> binary().
addr_gen() ->
    #{public := PK, secret := _} =
        libp2p_crypto:generate_keys(ecc_compact),
    libp2p_crypto:pubkey_to_bin(PK).

-endif.
