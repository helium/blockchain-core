%%% ===========================================================================
%%% Value contract validation.
%%% ===========================================================================
-module(blockchain_contracts).

-export_type([
    key/0,
    val/0,
    size/0,
    quantifier/0,
    forall/0,
    exists/0,
    either/0,
    contract/0,
    spec/0,
    failure/0,
    result/0
]).

-export([
     check/1,
     check/2,
     check_with_defined/1,
     are_satisfied/1
]).

-type key() :: atom().
-type val() :: term().

-type size() ::
      any
    | {exact, integer()}
    | {range, Min :: integer(), Max :: integer()}
    | {min, integer()}
    | {max, integer()}
    .

-type forall() :: forall | '∀'.  % and  ALL contracts must be satisfied
-type exists() :: exists | '∃'.  % or   AT LEAST ONE contract must be satisfied
-type either() :: either | '∃!'. % xor  EXACTLY ONE contract must be satisfied
-type quantifier() :: forall() | exists() | either().

-type contract() ::
      {quantifier(), [contract()]}
    | defined
    | undefined
    | {iodata, size()}
    | {binary, size()}
    | {list, size()}
    | {list_of, contract()}
    | {integer, size()}
    | {member, [any()]}
    | {address, libp2p}
    | {custom, Label :: term(), fun((val()) -> boolean())}
    | h3_string
    %% TODO txn contracts: txn, txn_well_formed
    .

-type spec() ::
    {key(), val(), contract()}.

-type failure() ::
      invalid_address
    | invalid_h3_string
    | {not_a_member_of, [val()]}
    | undefined
    | {not_an_integer, val()}
    | {not_a_list, val()}
    | {integer_out_of_range, Actual :: integer(), Required :: size()}
    | {not_a_binary, val()}
    | {binary_wrong_size, Actual :: non_neg_integer(), Required :: size()}
    | {list_wrong_size, Actual :: non_neg_integer(), Required :: size()}
    | {list_contains_invalid_elements, val()}
    .

-type result() ::
    ok | {error, {invalid, [{key(), failure()}]}}.

%% For internal use
-type test_result() ::
    pass | {fail, failure()}.

%% API ========================================================================

-spec are_satisfied([spec()]) -> boolean().
are_satisfied(Specs) ->
    result:to_bool(check(Specs)).

-spec check([spec()]) -> result().
check(Specs) ->
    check_specs(Specs).

-spec check([spec()], fun((contract()) -> contract())) -> result().
check(Specs0, F) ->
    Specs1 = [{K, V, F(R)}|| {K, V, R} <- Specs0],
    check(Specs1).

-spec check_with_defined([spec()]) -> result().
check_with_defined(Specs) ->
    check(Specs, fun(R) -> {forall, [defined, R]} end).

%% Internal ===================================================================
-spec check_specs([spec()]) -> result().
check_specs(Specs) ->
    case lists:flatten([check_spec(S) || S <- Specs]) of
        [] ->
            ok;
        [_|_]=Invalid ->
            {error, {invalid, Invalid}}
    end.

-spec check_spec(spec()) -> [{key(), failure()}].
check_spec({Key, Val, Contract}) ->
    case test(Val, Contract) of
        pass ->
            [];
        {fail, Failure} ->
            [{Key, Failure}]
    end.

-spec test(val(), contract()) -> test_result().
test(V, {custom, Label, IsValid}) -> test_custom(V, Label, IsValid);
test(V, defined)                  -> test_defined(V);
test(V, undefined)                -> test_undefined(V);
test(V, {iodata, SizeSpec})       -> test_iodata(V, SizeSpec);
test(V, {binary, SizeSpec})       -> test_binary(V, SizeSpec);
test(V, {list, SizeSpec})         -> test_list(V, SizeSpec);
test(V, {list_of, Contract})      -> test_list_of(V, Contract);
test(V, {integer, SizeSpec})      -> test_int(V, SizeSpec, integer_out_of_range);
test(V, {member, Vs})             -> test_membership(V, Vs);
test(V, {address, libp2p})        -> test_address_libp2p(V);
test(V, h3_string)                -> test_h3_string(V);
test(V, {ForAll, Contracts}) when ForAll =:= forall; ForAll =:= '∀'->
    test_forall(V, Contracts);
test(V, {Exists, Contracts}) when Exists =:= exists; Exists =:= '∃'  ->
    test_exists(V, Contracts);
test(V, {Either, Contracts}) when Either =:= either; Either =:= '∃!'  ->
    test_either(V, Contracts).

-spec test_forall(val(), [contract()]) -> test_result().
test_forall(V, Contracts) ->
    lists:foldl(
        fun (R, pass) -> test(V, R);
            (_, {fail, _}=Failed) -> Failed
        end,
        pass,
        Contracts
    ).

-spec test_exists(val(), [contract()]) -> test_result().
test_exists(V, Contracts) ->
    lists:foldl(
        fun (_, pass) -> pass;
            (R, {fail, _}) -> test(V, R)
        end,
        case Contracts of
            [] ->
                pass;
            [_|_] ->
                %% XXX Init failure must never escape this foldl
                {fail, {'BUG_IN', {?MODULE, 'test_exists', ?LINE}}}
        end,
        Contracts
    ).

-spec test_either(val(), [contract()]) -> test_result().
test_either(V, Contracts) ->
    Results = [test(V, R) || R <- Contracts],
    case lists:filter(fun res_to_bool/1, Results) of
        [] -> {fail, zero_contracts_satisfied};
        [_] -> pass;
        [_|_] -> {fail, multiple_contracts_satisfied}
    end.

-spec test_custom(val(), term(), fun((val()) -> boolean())) -> test_result().
test_custom(V, Label, IsValid) ->
    case IsValid(V) of
        true -> pass;
        false -> {fail, Label}
    end.

-spec test_defined(val()) -> test_result().
test_defined(undefined) ->
    {fail, undefined};
test_defined(_) ->
    pass.

-spec test_undefined(val()) -> test_result().
test_undefined(undefined) ->
    pass;
test_undefined(_) ->
    {failed, defined}.

-spec test_iodata(val(), size()) -> test_result().
test_iodata(V, SizeSpec) ->
    try erlang:iolist_size(V) of
        Size ->
            test_int(Size, SizeSpec, iodata_wrong_size)
    catch
        _:_ ->
            {fail, not_an_iodata}
    end.

-spec test_binary(val(), size()) -> test_result().
test_binary(V, SizeSpec) ->
    case is_binary(V) of
        false ->
            {fail, {not_a_binary, V}};
        true ->
            Size = byte_size(V),
            test_int(Size, SizeSpec, binary_wrong_size)
    end.

-spec test_list(val(), size()) -> test_result().
test_list(V, SizeSpec) ->
    case is_list(V) of
        false ->
            {fail, {not_a_list, V}};
        true ->
            Size = length(V),
            test_int(Size, SizeSpec, list_wrong_size)
    end.

-spec test_list_of(val(), contract()) -> test_result().
test_list_of(Xs, Contract) ->
    case is_list(Xs) of
        false ->
            {fail, {not_a_list, Xs}};
        true ->
            Invalid =
                lists:foldl(
                    fun (X, Invalid) ->
                        case test(X, Contract) of
                            pass -> Invalid;
                            {fail, _} -> [X | Invalid]
                        end
                    end,
                    [],
                    Xs
                ),
            case Invalid of
                [] ->
                    pass;
                [_|_] ->
                    {fail, {list_contains_invalid_elements, Invalid}}
            end
    end.

-spec test_int(integer(), size(), atom()) -> test_result().
test_int(Size, Spec, FailureLabel) ->
    case is_integer(Size) of
        false ->
            {fail, {not_an_integer, Size}};
        true ->
            IsPass =
                case Spec of
                    any ->
                        true;
                    {exact, Required} when is_integer(Required) ->
                        Size == Required;
                    {range, Min, Max} when is_integer(Min), is_integer(Max) ->
                        Size >= Min andalso
                        Size =< Max;
                    {min, Min} when is_integer(Min)->
                        Size >= Min;
                    {max, Max} when is_integer(Max) ->
                        Size =< Max
                end,
            res_of_bool(IsPass, {FailureLabel, Size, Spec})
    end.

-spec test_membership(val(), [val()]) -> test_result().
test_membership(V, Vs) ->
    res_of_bool(lists:member(V, Vs), {not_a_member_of, Vs}).

-spec test_address_libp2p(val()) -> test_result().
test_address_libp2p(V) ->
    try libp2p_crypto:bin_to_pubkey(V) of
        _ -> pass
    catch
        _:_ -> {fail, invalid_address}
    end.

-spec test_h3_string(val()) -> test_result().
test_h3_string(V) ->
    try h3:from_string(V) of
        _ -> pass
    catch
        _:_ -> {fail, invalid_h3_string}
    end.

-spec res_of_bool(boolean(), failure()) -> test_result().
res_of_bool(true, _) -> pass;
res_of_bool(false, Failure) -> {fail, Failure}.

-spec res_to_bool(test_result()) -> boolean().
res_to_bool(pass) -> true;
res_to_bool({fail, _}) -> false.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% Test cases =================================================================
logic_test_() ->
    [
        ?_assertEqual(pass, test(<<>>, {'∀', [defined, {binary, any}]})),
        ?_assertEqual(pass, test(<<>>, {forall, [defined, {binary, any}]})),
        ?_assertEqual(pass, test(<<>>, {exists, [defined, {binary, any}]})),
        ?_assertEqual(pass, test(<<>>, {'∃', [defined, {binary, {exact, 5}}]})),
        ?_assertEqual(pass, test(<<>>, {exists, [defined, {binary, {exact, 5}}]})),
        ?_assertEqual(
            pass,
            test(
                undefined,
                {exists, [
                    defined,
                    {binary, {exact, 5}},
                    {custom, is_atom, fun erlang:is_atom/1}
                ]}
            )
        ),
        ?_assertMatch(
            {fail, undefined},
            test(undefined, {forall, [defined, {binary, {exact, 5}}]})
        ),
        ?_assertMatch(
            {fail, {binary_wrong_size, 0, {exact, 5}}},
            test(<<>>, {forall, [defined, {binary, {exact, 5}}]})
        ),
        ?_assertEqual(pass, test(5, {either, [{integer, any}, {binary, any}]})),
        ?_assertEqual(pass, test(5, {'∃!', [{integer, any}, {binary, any}]})),
        ?_assertMatch(
            {fail, zero_contracts_satisfied},
            test(5, {either, [{integer, {max, 1}}, {integer, {exact, 10}}]})
        ),
        ?_assertMatch(
            {fail, multiple_contracts_satisfied},
            test(5, {either, [{integer, any}, {integer, any}]})
        ),
        ?_assertMatch(
            {fail, multiple_contracts_satisfied},
            test(5, {either, [{integer, any}, {integer, {range, 0, 10}}]})
        )
    ].

membership_test_() ->
    [
        ?_assertEqual(pass, test(x, {member, [x, y, x]})),
        ?_assertEqual({fail, {not_a_member_of, []}}, test(x, {member, []}))
    ].

integer_test_() ->
    [
        ?_assertEqual(pass, test(1, {integer, any})),
        ?_assertEqual(pass, test(1, {integer, {exact, 1}})),
        ?_assertEqual(
            {fail, {integer_out_of_range, 2, {exact, 1}}},
            test(2, {integer, {exact, 1}})
        )
    ].

custom_test_() ->
    RequireBar = {custom, not_bar, fun(X) -> X =:= bar end},
    Key = foo,
    [
        ?_assertEqual(pass, test(bar, RequireBar)),
        ?_assertEqual({fail, not_bar}, test(baz, RequireBar)),
        ?_assertEqual(ok, check([{Key, bar, RequireBar}])),
        ?_assertEqual(
            {error, {invalid, [{Key, not_bar}]}},
            check([{Key, baz, RequireBar}])
        )
    ].

defined_test_() ->
    Contract = defined,
    Key = foo,
    [
        ?_assertEqual(pass, test(bar, Contract)),
        ?_assertEqual({fail, undefined}, test(undefined, Contract)),
        ?_assertEqual(ok, check([{Key, bar, Contract}])),
        ?_assertEqual(
            {error, {invalid, [{Key, undefined}]}},
            check([{Key, undefined, Contract}])
        )
    ].

binary_test_() ->
    Key = foo,
    [
        ?_assertEqual(pass, test(<<>>, {binary, any})),
        ?_assertEqual(pass, test(<<>>, {binary, {exact, 0}})),
        ?_assertEqual(pass, test(<<>>, {binary, {range, 0, 1024}})),
        ?_assertEqual(
            {fail, {binary_wrong_size, 0, {range, 1, 1024}}},
            test(<<>>, {binary, {range, 1, 1024}})
        ),
        ?_assertEqual(pass, test(<<"a">>, {binary, {range, 1, 1024}})),
        ?_assertEqual(pass, test(<<"bar">>, {binary, {range, 3, 1024}})),
        ?_assertEqual(ok, check([{Key, <<>>, {binary, any}}])),
        ?_assertEqual(ok, check([{Key, <<>>, {binary, {exact, 0}}}])),
        ?_assertEqual(
            {error, {invalid, [{Key, {binary_wrong_size, 0, {range, 8, 1024}}}]}},
            check([{Key, <<>>, {binary, {range, 8, 1024}}}])
        )
    ].

list_test_() ->
    Key = foo,
    BadList = <<"trust me, i'm a list">>,
    [
        ?_assertEqual(pass, test([], {list, any})),
        ?_assertEqual(pass, test([], {list, {exact, 0}})),
        ?_assertEqual(pass, test([], {list, {range, 0, 1024}})),
        ?_assertEqual(
            {fail, {list_wrong_size, 0, {range, 1, 1024}}},
            test([], {list, {range, 1, 1024}})
        ),
        ?_assertEqual(pass, test([a], {list, {range, 1, 1024}})),
        ?_assertEqual(pass, test([a, b, c], {list, {range, 3, 1024}})),
        ?_assertEqual(pass, test([a, b, c, d, e, f], {list, {range, 3, 1024}})),
        ?_assertEqual(ok, check([{Key, [], {list, any}}])),
        ?_assertEqual(ok, check([{Key, [], {list, {exact, 0}}}])),
        ?_assertEqual(
            {error, {invalid, [{Key, {list_wrong_size, 0, {range, 8, 1024}}}]}},
            check([{Key, [], {list, {range, 8, 1024}}}])
        ),
        ?_assertEqual(
            {error, {invalid, [{Key, {not_a_list, BadList}}]}},
            check(
                [{Key, BadList, {list, {range, 8, 1024}}}]
            )
        )
    ].

list_of_test_() ->
    [
        ?_assertEqual(pass, test([], {list_of, {integer, any}})),
        ?_assertEqual(pass, test([], {list_of, {integer, {range, 1, 5}}})),
        ?_assertEqual(pass, test([1, 2, 3], {list_of, {integer, any}})),
        ?_assertEqual(pass, test([1, 2, 3], {list_of, {integer, {range, 1, 5}}})),
        ?_assertEqual(
            {fail, {list_contains_invalid_elements, [30]}},
            test([1, 2, 30], {list_of, {integer, {range, 1, 5}}})
        )
    ].

address_test_() ->
    Addr = addr_gen(),
    [
        ?_assertEqual(pass, test(Addr, {address, libp2p})),
        ?_assertEqual(
            {fail, invalid_address},
            test(<<"eggplant", Addr/binary>>, {address, libp2p})
        ),
        ?_assertEqual(
            pass,
            test(
                Addr,
                {forall, [
                    defined,
                    {binary, any},
                    {binary, {range, 0, 1024}},
                    {binary, {exact, 33}},
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
        ?_assertEqual(pass                                    , test_iodata(IOData, any)),
        ?_assertMatch({fail, {iodata_wrong_size, _, {min, _}}}, test_iodata(IOData, {min, iolist_size(IOData) + 1})),
        ?_assertMatch({fail, {iodata_wrong_size, _, {max, _}}}, test_iodata(IOData, {max, iolist_size(IOData) - 1})),

        ?_assertEqual({fail, not_an_iodata}, test_iodata(undefined, any)),
        ?_assertEqual({fail, not_an_iodata}, test_iodata([undefined], any)),
        ?_assertEqual({fail, not_an_iodata}, test_iodata(["foo", bar, <<"baz">>], any)),
        ?_assertEqual(pass, test_iodata(["foo", [["123"], [[], ["qux"]]], <<"baz">>], any)),
        ?_assertEqual({fail, not_an_iodata}, test_iodata(["foo", [["123"], [[hi], ["qux"]]], <<"baz">>], any)),
        ?_assertEqual({fail, not_an_iodata}, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMin - 1, <<"baz">>], any)),
        ?_assertEqual({fail, not_an_iodata}, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMax + 1, <<"baz">>], any)),
        ?_assertEqual(pass, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMin, <<"baz">>], any)),
        ?_assertEqual(pass, test_iodata(["foo", [["123"], [[], ["qux"]]], CharMax, <<"baz">>], any)),
        ?_assertEqual(pass, test([[], [<<"1">>], "2", <<"3">>], {list_of, {iodata, any}})),

        ?_assertMatch(pass                                       , test("12345678", {list_of, {integer, any}})),
        ?_assertMatch({fail, {list_contains_invalid_elements, _}}, test("12345678", {list_of, {iodata, any}})),
        ?_assertEqual(pass                                       , test("12345678", {iodata, any}))
    ].

%% Test helpers ===============================================================

-spec addr_gen() -> binary().
addr_gen() ->
    #{public := PK, secret := _} =
        libp2p_crypto:generate_keys(ecc_compact),
    libp2p_crypto:pubkey_to_bin(PK).

-endif.
