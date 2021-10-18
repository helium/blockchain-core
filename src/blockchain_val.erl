%%% ===========================================================================
%%% Generic value validation.
%%% ===========================================================================
-module(blockchain_val).

-export_type([
    key/0,
    val/0,
    size/0,
    quantifier/0,
    forall/0,
    exists/0,
    either/0,
    requirement/0,
    spec/0,
    failure/0,
    result/0
]).

-export([
     validate/1,
     validate/2,
     validate_all_defined/1
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

-type forall() :: forall | '∀'.
-type exists() :: exists | '∃'.
-type either() :: either | '∃!'.
-type quantifier() :: forall() | exists() | either().

-type requirement() ::
      {quantifier(), [requirement()]}
    | defined
    | undefined
    | {binary, size()}
    | {integer, size()}
    | {member, [any()]}
    | {address, libp2p}
    | {custom, Label :: term(), fun((val()) -> boolean())}
    | h3_string
    .

-type spec() ::
    {key(), val(), requirement()}.

-type failure() ::
      invalid_address
    | invalid_h3_string
    | {not_a_member_of, [val()]}
    | undefined
    | {not_an_integer, val()}
    | {integer_out_of_range, Actual :: integer(), Required :: size()}
    | {not_a_binary, val()}
    | {binary_wrong_size, Actual :: non_neg_integer(), Required :: size()}
    .

-type result() ::
    ok | {error, {invalid, [{key(), failure()}]}}.

-type test_result() ::
    pass | {fail, failure()}.

-spec validate([spec()]) -> result().
validate(Specs) ->
    validate_specs(Specs).

-spec validate([spec()], fun((requirement()) -> requirement())) -> result().
validate(Specs0, F) ->
    Specs1 = [{K, V, F(R)}|| {K, V, R} <- Specs0],
    validate(Specs1).

-spec validate_all_defined([spec()]) -> result().
validate_all_defined(Specs) ->
    validate(Specs, fun(R) -> {forall, [defined, R]} end).

-spec validate_specs([spec()]) -> result().
validate_specs(Specs) ->
    case lists:flatten([validate_spec(S) || S <- Specs]) of
        [] ->
            ok;
        [_|_]=Invalid ->
            {error, {invalid, Invalid}}
    end.

validate_spec({Key, Val, Requirement}) ->
    case test(Val, Requirement) of
        pass ->
            [];
        {fail, Failure} ->
            [{Key, Failure}]
    end.

-spec test(val(), requirement()) ->
    test_result().
test(V, {custom, Label, IsValid}) -> test_custom(V, Label, IsValid);
test(V, defined)                  -> test_defined(V);
test(V, undefined)                -> test_undefined(V);
test(V, {binary, SizeSpec})       -> test_binary(V, SizeSpec);
test(V, {integer, SizeSpec})      -> test_int(V, SizeSpec, integer_out_of_range);
test(V, {member, Vs})             -> test_membership(V, Vs);
test(V, {address, libp2p})        -> test_address_libp2p(V);
test(V, h3_string)                -> test_h3_string(V);
test(V, {ForAll, Requirements}) when ForAll =:= forall; ForAll =:= '∀'->
    test_forall(V, Requirements);
test(V, {Exists, Requirements}) when Exists =:= exists; Exists =:= '∃'  ->
    test_exists(V, Requirements);
test(V, {Either, Requirements}) when Either =:= either; Either =:= '∃!'  ->
    test_either(V, Requirements).

test_forall(V, Requirements) ->
    lists:foldl(
        fun (R, pass) -> test(V, R);
            (_, {fail, _}=Failed) -> Failed
        end,
        pass,
        Requirements
    ).

test_exists(V, Requirements) ->
    lists:foldl(
        fun (_, pass) -> pass;
            (R, {fail, _}) -> test(V, R)
        end,
        case Requirements of
            [] ->
                pass;
            [_|_] ->
                %% XXX Init failure must never escape this foldl
                {fail, {'BUG_IN', {?MODULE, 'test_exists', ?LINE}}}
        end,
        Requirements
    ).

test_either(V, Requirements) ->
    Results = [test(V, R) || R <- Requirements],
    case lists:filter(fun res_to_bool/1, Results) of
        [] -> {fail, zero_requirements_passed};
        [_] -> pass;
        [_|_] -> {fail, multiple_requirements_passed}
    end.

test_custom(V, Label, IsValid) ->
    case IsValid(V) of
        true -> pass;
        false -> {fail, Label}
    end.

test_defined(undefined) ->
    {fail, undefined};
test_defined(_) ->
    pass.

test_undefined(undefined) ->
    pass;
test_undefined(_) ->
    {failed, defined}.

test_binary(V, SizeSpec) ->
    case is_binary(V) of
        false ->
            {fail, {not_a_binary, V}};
        true ->
            Size = byte_size(V),
            test_int(Size, SizeSpec, binary_wrong_size)
    end.

-spec test_int(integer(), size(), atom()) ->
    test_result().
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

test_membership(V, Vs) ->
    res_of_bool(lists:member(V, Vs), {not_a_member_of, Vs}).

test_address_libp2p(V) ->
    try libp2p_crypto:bin_to_pubkey(V) of
        _ -> pass
    catch
        _:_ -> {fail, invalid_address}
    end.

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
            {fail, zero_requirements_passed},
            test(5, {either, [{integer, {max, 1}}, {integer, {exact, 10}}]})
        ),
        ?_assertMatch(
            {fail, multiple_requirements_passed},
            test(5, {either, [{integer, any}, {integer, any}]})
        ),
        ?_assertMatch(
            {fail, multiple_requirements_passed},
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
        ?_assertEqual(ok, validate([{Key, bar, RequireBar}])),
        ?_assertEqual(
            {error, {invalid, [{Key, not_bar}]}},
            validate([{Key, baz, RequireBar}])
        )
    ].

defined_test_() ->
    Requirement = defined,
    Key = foo,
    [
        ?_assertEqual(pass, test(bar, Requirement)),
        ?_assertEqual({fail, undefined}, test(undefined, Requirement)),
        ?_assertEqual(ok, validate([{Key, bar, Requirement}])),
        ?_assertEqual(
            {error, {invalid, [{Key, undefined}]}},
            validate([{Key, undefined, Requirement}])
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
        ?_assertEqual(ok, validate([{Key, <<>>, {binary, any}}])),
        ?_assertEqual(ok, validate([{Key, <<>>, {binary, {exact, 0}}}])),
        ?_assertEqual(
            {error, {invalid, [{Key, {binary_wrong_size, 0, {range, 8, 1024}}}]}},
            validate([{Key, <<>>, {binary, {range, 8, 1024}}}])
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

addr_gen() ->
    #{public := PK, secret := _} =
        libp2p_crypto:generate_keys(ecc_compact),
    libp2p_crypto:pubkey_to_bin(PK).

-endif.
