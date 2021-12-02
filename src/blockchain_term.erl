-module(blockchain_term).

-export([
    from_bin/1,
    binary_to_proplist/1
]).

-export_type([
    t/0
]).

-type t() :: term().

-spec from_bin(binary()) -> t().
from_bin(<<_/binary>>) ->
    error(not_implemented).

-spec binary_to_proplist(binary()) -> term().
binary_to_proplist(<<131, 108, Length:32/integer-unsigned-big, Rest/binary>>) ->
    {Res, <<>>} = decode_list(Rest, Length, []),
    Res.

decode_list(<<106, Rest/binary>>, 0, Acc) ->
    {lists:reverse(Acc), Rest};
decode_list(Rest, 0, Acc) ->
    %% tuples don't end with an empty list
    {lists:reverse(Acc), Rest};
decode_list(<<104, Size:8/integer, Bin/binary>>, Length, Acc) ->
    {List, Rest} = decode_list(Bin, Size, []),
    decode_list(Rest, Length - 1, [list_to_tuple(List)|Acc]);
decode_list(<<108, L2:32/integer-unsigned-big, Bin/binary>>, Length, Acc) ->
    {List, Rest} = decode_list(Bin, L2, []),
    decode_list(Rest, Length - 1, [List|Acc]);
decode_list(<<106, Rest/binary>>, Length, Acc) ->
    %% sometimes there's an embedded empty list
    decode_list(Rest, Length - 1, [[] |Acc]);
decode_list(Bin, Length, Acc) ->
    {Val, Rest} = decode_value(Bin),
    decode_list(Rest, Length -1, [Val|Acc]).

decode_value(<<97, Integer:8/integer, Rest/binary>>) ->
    {Integer, Rest};
decode_value(<<98, Integer:32/integer-big, Rest/binary>>) ->
    {Integer, Rest};
decode_value(<<100, AtomLen:16/integer-unsigned-big, Atom:AtomLen/binary, Rest/binary>>) ->
    {binary_to_atom(Atom, latin1), Rest};
decode_value(<<109, Length:32/integer-unsigned-big, Bin:Length/binary, Rest/binary>>) ->
    {Bin, Rest};
decode_value(<<110, N:8/integer, Sign:8/integer, Int:N/binary, Rest/binary>>) ->
    case decode_bigint(Int, 0, 0) of
        X when Sign == 0 ->
            {X, Rest};
        X when Sign == 1 ->
            {X * -1, Rest}
    end;
decode_value(<<111, N:32/integer-unsigned-big, Sign:8/integer, Int:N/binary, Rest/binary>>) ->
    case decode_bigint(Int, 0, 0) of
        X when Sign == 0 ->
            {X, Rest};
        X when Sign == 1 ->
            {X * -1, Rest}
    end;
decode_value(<<116, Arity:32/integer-unsigned-big, MapAndRest/binary>>) ->
    decode_map(MapAndRest, Arity, #{}).

decode_bigint(<<>>, _, Acc) ->
    Acc;
decode_bigint(<<B:8/integer, Rest/binary>>, Pos, Acc) ->
    decode_bigint(Rest, Pos + 1, Acc + (B bsl (8 * Pos))).

decode_map(Rest, 0, Acc) ->
    {Acc, Rest};
decode_map(Bin, Arity, Acc) ->
    {Key, T1} = decode_value(Bin),
    {Value, T2} = decode_value(T1),
    decode_map(T2, Arity - 1, maps:put(Key, Value, Acc)).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

nest(_, X, 0) -> X;
nest(F, X, N) -> nest(F, F(X), N - 1).

binary_to_proplist_test_() ->
    %% TODO After converting to exceptionless result:t/2, convert to cartesian product of keys and values.
    %%      So we can easier explore the input domain.
    [
        ?_assertMatch(Term, binary_to_proplist(term_to_binary(Term)))
    ||
        Term <- [
            [{1, [<<>>]}],
            [{1, [<<"abcdefghijklmnopqrstuvwxyz">>]}],
            [{786587658765876587, [<<"abcdefghijklmnopqrstuvwxyz">>]}],
            [{k, v}],
            [{hello, goodbye}],
            [{foo, []}],
            [{}],
            nest(fun (X) -> [X] end, {}, 100),
            [[]],
            [[[]]],
            [[[[]]]],
            nest(fun (X) -> [X] end, [], 100)
        ]
    ].

-endif.
