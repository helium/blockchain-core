-module(kvl).

-export_type([
    t/2
]).

-export([
    empty/0,
    get/2,
    set/3,
    map/2,
    serialize/1,
    serialize/2,
    deserialize/1,
    deserialize/2
]).

-type t(K, V) ::
    [{K, V}].

-spec empty() -> [{_, _}].
empty() ->
    [].

-spec map([{K0, V0}], fun(({K0, V0}) -> {K1, V1})) -> [{K1, V1}].
map(T, F) ->
    lists:map(F, T).

-spec serialize([{K, V}], fun(({K, V}) -> {iolist(), iolist()})) -> iolist().
serialize(T, F) ->
    serialize(map(T, F)).

-spec serialize([{iolist(), iolist()}]) -> iolist().
serialize(T) ->
    Data = lists:map(fun pair_to_iolist/1, T),
    Size = iolist_size(Data),
    [<<Size:64/integer-unsigned-big>>, Data].

%% TODO Error handling
-spec deserialize(binary(), fun(({binary(), binary()}) -> {K, V})) -> [{K, V}].
deserialize(Bin, F) ->
    {BinBinPairs, Rest} = deserialize(Bin),
    {map(BinBinPairs, F), Rest}.

%% TODO Error handling
-spec deserialize(binary()) -> {[{binary(), binary()}], binary()}.
deserialize(<<Size:64/integer-unsigned-big, Data:Size/binary, Rest/binary>>) ->
    {decode(Data, empty()), Rest}.

%% TODO Error handling
-spec decode(binary(), T) -> T when T :: [{binary(), binary()}].
decode(<<>>, T) ->
    T;
decode(
    <<
        SizeK:32/integer-unsigned-little, K:SizeK/binary,
        SizeV:32/integer-unsigned-little, V:SizeV/binary,
        Rest/binary
    >>,
    T
) ->
    decode(Rest, set(T, K, V)).

-spec get([{K, V}], K) -> none | {some, V}.
get(T, K) ->
    case lists:keyfind(K, 1, T) of
        false ->
            none;
        {K, V} ->
            {some, V}
    end.

-spec set([{K, V}], K, V) ->
    [{K, V}].
set(T, K, V) ->
    lists:keystore(K, 1, T, {K, V}).

-spec pair_to_iolist({iolist(), iolist()}) -> iolist().
pair_to_iolist({K, V}) ->
    lists:map(fun iolist_to_frame/1, [K, V]).

-spec iolist_to_frame(iolist()) -> iolist().
iolist_to_frame(Data) ->
    Size = iolist_size(Data),
    [<<Size:32/integer-unsigned-little>>, Data].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

serialization_test_() ->
    AtomAtom =
        [
            {k1, v1},
            {k2, v2},
            {k3, v3},
            {k4, v4},
            {k5, v5}
        ],
    AtomInt =
        [
            {foobarbazquux, 17687658765646747},
            {a, 1},
            {b, 2},
            {c, 3},
            {d, 4},
            {e, 5}
        ],
    [
        ?_assertEqual({[], <<>>}, deserialize(iolist_to_binary(serialize([])))),

        ?_assertEqual(
           {[{<<"k">>, <<"v">>}], <<>>},
            deserialize(iolist_to_binary(serialize([{["k", []], "v"}])))
        ),

        ?_assertEqual(
           {AtomAtom, <<>>},
            deserialize(
                iolist_to_binary(serialize(
                    AtomAtom,
                    fun ({K, V}) -> {atom_to_binary(K), atom_to_binary(V)} end
                )),
                fun({<<K/binary>>, <<V/binary>>}) ->
                    {binary_to_atom(K), binary_to_atom(V)}
                end
            )
        ),

        ?_assertEqual(
           {AtomInt, <<>>},
            deserialize(
                iolist_to_binary(serialize(
                    AtomInt,
                    fun ({K, V}) -> {atom_to_binary(K), <<V:64/integer>>} end
                )),
                fun({<<K/binary>>, <<V:64/integer>>}) ->
                    {binary_to_atom(K), V}
                end
            )
        )
    ].

-endif.
