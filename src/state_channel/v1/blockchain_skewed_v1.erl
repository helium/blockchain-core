-module(blockchain_skewed_v1).

-export([
         serialize/1,
         deserialize/1
        ]).

-include_lib("helium_proto/include/skewed_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% 1:1 match with skewed records from merkerl

-record(leaf, {
    hash :: skewed:hash(),
    value :: any()
}).

-record(empty, {
    hash = <<0:256>> :: skewed:hash()
}).

-record(node, {
    hash :: skewed:hash(),
    height = 0 :: non_neg_integer(),
    left :: #node{} | #empty{},
    right :: #leaf{}
}).

-record(skewed, {
    root :: #empty{} | #node{},
    count = 0 :: non_neg_integer()
}).

-type skewed_pb() :: #skewed_pb{}.

-spec serialize(skewed:skewed()) -> skewed_pb().
serialize(Skewed) ->
    build_proto(Skewed).

-spec deserialize(skewed_pb()) -> skewed:skewed().
deserialize(Skewed) ->
    build_skewed(Skewed).

build_proto(#skewed{root=Root, count=Count}) ->
    #skewed_pb{root=build_proto(Root), count=Count};
build_proto(#node{hash=Hash, height=Height, left=Left, right=Right}) ->
    {node, #node_pb{hash=Hash, height=Height, left=build_proto(Left), right=build_proto(Right)}};
build_proto(#leaf{hash=Hash, value=Value}) ->
    {leaf, #leaf_pb{hash=Hash, value=Value}};
build_proto(#empty{hash=Hash}) ->
    {empty, #empty_pb{hash=Hash}}.

build_skewed(#skewed_pb{root=Node, count=Count}) ->
    #skewed{root=build_skewed(Node), count=Count};
build_skewed({node, #node_pb{hash=Hash, height=Height, left=Left, right=Right}}) ->
    #node{hash=Hash, height=Height, left=build_skewed(Left), right=build_skewed(Right)};
build_skewed({leaf, #leaf_pb{hash=Hash, value=Value}}) ->
    #leaf{hash=Hash, value=Value};
build_skewed({empty, #empty_pb{hash=Hash}}) ->
    #empty{hash=Hash}.

-ifdef(TEST).

new_test() ->
    Skewed = skewed:new(),
    SkewedPb = serialize(Skewed),
    io:format("Skewed: ~p~n", [Skewed]),
    io:format("SkewedPB: ~p~n", [SkewedPb]),
    Deserialized = deserialize(SkewedPb),
    ?assertEqual(#skewed_pb{root={empty, #empty_pb{hash= <<0:256>>}}, count=0}, SkewedPb),
    ?assertEqual(Deserialized, Skewed).

new2_test() ->
    HashFun = fun skewed:hash_value/1,
    Size = 5,
    Tree = lists:foldl(
        fun(Value, Acc) ->
            skewed:add(Value, HashFun, Acc)
        end,
        skewed:new(),
        lists:seq(1, Size)
    ),
    SkewedTree = serialize(Tree),
    Deserialized = deserialize(SkewedTree),
    ?assertEqual(Deserialized, Tree).

-endif.
