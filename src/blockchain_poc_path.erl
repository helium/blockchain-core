%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC Path ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc_path).

-export([
    build/4,
    shortest/3,
    length/3,
    build_graph/3,
    target/3
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RESOLUTION, 8).
-define(RING_SIZE, 2).
% KRing of 1
%     Scale 3.57
%     Max distance 1.028 miles @ resolution 8
%     Max distance 0.38 miles @ resolution 9

% KRing of 2
%     Scale 5.42
%     Max distance 1.564 miles @ resolution 8 <---
%     Max distance 0.59 miles @ resolution 9

-type graph() :: #{any() => [{number(), any()}]}.

-export_type([graph/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build(Hash :: binary(),
            Target :: binary(),
            Gateways :: map(),
            Height :: non_neg_integer()) -> {ok, list()} | {error, any()}.
build(Hash, Target, Gateways, Height) ->
    Graph = ?MODULE:build_graph(Target, Gateways, Height),
    GraphList = maps:fold(
        fun(Addr, _, Acc) ->
            case Addr == Target of
                true ->
                    Acc;
                false ->
                    G = maps:get(Addr, Gateways),
                    {_, _, Score} = blockchain_ledger_gateway_v1:score(G, Height),
                    [{Score, Addr}|Acc]
            end
        end,
        [],
        Graph
    ),
    case erlang:length(GraphList) >= 2 of
        false ->
            lager:error("target/gateways ~p", [{Target, Gateways}]),
            lager:error("graph: ~p GraphList ~p", [Graph, GraphList]),
            {error, not_enough_gateways};
        true ->
            [{_, Start}, {_, End}|_] = lists:sort(
                fun({ScoreA, AddrA}, {ScoreB, AddrB}) ->
                    ScoreA * ?MODULE:length(Graph, Target, AddrA) >
                    ScoreB * ?MODULE:length(Graph, Target, AddrB)
                end,
                blockchain_utils:shuffle_from_hash(Hash, GraphList)
            ),
            {_, Path1} = ?MODULE:shortest(Graph, Start, Target),
            {_, [Target|Path2]} = ?MODULE:shortest(Graph, Target, End),
            %% NOTE: It is possible the path contains dupes, these are also considered valid
            Path3 = Path1 ++ Path2,
            case erlang:length(Path3) > 2 of
                false ->
                    lager:error("target/gateways ~p", [{Target, Gateways}]),
                    lager:error("graph: ~p GraphList ~p", [Graph, GraphList]),
                    lager:error("path: ~p", [Path3]),
                    {error, path_too_small};
                true ->
                    blockchain_utils:rand_from_hash(Hash),
                    case rand:uniform(2) of
                        1 ->
                            {ok, Path3};
                        2 ->
                            {ok, lists:reverse(Path3)}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec shortest(Graph :: graph(), Start :: any(), End :: any()) -> {number(), list()}.
shortest(Graph, Start, End) ->
    path(Graph, [{0, [Start]}], End, #{}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec length(Graph :: graph(), Start :: any(), End :: any()) -> integer().
length(Graph, Start, End) ->
    {_Cost, Path} = ?MODULE:shortest(Graph, Start, End),
    erlang:length(Path).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build_graph(Address :: binary(), Gateways :: map(), Height :: non_neg_integer()) -> graph().
build_graph(Address, Gateways, Height) ->
    build_graph([Address], Gateways, Height, maps:new()).

-spec build_graph([binary()], Gateways :: map(), Height :: non_neg_integer(), Graph :: graph()) -> graph().
build_graph([], _Gateways, _Height, Graph) ->
    Graph;
build_graph([Address0|Addresses], Gateways, Height, Graph0) ->
    Neighbors0 = neighbors(Address0, Gateways, Height),
    Graph1 = lists:foldl(
        fun({_W, Address1}, Acc) ->
            case maps:is_key(Address1, Acc) of
                true ->
                    Acc;
                false ->
                    Neighbors1 = neighbors(Address1, Gateways, Height),
                    Graph1 = maps:put(Address1, Neighbors1, Acc),
                    build_graph([A || {_, A} <- Neighbors1], Gateways, Height, Graph1)
            end
        end,
        maps:put(Address0, Neighbors0, Graph0),
        Neighbors0
    ),
    case maps:size(Graph1) > 100 of
        false ->
            build_graph(Addresses, Gateways, Height, Graph1);
        true ->
            Graph1
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec path(Graph :: graph(),
           Path :: list(),
           End :: any(),
           Seen :: map()) -> {number(), list()}.
path(_Graph, [], _End, _Seen) ->
    % nowhere to go
    {0, []};
path(_Graph, [{Cost, [End | _] = Path} | _], End, _Seen) ->
    % base case
    {Cost, lists:reverse(Path)};
path(Graph, [{Cost, [Node | _] = Path} | Routes], End, Seen) ->
    NewRoutes = [{Cost + NewCost, [NewNode | Path]} || {NewCost, NewNode} <- maps:get(Node, Graph, [{0, []}]), not maps:get(NewNode, Seen, false)],
    path(Graph, lists:sort(NewRoutes ++ Routes), End, Seen#{Node => true}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
neighbors(Address, Gateways, Height) ->
    TargetGw = maps:get(Address, Gateways),
    Index = blockchain_ledger_gateway_v1:location(TargetGw),
    KRing = case h3:get_resolution(Index) of
        Res when Res < ?RESOLUTION ->
            [];
        ?RESOLUTION ->
            h3:k_ring(Index, ?RING_SIZE);
        Res when Res > ?RESOLUTION ->
            Parent = h3:parent(Index, ?RESOLUTION),
            h3:k_ring(Parent, ?RING_SIZE)
    end,
    GwInRing = maps:to_list(maps:filter(
        fun(A, G) ->
            case blockchain_ledger_gateway_v1:location(G) of
                undefined -> false;
                I ->
                    I1 = case h3:get_resolution(I) of
                             R when R =< ?RESOLUTION -> I;
                             R when R > ?RESOLUTION -> h3:parent(I, ?RESOLUTION)
                         end,
                    lists:member(I1, KRing)
                    andalso Address =/= A
            end
        end,
        Gateways
    )),
    [{edge_weight(TargetGw, G, Height), A} || {A, G} <- GwInRing].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec edge_weight(Gw1 :: blockchain_ledger_gateway_v1:gateway(),
                  Gw2 :: blockchain_ledger_gateway_v1:gateway(),
                  Height :: non_neg_integer()) -> float().
edge_weight(Gw1, Gw2, Height) ->
    io:format("Gw1: ~p~n", [Gw1]),
    {_, _, S1} = blockchain_ledger_gateway_v1:score(Gw1, Height),
    {_, _, S2} = blockchain_ledger_gateway_v1:score(Gw2, Height),
    1 - abs(prob_fun(S1) -  prob_fun(S2)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec target(Hash :: binary(),
             Ledger :: blockchain_ledger_v1:ledger(), libp2p_crypto:pubkey_bin()) -> {libp2p_crypto:pubkey_bin(), map()}.
target(Hash, Ledger, Challenger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ActiveGateways = active_gateways(Ledger, Challenger),
    ProbsAndGatewayAddrs = create_probs(ActiveGateways, Height),
    Entropy = entropy(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    Target = select_target(ProbsAndGatewayAddrs, RandVal),
    {Target, ActiveGateways}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_probs(Gateways :: map(), Height :: non_neg_integer()) -> [{float(), libp2p_crypto:pubkey_bin()}].
create_probs(Gateways, Height) ->
    %% GwScores = [{A, prob_fun(blockchain_ledger_gateway_v1:score(G))} || {A, G} <- maps:to_list(Gateways)],
    GwScores = lists:foldl(fun({A, G}, Acc) ->
                                   {_, _, Score} = blockchain_ledger_gateway_v1:score(G, Height),
                                   [{A, prob_fun(Score)} | Acc]
                           end,
                           [],
                           maps:to_list(Gateways)),
    Scores = [S || {_A, S} <- GwScores],
    LenGwScores = erlang:length(GwScores),
    SumGwScores = lists:sum(Scores),
    [{prob(Score, LenGwScores, SumGwScores), GwAddr} || {GwAddr, Score} <- GwScores].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec entropy(Entropy :: binary()) -> rand:state().
entropy(Entropy) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Entropy),
    rand:seed_s(exs1024s, {A, B, C}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec active_gateways(blockchain_ledger_v1:ledger(), libp2p_crypto:pubkey_bin()) ->  #{libp2p_crypto:pubkey_bin() => blockchain_ledger_gateway_v1:gateway()}.
active_gateways(Ledger, Challenger) ->
    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    maps:fold(
        fun(PubkeyBin, Gateway, Acc0) ->
            % TODO: Maybe do some find of score check here
            case
                PubkeyBin == Challenger orelse
                blockchain_ledger_gateway_v1:location(Gateway) == undefined orelse
                maps:is_key(PubkeyBin, Acc0)
            of
                true ->
                    Acc0;
                false ->
                    Graph = ?MODULE:build_graph(PubkeyBin, Gateways, Height),
                    case maps:size(Graph) > 2 of
                        false ->
                            Acc0;
                        true ->
                            maps:fold(
                                fun(Addr, Neighbors, Acc1) ->
                                    Acc2 = case Addr == Challenger of
                                        true ->
                                            Acc1;
                                        false ->
                                            maps:put(Addr, maps:get(Addr, Gateways), Acc1)
                                    end,
                                    lists:foldl(
                                        fun({_, Neighbor}, Acc3) ->
                                            case Neighbor == Challenger of
                                                true ->
                                                    Acc3;
                                                false ->
                                                    maps:put(Neighbor, maps:get(Neighbor, Gateways), Acc3)
                                            end
                                        end,
                                        Acc2,
                                        Neighbors
                                    )
                                end,
                                Acc0,
                                Graph
                            )
                    end
            end
        end,
        #{},
        Gateways
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
select_target([{Prob1, GwAddr1}=_Head | _], Rnd) when Rnd - Prob1 < 0 ->
   GwAddr1;
select_target([{Prob1, _GwAddr1} | Tail], Rnd) ->
   select_target(Tail, Rnd - Prob1).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec prob(Score :: float(),
           LenScores :: pos_integer(),
           SumScores :: float()) -> float().
prob(Score, _LenScores, SumScores) ->
    Score / SumScores.

%%--------------------------------------------------------------------
%% @doc An adjustment curve which favors hotspots closer to a score of 0.25,
%% when selecting a target
%% @end
%%--------------------------------------------------------------------
prob_fun(Score) when Score =< 0.25 ->
    -16 * math:pow((Score - 0.25), 2) + 1;
prob_fun(Score) ->
    -1.77 * math:pow((Score - 0.25), 2) + 1.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

target_test() ->
    {timeout,
     60000,
     fun() ->
             io:format("Target test~n"),
             BaseDir = test_utils:tmp_dir("target_test"),
             io:format("BaseDir: ~p~n", [BaseDir]),
             Ledger = blockchain_ledger_v1:new(BaseDir),
             io:format("Ledger: ~p~n", [Ledger]),
             Ledger1 = blockchain_ledger_v1:new_context(Ledger),
             io:format("Ledger1: ~p~n", [Ledger1]),

             meck:new(blockchain_swarm, [passthrough]),
             meck:expect(blockchain_swarm, pubkey_bin, fun() ->
                                                               <<"yolo">>
                                                       end),

             Gateways = [{O, G} || {{O, _}, {G, _}} <- lists:zip(test_utils:generate_keys(4), test_utils:generate_keys(4))],
             io:format("Gateways: ~p~n", [Gateways]),

             lists:map(fun({Owner, Gw}) ->
                               blockchain_ledger_v1:add_gateway(Owner, Gw, 16#8c283475d4e89ff, 0, Ledger1)
                       end, Gateways),
             blockchain_ledger_v1:commit_context(Ledger1),

             Iterations = 500,
             Results = dict:to_list(lists:foldl(fun(_, Acc) ->
                                                        {Target, _} = target(crypto:strong_rand_bytes(32), Ledger1, <<>>),
                                                        dict:update_counter(Target, 1, Acc)
                                                end,
                                                dict:new(),
                                                lists:seq(1, Iterations))),

             lists:foreach(
               fun({_Gw, Count}) ->
                       Prob = Count/Iterations,
                       ?assert(Prob < 0.27),
                       ?assert(Prob > 0.23)
               end,
               Results
              ),

             ?assert(meck:validate(blockchain_swarm)),
             meck:unload(blockchain_swarm),
             ok
     end}.

neighbors_test() ->
    LatLongs = [
        {{37.782061, -122.446167}, 1.0, 1.0}, % This should be excluded cause target
        {{37.782604, -122.447857}, 1000.0, 0.1},
        {{37.782074, -122.448528}, 1000.0, 0.1},
        {{37.782002, -122.44826}, 1000.0, 0.1},
        {{37.78207, -122.44613}, 1000.0, 0.1},
        {{37.781909, -122.445411}, 1000.0, 0.1},
        {{37.783371, -122.447879}, 1000.0, 0.1},
        {{37.780827, -122.44716}, 1000.0, 0.1},
        {{38.897675, -77.036530}, 100.0, 10.0} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),
    Neighbors = neighbors(Target, Gateways, 1),

    ?assertEqual(erlang:length(maps:keys(Gateways)) - 3, erlang:length(Neighbors)),
    {LL1, _, _} = lists:last(LatLongs),
    TooFar = crypto:hash(sha256, erlang:term_to_binary(LL1)),
    lists:foreach(
        fun({_, Address}) ->
            ?assert(Address =/= Target),
            ?assert(Address =/= TooFar)
        end,
        Neighbors
    ),
    ok.

build_graph_test() ->
    LatLongs = [
        {{37.782061, -122.446167}, 1.0, 1.0}, % This should be excluded cause target
        {{37.782604, -122.447857}, 1000.0, 0.1},
        {{37.782074, -122.448528}, 1000.0, 0.1},
        {{37.782002, -122.44826}, 1000.0, 0.1},
        {{37.78207, -122.44613}, 1000.0, 0.1},
        {{37.781909, -122.445411}, 1000.0, 0.1},
        {{37.783371, -122.447879}, 1000.0, 0.1},
        {{37.780827, -122.44716}, 1000.0, 0.1},
        {{38.897675, -77.036530}, 100.0, 10.0} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),

    Graph = build_graph(Target, Gateways, 1),
    ?assertEqual(8, maps:size(Graph)),

    {LL1, _, _} = lists:last(LatLongs),
    TooFar = crypto:hash(sha256, erlang:term_to_binary(LL1)),
    ?assertNot(lists:member(TooFar, maps:keys(Graph))),
    ok.


build_graph_in_line_test() ->
    % All these point are in a line one after the other (except last)
    LatLongs = [
        {{37.780586, -122.469471}, 1.0, 1.0},
        {{37.780959, -122.467496}, 1000.0, 0.1},
        {{37.78101, -122.465372}, 1000.0, 0.1},
        {{37.781179, -122.463226}, 1000.0, 0.1},
        {{37.781281, -122.461038}, 1000.0, 0.1},
        {{37.781349, -122.458892}, 1000.0, 0.1},
        {{37.781468, -122.456617}, 1000.0, 0.1},
        {{37.781637, -122.4543}, 1000.0, 0.1},
        {{38.897675, -77.036530}, 100.0, 10.0} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),

    Graph = build_graph(Target, Gateways, 1),
    ?assertEqual(8, maps:size(Graph)),

    {LL1, _, _} = lists:last(LatLongs),
    TooFar = crypto:hash(sha256, erlang:term_to_binary(LL1)),
    ?assertNot(lists:member(TooFar, maps:keys(Graph))),

    Addresses = lists:droplast([crypto:hash(sha256, erlang:term_to_binary(X)) || {X, _, _} <- LatLongs]),
    Size = erlang:length(Addresses),

    lists:foldl(
        fun(Address, Acc) when Acc =:= 1 ->
            Next = lists:nth(Acc + 1, Addresses),
            GraphPart = maps:get(Address, Graph, []),
            ?assert(lists:member(Next, [A || {_, A} <- GraphPart])),
            Acc + 1;
        (Address, Acc) when Size =:= Acc ->
            Prev = lists:nth(Acc - 1, Addresses),
            GraphPart = maps:get(Address, Graph, []),
            ?assert(lists:member(Prev, [A || {_, A} <- GraphPart])),
            0;
        (Address, Acc) ->
            % Each hotspot should at least see the next / prev one
            Next = lists:nth(Acc + 1, Addresses),
            Prev = lists:nth(Acc - 1, Addresses),
            GraphPart = maps:get(Address, Graph, []),
            ?assert(lists:member(Next, [A || {_, A} <- GraphPart])),
            ?assert(lists:member(Prev, [A || {_, A} <- GraphPart])),
            Acc + 1
        end,
        1,
        Addresses
    ),
    ok.

build_test() ->
    % All these point are in a line one after the other (except last)
    LatLongs = [
        {{37.780959, -122.467496}, 200.0, 10.0},
        {{37.78101, -122.465372}, 300.0, 10.0},
        {{37.780586, -122.469471}, 1000.0, 10.0},
        {{37.781179, -122.463226}, 1000.0, 500.0},
        {{37.781281, -122.461038}, 10.0, 1000.0},
        {{37.781349, -122.458892}, 100.0, 50.0},
        {{37.781468, -122.456617}, 100.0, 40.0},
        {{37.781637, -122.4543}, 1000.0, 20.0},
        {{38.897675, -77.036530}, 100.0, 30.0} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),

    {ok, Path} = build(crypto:strong_rand_bytes(32), Target, Gateways, 1),

    ?assertNotEqual(Target, hd(Path)),
    ?assert(lists:member(Target, Path)),
    ?assertNotEqual(Target, lists:last(Path)),
    ok.

build_only_2_test() ->
    % All these point are in a line one after the other
    LatLongs = [
        {{37.780959, -122.467496}, 1000.0, 100.0},
        {{37.78101, -122.465372}, 10.0, 1000.0},
        {{37.780586, -122.469471}, 100.0, 20.0}
    ],
    {Target, Gateways} = build_gateways(LatLongs),

    {ok, Path} = build(crypto:strong_rand_bytes(32), Target, Gateways, 1),

    ?assertNotEqual(Target, hd(Path)),
    ?assert(lists:member(Target, Path)),
    ?assertNotEqual(Target, lists:last(Path)),
    ok.

build_prob_test_() ->
    {timeout,
     60000,
     fun() ->
             LatLongs = [
                         {{37.780586, -122.469471}, 1.0, 1.0},
                         {{37.780959, -122.467496}, 1.0, 1.0},
                         {{37.78101, -122.465372}, 1.0, 1.0},
                         {{37.78102, -122.465372}, 1.0, 1.0},
                         {{37.78103, -122.465372}, 1.0, 1.0},
                         {{37.78104, -122.465372}, 1.0, 1.0}
                        ],
             {Target, Gateways} = build_gateways(LatLongs),

             Iteration = 1000,
             Size = erlang:length(LatLongs)-1,
             Av = Iteration / Size,

             Starters = lists:foldl(
                          fun(_, Acc) ->
                                  {ok, [P1|_]} = blockchain_poc_path:build(crypto:strong_rand_bytes(64), Target, Gateways, 1),
                                  V = maps:get(P1, Acc, 0),
                                  maps:put(P1, V+1, Acc)
                          end,
                          #{},
                          lists:seq(1, Iteration)
                         ),

             io:format("Starters: ~p~n", [Starters]),

             ?assertEqual(Size, maps:size(Starters)),

             maps:fold(
               fun(_, V, _) ->
                       ?assert(V >= Av-(Av/10) orelse V =< Av+(Av/10))
               end,
               ok,
               Starters
              ),
             ok
     end}.

build_failed_test() ->
    % All these point are in a line one after the other (except last)
    LatLongs = [
        {{37.780959, -122.467496}, 1000.0, 10.0},
        {{37.78101, -122.465372}, 10.0, 1000.0},
        {{12.780586, -122.469471}, 1000.0, 20.0}
    ],
    {Target, Gateways} = build_gateways(LatLongs),
    ?assertEqual({error, not_enough_gateways}, build(crypto:strong_rand_bytes(32), Target, Gateways, 1)),
    ok.

build_with_zero_score_test() ->
    % All these point are in a line one after the other (except last)
    LatLongs = [
        {{37.780586, -122.469471}, 1.0, 1.0},
        {{37.780959, -122.467496}, 1.0, 1.0},
        {{37.78101, -122.465372}, 1.0, 1.0},
        {{37.781179, -122.463226}, 1.0, 1.0},
        {{37.781281, -122.461038}, 1.0, 1.0},
        {{37.781349, -122.458892}, 1.0, 1.0},
        {{37.781468, -122.456617}, 1.0, 1.0},
        {{37.781637, -122.4543}, 1.0, 1.0},
        {{38.897675, -77.036530}, 1.0, 1.0} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),
    {ok, Path} = build(crypto:strong_rand_bytes(32), Target, Gateways, 1),
    ?assert(lists:member(Target, Path)),
    ok.

build_with_zero_score_2_test() ->
    % All these point are together
    LatLongs = [
        {{48.854918, 2.345903}, 1.0, 1.0},
        {{48.854918, 2.345902}, 1.0, 1.0},
        {{48.852969, 2.349872}, 1.0, 1.0},
        {{48.855425, 2.344980}, 1.0, 1.0},
        {{48.854127, 2.344637}, 1.0, 1.0},
        {{48.855228, 2.347126}, 1.0, 1.0}
    ],
    {Target, Gateways} = build_gateways(LatLongs),
    {ok, Path} = build(crypto:strong_rand_bytes(32), Target, Gateways, 1),
    ?assertEqual(3, erlang:length(Path)),
    [_P1, P2, _P3] = Path,
    ?assertEqual(Target, P2),
    ok.

active_gateways_test() ->
    % 2 First points are grouped together and next ones form a group also
    LatLongs = [
        {{48.858391, 2.294469}, 1.0, 1.0},
        {{48.856696, 2.293997}, 1.0, 1.0},
        {{48.852969, 2.349872}, 1.0, 1.0},
        {{48.855425, 2.344980}, 1.0, 1.0},
        {{48.854127, 2.344637}, 1.0, 1.0},
        {{48.855228, 2.347126}, 1.0, 1.0}
    ],
    {_Target0, Gateways} = build_gateways(LatLongs),

    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1,
                active_gateways,
                fun(_) ->
                        Gateways
                end),
    meck:expect(blockchain_ledger_v1,
                current_height,
                fun(_) ->
                        {ok, 1}
                end),

    [{LL0, _, _}, {LL1, _, _}, {LL2, _, _}|_] = LatLongs,
    Challenger = crypto:hash(sha256, erlang:term_to_binary(LL2)),
    ActiveGateways = active_gateways(fake_ledger, Challenger),

    ?assertNot(maps:is_key(Challenger, ActiveGateways)),
    ?assertNot(maps:is_key(crypto:hash(sha256, erlang:term_to_binary(LL0)), ActiveGateways)),
    ?assertNot(maps:is_key(crypto:hash(sha256, erlang:term_to_binary(LL1)), ActiveGateways)),
    ?assertEqual(3, maps:size(ActiveGateways)),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ok.

build_gateways(LatLongs) ->
    Gateways = lists:foldl(
        fun({LatLong, Alpha, Beta}, Acc) ->
            Owner = <<"test">>,
            Address = crypto:hash(sha256, erlang:term_to_binary(LatLong)),
            Res = rand:uniform(7) + 8,
            Index = h3:from_geo(LatLong, Res),
            G0 = blockchain_ledger_gateway_v1:new(Owner, Index),
            G1 = blockchain_ledger_gateway_v1:set_alpha_beta_delta(Alpha, Beta, 1, G0),
            maps:put(Address, G1, Acc)

        end,
        maps:new(),
        LatLongs
    ),
    [{LL, _, _}|_] = LatLongs,
    Target = crypto:hash(sha256, erlang:term_to_binary(LL)),
    {Target, Gateways#{crypto:strong_rand_bytes(32) => blockchain_ledger_gateway_v1:new(<<"test">>, undefined)}}.

-endif.
