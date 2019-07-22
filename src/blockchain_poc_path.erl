%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC Path ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc_path).

-export([
         build/5,
         shortest/3,
         length/3,
         build_graph/4,
         target/3
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% KRing of 1
%     Scale 3.57
%     Max distance 1.028 miles @ resolution 8
%     Max distance 0.38 miles @ resolution 9

% KRing of 2
%     Scale 5.42
%     Max distance 1.564 miles @ resolution 8 <---
%     Max distance 0.59 miles @ resolution 9
%
% KRing of 3
%   Scale: unknown
%   Max distance: unknown, presumably larger than 1.54 miles

-type graph() :: #{any() => [{number(), any()}]}.

-export_type([graph/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build(Hash :: binary(),
            Target :: binary(),
            Gateways :: map(),
            Height :: non_neg_integer(),
            Ledger :: blockchain_ledger_v1:ledger()) -> {ok, list()} | {error, any()}.
build(Hash, Target, Gateways, Height, Ledger) ->
    Graph = ?MODULE:build_graph(Target, Gateways, Height, Ledger),
    GraphList = maps:fold(
                  fun(Addr, _, Acc) ->
                          case Addr == Target of
                              true ->
                                  Acc;
                              false ->
                                  G = maps:get(Addr, Gateways),
                                  {_, _, Score} = blockchain_ledger_gateway_v1:score(Addr, G, Height, Ledger),
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
-spec build_graph(Address :: binary(),
                  Gateways :: map(),
                  Height :: non_neg_integer(),
                  Ledger :: blockchain_ledger_v1:ledger()) -> graph().
build_graph(Address, Gateways, Height, Ledger) ->
    build_graph([Address], Gateways, Height, Ledger, maps:new()).

-spec build_graph([binary()],
                  Gateways :: map(),
                  Height :: non_neg_integer(),
                  Ledger :: blockchain_ledger_v1:ledger(),
                  Graph :: graph()) -> graph().
build_graph([], _Gateways, _Height, _Ledger, Graph) ->
    Graph;
build_graph([Address0|Addresses], Gateways, Height, Ledger, Graph0) ->
    Neighbors0 = neighbors(Address0, Gateways, Height, Ledger),
    Graph1 = lists:foldl(
               fun({_W, Address1}, Acc) ->
                       case maps:is_key(Address1, Acc) of
                           true ->
                               Acc;
                           false ->
                               Neighbors1 = neighbors(Address1, Gateways, Height, Ledger),
                               Graph1 = maps:put(Address1, Neighbors1, Acc),
                               build_graph([A || {_, A} <- Neighbors1], Gateways, Height, Ledger, Graph1)
                       end
               end,
               maps:put(Address0, Neighbors0, Graph0),
               Neighbors0
              ),
    case maps:size(Graph1) > 100 of
        false ->
            build_graph(Addresses, Gateways, Height, Ledger, Graph1);
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
neighbors(PubkeyBin, Gateways, Height, Ledger) ->
    Gw = maps:get(PubkeyBin, Gateways),
    GwH3 = blockchain_ledger_gateway_v1:location(Gw),
    {ok, H3ExclusionRingDist} = blockchain:config(h3_exclusion_ring_dist, Ledger),
    {ok, H3MaxGridDistance} = blockchain:config(h3_max_grid_distance, Ledger),
    {ok, H3NeighborRes} = blockchain:config(h3_neighbor_res, Ledger),
    {ok, MinScore} = blockchain:config(min_score, Ledger),
    ExclusionIndices = h3:k_ring(GwH3, H3ExclusionRingDist),
    ScaledGwH3 = h3:parent(GwH3, H3NeighborRes),

    ToInclude = lists:foldl(fun({A, G}, Acc) ->
                                    case blockchain_ledger_gateway_v1:location(G) of
                                        undefined -> Acc;
                                        Index ->
                                            case blockchain_ledger_v1:gateway_score(A, Ledger) of
                                                S when S >= MinScore ->
                                                    ScaledIndex = case h3:get_resolution(Index) of
                                                                      R when R > H3NeighborRes ->
                                                                          h3:parent(Index, H3NeighborRes);
                                                                      _ ->
                                                                          Index
                                                                  end,
                                                    case lists:member(ScaledIndex, ExclusionIndices) of
                                                        false ->
                                                            case (catch h3:grid_distance(ScaledGwH3, ScaledIndex)) of
                                                                {'EXIT', _} -> Acc;
                                                                D when D > H3MaxGridDistance -> Acc;
                                                                _ ->
                                                                    [{A, G} | Acc]
                                                            end;
                                                        true -> Acc
                                                    end;
                                                _ -> Acc
                                            end
                                    end
                            end,
                            [],
                            maps:to_list(Gateways)),

    [{edge_weight(PubkeyBin, Gw, A, G, Height, Ledger), A} || {A, G} <- ToInclude].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec edge_weight(A1 :: libp2p_crypto:pubkey_bin(),
                  Gw1 :: blockchain_ledger_gateway_v1:gateway(),
                  A2 :: libp2p_crypto:pubkey_bin(),
                  Gw2 :: blockchain_ledger_gateway_v1:gateway(),
                  Height :: non_neg_integer(),
                  Ledger :: blockchain_ledger_v1:ledger()) -> float().
edge_weight(A1, Gw1, A2, Gw2, Height, Ledger) ->
    {_, _, S1} = blockchain_ledger_gateway_v1:score(A1, Gw1, Height, Ledger),
    {_, _, S2} = blockchain_ledger_gateway_v1:score(A2, Gw2, Height, Ledger),
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
    ProbsAndGatewayAddrs = create_probs(ActiveGateways, Height, Ledger),
    Entropy = entropy(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    Target = select_target(ProbsAndGatewayAddrs, RandVal),
    {Target, ActiveGateways}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_probs(Gateways :: map(),
                   Height :: non_neg_integer(),
                   Ledger :: blockchain_ledger_v1:ledger()) -> [{float(), libp2p_crypto:pubkey_bin()}].
create_probs(Gateways, Height, Ledger) ->
    GwScores = lists:foldl(fun({A, G}, Acc) ->
                                   {_, _, Score} = blockchain_ledger_gateway_v1:score(A, G, Height, Ledger),
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
    {ok, MinScore} = blockchain:config(min_score, Ledger),
    maps:fold(
      fun(PubkeyBin, Gateway, Acc0) ->
              {ok, Score} = blockchain_ledger_v1:gateway_score(PubkeyBin, Ledger),
              case
                  PubkeyBin == Challenger orelse
                  blockchain_ledger_gateway_v1:location(Gateway) == undefined orelse
                  maps:is_key(PubkeyBin, Acc0) orelse
                  Score =< MinScore
              of
                  true ->
                      Acc0;
                  false ->
                      Graph = ?MODULE:build_graph(PubkeyBin, Gateways, Height, Ledger),
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

target_test_() ->
    {timeout,
     60000,
     fun() ->
             BaseDir = test_utils:tmp_dir("target_test"),
             LatLongs = [
                         {{37.782061, -122.446167}, 1.0, 1.0}, % This should be excluded cause target
                         {{37.782604, -122.447857}, 1.0, 1.0},
                         {{37.782074, -122.448528}, 1.0, 1.0},
                         {{37.782002, -122.44826}, 1.0, 1.0},
                         {{37.78207, -122.44613}, 1.0, 1.0}, %% This should be excluded cuz too close
                         {{37.781909, -122.445411}, 1.0, 1.0},
                         {{37.783371, -122.447879}, 1.0, 1.0},
                         {{37.780827, -122.44716}, 1.0, 1.0},
                         {{38.897675, -77.036530}, 1.0, 1.0} % This should be excluded cause too far
                        ],
             Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),
             ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),

             Challenger = hd(maps:keys(ActiveGateways)),
             Iterations = 1000,
             Results = dict:to_list(lists:foldl(fun(_, Acc) ->
                                                        {Target, _} = target(crypto:strong_rand_bytes(32), Ledger, Challenger),
                                                        dict:update_counter(Target, 1, Acc)
                                                end,
                                                dict:new(),
                                                lists:seq(1, Iterations))),

             %% Each N-1 (excluding the challenger itself) gateway should have an
             %% approximately equal chance of getting picked as target
             ApproxProbability = 1 / length(Results),
             %% Acceptable error, probably could go lower?
             ErrorEpsilon = 0.1,

             lists:foreach(
               fun({_Gw, Count}) ->
                       Prob = Count/Iterations,
                       ?assert(Prob < ApproxProbability + ErrorEpsilon),
                       ?assert(Prob > ApproxProbability - ErrorEpsilon)
               end,
               Results
              ),

             unload_meck(),
             ok
     end}.


neighbors_test() ->
    BaseDir = test_utils:tmp_dir("neighbors_test"),
    LatLongs = [
                {{37.782061, -122.446167}, 1.0, 1.0}, % This should be excluded cause target
                {{37.782604, -122.447857}, 1.0, 1.0},
                {{37.782074, -122.448528}, 1.0, 1.0},
                {{37.782002, -122.44826}, 1.0, 1.0},
                {{37.78207, -122.44613}, 1.0, 1.0}, %% This should be excluded cuz too close
                {{37.781909, -122.445411}, 1.0, 1.0},
                {{37.783371, -122.447879}, 1.0, 1.0},
                {{37.780827, -122.44716}, 1.0, 1.0},
                {{38.897675, -77.036530}, 1.0, 1.0} % This should be excluded cause too far
               ],
    {Target, Gateways} = build_gateways(LatLongs),
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),
    Neighbors = neighbors(Target, Gateways, 1, Ledger),
    ?assertEqual(6, erlang:length(Neighbors)),
    {LL1, _, _} = lists:last(LatLongs),
    TooFar = crypto:hash(sha256, erlang:term_to_binary(LL1)),
    lists:foreach(
      fun({_, Address}) ->
              ?assert(Address =/= Target),
              ?assert(Address =/= TooFar)
      end,
      Neighbors
     ),
    unload_meck(),
    ok.

build_graph_test() ->
    BaseDir = test_utils:tmp_dir("build_graph_test"),
    LatLongs = [
                {{37.782061, -122.446167}, 1.0, 1.0}, % This should be excluded cause target
                {{37.782604, -122.447857}, 1.0, 1.0},
                {{37.782074, -122.448528}, 1.0, 1.0},
                {{37.782002, -122.44826}, 1.0, 1.0},
                {{37.78207, -122.44613}, 1.0, 1.0}, %% This should be excluded cuz too close
                {{37.781909, -122.445411}, 1.0, 1.0},
                {{37.783371, -122.447879}, 1.0, 1.0},
                {{37.780827, -122.44716}, 1.0, 1.0},
                {{38.897675, -77.036530}, 1.0, 1.0} % This should be excluded cause too far
               ],
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),
    {Target, Gateways} = build_gateways(LatLongs),

    Graph = build_graph(Target, Gateways, 1, Ledger),
    ?assertEqual(8, maps:size(Graph)),

    {LL1, _, _} = lists:last(LatLongs),
    TooFar = crypto:hash(sha256, erlang:term_to_binary(LL1)),
    ?assertNot(lists:member(TooFar, maps:keys(Graph))),
    unload_meck(),
    ok.

build_graph_in_line_test() ->
    % All these point are in a line one after the other (except last)
    BaseDir = test_utils:tmp_dir("build_graph_in_line_test"),
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
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),

    Graph = build_graph(Target, Gateways, 1, Ledger),
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
    unload_meck(),
    ok.

build_test() ->
    e2qc:teardown(score_cache),
    BaseDir = test_utils:tmp_dir("build_test"),
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
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),

    {ok, Path} = build(crypto:strong_rand_bytes(32), Target, Gateways, 1, Ledger),

    ?assertNotEqual(Target, hd(Path)),
    ?assert(lists:member(Target, Path)),
    ?assertNotEqual(Target, lists:last(Path)),
    unload_meck(),
    ok.

build_only_2_test() ->
    e2qc:teardown(score_cache),
    BaseDir = test_utils:tmp_dir("build_only_2_test"),
    % All these point are in a line one after the other
    LatLongs = [
                {{37.780959, -122.467496}, 1000.0, 100.0},
                {{37.78101, -122.465372}, 10.0, 1000.0},
                {{37.780586, -122.469471}, 100.0, 20.0}
               ],
    {Target, Gateways} = build_gateways(LatLongs),
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),

    {ok, Path} = build(crypto:strong_rand_bytes(32), Target, Gateways, 1, Ledger),

    ?assertNotEqual(Target, hd(Path)),
    ?assert(lists:member(Target, Path)),
    ?assertNotEqual(Target, lists:last(Path)),
    unload_meck(),
    ok.

build_prob_test_() ->
    {timeout,
     60000,
     fun() ->
             BaseDir = test_utils:tmp_dir("build_prob_test_"),
             e2qc:teardown(score_cache),
             LatLongs = [
                         {{37.782061, -122.446167}, 1.0, 1.0}, % This should be excluded cause target
                         {{37.782604, -122.447857}, 1.0, 1.0},
                         {{37.782074, -122.448528}, 1.0, 1.0},
                         {{37.782002, -122.44826}, 1.0, 1.0},
                         {{37.78207, -122.44613}, 1.0, 1.0}, %% This should be excluded cuz too close
                         {{37.781909, -122.445411}, 1.0, 1.0},
                         {{37.783371, -122.447879}, 1.0, 1.0},
                         {{37.780827, -122.44716}, 1.0, 1.0},
                         {{38.897675, -77.036530}, 1.0, 1.0} % This should be excluded cause too far
                        ],
             {Target, Gateways} = build_gateways(LatLongs),
             Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),

             Iteration = 1000,
             Size = erlang:length(LatLongs)-2,
             Av = Iteration / Size,

             Starters = lists:foldl(
                          fun(_, Acc) ->
                                  {ok, [P1|_]} = blockchain_poc_path:build(crypto:strong_rand_bytes(64), Target, Gateways, 1, Ledger),
                                  V = maps:get(P1, Acc, 0),
                                  maps:put(P1, V+1, Acc)
                          end,
                          #{},
                          lists:seq(1, Iteration)
                         ),

             ?assertEqual(Size, maps:size(Starters)),

             maps:fold(
               fun(_, V, _) ->
                       ?assert(V >= Av-(Av/10) orelse V =< Av+(Av/10))
               end,
               ok,
               Starters
              ),
             unload_meck(),
             ok
     end}.

build_failed_test() ->
    BaseDir = test_utils:tmp_dir("build_failed_test"),
    % All these point are in a line one after the other (except last)
    LatLongs = [
                {{37.780959, -122.467496}, 1000.0, 10.0},
                {{37.78101, -122.465372}, 10.0, 1000.0},
                {{12.780586, -122.469471}, 1000.0, 20.0}
               ],
    {Target, Gateways} = build_gateways(LatLongs),
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),
    ?assertEqual({error, not_enough_gateways}, build(crypto:strong_rand_bytes(32), Target, Gateways, 1, Ledger)),
    unload_meck(),
    ok.

build_with_default_score_test() ->
    BaseDir = test_utils:tmp_dir("build_with_default_score_test"),
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
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),
    {ok, Path} = build(crypto:strong_rand_bytes(32), Target, Gateways, 1, Ledger),
    ?assert(lists:member(Target, Path)),
    unload_meck(),
    ok.

active_gateways_test() ->
    BaseDir = test_utils:tmp_dir("active_gateways_test"),
    % 2 First points are grouped together and next ones form a group also
    LatLongs = [
                {{48.858391, 2.294469}, 1.0, 1.0},
                {{48.856696, 2.293997}, 1.0, 1.0},
                {{48.852969, 2.349872}, 1.0, 1.0},
                {{48.855425, 2.344980}, 1.0, 1.0},
                {{48.854127, 2.344637}, 1.0, 1.0},
                {{48.855228, 2.347126}, 1.0, 1.0}
               ],
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),

    [{LL0, _, _}, {LL1, _, _}, {LL2, _, _}|_] = LatLongs,
    Challenger = crypto:hash(sha256, erlang:term_to_binary(LL2)),
    ActiveGateways = active_gateways(Ledger, Challenger),

    ?assertNot(maps:is_key(Challenger, ActiveGateways)),
    ?assertNot(maps:is_key(crypto:hash(sha256, erlang:term_to_binary(LL0)), ActiveGateways)),
    ?assertNot(maps:is_key(crypto:hash(sha256, erlang:term_to_binary(LL1)), ActiveGateways)),
    ?assertEqual(3, maps:size(ActiveGateways)),

    unload_meck(),
    ok.

active_gateways_low_score_test() ->
    BaseDir = test_utils:tmp_dir("active_gateways_low_score_test"),
    % 2 First points are grouped together and next ones form a group also
    LatLongs = [
                {{48.858391, 2.294469}, 1.0, 1.0},
                {{48.856696, 2.293997}, 1.0, 1.0},
                {{48.852969, 2.349872}, 1.0, 1.0},
                {{48.855425, 2.344980}, 1.0, 1.0},
                {{48.854127, 2.344637}, 1.0, 1.0},
                {{48.855228, 2.347126}, 1.0, 1.0}
               ],
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.01),

    [{_LL0, _, _}, {_LL1, _, _}, {LL2, _, _}|_] = LatLongs,
    Challenger = crypto:hash(sha256, erlang:term_to_binary(LL2)),
    ActiveGateways = active_gateways(Ledger, Challenger),

    ?assertNot(maps:is_key(Challenger, ActiveGateways)),

    %% No gateway should be in active gateways map
    ?assertEqual(0, maps:size(ActiveGateways)),

    unload_meck(),
    ok.

no_neighbor_test() ->
    BaseDir = test_utils:tmp_dir("lone_target_test"),
    LatLongs = [
                %% All these points are wayyy far from each other
                {{27.175301, 78.042144}, 1.0, 1.0},
                {{29.979495, 31.134170}, 1.0, 1.0},
                {{-22.951610, -43.210434}, 1.0, 1.0},
                {{30.328760, 35.444362}, 1.0, 1.0},
                {{20.679464, -88.568252}, 1.0, 1.0},
                {{41.890450, 12.492263}, 1.0, 1.0},
                {{-13.162870, -72.544952}, 1.0, 1.0},
                %% These are in SF and close by
                {{37.780586, -122.469471}, 1.0, 1.0},
                {{37.780959, -122.467496}, 1.0, 1.0},
                {{37.78101, -122.465372}, 1.0, 1.0},
                {{37.781179, -122.463226}, 1.0, 1.0},
                {{37.781281, -122.461038}, 1.0, 1.0},
                {{37.781349, -122.458892}, 1.0, 1.0},
                {{37.781468, -122.456617}, 1.0, 1.0},
                {{37.781637, -122.4543}, 1.0, 1.0}
               ],
    {Target, Gateways} = build_gateways(LatLongs),
    Ledger = build_fake_ledger(BaseDir, LatLongs, 0.25),
    Neighbors = neighbors(Target, Gateways, 1, Ledger),
    ?assertEqual([], Neighbors),
    ?assertEqual({error, not_enough_gateways}, build(crypto:strong_rand_bytes(32), Target, Gateways, 1, Ledger)),
    unload_meck(),
    ok.

build_gateways(LatLongs) ->
    Gateways = lists:foldl(
                 fun({LatLong, Alpha, Beta}, Acc) ->
                         Owner = <<"test">>,
                         Address = crypto:hash(sha256, erlang:term_to_binary(LatLong)),
                         Res = 12,
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

build_fake_ledger(TestDir, LatLongs, DefaultScore) ->
    Ledger = blockchain_ledger_v1:new(TestDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    meck:new(blockchain_swarm, [passthrough]),
    meck:expect(blockchain_swarm,
                pubkey_bin,
                fun() ->
                        <<"yolo">>
                end),
    meck:expect(blockchain_ledger_v1,
                current_height,
                fun(_) ->
                        {ok, 1}
                end),
    meck:expect(blockchain,
                config,
                fun(min_score, _) ->
                        {ok, 0.2};
                   (h3_exclusion_ring_dist, _) ->
                        {ok, 2};
                   (h3_max_grid_distance, _) ->
                        {ok, 13};
                   (h3_neighbor_res, _) ->
                        {ok, 12};
                   (alpha_decay, _) ->
                        {ok, 0.007};
                   (beta_decay, _) ->
                        {ok, 0.0005};
                   (max_staleness, _) ->
                        {ok, 100000}
                end),
    meck:expect(blockchain_ledger_v1,
                gateway_score,
                fun(_, _) ->
                        {ok, DefaultScore}
                end),

    N = length(LatLongs),
    Res = 12,
    OwnerAndGateways = [{O, G} || {{O, _}, {G, _}} <- lists:zip(test_utils:generate_keys(N), test_utils:generate_keys(N))],

    _ = lists:map(fun({{Owner, Gw}, {Coordinate, _, _}}) ->
                          blockchain_ledger_v1:add_gateway(Owner, Gw, h3:from_geo(Coordinate, Res), DefaultScore, Ledger1)
                  end, lists:zip(OwnerAndGateways, LatLongs)),
    blockchain_ledger_v1:commit_context(Ledger1),
    Ledger1.

unload_meck() ->
    ?assert(meck:validate(blockchain_swarm)),
    meck:unload(blockchain_swarm),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain)),
    meck:unload(blockchain).

-endif.
