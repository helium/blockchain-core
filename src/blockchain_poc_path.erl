%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC Path ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc_path).

-export([
    build/2,
    shortest/3,
    length/3,
    build_graph/2,
    target/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RESOLUTION, 9).
-define(RING_SIZE, 2).
% KRing of 1
%     Scale 3.57
%     Max distance 1.028 miles @ resolution 8
%     Max distance 0.38 miles @ resolution 9

% KRing of 2
%     Scale 5.42
%     Max distance 1.564 miles @ resolution 8
%     Max distance 0.59 miles @ resolution 9  <----

-type graph() :: #{any() => [{number(), any()}]}.

-export_type([graph/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build(Target :: binary(), Gateways :: map()) -> {ok, list()} | {error, any()}.
build(Target, Gateways) ->
    Graph = ?MODULE:build_graph(Target, Gateways),
    GraphList = maps:fold(
        fun(Addr, _, Acc) ->
            G = maps:get(Addr, Gateways),
            Score = blockchain_ledger_gateway_v1:score(G),
            [{Score, Addr}|Acc]
        end,
        [],
        Graph
    ),
    case erlang:length(GraphList) > 2 of
        false ->
            {error, not_enough_gateways};
        true ->
            [{_, Start}, {_, End}|_] = lists:sort(fun({ScoreA, AddrA}, {ScoreB, AddrB}) ->
                                                          ScoreA * ?MODULE:length(Graph, Target, AddrA) >
                                                          ScoreB * ?MODULE:length(Graph, Target, AddrB)
                                                  end, GraphList),
            {_, Path1} = ?MODULE:shortest(Graph, Start, Target),
            {_, [Target|Path2]} = ?MODULE:shortest(Graph, Target, End),
            %% NOTE: It is possible the path contains dupes, these are also considered valid
            {ok, Path1 ++ Path2}
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
-spec build_graph(Address :: binary(), Gateways :: map()) -> graph().
build_graph(Address, Gateways) ->
    build_graph([Address], Gateways, maps:new()).

-spec build_graph([binary()], Gateways :: map(), Graph :: graph()) -> graph().
build_graph([], _Gateways, Graph) ->
    Graph;
build_graph([Address0|Addresses], Gateways, Graph0) ->
    Neighbors0 = neighbors(Address0, Gateways),
    Graph1 = lists:foldl(
        fun({_W, Address1}, Acc) ->
            case maps:is_key(Address1, Acc) of
                true -> Acc;
                false ->
                    Neighbors1 = neighbors(Address1, Gateways),
                    Graph1 = maps:put(Address1, Neighbors1, Acc),
                    build_graph([A || {_, A} <- Neighbors1], Gateways, Graph1)
            end
        end,
        maps:put(Address0, Neighbors0, Graph0),
        Neighbors0
    ),
    case maps:size(Graph1) > 100 of
        false ->
            build_graph(Addresses, Gateways, Graph1);
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
neighbors(Address, Gateways) ->
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
            I = blockchain_ledger_gateway_v1:location(G),
            I1 = case h3:get_resolution(I) of
                R when R =< ?RESOLUTION -> I;
                R when R > ?RESOLUTION -> h3:parent(I, ?RESOLUTION)
            end,
            lists:member(I1, KRing)
                andalso Address =/= A
        end,
        Gateways
    )),
    [{edge_weight(TargetGw, G), A} || {A, G} <- GwInRing].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
edge_weight(Gw1, Gw2) ->
    1 - abs(blockchain_ledger_gateway_v1:score(Gw1) -  blockchain_ledger_gateway_v1:score(Gw2)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec target(Hash :: binary(),
             Ledger :: blockchain_ledger_v1:ledger()) -> {libp2p_crypto:pubkey_bin(), map()}.
target(Hash, Ledger) ->
    ActiveGateways = active_gateways(Ledger),
    ProbsAndGatewayAddrs = create_probs(ActiveGateways),
    Probs = [P || {P, _} <- ProbsAndGatewayAddrs],
    Entropy = entropy(Hash),
    {NewEntropy, ShuffledProbsAndGatewayAddrs} = shuffle(Entropy, ProbsAndGatewayAddrs),
    {RandVal, _} = rand:uniform_s(NewEntropy),
    TargetVal = RandVal * lists:sum(Probs),
    Target = select_target(ShuffledProbsAndGatewayAddrs, TargetVal),
    {Target, ActiveGateways}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_probs(Gateways :: map()) -> [{float(), libp2p_crypto:pubkey_bin()}].
create_probs(Gateways) ->
    GwScores = [{A, blockchain_ledger_gateway_v1:score(G)} || {A, G} <- maps:to_list(Gateways)],
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

-spec shuffle(rand:state(), [{float(), libp2p_crypto:pubkey_bin()}]) -> {rand:state(), [{float(), libp2p_crypto:pubkey_bin()}]}.
shuffle(InSeed, List) ->
    {OutSeed, TaggedList} = lists:foldl(fun(E, {Seed, Acc}) ->
                                                {R, NewSeed} = rand:uniform_s(Seed),
                                                {NewSeed, [{R,E}|Acc]}
                                        end, {InSeed, []}, List),
    OutList = element(2, lists:unzip(lists:keysort(1, TaggedList))),
    {OutSeed, OutList}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec active_gateways(Ledger :: blockchain_ledger_v1:ledger()) -> map().
active_gateways(Ledger) ->
    ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
    maps:filter(
        fun(PubkeyBin, Gateway) ->
            % TODO: Maybe do some find of score check here
            PubkeyBin =/= blockchain_swarm:pubkey_bin()
            andalso blockchain_ledger_gateway_v1:location(Gateway) =/= undefined
        end
        ,ActiveGateways
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
select_target([{Prob1,  GwAddr1}=_Head | _], Rnd) when Rnd - Prob1 < 0 ->
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
prob(Score, LenScores, SumScores) ->
    (1.0 - Score) / (LenScores - SumScores).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

select_target_test() ->
    Iterations = 10000,
    Results = lists:foldl(fun(_I, Acc) ->
                                  Entropy = entropy(crypto:strong_rand_bytes(32)),
                                  {Rand, _} = rand:uniform_s(Entropy),
                                  ProbsAndGws = [{0.1, <<"gw1">>}, {0.2, <<"gw2">>}, {0.4, <<"gw3">>}, {0.3, <<"gw4">>}],
                                  Selected = select_target(ProbsAndGws, Rand),
                                  case maps:get(Selected, Acc, 0) of
                                      0 -> maps:put(Selected, 1, Acc);
                                      V -> maps:put(Selected, V+1, Acc)
                                  end
                          end,
                          #{},
                          lists:seq(1, Iterations)),

    ProbGw3 = maps:get(<<"gw3">>, Results)/Iterations,
    io:format("ProbGw3: ~p~n", [ProbGw3]),
    ?assert(ProbGw3 > 0.38),
    ?assert(ProbGw3 < 0.42),
    ok.

neighbors_test() ->
    LatLongs = [
        {{37.782061, -122.446167}, 0.1}, % This should be excluded cause target
        {{37.782604, -122.447857}, 0.99},
        {{37.782074, -122.448528}, 0.99},
        {{37.782002, -122.44826}, 0.99},
        {{37.78207, -122.44613}, 0.99},
        {{37.781909, -122.445411}, 0.99},
        {{37.783371, -122.447879}, 0.99},
        {{37.780827, -122.44716}, 0.99},
        {{38.897675, -77.036530}, 0.12} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),
    Neighbors = neighbors(Target, Gateways),

    ?assertEqual(erlang:length(maps:keys(Gateways)) - 2, erlang:length(Neighbors)),
    {LL1, _} =  lists:last(LatLongs),
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
        {{37.782061, -122.446167}, 0.1},
        {{37.782604, -122.447857}, 0.99},
        {{37.782074, -122.448528}, 0.99},
        {{37.782002, -122.44826}, 0.99},
        {{37.78207, -122.44613}, 0.99},
        {{37.781909, -122.445411}, 0.99},
        {{37.783371, -122.447879}, 0.99},
        {{37.780827, -122.44716}, 0.99},
        {{38.897675, -77.036530}, 0.12} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),

    Graph = build_graph(Target, Gateways),
    ?assertEqual(8, maps:size(Graph)),

    {LL1, _} = lists:last(LatLongs),
    TooFar = crypto:hash(sha256, erlang:term_to_binary(LL1)),
    ?assertNot(lists:member(TooFar, maps:keys(Graph))),
    ok.


build_graph_in_line_test() ->
    % All these point are in a line one after the other (except last)
    LatLongs = [
        {{37.780586, -122.469471}, 0.1},
        {{37.780959, -122.467496}, 0.99},
        {{37.78101, -122.465372}, 0.98},
        {{37.781179, -122.463226}, 0.97},
        {{37.781281, -122.461038}, 0.96},
        {{37.781349, -122.458892}, 0.95},
        {{37.781468, -122.456617}, 0.94},
        {{37.781637, -122.4543}, 0.93},
        {{38.897675, -77.036530}, 0.12} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),

    Graph = build_graph(Target, Gateways),
    ?assertEqual(8, maps:size(Graph)),

    {LL1, _} = lists:last(LatLongs),
    TooFar = crypto:hash(sha256, erlang:term_to_binary(LL1)),
    ?assertNot(lists:member(TooFar, maps:keys(Graph))),

    Addresses = lists:droplast([crypto:hash(sha256, erlang:term_to_binary(X)) || {X, _} <- LatLongs]),
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
        {{37.780959, -122.467496}, 0.65},
        {{37.78101, -122.465372}, 0.75},
        {{37.780586, -122.469471}, 0.99},
        {{37.781179, -122.463226}, 0.75},
        {{37.781281, -122.461038}, 0.1},
        {{37.781349, -122.458892}, 0.75},
        {{37.781468, -122.456617}, 0.75},
        {{37.781637, -122.4543}, 0.95},
        {{38.897675, -77.036530}, 0.12} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),

    {ok, Path} = build(Target, Gateways),

    ?assertNotEqual(Target, hd(Path)),
    ?assert(lists:member(Target, Path)),
    ?assertNotEqual(Target, lists:last(Path)),
    ok.

build_with_zero_score_test() ->
    % All these point are in a line one after the other (except last)
    LatLongs = [
        {{37.780586, -122.469471}, 0.0},
        {{37.780959, -122.467496}, 0.0},
        {{37.78101, -122.465372}, 0.0},
        {{37.781179, -122.463226}, 0.0},
        {{37.781281, -122.461038}, 0.0},
        {{37.781349, -122.458892}, 0.0},
        {{37.781468, -122.456617}, 0.0},
        {{37.781637, -122.4543}, 0.0},
        {{38.897675, -77.036530}, 0.0} % This should be excluded cause too far
    ],
    {Target, Gateways} = build_gateways(LatLongs),
    {ok, Path} = build(Target, Gateways),
    ?assert(lists:member(Target, Path)),
    ok.

build_gateways(LatLongs) ->
    Gateways = lists:foldl(
        fun({LatLong, Score}, Acc) ->
            Owner = <<"test">>,
            Address = crypto:hash(sha256, erlang:term_to_binary(LatLong)),
            Res = rand:uniform(7) + 8,
            Index = h3:from_geo(LatLong, Res),
            G0 = blockchain_ledger_gateway_v1:new(Owner, Index),
            G1 = blockchain_ledger_gateway_v1:score(Score, G0),
            maps:put(Address, G1, Acc)

        end,
        maps:new(),
        LatLongs
    ),
    [{LL, _}|_] = LatLongs,
    Target = crypto:hash(sha256, erlang:term_to_binary(LL)),
    {Target, Gateways}.

-endif.
