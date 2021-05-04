%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC Path ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc_path).

-include("blockchain_vars.hrl").
-include("blockchain_caps.hrl").

-export([
         build/5,
         shortest/3, shortest/4,
         length/3, length/4,
         build_graph/4,
         target/3,
         neighbors/3,
         entropy/1,
         check_sync/2,
         active_gateways/2 %% exported for debug purposes
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
-type gateways() :: #{libp2p_crypto:pubkey_bin() => {blockchain_ledger_gateway_v2:gateway(), float()}}.

-export_type([graph/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build(Hash :: binary(),
            Target :: binary(),
            Gateways :: gateways(),
            Height :: non_neg_integer(),
            Ledger :: blockchain_ledger_v1:ledger()) -> {ok, list()} | {error, any()}.
build(Hash, Target, Gateways, Height, Ledger) ->
    Graph = build_graph_int([Target], Gateways, Height, Ledger, #{}),
    GraphList = maps:fold(
                  fun(Addr, _, Acc) ->
                          case Addr == Target of
                              true ->
                                  Acc;
                              false ->
                                  {_G, Score} = maps:get(Addr, Gateways),
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
            PathLimit = case blockchain:config(?poc_version, Ledger) of
                            {ok, POCVersion0} when POCVersion0 >= 3 ->
                                case blockchain:config(?poc_path_limit, Ledger) of
                                    {ok, Val0} when is_integer(Val0) ->
                                        %% we're only interested in half paths up to
                                        %% the half total path limit
                                        ceil(Val0/2);
                                    _ ->
                                        infinity
                                end;
                            _ ->
                                infinity
                        end,
            %% find the longest, highest scoring paths that don't exceed any path limits
            %% paths that are too long are filtered because their score ends up as 0
            Lengths =
                [ {S, G} || {S, G} <- [{Score * ?MODULE:length(Graph, Target, Addr, PathLimit), G}
                 || {Score, Addr} = G <- blockchain_utils:shuffle_from_hash(Hash, GraphList)],  S > 0 ],
            %% sort the highest scoring paths first
            [{_, {_, Start}}, {_, {_, End}}|_] = lists:sort(fun({S1, _}, {S2, _}) -> S1 > S2 end,
                                                            Lengths),
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
                    Path4 = case rand:uniform(2) of
                                1 ->
                                    Path3;
                                2 ->
                                    lists:reverse(Path3)
                            end,
                    case blockchain:config(?poc_version, Ledger) of
                        {error, not_found} ->
                            {ok, Path4};
                        {ok, POCVersion} when POCVersion >= 2 ->
                            case blockchain:config(?poc_path_limit, Ledger) of
                                {error, not_found} ->
                                    {ok, Path4};
                                {ok, Val} ->
                                    %% NOTE: The tradeoff here is that we may potentially lose target and end
                                    %% from the path, but the fact that we would still have constructed it should
                                    %% suffice to build interesting paths which conform to the given path_limit
                                    {ok, lists:sublist(Path4, Val)}
                            end
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec shortest(Graph :: graph(), Start :: any(), End :: any()) -> {number(), list()}.
shortest(Graph, Start, End) ->
    shortest(Graph, Start, End, infinity).

-spec shortest(Graph :: graph(), Start :: any(), End :: any(), Limit :: pos_integer() | 'infinity') -> {number(), list()}.
shortest(Graph, Start, End, Limit) ->
    path(Graph, [{0, [Start]}], End, #{}, Limit).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec length(Graph :: graph(), Start :: any(), End :: any()) -> integer().
length(Graph, Start, End) ->
    length(Graph, Start, End, infinity).

-spec length(Graph :: graph(), Start :: any(), End :: any(), Limit :: pos_integer() | 'infinity') -> integer().
length(Graph, Start, End, Limit) ->
    {_Cost, Path} = ?MODULE:shortest(Graph, Start, End, Limit),
    erlang:length(Path).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build_graph(Address :: binary(),
                  Gateways :: gateways(),
                  Height :: non_neg_integer(),
                  Ledger :: blockchain_ledger_v1:ledger()) -> graph().
build_graph(Address, Gateways, Height, Ledger) ->
    build_graph_int([Address], Gateways, Height, Ledger, #{}).

-spec build_graph_int([binary()],
                      Gateways :: gateways(),
                      Height :: non_neg_integer(),
                      Ledger :: blockchain_ledger_v1:ledger(),
                      Graph :: graph()) -> graph().
build_graph_int([], _Gateways, _Height, _Ledger, Graph) ->
    Graph;
build_graph_int([Address0|Addresses], Gateways, Height, Ledger, Graph0) ->
    %% find all the neighbors of address 0
    case Gateways of
        #{Address0 := {Gw0, Score0}} ->
            Neighbors0_0 = blockchain_ledger_gateway_v2:neighbors(Gw0),
            Neighbors0 = filter_neighbors(Address0, Score0, Neighbors0_0, Gateways, Height, Ledger),
            %% fold over the list of neighbors
            Graph1 = lists:foldl(
                       fun({_W, Address1}, Acc) ->
                               %% if the neighbor address is already in the
                               %% graph, skip it.
                               case maps:is_key(Address1, Acc) of
                                   true ->
                                       Acc;
                                   false ->
                                       %% otherwise, calculate its neighbors
                                       #{Address1 := {Gw1, Score1}} = Gateways,
                                       Neighbors1_0 = blockchain_ledger_gateway_v2:neighbors(Gw1),
                                       Neighbors1 = filter_neighbors(Address1, Score1, Neighbors1_0, Gateways, Height, Ledger),
                                       Graph1 = maps:put(Address1, Neighbors1, Acc),
                                       %% and append all of its neighbor's neighbors?
                                       build_graph_int([A || {_, A} <- Neighbors1,
                                                             A /= maps:is_key(A, Graph1)],
                                                       Gateways, Height, Ledger, Graph1)
                               end
                       end,
                       %% first, map address to neighbors
                       maps:put(Address0, Neighbors0, Graph0),
                       Neighbors0
                      ),
            FilteredAddresses = lists:filter(fun(A) -> not maps:is_key(A, Graph1) end,
                                             Addresses),
            build_graph_int(FilteredAddresses, Gateways, Height, Ledger, Graph1);
        _ ->
            Graph0
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec path(Graph :: graph(),
           Path :: [{number(), list()}],
           End :: any(),
           Seen :: map(),
           Limit :: pos_integer() | 'infinity') -> {number(), list()}.
path(_Graph, [], _End, _Seen, _Limit) ->
    % nowhere to go
    {0, []};
path(_Graph, [{Cost, [End | _] = Path} | _], End, _Seen, _Limit) ->
    % base case
    {Cost, lists:reverse(Path)};
path(Graph, [{Cost, [Node | _] = Path} | Routes] = _OldRoutes, End, Seen, Limit) ->
    NewRoutes = lists:filter(fun({_, P}) -> length(P) =< Limit end,
                             [{Cost + NewCost, [NewNode | Path]} || {NewCost, NewNode} <- maps:get(Node, Graph, [{0, []}]), not maps:get(NewNode, Seen, false)]),
    NextRoutes = cheapest_to_front(NewRoutes ++ Routes),
    path(Graph, NextRoutes, End, Seen#{Node => true}, Limit).

cheapest_to_front([]) -> [];
cheapest_to_front([H | T]) ->
    cheapest_to_front(H, T, []).

cheapest_to_front(C, [], Acc) ->
    [C | Acc];
cheapest_to_front(C, [H | T], Acc) ->
    case C > H of
        true ->
            cheapest_to_front(H, T, [C | Acc]);
        _ ->
            cheapest_to_front(C, T, [H | Acc])
    end.

%%--------------------------------------------------------------------
%% @doc neighbors iterates through `Gateways` to find any Gateways
%% that are within max grid distance from the address in pubkeybin
%% @end
%%--------------------------------------------------------------------
neighbors(PubkeyBin, Gateways, Ledger) when is_binary(PubkeyBin) ->
    case maps:get(PubkeyBin, Gateways, undefined) of
        undefined ->
            {error, bad_gateway};
        {Gw, _S} ->
            neighbors(Gw, Gateways, Ledger);
        Gw ->
            neighbors(Gw, Gateways, Ledger)
    end;
neighbors(Gw, Gateways, Ledger) ->
    GwH3 = blockchain_ledger_gateway_v2:location(Gw),
    {ok, H3ExclusionRingDist} = blockchain:config(?h3_exclusion_ring_dist, Ledger),
    {ok, H3MaxGridDistance} = blockchain:config(?h3_max_grid_distance, Ledger),
    {ok, H3NeighborRes} = blockchain:config(?h3_neighbor_res, Ledger),
    ExclusionIndices = h3:k_ring(GwH3, H3ExclusionRingDist),
    ScaledGwH3 = h3:parent(GwH3, H3NeighborRes),

    lists:foldl(
      fun({A, G0}, Acc) ->
              G = case G0 of
                  {G1, _S} ->
                      G1;
                  _ ->
                      G0
              end,
              case {blockchain_ledger_gateway_v2:location(G),
                    blockchain_ledger_gateway_v2:is_valid_capability(G, ?GW_CAPABILITY_POC_CHALLENGEE, Ledger)} of
                  {undefined, _} -> Acc;
                  {_, false} -> Acc;
                  {Index, _} ->
                      ScaledIndex = scale(Index, H3NeighborRes),
                      case lists:member(ScaledIndex, ExclusionIndices) of
                          false ->
                              case (catch h3:grid_distance(ScaledGwH3, ScaledIndex)) of
                                  {'EXIT', _} -> Acc;
                                  D when D > H3MaxGridDistance -> Acc;
                                  _ ->
                                      [A | Acc]
                              end;
                          true -> Acc
                              end
              end
      end,
      [],
      maps:to_list(Gateways)).

filter_neighbors(Addr, Score, Neighbors, Gateways, Height, Ledger) ->
    Gw = maps:get(Addr, Gateways),
    {ok, MinScore} = blockchain:config(?min_score, Ledger),
    lists:reverse(lists:foldl(
                    fun(A, Acc) ->
                            case maps:get(A, Gateways, undefined) of
                                {G, S} when S >= MinScore ->
                                    [{edge_weight(Addr, Gw, Score, A, G, S, Height, Ledger), A}|Acc];
                                _ ->
                                    Acc
                            end
                    end,
                    [], Neighbors)).

scale(Index, Res) ->
    case h3:get_resolution(Index) of
        R when R > Res ->
            h3:parent(Index, Res);
        _ ->
            Index
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec edge_weight(A1 :: libp2p_crypto:pubkey_bin(),
                  Gw1 :: blockchain_ledger_gateway_v2:gateway(),
                  S1 :: float(),
                  A2 :: libp2p_crypto:pubkey_bin(),
                  Gw2 :: blockchain_ledger_gateway_v2:gateway(),
                  S2 :: float(),
                  Height :: non_neg_integer(),
                  Ledger :: blockchain_ledger_v1:ledger()) -> float().
edge_weight(_A1, _Gw1, S1, _A2, _Gw2, S2, _Height, _Ledger) ->
    1 - abs(prob_fun(S1) - prob_fun(S2)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec target(Hash :: binary(),
             Ledger :: blockchain_ledger_v1:ledger(), libp2p_crypto:pubkey_bin()) ->
                    {libp2p_crypto:pubkey_bin(), gateways()} | no_target.
target(Hash, Ledger, Challenger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ActiveGateways = active_gateways(Ledger, Challenger),
    ProbsAndGatewayAddrs = create_probs(ActiveGateways, Height, Ledger),
    Entropy = entropy(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    case select_target(ProbsAndGatewayAddrs, RandVal) of
        {ok, Target} ->
            {Target, ActiveGateways};
        _ ->
            no_target
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_probs(Gateways :: gateways(),
                   Height :: non_neg_integer(),
                   Ledger :: blockchain_ledger_v1:ledger()) -> [{float(), libp2p_crypto:pubkey_bin()}].
create_probs(Gateways, _Height, _Ledger) ->
    GwScores = lists:foldl(fun({A, {_G, Score}}, Acc) ->
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
-spec active_gateways(blockchain_ledger_v1:ledger(), libp2p_crypto:pubkey_bin()) -> gateways().
active_gateways(Ledger, Challenger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    Gateways = blockchain_utils:score_gateways(Ledger),
    {ok, MinScore} = blockchain:config(?min_score, Ledger),
    %% fold over all the gateways
    maps:fold(
      fun(PubkeyBin, {Gateway, Score}, Acc0) ->
              CheckSync = check_sync(Gateway, Ledger),
              case
                  %% if we're some other gateway who has a location
                  %% and hasn't been added to the graph and our score
                  %% is good enough and we also have the required capability
                  CheckSync andalso
                  (PubkeyBin == Challenger orelse
                   blockchain_ledger_gateway_v2:location(Gateway) == undefined orelse
                   maps:is_key(PubkeyBin, Acc0) orelse
                   Score =< MinScore) orelse
                   not blockchain_ledger_gateway_v2:is_valid_capability(Gateway, ?GW_CAPABILITY_POC_CHALLENGEE, Ledger)

              of
                  true ->
                      Acc0;
                  false ->
                      %% build the graph originating at this location
                      Graph = build_graph_int([PubkeyBin], Gateways, Height, Ledger, #{}),
                      case maps:size(Graph) > 2 of
                          false ->
                              Acc0;
                          true ->
                              %% then filter the graph, removing the challenger for some
                              %% reason.  is challenger here the path start?
                              maps:fold(
                                fun(Addr, Neighbors, Acc1) ->
                                        Acc2 = case Addr == Challenger of
                                                   true ->
                                                       Acc1;
                                                   false ->
                                                       %% if we're not the challenger, add
                                                       %% our full gw information into the acc
                                                       maps:put(Addr, maps:get(Addr, Gateways), Acc1)
                                               end,
                                        %% fold over the neighbors, adding them to the
                                        %% list if they're not the challenger
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
select_target([], _Rnd) ->
    no_target;
select_target([{Prob1, GwAddr1}=_Head | _], Rnd) when Rnd - Prob1 < 0 ->
    {ok, GwAddr1};
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

check_sync(Gateway, Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case blockchain:config(?poc_version, Ledger) of
        {error, not_found} ->
            %% Follow old code path, allow to be challenged
            true;
        {ok, POCVersion} when POCVersion >= 2 ->
            case blockchain:config(?poc_challenge_sync_interval, Ledger) of
                {error, not_found} ->
                    %% poc_challenge_sync_interval is not set, allow
                    true;
                {ok, I} ->
                    case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
                        undefined ->
                            %% Ignore
                            false;
                        L ->
                            case (Height - L) =< I of
                                true ->
                                    %% ledger_height - last_poc_challenge is within our set interval,
                                    %% allow to participate in poc challenge
                                    true;
                                false ->
                                    %% Ignore
                                    false
                            end
                    end
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.
