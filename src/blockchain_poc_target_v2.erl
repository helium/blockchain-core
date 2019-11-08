%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v2 implementation.
%%%
%%% The targeting mechanism is based on the following conditions:
%%% - Favor high scoring hotspots, based on score_weight
%%% - Favor loosely connected hotspots, based on edge_weight
%%% - TargetProbability = ScoreProbWeight * ScoreProb + EdgeProbWeight * EdgeProb
%%% - Filter hotspots which haven't done a poc request for a long time
%%% - Given a map of gateway_scores, we must ALWAYS find a target
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v2).

-define(POC_V4_TARGET_SCORE_CURVE, 5).
-define(POC_V4_TARGET_CHALLENGE_AGE, 300).
-define(POC_V4_TARGET_PROB_SCORE_WT, 0.8).
-define(POC_V4_TARGET_PROB_EDGE_WT, 0.2).
-define(POC_V4_PARENT_RES, 11). %% normalize to 11 res

-export([
         target/3, filter/4
        ]).

-type gateway_score_map() :: #{libp2p_crypto:pubkey_bin() => {float(), blockchain_ledger_gateway_v2:gateway()}}.
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

%% @doc Finds a potential target to start the path from.
%% This must always return a target.
%% Favors high scoring gateways, dependent on score^5 curve.
-spec target(Hash :: binary(),
             GatewayScoreMap :: gateway_score_map(),
             Vars :: map()) -> {ok, libp2p_crypto:pubkey_bin()}.
target(Hash, GatewayScoreMap, Vars) ->
    ProbScores = score_prob(GatewayScoreMap, Vars),
    ProbEdges = edge_prob(GatewayScoreMap, Vars),
    ProbTarget = target_prob(ProbScores, ProbEdges, Vars),
    ScaledProbs = maps:to_list(scaled_prob(ProbTarget)),
    Entropy = blockchain_utils:rand_state(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    select_target(ScaledProbs, RandVal).

%% @doc Filter gateways based on these conditions:
%% - Inactive gateways (those which haven't challenged in a long time).
%% - Dont target the challenger gateway itself.
-spec filter(GatewayScoreMap :: gateway_score_map(),
             Challenger :: libp2p_crypto:pubkey_bin(),
             Height :: pos_integer(),
             Vars :: map()) -> gateway_score_map().
filter(GatewayScoreMap, Challenger, Height, Vars) ->
    maps:filter(fun(_Addr, {_Score, Gateway}) ->
                        case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
                            undefined ->
                                %% No POC challenge, don't include
                                false;
                            C ->
                                (Height - C) < challenge_age(Vars)
                        end
                end,
                maps:without([Challenger], GatewayScoreMap)).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec score_prob(GatewayScoreMap :: gateway_score_map(), Vars :: map()) -> prob_map().
score_prob(GatewayScoreMap, Vars) ->
    %% Assign probability to each gateway
    ProbScores = maps:map(fun(_Addr, {Score, _G}) ->
                                  score_curve(Score, Vars)
                          end,
                          GatewayScoreMap),
    %% Calculate sum of all probs
    SumScores = lists:sum([S || {_A, S} <- maps:to_list(ProbScores)]),
    %% Scale probabilities so they add up to 1.0
    maps:map(fun(_, S) ->
                     S / SumScores
             end,
             ProbScores).

-spec edge_prob(GatewayScoreMap :: gateway_score_map(), Vars :: map()) -> prob_map().
edge_prob(GatewayScoreMap, Vars) ->
    %% Get all locations
    Locations = locations(GatewayScoreMap),
    %% Assign probability to each gateway
    ProbEdges = maps:map(fun(_Addr, {_Score, Gateway}) ->
                                     calc_edge_prob(Locations, Gateway, Vars)
                             end,
                             GatewayScoreMap),
    %% Calculate sum of all probs
    SumEdgeProbs = lists:sum([S || {_A, S} <- maps:to_list(ProbEdges)]),
    %% Scale probabilities so they add up to 1.0
    maps:map(fun(_, E) ->
                     E / SumEdgeProbs
             end,
             ProbEdges).

-spec target_prob(ProbScores :: prob_map(),
                  ProbEdges :: prob_map(),
                  Vars :: map()) -> prob_map().
target_prob(ProbScores, ProbEdges, Vars) ->
    %% P(Target) = ScoreWeight*P(Score) + EdgeWeight*P(Edge)
    maps:map(fun(Addr, PScore) ->
                     (prob_score_wt(Vars) * PScore) +
                     (prob_edge_wt(Vars) * maps:get(Addr, ProbEdges))
             end,
             ProbScores).

-spec scaled_prob(PTarget :: prob_map()) -> prob_map().
scaled_prob(PTarget) ->
    SumProbs = lists:sum(maps:values(PTarget)),
    maps:map(fun(_Addr, P) ->
                     P / SumProbs
             end, PTarget).

-spec calc_edge_prob(Locations :: [h3:index()],
                     Gateway :: blockchain_ledger_gateway_v2:gateway(),
                     Vars :: map()) -> float().
calc_edge_prob(Locations, Gateway, Vars) ->
    Loc = blockchain_ledger_gateway_v2:location(Gateway),
    %% Get resolution
    Res = h3:get_resolution(Loc),
    %% Find parent location
    GatewayParent = h3:parent(Loc, parent_res(Vars)),
    %% Get all children for the gateway parent at gateway res
    Children = h3:children(GatewayParent, Res),
    %% Intersection of children with known locations
    Intersect = sets:to_list(sets:intersection(sets:from_list(Children), sets:from_list(Locations))),
    %% Prob of being loosely connected
    1 - (length(Intersect)/length(Locations)).

-spec select_target([{libp2p_crypto:pubkey_bin(), float()}], float()) -> {ok, libp2p_crypto:pubkey_bin()}.
select_target([{GwAddr, Prob}=_Head | _], Rnd) when Rnd - Prob =< 0 ->
    {ok, GwAddr};
select_target([{_GwAddr, Prob} | Tail], Rnd) ->
    select_target(Tail, Rnd - Prob).

-spec challenge_age(Vars :: map()) -> pos_integer().
challenge_age(Vars) ->
    maps:get(poc_v4_target_challenge_age, Vars, ?POC_V4_TARGET_CHALLENGE_AGE).

-spec prob_score_wt(Vars :: map()) -> float().
prob_score_wt(Vars) ->
    maps:get(poc_v4_target_prob_score_wt, Vars, ?POC_V4_TARGET_PROB_SCORE_WT).

-spec prob_edge_wt(Vars :: map()) -> float().
prob_edge_wt(Vars) ->
    maps:get(poc_v4_target_prob_edge_wt, Vars, ?POC_V4_TARGET_PROB_EDGE_WT).

-spec score_curve(Score :: float(), Vars :: map()) -> float().
score_curve(Score, Vars) ->
    Exp = maps:get(poc_v4_target_score_curve, Vars, ?POC_V4_TARGET_SCORE_CURVE),
    math:pow(Score, Exp).

-spec parent_res(Vars :: map()) -> pos_integer().
parent_res(Vars) ->
    maps:get(poc_v4_parent_res, Vars, ?POC_V4_PARENT_RES).

-spec locations(GatewayScoreMap :: gateway_score_map()) -> [h3:index()].
locations(GatewayScoreMap) ->
    %% Get all locations from score map
    [blockchain_ledger_gateway_v2:location(G) || {_, G} <- maps:values(GatewayScoreMap)].
