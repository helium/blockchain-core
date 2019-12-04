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
%% Normalize to 11 res by default unless chain var specified.
-define(POC_V4_PARENT_RES, 11).
%% Exclude 6000 grid cells when trying to find a potential by default
%% target from a given challenger. Normalized to parent_res.
%% This is roughly 100KM.
-define(POC_V4_TARGET_EXCLUSION_CELLS, 6000).

-import(blockchain_utils, [normalize_float/1]).

-export([
         target/3, filter/5
        ]).

-type gateway_score_map() :: #{libp2p_crypto:pubkey_bin() => {float(), blockchain_ledger_gateway_v2:gateway()}}.
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

%% @doc Finds a potential target to start the path from.
%% This must always return a target.
%% Favors high scoring gateways, dependent on score^poc_v4_target_score_curve curve.
-spec target(Hash :: binary(),
             GatewayScoreMap :: gateway_score_map(),
             Vars :: map()) -> {ok, libp2p_crypto:pubkey_bin()} | {error, no_target}.
target(Hash, GatewayScoreMap, Vars) ->
    ProbScores = score_prob(GatewayScoreMap, Vars),
    ProbEdges = edge_prob(GatewayScoreMap, Vars),
    ProbTarget = target_prob(ProbScores, ProbEdges, Vars),
    %% Sort the scaled probabilities in default order by gateway pubkey_bin
    ScaledProbs = lists:sort(maps:to_list(scaled_prob(ProbTarget))),
    Entropy = blockchain_utils:rand_state(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    select_target(ScaledProbs, RandVal).

%% @doc Filter gateways based on these conditions:
%% - Inactive gateways (those which haven't challenged in a long time).
%% - Dont target the challenger gateway itself.
%% - Ensure that potential target is far from the challenger to avoid collusion.
-spec filter(GatewayScoreMap :: gateway_score_map(),
             ChallengerAddr :: libp2p_crypto:pubkey_bin(),
             ChallengerLoc :: h3:index(),
             Height :: pos_integer(),
             Vars :: map()) -> gateway_score_map().
filter(GatewayScoreMap, ChallengerAddr, ChallengerLoc, Height, Vars) ->
    maps:filter(fun(_Addr, {Gateway, _Score}) ->
                        case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
                            undefined ->
                                %% No POC challenge, don't include
                                false;
                            C ->
                                %% Check challenge age is recent depending on the set chain var
                                (Height - C) < challenge_age(Vars) andalso
                                %% Check that the potential target is far enough from the challenger
                                %% NOTE: If we have a defined poc_challenge the gateway location cannot be undefined
                                %% so this should be safe.
                                case application:get_env(blockchain, disable_poc_v4_target_challenge_age, false) of
                                    false ->
                                        check_challenger_distance(ChallengerLoc, blockchain_ledger_gateway_v2:location(Gateway), Vars);
                                    true ->
                                        %% Allow to be included in testing
                                        true
                                end
                        end
                end,
                maps:without([ChallengerAddr], GatewayScoreMap)).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec score_prob(GatewayScoreMap :: gateway_score_map(), Vars :: map()) -> prob_map().
score_prob(GatewayScoreMap, Vars) ->
    %% Assign probability to each gateway
    ProbScores = maps:map(fun(_Addr, {_G, Score}) ->
                                  score_curve(Score, Vars)
                          end,
                          GatewayScoreMap),
    %% Calculate sum of all probs
    SumScores = lists:sum([S || {_A, S} <- maps:to_list(ProbScores)]),
    %% Scale probabilities so they add up to 1.0
    maps:map(fun(_, S) ->
                     normalize_float(S / SumScores)
             end,
             ProbScores).

-spec edge_prob(GatewayScoreMap :: gateway_score_map(), Vars :: map()) -> prob_map().
edge_prob(GatewayScoreMap, Vars) ->
    %% Get all locations
    case prob_edge_wt(Vars) of
        %% if we're just going to throw this away, no reason to do
        %% this work at all.
        0.0 ->
            maps:map(fun(_, _) ->
                             0.0
                     end,
                     GatewayScoreMap);
        _ ->
            Locations = locations(GatewayScoreMap, Vars),
            %% Assign probability to each gateway
            %% TODO: this is basically n^2
            LocSz = maps:size(Locations),
            ParentRes = parent_res(Vars),
            ProbEdges =
                maps:map(
                  fun(_Addr, {Gateway, _Score}) ->
                          Loc = blockchain_ledger_gateway_v2:location(Gateway),
                          GatewayParent = h3:parent(Loc, ParentRes),
                          PopCt = maps:get(GatewayParent, Locations),
                          normalize_float(1 - normalize_float(PopCt/LocSz))
                  end,
                  GatewayScoreMap),
            %% Calculate sum of all probs
            SumEdgeProbs = lists:sum([S || {_A, S} <- maps:to_list(ProbEdges)]),
            %% Scale probabilities so they add up to 1.0
            maps:map(fun(_, E) ->
                             normalize_float(E / SumEdgeProbs)
                     end,
                     ProbEdges)
    end.

-spec target_prob(ProbScores :: prob_map(),
                  ProbEdges :: prob_map(),
                  Vars :: map()) -> prob_map().
target_prob(ProbScores, ProbEdges, Vars) ->
    %% P(Target) = ScoreWeight*P(Score) + EdgeWeight*P(Edge)
    maps:map(fun(Addr, PScore) ->
                     normalize_float(prob_score_wt(Vars) * PScore) +
                     normalize_float(prob_edge_wt(Vars) * maps:get(Addr, ProbEdges))
             end,
             ProbScores).

-spec scaled_prob(PTarget :: prob_map()) -> prob_map().
scaled_prob(PTarget) ->
    SumProbs = lists:sum(maps:values(PTarget)),
    maps:map(fun(_Addr, P) ->
                     normalize_float(P / SumProbs)
             end, PTarget).

-spec locations(GatewayScoreMap :: gateway_score_map(), Vars :: #{}) -> #{h3:index() => integer()}.
locations(GatewayScoreMap, Vars) ->
    %% Get all locations from score map
    Res = parent_res(Vars),
    AllRes = [h3:parent(blockchain_ledger_gateway_v2:location(G), Res) || {G, _S} <- maps:values(GatewayScoreMap)],
    lists:foldl(fun(R, M) ->
                        maps:update_with(R, fun(V) -> V + 1 end, 1, M)
                end,
                #{},
                AllRes).

-spec select_target([{libp2p_crypto:pubkey_bin(), float()}], float()) -> {error, no_target} | {ok, libp2p_crypto:pubkey_bin()}.
select_target([], _) ->
    {error, no_target};
select_target([{GwAddr, Prob}=_Head | _], Rnd) when Rnd - Prob =< 0 ->
    {ok, GwAddr};
select_target([{_GwAddr, Prob} | Tail], Rnd) ->
    select_target(Tail, normalize_float(Rnd - Prob)).

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

-spec target_exclusion_cells(Vars :: map()) -> pos_integer().
target_exclusion_cells(Vars) ->
    maps:get(poc_v4_target_exclusion_cells, Vars, ?POC_V4_TARGET_EXCLUSION_CELLS).

-spec check_challenger_distance(ChallengerLoc :: h3:index(),
                                GatewayLoc :: h3:index(),
                                Vars :: map()) -> boolean().
check_challenger_distance(ChallengerLoc, GatewayLoc, Vars) ->
    %% Number of grid cells to exclude when considering the gateway_loc as a potential target
    ExclusionCells = target_exclusion_cells(Vars),
    %% Normalizing resolution
    ParentRes = parent_res(Vars),
    %% Parent h3 index of the challenger
    ChallengerParent = h3:parent(ChallengerLoc, ParentRes),
    %% Parent h3 index of the gateway being considered
    GatewayParent = h3:parent(GatewayLoc, ParentRes),
    %% Check that they are far
    try h3:grid_distance(ChallengerParent, GatewayParent) > ExclusionCells of
        Res -> Res
    catch
        %% Grid distance may badarg because of pentagonal distortion or
        %% non matching resolutions or just being too far.
        %% In either of those cases, we assume that the gateway
        %% is potentially legitimate to be a target.
        _:_ -> true
    end.
