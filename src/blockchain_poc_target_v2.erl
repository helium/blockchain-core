%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v2 implementation.
%%%
%%% The targeting mechanism is based on the following conditions:
%%% - Favor high scoring hotspots
%%% - Filter hotspots which haven't done a poc request for a long time
%%% - Given a map of gateway_scores, we must ALWAYS find a target
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v2).

-define(POC_V4_TARGET_CHALLENGE_AGE, 300).

-export([
         target/2, filter/4
        ]).

-type gateway_scores() :: [{libp2p_crypto:pubkey_bin(), float()}].
-type gateway_score_map() :: #{libp2p_crypto:pubkey_bin() => {float(), blockchain_ledger_gateway_v2:gateway()}}.

%% @doc Finds a potential target to start the path from.
%% This must always return a target.
%% Favors high scoring gateways, dependent on score^5 curve.
-spec target(Hash :: binary(), GatewayScores :: gateway_scores()) -> {ok, libp2p_crypto:pubkey_bin()}.
target(Hash, GatewayScores) ->
    ProbsAndGatewayAddrs = create_probs(GatewayScores),
    Entropy = blockchain_utils:rand_state(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    select_target(ProbsAndGatewayAddrs, RandVal).

%% @doc Filter gateways based on these conditions:
%% - Inactive gateways (those which haven't challenged in a long time).
%% - Dont target the challenger gateway itself.
-spec filter(GatewayScoreMap :: gateway_score_map(),
             Challenger :: libp2p_crypto:pubkey_bin(),
             Height :: pos_integer(),
             Vars :: map()) -> gateway_scores().
filter(GatewayScoreMap, Challenger, Height, Vars) ->
    lists:foldl(fun({Addr, {Score, Gateway}}, Acc) ->
                        case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
                            undefined ->
                                %% No POC challenge, don't include
                                Acc;
                            C ->
                                case (Height - C) < challenge_age(Vars) of
                                    false ->
                                        %% Last challenge too old, don't include
                                        Acc;
                                    true ->
                                        [{Addr, Score} | Acc]
                                end
                        end
                end,
                [],
                %% exclude the challenger itself
                lists:keysort(1, maps:to_list(maps:without([Challenger], GatewayScoreMap)))).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec create_probs(GatewayScores :: gateway_scores()) -> [{float(), libp2p_crypto:pubkey_bin()}].
create_probs(GatewayScores) ->
    GwScores = lists:foldl(fun({Addr, Score}, Acc) ->
                                   [{Addr, prob(Score)} | Acc]
                           end,
                           [],
                           GatewayScores),
    Scores = [S || {_A, S} <- GwScores],
    [{Score/lists:sum(Scores), GwAddr} || {GwAddr, Score} <- GwScores].

-spec prob(Score :: float()) -> float().
prob(Score) ->
    %% x^5
    Score * Score * Score * Score * Score.

-spec select_target([{float(), libp2p_crypto:pubkey_bin()}], float()) -> {ok, libp2p_crypto:pubkey_bin()}.
select_target([{Prob1, GwAddr1}=_Head | _], Rnd) when Rnd - Prob1 =< 0 ->
    {ok, GwAddr1};
select_target([{Prob1, _GwAddr1} | Tail], Rnd) ->
    select_target(Tail, Rnd - Prob1).

-spec challenge_age(Vars :: map()) -> pos_integer().
challenge_age(Vars) ->
    maps:get(poc_v4_target_challenge_age, Vars, ?POC_V4_TARGET_CHALLENGE_AGE).
