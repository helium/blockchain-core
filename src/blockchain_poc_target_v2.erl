%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v2 implementation.
%%%
%%% TODO: Explain target selection...
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v2).

-export([
         target/2, filter/3
        ]).

-type gateway_scores() :: [{libp2p_crypto:pubkey_bin(), float()}].

-spec target(Hash :: binary(), GatewayScores :: gateway_scores()) -> {ok, libp2p_crypto:pubkey_bin()}.
target(Hash, GatewayScores) ->
    ProbsAndGatewayAddrs = create_probs(GatewayScores),
    Entropy = blockchain_utils:rand_state(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    select_target(ProbsAndGatewayAddrs, RandVal).

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
    %% x^3
    Score * Score * Score.

-spec select_target([{float(), libp2p_crypto:pubkey_bin()}], float()) -> {ok, libp2p_crypto:pubkey_bin()}.
select_target([{Prob1, GwAddr1}=_Head | _], Rnd) when Rnd - Prob1 =< 0 ->
    {ok, GwAddr1};
select_target([{Prob1, _GwAddr1} | Tail], Rnd) ->
    select_target(Tail, Rnd - Prob1).

-spec filter(Ledger :: blockchain_ledger_v1:ledger(),
                    Challenger :: libp2p_crypto:pubkey_bin(),
                    Height :: pos_integer()) -> gateway_scores().
filter(Ledger, Challenger, Height) ->
    ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
    lists:foldl(fun({Addr, Gateway}, Acc) ->
                        case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
                            undefined ->
                                %% No POC challenge, don't include
                                Acc;
                            C ->
                                case (Height - C) < 10 * blockchain_utils:challenge_interval(Ledger) of
                                    false ->
                                        %% Last challenge too old, don't include
                                        Acc;
                                    true ->
                                        {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, Gateway, Height, Ledger),
                                        [{Addr, Score} | Acc]
                                end
                        end
                end,
                [],
                %% exclude the challenger itself
                lists:keysort(1, maps:to_list(maps:without([Challenger], ActiveGateways)))).
