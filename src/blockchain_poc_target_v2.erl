%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v2 implementation.
%%%
%%% TODO: Explain target selection...
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v2).

-export([
    target/3
]).

-spec target(Hash :: binary(),
             Ledger :: blockchain_ledger_v1:ledger(),
             Challenger :: libp2p_crypto:pubkey_bin()) -> {ok, libp2p_crypto:pubkey_bin()}.
target(Hash, Ledger, Challenger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ActiveGateways = filter_gateways(Ledger, Challenger, Height),
    ProbsAndGatewayAddrs = create_probs(ActiveGateways, Height, Ledger),
    Entropy = blockchain_utils:rand_state(Hash),
    {RandVal, _} = rand:uniform_s(Entropy),
    select_target(ProbsAndGatewayAddrs, RandVal).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec filter_gateways(Ledger :: blockchain_ledger_v1:ledger(),
                      Challenger :: libp2p_crypto:pubkey_bin(),
                      Height :: pos_integer()) -> blockchain_ledger_v1:active_gateways().
filter_gateways(Ledger, Challenger, Height) ->
    ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
    maps:filter(fun(_, Gateway) ->
                        case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
                            undefined ->
                                false;
                            C ->
                                (Height - C) < 10 * blockchain_utils:challenge_interval(Ledger)
                        end
                end,
                %% exclude the challenger itself
                maps:without([Challenger], ActiveGateways)).

-spec create_probs(Gateways :: blockchain_ledger_v1:active_gateways(),
                   Height :: pos_integer(),
                   Ledger :: blockchain_ledger_v1:ledger()) -> [{float(), libp2p_crypto:pubkey_bin()}].
create_probs(Gateways, Height, Ledger) ->
    GwScores = lists:foldl(fun({A, G}, Acc) ->
                                   {_, _, Score} = blockchain_ledger_gateway_v2:score(A, G, Height, Ledger),
                                   [{A, prob_fun(Score)} | Acc]
                           end,
                           [],
                           maps:to_list(Gateways)),
    Scores = [S || {_A, S} <- GwScores],
    [{Score/lists:sum(Scores), GwAddr} || {GwAddr, Score} <- GwScores].

-spec prob_fun(Score :: float()) -> float().
prob_fun(Score) when Score =< 0.25 ->
    -16 * math:pow((Score - 0.25), 2) + 1;
prob_fun(Score) ->
    -1.77 * math:pow((Score - 0.25), 2) + 1.

-spec select_target([{float(), libp2p_crypto:pubkey_bin()}], float()) -> {ok, libp2p_crypto:pubkey_bin()}.
select_target([{Prob1, GwAddr1}=_Head | _], Rnd) when Rnd - Prob1 =< 0 ->
    {ok, GwAddr1};
select_target([{Prob1, _GwAddr1} | Tail], Rnd) ->
    select_target(Tail, Rnd - Prob1).
