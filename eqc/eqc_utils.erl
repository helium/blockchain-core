-module(eqc_utils).

-export([find_challenger/2]).

find_challenger(ChallengerIndex, ActiveGateways) ->
    find_challenger(ChallengerIndex, ActiveGateways, 0).

find_challenger(ChallengerIndex, ActiveGateways, Iteration) ->
    Idx = case abs(ChallengerIndex + Iteration) rem maps:size(ActiveGateways) of
              0 -> maps:size(ActiveGateways);
              N -> N
          end,
    Challenger = lists:nth(Idx, maps:keys(ActiveGateways)),
    case blockchain_ledger_gateway_v2:location(maps:get(Challenger, ActiveGateways)) of
        undefined ->
            find_challenger(ChallengerIndex, ActiveGateways, next_iteration(Iteration));
        ChallengerLoc ->
            {Challenger, ChallengerLoc}
    end.

next_iteration(0) -> 1;
next_iteration(N) when N > 0 ->
    N * -1;
next_iteration(N) ->
    (N * -1) + 1.
