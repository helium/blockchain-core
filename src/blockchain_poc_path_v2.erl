%%%-------------------------------------------------------------------
%% Public
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_v2).

-export([
    build/3
]).

-define(PROB, 0.01).

-type path() :: [libp2p_crypto:pubkey_bin()]. %% XXX: or return [blockchain_ledger_gateway_v2:gateway()]
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

-spec build(TargetGw :: blockchain_ledger_gateway_v2:gateway(),
            ActiveGateways :: blockchain_ledger_v1:active_gateways(),
            HeadBlockTime :: non_neg_integer()) -> path().
build(TargetGw, ActiveGateways, HeadBlockTime) ->
    build_(TargetGw, ActiveGateways, HeadBlockTime, []).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
build_(TargetGw, ActiveGateways, HeadBlockTime, Path) ->
    %% TODO: Limit based on path limit chain var
    %% or stop looking if there are no more eligible next hop witnesses
    NextHopGw = next_hop(TargetGw, ActiveGateways, HeadBlockTime),
    build_(NextHopGw, ActiveGateways, HeadBlockTime, [NextHopGw | Path]).

-spec next_hop(Gateway :: blockchain_ledger_gateway_v2:gateway(),
               ActiveGateways :: blockchain_ledger_v1:active_gateways(),
               HeadBlockTime :: non_neg_integer()) -> libp2p_crypto:pubkey_bin().
next_hop(Gateway, _ActiveGateways, HeadBlockTime) ->
    Witnesses = blockchain_ledger_gateway_v2:witnesses(Gateway),
    P1Map = bayes_probs(Witnesses),
    P2Map = time_probs(HeadBlockTime, Witnesses),
    Probs = maps:map(fun(WitnessAddr, P2) ->
                             P2 * maps:get(WitnessAddr, P1Map)
                     end, P2Map),
    ScaledProbs = maps:map(fun(_WitnessAddr, P) ->
                                   P / lists:sum(maps:values(Probs))
                           end, Probs),
    %% XXX: Use something else here
    select_witness(ScaledProbs, entropy(HeadBlockTime)).

-spec bayes_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
bayes_probs(Witnesses) ->
    lists:foldl(fun({WitnessAddr, Witness}, Acc) ->
                        RSSIs = blockchain_ledger_gateway_v2:witness_hist(Witness),
                        SumRSSIs = lists:sum(maps:values(RSSIs)),
                        %% TODO: Better binning for rssi histogram
                        BadRSSICount = maps:get(28, RSSIs, 0),
                        Prob = case SumRSSIs == 0 orelse BadRSSICount == 0 of
                                   true ->
                                       %% Default to equal prob
                                       1/length(Witnesses);
                                   false ->
                                       %% P(A|B) = P(B|A)*P(A)/P(B), where
                                       %% P(A): prob of selecting any gateway
                                       %% P(B): prob of selecting a gateway with known bad rssi value
                                       %% P(B|A): prob of selecting B given that A is true
                                       ?PROB * (1/length(Witnesses))/(BadRSSICount / SumRSSIs)
                               end,
                        maps:put(WitnessAddr, Prob, Acc)
                end,
                #{},
                maps:to_list(Witnesses)).

-spec time_probs(HeadBlockTime :: non_neg_integer(),
                 Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
time_probs(HeadBlockTime, Witnesses) ->
    Deltas = lists:foldl(fun({WitnessAddr, Witness}, Acc) ->
                                 %% XXX: Needs more thought
                                 case blockchain_ledger_gateway_v2:witness_recent_time(Witness) of
                                     undefined ->
                                         maps:put(WitnessAddr, HeadBlockTime, Acc);
                                     T ->
                                         maps:put(WitnessAddr, (HeadBlockTime - T), Acc)
                                 end
                         end, #{},
                         maps:to_list(Witnesses)),

    DeltaSum = lists:sum(maps:values(Deltas)),

    %% NOTE: Use inverse of the probabilities to bias against staler witnesses, hence the one minus
    maps:map(fun(_WitnessAddr, Delta) -> (1 - Delta/DeltaSum) end, Deltas).

select_witness([], _Rnd) ->
    {error, no_witness};
select_witness([{WitnessAddr, Prob}=_Head | _], Rnd) when Rnd - Prob < 0 ->
    {ok, WitnessAddr};
select_witness([{Prob, _WitnessAddr} | Tail], Rnd) ->
    select_witness(Tail, Rnd - Prob).

entropy(Entropy) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Entropy),
    rand:seed_s(exs1024s, {A, B, C}).
