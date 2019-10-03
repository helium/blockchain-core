%%%-------------------------------------------------------------------
%% Public
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_v2).

-export([
    build/3
]).

-define(PROB, 0.01).

-type path() :: [libp2p_crypto:pubkey_bin()]. %% XXX: or return [blockchain_ledger_gateway_v2:gateway()]

-spec build(TargetGw :: blockchain_ledger_gateway_v2:gateway(),
            Ledger :: blockchain_ledger_v1:ledger(),
            Chain :: blockchain:blockchain()) -> path().
build(TargetGw, Ledger, Chain) ->
    CurrentTime = current_time(Ledger, Chain),
    build_(TargetGw, CurrentTime, []).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
build_(TargetGw, CurrentTime, Path) ->
    %% TODO: Limit based on path limit chain var
    %% or stop looking if there are no more eligible next hop witnesses
    NextHopGw = next_hop(TargetGw, CurrentTime, Path),
    build_(NextHopGw, CurrentTime, [NextHopGw | Path]).

-spec next_hop(Gateway :: blockchain_ledger_gateway_v2:gateway(),
               CurrentTime :: non_neg_integer(),
               Path :: path()) -> blockchain_ledger_gateway_v2:gateway().
next_hop(Gateway, CurrentTime, Path) ->
    Witnesses = blockchain_ledger_gateway_v2:witnesses(Gateway),
    Probs1 = bayes_probs(Witnesses),
    Probs2 = time_probs(CurrentTime, Witnesses),
    Probs = lists:foldl(fun({P1, P2}) -> P1 * P2 end, lists:zip(Probs1, Probs2)),
    %% TODO: Do the thing. Select using Probs from Witnesses
    ok.

-spec bayes_probs(Witnesses :: [blockchain_ledger_gateway_v2:gateway_witness()]) -> [float()].
bayes_probs(Witnesses) ->
    Probs = lists:foldl(fun(Witness, Acc) ->
                                RSSIs = blockchain_ledger_gateway_v2:witness_hist(Witness),
                                SumRSSIs = lists:sum(maps:values(RSSIs)),
                                %% TODO: Better binning for rssi histogram
                                BadRSSICount = maps:get(28, RSSIs, 0),
                                Prob = case SumRSSIs == 0 orelse BadRSSICount == 0 of
                                           true ->
                                               %% Default to equal prob
                                               [1/length(Witnesses) | Acc];
                                           false ->
                                               %% P(A|B) = P(B|A)*P(A)/P(B), where
                                               %% P(A): prob of selecting any gateway
                                               %% P(B): prob of selecting a gateway with known bad rssi value
                                               %% P(B|A): prob of selecting B given that A is true
                                               [(?PROB) * (1/length(Witnesses))/(BadRSSICount / SumRSSIs) | Acc]
                                       end,
                                [Prob | Acc]
                        end,
                        [],
                        Witnesses),
    lists:reverse(Probs).

-spec time_probs(CurrentTime :: non_neg_integer(),
                 Witnesses :: [blockchain_ledger_gateway_v2:gateway_witness()]) -> [float()].
time_probs(CurrentTime, Witnesses) ->
    Deltas = lists:foldl(fun(Witness, Acc) ->
                                 %% XXX: Needs more thought
                                 case blockchain_ledger_gateway_v2:witness_recent_time(Witness) of
                                     undefined ->
                                         [CurrentTime | Acc];
                                     T ->
                                         [(CurrentTime - T) | Acc]
                                 end
                         end, [],
                         Witnesses),
    %% NOTE: Use inverse of the probabilities to bias against staler witnesses, hence the one minus
    [(1 - X/lists:sum(Deltas)) || X <- lists:reverse(Deltas)].

-spec current_time(Ledger :: blockchain_ledger_v1:ledger(),
                   Chain :: blockchain:blockchain()) -> non_neg_integer().
current_time(Ledger, Chain) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    {ok, Block} = blockchain:get_block(Height, Chain),
    blockchain_block:time(Block).

