%%%-------------------------------------------------------------------
%% Public
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_v2).

-export([
    build/5
]).

-define(PROB, 0.01).
-define(PARENT_RES, 8).

-type path() :: [libp2p_crypto:pubkey_bin()]. %% XXX: or return [blockchain_ledger_gateway_v2:gateway()]
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

-spec build(TargetGw :: blockchain_ledger_gateway_v2:gateway(),
            ActiveGateways :: blockchain_ledger_v1:active_gateways(),
            HeadBlockTime :: non_neg_integer(),
            Entropy :: binary(),
            Limit :: pos_integer()) -> path().
build(TargetGw, ActiveGateways, HeadBlockTime, Entropy, Limit) ->
    build_(TargetGw, ActiveGateways, HeadBlockTime, Entropy, Limit, [blockchain_ledger_gateway_v2:location(maps:get(TargetGw, ActiveGateways))], [TargetGw]).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
build_(TargetGw, ActiveGateways, HeadBlockTime, Entropy, Limit, Indices, Path) when length(Path) < Limit ->
    %% TODO: Limit based on path limit chain var
    %% or stop looking if there are no more eligible next hop witnesses
    case next_hop(TargetGw, ActiveGateways, HeadBlockTime, Entropy, Indices) of
        {error, no_witness} ->
            case next_hop(lists:last(Path), ActiveGateways, HeadBlockTime, Entropy, Indices) of
                {error, no_witness} ->
                    Path;
                {ok, WitnessAddr0} ->
                    NextHopGw0 = maps:get(WitnessAddr0, ActiveGateways),
                    Res = blockchain_ledger_gateway_v2:location(NextHopGw0),
                    build_(NextHopGw0, ActiveGateways, HeadBlockTime, Entropy, Limit, [Res | Indices], [NextHopGw0 | Path])
            end;
        {ok, WitnessAddr} ->
            NextHopGw = maps:get(WitnessAddr, ActiveGateways),
            Res = blockchain_ledger_gateway_v2:location(NextHopGw),
            NewPath = [NextHopGw | Path],
            build_(lists:last(NewPath), ActiveGateways, HeadBlockTime, Entropy, Limit, [Res | Indices], lists:reverse(NewPath))
    end;
build_(_TargetGw, _ActiveGateways, _HeadBlockTime, _Entropy, _Limit, _Indices, Path) ->
    Path.

-spec next_hop(Gateway :: blockchain_ledger_gateway_v2:gateway(),
               ActiveGateways :: blockchain_ledger_v1:active_gateways(),
               HeadBlockTime :: non_neg_integer(),
               Entropy :: binary(),
               Indices :: [h3:h3_index()]) -> libp2p_crypto:pubkey_bin().
next_hop(Gateway, ActiveGateways, HeadBlockTime, Entropy, Indices) ->
    %% Get all the witnesses for this Gateway
    Witnesses = blockchain_ledger_gateway_v2:witnesses(Gateway),
    %% Filter out those witnesses which are in the same hex as this Gateway
    FilteredWitnesses0 = filter_same_hex_witnesses(Gateway, Witnesses, ActiveGateways, ?PARENT_RES),
    %% Filter out those witnesses which belong to the same hex we have already traversed
    FilteredWitnesses = filter_traversed_indices(Indices, FilteredWitnesses0, ActiveGateways),
    %% Assign probabilities to filtered witnesses
    P1Map = bayes_probs(FilteredWitnesses),
    P2Map = time_probs(HeadBlockTime, FilteredWitnesses),
    Probs = maps:map(fun(WitnessAddr, P2) ->
                             P2 * maps:get(WitnessAddr, P1Map)
                     end, P2Map),
    %% Scale probabilities assigned to filtered witnesses so they add up to 1 to do the selection
    SumProbs = lists:sum(maps:values(Probs)),
    ScaledProbs = maps:map(fun(_WitnessAddr, P) ->
                                   P / SumProbs
                           end, Probs),
    %% Pick one
    select_witness(ScaledProbs, Entropy).

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

filter_same_hex_witnesses(Gateway, Witnesses, ActiveGateways, ParentRes) ->
    maps:filter(fun(WitnessAddr, _Witness) ->
                                    h3:parent(blockchain_ledger_gateway_v2:location(Gateway), ParentRes) ==
                                    h3:parent(blockchain_ledger_gateway_v2:location(maps:get(WitnessAddr, ActiveGateways)), ParentRes)
                            end,
                            Witnesses).

filter_traversed_indices(Indices, Witnesses, ActiveGateways) ->
    maps:filter(fun(WitnessAddr, _Witness) ->
                        WitnessLoc = blockchain_ledger_gateway_v2:location(maps:get(WitnessAddr, ActiveGateways)),
                        lists:member(WitnessLoc, Indices)
                end,
                Witnesses).
