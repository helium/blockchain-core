%%%-------------------------------------------------------------------
%% Public
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_v2).

-export([
    build/5
]).

%% XXX: Maybe these need to be chain vars?
-define(PROB, 0.01).
-define(PARENT_RES, 7).

-type path() :: [blockchain_ledger_gateway_v2:gateway()].
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

-spec build(TargetGw :: blockchain_ledger_gateway_v2:gateway(),
            ActiveGateways :: blockchain_ledger_v1:active_gateways(),
            HeadBlockTime :: non_neg_integer(),
            Entropy :: binary(),
            Limit :: pos_integer()) -> path().
build(TargetGw, ActiveGateways, HeadBlockTime, Entropy, Limit) ->
    %% Initialize with the TargetGw already in Indices and Path list
    build_(TargetGw,
           ActiveGateways,
           HeadBlockTime,
           rand_from_entropy(Entropy),
           Limit,
           [blockchain_ledger_gateway_v2:location(TargetGw)],
           [TargetGw]).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
build_(TargetGw, ActiveGateways, HeadBlockTime, Entropy, Limit, Indices, Path) when length(Path) < Limit ->
    %% Try to find a next hop
    case next_hop(TargetGw, ActiveGateways, HeadBlockTime, Entropy, Indices) of
        {error, no_witness} ->
            %% Try the last hotspot in the path if no witness found
            case next_hop(lists:last(Path), ActiveGateways, HeadBlockTime, Entropy, Indices) of
                {error, no_witness} ->
                    %% Stop
                    Path;
                {ok, WitnessAddr0} ->
                    %% Keep going
                    NextHopGw0 = maps:get(WitnessAddr0, ActiveGateways),
                    Res = blockchain_ledger_gateway_v2:location(NextHopGw0),
                    build_(NextHopGw0, ActiveGateways, HeadBlockTime, Entropy, Limit, [Res | Indices], [NextHopGw0 | Path])
            end;
        {ok, WitnessAddr} ->
            %% Try the last hop in the new path, basically flip so we search in two directions
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
    io:format("P1Map: ~p~n", [P1Map]),
    P2Map = time_probs(HeadBlockTime, FilteredWitnesses),
    io:format("P2Map: ~p~n", [P2Map]),
    Probs = maps:map(fun(WitnessAddr, P2) ->
                             P2 * maps:get(WitnessAddr, P1Map)
                     end, P2Map),
    %% Scale probabilities assigned to filtered witnesses so they add up to 1 to do the selection
    SumProbs = lists:sum(maps:values(Probs)),
    io:format("SumProbs: ~p~n", [SumProbs]),
    ScaledProbs = maps:to_list(maps:map(fun(_WitnessAddr, P) ->
                                                P / SumProbs
                                        end, Probs)),
    %% Pick one
    select_witness(ScaledProbs, Entropy).

-spec bayes_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
bayes_probs([_Witness]=_Witnesses) ->
    %% There is only a single witness, probabilitiy of picking it is 1
    1.0;
bayes_probs(Witnesses) ->
    WitnessList = maps:to_list(Witnesses),
    WitnessListLength = length(WitnessList),
    lists:foldl(fun({WitnessAddr, Witness}, Acc) ->
                        RSSIs = blockchain_ledger_gateway_v2:witness_hist(Witness),
                        SumRSSIs = lists:sum(maps:values(RSSIs)),
                        %% TODO: Better binning for rssi histogram
                        BadRSSICount = maps:get(28, RSSIs, 0),
                        Prob = case SumRSSIs == 0 orelse BadRSSICount == 0 of
                                   true ->
                                       %% Default to equal prob
                                       1/WitnessListLength;
                                   false ->
                                       %% P(A|B) = P(B|A)*P(A)/P(B), where
                                       %% P(A): prob of selecting any gateway
                                       %% P(B): prob of selecting a gateway with known bad rssi value
                                       %% P(B|A): prob of selecting B given that A is true
                                       ?PROB * (1/WitnessListLength)/(BadRSSICount / SumRSSIs)
                               end,
                        io:format("WitnessAddr: ~p, Prob: ~p~n", [WitnessAddr, Prob]),
                        maps:put(WitnessAddr, Prob, Acc)
                end,
                #{},
                WitnessList).

-spec time_probs(HeadBlockTime :: non_neg_integer(),
                 Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
time_probs(_, [_Witness]=_Witnesses) ->
    %% There is only a single witness, probabilitiy of picking it is 1.0
    1.0;
time_probs(HeadBlockTime, Witnesses) ->
    Deltas = lists:foldl(fun({WitnessAddr, Witness}, Acc) ->
                                 %% XXX: Needs more thought
                                 case blockchain_ledger_gateway_v2:witness_recent_time(Witness) of
                                     undefined ->
                                         maps:put(WitnessAddr, HeadBlockTime*1000000000, Acc);
                                     T ->
                                         maps:put(WitnessAddr, (HeadBlockTime*1000000000 - T), Acc)
                                 end
                         end, #{},
                         maps:to_list(Witnesses)),


    io:format("Deltas: ~p~n", [Deltas]),
    DeltaSum = lists:sum(maps:values(Deltas)),
    io:format("DeltaSum: ~p~n", [DeltaSum]),

    %% NOTE: Use inverse of the probabilities to bias against staler witnesses, hence the one minus
    maps:map(fun(_WitnessAddr, Delta) ->
                     case (1 - Delta/DeltaSum) of
                         0.0 ->
                             %% There is only one
                             1.0;
                         X ->
                             X
                     end
             end, Deltas).

select_witness([], _Rnd) ->
    {error, no_witness};
select_witness([{WitnessAddr, Prob}=_Head | _], Rnd) when Rnd - Prob < 0 ->
    {ok, WitnessAddr};
select_witness([{_WitnessAddr, Prob} | Tail], Rnd) ->
    select_witness(Tail, Rnd - Prob).

filter_same_hex_witnesses(Gateway, Witnesses, ActiveGateways, ParentRes) ->
    maps:filter(fun(WitnessAddr, _Witness) ->
                        h3:parent(blockchain_ledger_gateway_v2:location(Gateway), ParentRes) /=
                        h3:parent(blockchain_ledger_gateway_v2:location(maps:get(WitnessAddr, ActiveGateways)), ParentRes)
                end,
                Witnesses).

filter_traversed_indices(Indices, Witnesses, ActiveGateways) ->
    maps:filter(fun(WitnessAddr, _Witness) ->
                        WitnessLoc = blockchain_ledger_gateway_v2:location(maps:get(WitnessAddr, ActiveGateways)),
                        not(lists:member(WitnessLoc, Indices))
                end,
                Witnesses).

-spec rand_from_entropy(Entropy :: binary()) -> float().
rand_from_entropy(Entropy) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Entropy),
    {RandVal, _} = rand:uniform_s(rand:seed_s(exs1024s, {A, B, C})),
    RandVal.
