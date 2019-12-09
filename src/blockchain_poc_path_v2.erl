%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_path_v2 implementation.
%%%
%%% The way paths are built depends solely on witnessing data we have accumulated
%%% in the blockchain ledger.
%%%
%%% Consider X having [A, B, C, D] as its geographic neighbors but only
%%% having [A, C, E] as it's transmission witnesses. It stands to reason
%%% that we would expect packets from X -> any[A, C, E] to work with relatively
%%% high confidence compared to its geographic neighbors. RF varies
%%% heavily depending on surroundings therefore relying only on geographic
%%% vicinity is not enough to build potentially interesting paths.
%%%
%%% In order to build a path, we first find a target gateway using
%%% blockchain_poc_target_v2:target/3 and build a path outward from it.
%%%
%%% Once we have a target we recursively find a potential next hop from the target
%%% gateway by looking into its witness list.
%%%
%%% Before we calculate the probability associated with each witness in witness
%%% list, we filter out potentially useless paths, depending on the following filters:
%%% - Next hop witness must not be in the same hex index as the target
%%% - Every hop in the path must be unique
%%% - Every hop in the path must have a minimum exclusion distance
%%%
%%% The criteria for a potential next hop witness are biased like so:
%%% - P(WitnessRSSI)  = Probability that the witness has a good (valid) RSSI.
%%% - P(WitnessTime)  = Probability that the witness timestamp is not stale.
%%% - P(WitnessCount) = Probability that the witness is infrequent.
%%%
%%% The overall probability of picking a next witness is additive depending on
%%% chain var configurable weights for each one of the calculated probs.
%%% P(Witness) = RSSIWeight*P(WitnessRSSI) + TimeWeight*P(WitnessTime) + CountWeight*P(WitnessCount)
%%%
%%% We scale these probabilities and run an ICDF to select the witness from
%%% the witness list. Once we have a potential next hop, we simply do the same process
%%% for the next hop and continue building till the path limit is reached or there
%%% are no more witnesses to continue with.
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_path_v2).

-export([
    build/5
]).

-include("blockchain_utils.hrl").

-define(POC_V4_EXCLUSION_CELLS, 10). %% exclude 10 grid cells for parent_res: 11
-define(POC_V4_PARENT_RES, 11). %% normalize to 11 res
%% weights associated with each witness probability type

%% RSSI probabilities
-define(POC_V4_PROB_NO_RSSI, 0.5).
-define(POC_V4_PROB_GOOD_RSSI, 1.0).
-define(POC_V4_PROB_BAD_RSSI, 0.01).

%% NOTE: These _must_ sum to 1.0
-define(POC_V4_PROB_RSSI_WT, 0.3).
-define(POC_V4_PROB_TIME_WT, 0.3).
-define(POC_V4_PROB_COUNT_WT, 0.3).
%% Randomness weight
-define(POC_V4_RANDOMNESS_WT, 0.1).

-type path() :: [libp2p_crypto:pubkey_bin()].
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

%% @doc Build a path starting at `TargetPubkeyBin`.
%% It is expected that the "ActiveGateways" being passed to build/6 fun
%% has already been pre-filtered to remove "inactive" gateways.
-spec build(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
            ActiveGateways :: blockchain_ledger_v1:active_gateways(),
            HeadBlockTime :: pos_integer(),
            Hash :: binary(),
            Vars :: map()) -> path().
build(TargetPubkeyBin, ActiveGateways, HeadBlockTime, Hash, Vars) ->
    true =  maps:is_key(TargetPubkeyBin, ActiveGateways),
    TargetGwLoc = blockchain_ledger_gateway_v2:location(maps:get(TargetPubkeyBin, ActiveGateways)),
    RandState = blockchain_utils:rand_state(Hash),
    build_(TargetPubkeyBin,
           ActiveGateways,
           HeadBlockTime,
           Vars,
           RandState,
           [TargetGwLoc],
           [TargetPubkeyBin]).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec build_(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
             ActiveGateways :: blockchain_ledger_v1:active_gateways(),
             HeadBlockTime :: pos_integer(),
             Vars :: map(),
             RandState :: rand:state(),
             Indices :: [h3:h3_index()],
             Path :: path()) -> path().
build_(TargetPubkeyBin,
       ActiveGateways,
       HeadBlockTime,
       #{poc_path_limit := Limit} = Vars,
       RandState,
       Indices,
       Path) when length(Path) < Limit ->
    %% Try to find a next hop
    {NewRandVal, NewRandState} = rand:uniform_s(RandState),
    case next_hop(TargetPubkeyBin, ActiveGateways, HeadBlockTime, Vars, NewRandVal, Indices) of
        {error, no_witness} ->
            lists:reverse(Path);
        {ok, WitnessPubkeyBin} ->
            %% Try the next hop in the new path, continue building forward
            NextHopGw = maps:get(WitnessPubkeyBin, ActiveGateways),
            Index = blockchain_ledger_gateway_v2:location(NextHopGw),
            NewPath = [WitnessPubkeyBin | Path],
            build_(WitnessPubkeyBin,
                   ActiveGateways,
                   HeadBlockTime,
                   Vars,
                   NewRandState,
                   [Index | Indices],
                   NewPath)
    end;
build_(_TargetPubkeyBin, _ActiveGateways, _HeadBlockTime, _Vars, _RandState, _Indices, Path) ->
    lists:reverse(Path).

-spec next_hop(GatewayBin :: blockchain_ledger_gateway_v2:gateway(),
               ActiveGateways :: blockchain_ledger_v1:active_gateways(),
               HeadBlockTime :: pos_integer(),
               Vars :: map(),
               RandVal :: float(),
               Indices :: [h3:h3_index()]) -> {error, no_witness} | {ok, libp2p_crypto:pubkey_bin()}.
next_hop(GatewayBin, ActiveGateways, HeadBlockTime, Vars, RandVal, Indices) ->
    %% Get gateway
    Gateway = maps:get(GatewayBin, ActiveGateways),
    case blockchain_ledger_gateway_v2:witnesses(Gateway) of
        W when map_size(W) == 0 ->
            {error, no_witness};
        Witnesses ->
            %% If this gateway has witnesses, it is implied that it's location cannot be undefined
            GatewayLoc = blockchain_ledger_gateway_v2:location(Gateway),
            %% Filter witnesses
            FilteredWitnesses = filter_witnesses(GatewayLoc, Indices, Witnesses, ActiveGateways, Vars),
            %% Assign probabilities to filtered witnesses
            %% P(WitnessRSSI)  = Probability that the witness has a good (valid) RSSI.
            PWitnessRSSI = rssi_probs(FilteredWitnesses, Vars),
            %% P(WitnessTime)  = Probability that the witness timestamp is not stale.
            PWitnessTime = time_probs(HeadBlockTime, FilteredWitnesses, Vars),
            %% P(WitnessCount) = Probability that the witness is infrequent.
            PWitnessCount = witness_count_probs(FilteredWitnesses, Vars),
            %% P(Witness) = RSSIWeight*P(WitnessRSSI) + TimeWeight*P(WitnessTime) + CountWeight*P(WitnessCount)
            PWitness = witness_prob(Vars, PWitnessRSSI, PWitnessTime, PWitnessCount),
            %% Scale probabilities assigned to filtered witnesses so they add up to 1 to do the selection
            ScaledProbs = maps:to_list(scaled_prob(PWitness, Vars)),
            %% Pick one
            select_witness(ScaledProbs, RandVal, Vars)
    end.

-spec scaled_prob(PWitness :: prob_map(),
                  Vars :: map()) -> prob_map().
scaled_prob(PWitness, Vars) ->
    %% Scale probabilities assigned to filtered witnesses so they add up to 1 to do the selection
    SumProbs = lists:sum(maps:values(PWitness)),
    maps:map(fun(_WitnessPubkeyBin, P) ->
                     ?normalize_float(P / SumProbs, Vars)
             end, PWitness).

-spec witness_prob(Vars :: map(), PWitnessRSSI :: prob_map(), PWitnessTime :: prob_map(), PWitnessCount :: prob_map()) -> prob_map().
witness_prob(Vars, PWitnessRSSI, PWitnessTime, PWitnessCount) ->
    %% P(Witness) = RSSIWeight*P(WitnessRSSI) + TimeWeight*P(WitnessTime) + CountWeight*P(WitnessCount)
    maps:map(fun(WitnessPubkeyBin, PTime) ->
                     ?normalize_float((time_weight(Vars) * PTime), Vars) +
                     ?normalize_float(rssi_weight(Vars) * maps:get(WitnessPubkeyBin, PWitnessRSSI), Vars) +
                     ?normalize_float(count_weight(Vars) * maps:get(WitnessPubkeyBin, PWitnessCount), Vars) +
                     %% NOTE: The randomness weight is always multiplied with a probability of 1.0
                     %% So we can do something like:
                     %%  - Set all the other weights to 0.0
                     %%  - Set randomness_wt to 1.0
                     %% Doing that would basically eliminate the other associated weights and
                     %% make each witness have equal 1.0 probability of getting picked as next hop
                     ?normalize_float((randomness_wt(Vars) * 1.0), Vars)
             end, PWitnessTime).

-spec rssi_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                 Vars :: map()) -> prob_map().
rssi_probs(Witnesses, _Vars) when map_size(Witnesses) == 1 ->
    %% There is only a single witness, probabilitiy of picking it is 1
    maps:map(fun(_, _) -> 1.0 end, Witnesses);
rssi_probs(Witnesses, Vars) ->
    WitnessList = maps:to_list(Witnesses),
    lists:foldl(fun({WitnessPubkeyBin, Witness}, Acc) ->
                        try
                            blockchain_ledger_gateway_v2:witness_hist(Witness)
                        of
                            RSSIs ->
                                SumRSSI = lists:sum(maps:values(RSSIs)),
                                BadRSSI = maps:get(28, RSSIs, 0),

                                case {SumRSSI, BadRSSI} of
                                    {0, _} ->
                                        %% No RSSI but we have it in the witness list,
                                        %% possibly because of next hop poc receipt.
                                        maps:put(WitnessPubkeyBin, prob_no_rssi(Vars), Acc);
                                    {_S, 0} ->
                                        %% No known bad rssi value
                                        maps:put(WitnessPubkeyBin, prob_good_rssi(Vars), Acc);
                                    {S, S} ->
                                        %% All bad RSSI values
                                        maps:put(WitnessPubkeyBin, prob_bad_rssi(Vars), Acc);
                                    {S, B} ->
                                        %% Invert the "bad" probability
                                        maps:put(WitnessPubkeyBin, ?normalize_float((1 - ?normalize_float(B/S, Vars)), Vars), Acc)
                                end
                        catch
                            error:no_histogram ->
                                maps:put(WitnessPubkeyBin, prob_no_rssi(Vars), Acc)
                        end
                end, #{},
                WitnessList).


-spec time_probs(HeadBlockTime :: pos_integer(),
                 Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                 Vars :: map()) -> prob_map().
time_probs(_, Witnesses, _Vars) when map_size(Witnesses) == 1 ->
    %% There is only a single witness, probabilitiy of picking it is 1.0
    maps:map(fun(_, _) -> 1.0 end, Witnesses);
time_probs(HeadBlockTime, Witnesses, Vars) ->
    Deltas = lists:foldl(fun({WitnessPubkeyBin, Witness}, Acc) ->
                                 case blockchain_ledger_gateway_v2:witness_recent_time(Witness) of
                                     undefined ->
                                         maps:put(WitnessPubkeyBin, nanosecond_time(HeadBlockTime), Acc);
                                     T ->
                                         maps:put(WitnessPubkeyBin, (nanosecond_time(HeadBlockTime) - T), Acc)
                                 end
                         end, #{},
                         maps:to_list(Witnesses)),

    DeltaSum = lists:sum(maps:values(Deltas)),

    %% NOTE: Use inverse of the probabilities to bias against staler witnesses, hence the one minus
    maps:map(fun(_WitnessPubkeyBin, Delta) ->
                     case ?normalize_float((1 - ?normalize_float(Delta/DeltaSum, Vars)), Vars) of
                         0.0 ->
                             %% There is only one
                             1.0;
                         X ->
                             X
                     end
             end, Deltas).

-spec witness_count_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                          Vars :: map()) -> prob_map().
witness_count_probs(Witnesses, _Vars) when map_size(Witnesses) == 1 ->
    %% only a single witness, probability = 1.0
    maps:map(fun(_, _) -> 1.0 end, Witnesses);
witness_count_probs(Witnesses, Vars) ->
    TotalRSSIs = maps:map(fun(_WitnessPubkeyBin, Witness) ->
                                  RSSIs = blockchain_ledger_gateway_v2:witness_hist(Witness),
                                  lists:sum(maps:values(RSSIs))
                          end,
                          Witnesses),

    maps:map(fun(WitnessPubkeyBin, _Witness) ->
                     case maps:get(WitnessPubkeyBin, TotalRSSIs) of
                         0 ->
                             %% No RSSIs at all, default to 1.0
                             1.0;
                         S ->
                             %% Scale and invert this prob
                             ?normalize_float((1 - ?normalize_float(S/lists:sum(maps:values(TotalRSSIs)), Vars)), Vars)
                     end
             end, Witnesses).

-spec select_witness(WitnessProbs :: [{libp2p_crypto:pubkey_bin(), float()}],
                     Rnd :: float(),
                     Vars :: map()) -> {error, no_witness} | {ok, libp2p_crypto:pubkey_bin()}.
select_witness([], _Rnd, _Vars) ->
    {error, no_witness};
select_witness([{WitnessPubkeyBin, Prob}=_Head | _], Rnd, _Vars) when Rnd - Prob < 0 ->
    {ok, WitnessPubkeyBin};
select_witness([{_WitnessPubkeyBin, Prob} | Tail], Rnd, Vars) ->
    select_witness(Tail, ?normalize_float((Rnd - Prob), Vars), Vars).

-spec filter_witnesses(GatewayLoc :: h3:h3_index(),
                       Indices :: [h3:h3_index()],
                       Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                       ActiveGateways :: blockchain_ledger_v1:active_gateways(),
                       Vars :: map()) -> blockchain_ledger_gateway_v2:witnesses().
filter_witnesses(GatewayLoc, Indices, Witnesses, ActiveGateways, Vars) ->
    ParentRes = parent_res(Vars),
    ExclusionCells = exclusion_cells(Vars),
    GatewayParent = h3:parent(GatewayLoc, ParentRes),
    ParentIndices = [h3:parent(Index, ParentRes) || Index <- Indices],
    maps:filter(fun(WitnessPubkeyBin, Witness) ->
                        case maps:is_key(WitnessPubkeyBin, ActiveGateways) of
                            false ->
                                %% Don't include if the witness is not in ActiveGateways
                                false;
                            true ->
                                WitnessGw = maps:get(WitnessPubkeyBin, ActiveGateways),
                                WitnessLoc = blockchain_ledger_gateway_v2:location(WitnessGw),
                                WitnessParent = h3:parent(WitnessLoc, ParentRes),
                                %% Dont include any witnesses in any parent cell we've already visited
                                not(lists:member(WitnessLoc, Indices)) andalso
                                %% Don't include any witness whose parent is the same as the gateway we're looking at
                                (GatewayParent /= WitnessParent) andalso
                                %% Don't include any witness whose parent is too close to any of the indices we've already seen
                                check_witness_distance(WitnessParent, ParentIndices, ExclusionCells) andalso
                                check_witness_inclusion(WitnessPubkeyBin, ActiveGateways, Vars) andalso
                                check_witness_bad_rssi(Witness, Vars)
                        end
                end,
                Witnesses).

-spec check_witness_distance(WitnessParent :: h3:h3_index(),
                             ParentIndices :: [h3:h3_index()],
                             ExclusionCells :: pos_integer()) -> boolean().
check_witness_distance(WitnessParent, ParentIndices, ExclusionCells) ->
    not(lists:any(fun(ParentIndex) ->
                          h3:grid_distance(WitnessParent, ParentIndex) < ExclusionCells
                  end, ParentIndices)).

-spec check_witness_bad_rssi(Witness :: blockchain_ledger_gateway_v2:gateway_witness(),
                             Vars :: map()) -> boolean().
check_witness_bad_rssi(Witness, Vars) ->
    case poc_version(Vars) of
        V when is_integer(V), V > 4 ->
            try
                blockchain_ledger_gateway_v2:witness_hist(Witness)
            of
                Hist ->
                    case maps:get(28, Hist, 0) of
                        0 ->
                            %% No bad RSSIs found, include
                            true;
                        BadCount ->
                            %% If the bad RSSI count does not dominate
                            %% the overall RSSIs this witness has, include,
                            %% otherwise exclude
                            BadCount < lists:sum(maps:values(Hist))
                    end
            catch
                error:no_histogram ->
                    %% No histogram found, include
                    true
            end;
        _ ->
            true
    end.

-spec check_witness_inclusion(WitnessPubkeyBin :: libp2p_crypto:pubkey_bin(),
                              ActiveGateways :: blockchain_ledger_v1:active_gateways(),
                              Vars :: map()) -> boolean().
check_witness_inclusion(WitnessPubkeyBin, ActiveGateways, Vars) ->
    case poc_version(Vars) of
        V when is_integer(V), V > 4 ->
            maps:is_key(WitnessPubkeyBin, ActiveGateways);
        _ ->
            true
    end.

-spec rssi_weight(Vars :: map()) -> float().
rssi_weight(Vars) ->
    maps:get(poc_v4_prob_rssi_wt, Vars, ?POC_V4_PROB_RSSI_WT).

-spec time_weight(Vars :: map()) -> float().
time_weight(Vars) ->
    maps:get(poc_v4_prob_time_wt, Vars, ?POC_V4_PROB_TIME_WT).

-spec count_weight(Vars :: map()) -> float().
count_weight(Vars) ->
    maps:get(poc_v4_prob_count_wt, Vars, ?POC_V4_PROB_COUNT_WT).

-spec prob_no_rssi(Vars :: map()) -> float().
prob_no_rssi(Vars) ->
    maps:get(poc_v4_prob_no_rssi, Vars, ?POC_V4_PROB_NO_RSSI).

-spec prob_good_rssi(Vars :: map()) -> float().
prob_good_rssi(Vars) ->
    maps:get(poc_v4_prob_good_rssi, Vars, ?POC_V4_PROB_GOOD_RSSI).

-spec prob_bad_rssi(Vars :: map()) -> float().
prob_bad_rssi(Vars) ->
    maps:get(poc_v4_prob_bad_rssi, Vars, ?POC_V4_PROB_BAD_RSSI).

-spec parent_res(Vars :: map()) -> pos_integer().
parent_res(Vars) ->
    maps:get(poc_v4_parent_res, Vars, ?POC_V4_PARENT_RES).

-spec exclusion_cells(Vars :: map()) -> pos_integer().
exclusion_cells(Vars) ->
    maps:get(poc_v4_exclusion_cells, Vars, ?POC_V4_EXCLUSION_CELLS).

-spec nanosecond_time(Time :: integer()) -> integer().
nanosecond_time(Time) ->
    erlang:convert_time_unit(Time, millisecond, nanosecond).

-spec randomness_wt(Vars :: map()) -> float().
randomness_wt(Vars) ->
    maps:get(poc_v4_randomness_wt, Vars, ?POC_V4_RANDOMNESS_WT).

-spec poc_version(Vars :: map()) -> undefined | float().
poc_version(Vars) ->
    maps:get(poc_version, Vars, undefined).
