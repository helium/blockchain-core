%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_path_v4 implementation.
%%%
%%% The way paths are built depends solely on witnessing data we have accumulated
%%% in the blockchain ledger.
%%%
%%% Assuming we already have a `TargetPubkeyBin` and `TargetRandState` using `target_v3:target`.
%%% We recursively start building a path starting at `TargetPubkeyBin`.
%%%
%%% Each potential hop's witnesses goes through the following checks:
%%%
%%% * Don't include any witnesses in any parent cell we've already visited
%%% * Don't include any witness whose parent is the same as the gateway we're looking at
%%% * Don't include any witness whose parent is too close to any of the indices we've already seen
%%% * Don't include any witness who have bad rssi range
%%% * Don't include any witness who are too far from the current gateway
%%% * Dont include any witness who do not have the required capability
%%%
%%% We then assign cumulative probabilities to each filtered witness. Each of those
%%% probabilities have an associated weight to them governed by chain variables.
%%% Currently supported weights are:
%%%
%%% * time_weight
%%% * rssi_weight
%%% * count_weight
%%% * randomness_wt
%%% * centrality_wt
%%%
%%% We scale these probabilities and run an ICDF to select the witness from
%%% the witness list. Once we have a potential next hop, we simply do the same process
%%% for the next hop and continue building till the path limit is reached or there
%%% are no more witnesses to continue with.
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_path_v4).

-export([
    build/5
]).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_caps.hrl").

-type path() :: [libp2p_crypto:pubkey_bin()].
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

%% @doc Build a path starting at `TargetPubkeyBin`.
-spec build(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
            TargetRandState :: rand:state(),
            Ledger :: blockchain:ledger(),
            HeadBlockTime :: pos_integer(),
            Vars :: map()) -> path().
build(TargetPubkeyBin, TargetRandState, Ledger, HeadBlockTime, Vars) ->
    TargetGw = find(TargetPubkeyBin, Ledger),
    TargetGwLoc = blockchain_ledger_gateway_v2:location(TargetGw),
    build_(TargetPubkeyBin,
           Ledger,
           HeadBlockTime,
           Vars,
           TargetRandState,
           [TargetGwLoc],
           [TargetPubkeyBin]).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec build_(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
             Ledger :: blockchain:ledger(),
             HeadBlockTime :: pos_integer(),
             Vars :: map(),
             RandState :: rand:state(),
             Indices :: [h3:h3_index()],
             Path :: path()) -> path().
build_(TargetPubkeyBin,
       Ledger,
       HeadBlockTime,
       #{poc_path_limit := Limit} = Vars,
       RandState,
       Indices,
       Path) when length(Path) < Limit ->
    %% Try to find a next hop
    case next_hop(TargetPubkeyBin, Ledger, HeadBlockTime, Vars, RandState, Indices) of
        {error, _} ->
            lists:reverse(Path);
        {ok, {WitnessPubkeyBin, NewRandState}} ->
            %% Try the next hop in the new path, continue building forward
            NextHopGw = find(WitnessPubkeyBin, Ledger),
            Index = blockchain_ledger_gateway_v2:location(NextHopGw),
            NewPath = [WitnessPubkeyBin | Path],
            build_(WitnessPubkeyBin,
                   Ledger,
                   HeadBlockTime,
                   Vars,
                   NewRandState,
                   [Index | Indices],
                   NewPath)
    end;
build_(_TargetPubkeyBin, _Ledger, _HeadBlockTime, _Vars, _RandState, _Indices, Path) ->
    lists:reverse(Path).

-spec next_hop(GatewayBin :: libp2p_crypto:pubkey_bin(),
               Ledger :: blockchain:ledger(),
               HeadBlockTime :: pos_integer(),
               Vars :: map(),
               RandState :: rand:state(),
               Indices :: [h3:h3_index()]) -> {error, no_witness} |
                                              {error, all_witnesses_too_close} |
                                              {error, zero_weight} |
                                              {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
next_hop(GatewayBin, Ledger, HeadBlockTime, Vars, RandState, Indices) ->
    %% Get gateway
    Gateway = find(GatewayBin, Ledger),
    case blockchain_ledger_gateway_v2:witnesses(GatewayBin, Gateway, Ledger) of
        W when map_size(W) == 0 ->
            {error, no_witness};
        Witnesses0 ->
            {RandState1, Witnesses} = limit_witnesses(Vars, RandState, Witnesses0),
            %% If this gateway has witnesses, it is implied that it's location cannot be undefined
            GatewayLoc = blockchain_ledger_gateway_v2:location(Gateway),
            %% Filter witnesses
            FilteredWitnesses = filter_witnesses(GatewayLoc, Indices, Witnesses, Ledger, Vars),

            case maps:size(FilteredWitnesses) of
                S when S > 0 ->
                    %% Assign probabilities to filtered witnesses
                    %% P(WitnessRSSI)  = Probability that the witness has a good (valid) RSSI.
                    PWitnessRSSI = rssi_probs(FilteredWitnesses, Vars),
                    %% P(WitnessTime)  = Probability that the witness timestamp is not stale.
                    PWitnessTime = time_probs(HeadBlockTime, FilteredWitnesses, Vars),
                    %% P(WitnessCount) = Probability that the witness is infrequent.
                    PWitnessCount = witness_count_probs(FilteredWitnesses, Vars),
                    %% P(RSSICentrality) = Probability that the witness rssi lies within a good range
                    PWitnessRSSICentrality = witness_rssi_centrality_probs(FilteredWitnesses, Vars),
                    %% P(Witness) = RSSIWeight*P(WitnessRSSI) + TimeWeight*P(WitnessTime) + CountWeight*P(WitnessCount)
                    PWitness = witness_prob(Vars, PWitnessRSSI, PWitnessTime, PWitnessCount, PWitnessRSSICentrality),
                    PWitnessList = lists:keysort(1, maps:to_list(PWitness)),
                    %% Select witness using icdf
                    {RandVal, NewRandState} = rand:uniform_s(RandState1),
                    case blockchain_utils:icdf_select(PWitnessList, RandVal) of
                        {error, _}=E ->
                            E;
                        {ok, SelectedWitnessPubkeybin} ->
                            {ok, {SelectedWitnessPubkeybin, NewRandState}}
                    end;
                _ ->
                    {error, all_witnesses_too_close}
            end
    end.

-spec witness_prob(Vars :: map(),
                   PWitnessRSSI :: prob_map(),
                   PWitnessTime :: prob_map(),
                   PWitnessCount :: prob_map(),
                   PWitnessRSSICentrality :: prob_map()) -> prob_map().
witness_prob(Vars, PWitnessRSSI, PWitnessTime, PWitnessCount, PWitnessRSSICentrality) ->
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
                     ?normalize_float((randomness_wt(Vars) * 1.0), Vars) +
                     ?normalize_float((centrality_wt(Vars) * maps:get(WitnessPubkeyBin, PWitnessRSSICentrality)), Vars)
             end, PWitnessTime).

-spec rssi_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                 Vars :: map()) -> prob_map().
rssi_probs(Witnesses, _Vars) when map_size(Witnesses) == 1 ->
    assign_single_witness_prob(Witnesses);
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
    assign_single_witness_prob(Witnesses);
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
    assign_single_witness_prob(Witnesses);
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

-spec witness_rssi_centrality_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                                    Vars :: map()) -> prob_map().
witness_rssi_centrality_probs(Witnesses, _Vars) when map_size(Witnesses) == 1 ->
    assign_single_witness_prob(Witnesses);
witness_rssi_centrality_probs(Witnesses, Vars) ->
    maps:map(fun(_WitnessPubkeyBin, Witness) ->
                     try
                         blockchain_ledger_gateway_v2:witness_hist(Witness)
                     of
                         Hist ->
                             %% The closer these values are to 0.0, the more confident we
                             %% are that this witness has a reasonable looking RSSI, therefore
                             %% we bias _for_ picking that witness
                             {MaxMetric, MeanMetric} = centrality_metrics(Hist, Vars),
                             blockchain_utils:normalize_float((1 - MaxMetric) * (1 - MeanMetric))
                     catch
                         error:no_histogram ->
                             0.0
                     end
             end,
             Witnesses).


-spec filter_witnesses(GatewayLoc :: h3:h3_index(),
                       Indices :: [h3:h3_index()],
                       Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                       Ledger :: blockchain:ledger(),
                       Vars :: map()) -> blockchain_ledger_gateway_v2:witnesses().
filter_witnesses(GatewayLoc, Indices, Witnesses, Ledger, Vars) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ParentRes = parent_res(Vars),
    ExclusionCells = exclusion_cells(Vars),
    GatewayParent = h3:parent(GatewayLoc, ParentRes),
    ParentIndices = [h3:parent(Index, ParentRes) || Index <- Indices],
    maps:filter(fun(WitnessPubkeyBin, Witness) ->
                        WitnessGw = find(WitnessPubkeyBin, Ledger),
                        case is_witness_stale(WitnessGw, Height, Vars) of
                            true ->
                                false;
                            false ->
                                WitnessLoc = blockchain_ledger_gateway_v2:location(WitnessGw),
                                WitnessParent = h3:parent(WitnessLoc, ParentRes),
                                %% check the GW is allowed to witness, if not they dont do all the other checks
                                case blockchain_ledger_gateway_v2:is_valid_capability(WitnessGw, ?GW_CAPABILITY_POC_WITNESS, Ledger) of
                                    false -> false;
                                    true ->
                                        %% Dont include any witnesses in any parent cell we've already visited
                                        not(lists:member(WitnessLoc, Indices)) andalso
                                        %% Don't include any witness whose parent is the same as the gateway we're looking at
                                        (GatewayParent /= WitnessParent) andalso
                                        %% Don't include any witness whose parent is too close to any of the indices we've already seen
                                        check_witness_distance(WitnessParent, ParentIndices, ExclusionCells) andalso
                                        %% Don't include any witness who have a bad rssi
                                        check_witness_bad_rssi(Witness, Vars) andalso
                                        %% Don't include any witness who have bad rssi range
                                        check_witness_bad_rssi_centrality(Witness, Vars) andalso
                                        %% Don't include any witness who are too far from the current gateway
                                        check_witness_too_far(WitnessLoc, GatewayLoc, Vars)
                                end
                        end
                end,
                Witnesses).

-spec check_witness_too_far(WitnessLoc :: h3:h3_index(),
                            GatewayLoc :: h3:h3_index(),
                            Vars :: map()) -> boolean().
check_witness_too_far(WitnessLoc, GatewayLoc, Vars) ->
    POCMaxHopCells = poc_max_hop_cells(Vars),
    try h3:grid_distance(WitnessLoc, GatewayLoc) of
        Res ->
            Res < POCMaxHopCells
    catch
        %% Grid distance may badarg because of pentagonal distortion or
        %% non matching resolutions or just being too far.
        %% In either of those cases, we assume that the gateway
        %% is potentially illegitimate to be a target.
        _:_ -> false
    end.

-spec check_witness_distance(WitnessParent :: h3:h3_index(),
                             ParentIndices :: [h3:h3_index()],
                             ExclusionCells :: pos_integer()) -> boolean().
check_witness_distance(WitnessParent, ParentIndices, ExclusionCells) ->
    not(lists:any(fun(ParentIndex) ->
                          try h3:grid_distance(WitnessParent, ParentIndex) < ExclusionCells of
                              Res -> Res
                          catch
                              %% Grid distance may badarg because of pentagonal distortion or
                              %% non matching resolutions or just being too far.
                              %% In either of those cases, we assume that the gateway
                              %% is potentially legitimate to be a target.
                              _:_ -> true
                          end
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
                        BadCount when is_integer(V), V > 5 ->
                            %% Activate with PoC v6
                            %% Check that the bad rssi count is less than
                            %% the sum of other known good rssi
                            BadCount < lists:sum(maps:values(maps:without([28], Hist)));
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

-spec check_witness_bad_rssi_centrality(Witness :: blockchain_ledger_gateway_v2:gateway_witness(),
                                        Vars :: map()) -> boolean().
check_witness_bad_rssi_centrality(Witness, Vars) ->
    try
        blockchain_ledger_gateway_v2:witness_hist(Witness)
    of
        Hist ->
            case centrality_metrics(Hist, Vars) of
                %% TODO: Check more conditions?
                %% Check whether the ratio of maxbad/maxgood or meanbad/meangood exceeds 1.0
                %% If so, we exclude that witness
                {M1, M2} when M1 >= 1.0 orelse M2 >= 1.0 ->
                    false;
                _ ->
                    true
            end
    catch
        error:no_histogram ->
            false
    end.

-spec is_witness_stale(Gateway :: blockchain_ledger_gateway_v2:gateway(),
                       Height :: pos_integer(),
                       Vars :: map()) -> boolean().
is_witness_stale(Gateway, Height, Vars) ->
    case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
        undefined ->
            %% No POC challenge, don't include
            true;
        C ->
            %% Check challenge age is recent depending on the set chain var
            (Height - C) >= challenge_age(Vars)
    end.

-spec rssi_weight(Vars :: map()) -> float().
rssi_weight(Vars) ->
    maps:get(poc_v4_prob_rssi_wt, Vars).

-spec time_weight(Vars :: map()) -> float().
time_weight(Vars) ->
    maps:get(poc_v4_prob_time_wt, Vars).

-spec count_weight(Vars :: map()) -> float().
count_weight(Vars) ->
    maps:get(poc_v4_prob_count_wt, Vars).

-spec prob_no_rssi(Vars :: map()) -> float().
prob_no_rssi(Vars) ->
    maps:get(poc_v4_prob_no_rssi, Vars).

-spec prob_good_rssi(Vars :: map()) -> float().
prob_good_rssi(Vars) ->
    maps:get(poc_v4_prob_good_rssi, Vars).

-spec prob_bad_rssi(Vars :: map()) -> float().
prob_bad_rssi(Vars) ->
    maps:get(poc_v4_prob_bad_rssi, Vars).

-spec parent_res(Vars :: map()) -> pos_integer().
parent_res(Vars) ->
    maps:get(poc_v4_parent_res, Vars).

-spec exclusion_cells(Vars :: map()) -> pos_integer().
exclusion_cells(Vars) ->
    maps:get(poc_v4_exclusion_cells, Vars).

-spec nanosecond_time(Time :: integer()) -> integer().
nanosecond_time(Time) ->
    erlang:convert_time_unit(Time, millisecond, nanosecond).

-spec randomness_wt(Vars :: map()) -> float().
randomness_wt(Vars) ->
    maps:get(poc_v4_randomness_wt, Vars).

-spec centrality_wt(Vars :: map()) -> float().
centrality_wt(Vars) ->
    maps:get(poc_centrality_wt, Vars).

-spec poc_version(Vars :: map()) -> pos_integer().
poc_version(Vars) ->
    maps:get(poc_version, Vars).

-spec challenge_age(Vars :: map()) -> pos_integer().
challenge_age(Vars) ->
    maps:get(poc_v4_target_challenge_age, Vars).

-spec poc_good_bucket_low(Vars :: map()) -> integer().
poc_good_bucket_low(Vars) ->
    maps:get(poc_good_bucket_low, Vars).

-spec poc_good_bucket_high(Vars :: map()) -> integer().
poc_good_bucket_high(Vars) ->
    maps:get(poc_good_bucket_high, Vars).

-spec poc_max_hop_cells(Vars :: map()) -> integer().
poc_max_hop_cells(Vars) ->
    maps:get(poc_max_hop_cells, Vars).

%% ==================================================================
%% Helper Functions
%% ==================================================================

%% we assume that everything that has made it into build has already
%% been asserted, and thus the lookup will never fail. This function
%% in no way exists simply because
%% blockchain_gateway_cache:get is too much to type a bunch
%% of times.
-spec find(libp2p_crypto:pubkey_bin(), blockchain_ledger_v1:ledger()) -> blockchain_ledger_gateway_v2:gateway().
find(Addr, Ledger) ->
    {ok, Gw} = blockchain_gateway_cache:get(Addr, Ledger),
    Gw.

-spec split_hist(Hist :: blockchain_ledger_gateway_v2:histogram(),
                 Vars :: map()) -> {blockchain_ledger_gateway_v2:histogram(),
                                    blockchain_ledger_gateway_v2:histogram()}.
split_hist(Hist, Vars) ->
    GoodBucketLow = poc_good_bucket_low(Vars),
    GoodBucketHigh = poc_good_bucket_high(Vars),

    %% Split the histogram into two buckets
    GoodBucket = maps:filter(fun(Bucket, _) ->
                                     lists:member(Bucket, lists:seq(GoodBucketLow, GoodBucketHigh))
                             end,
                             Hist),
    BadBucket = maps:without(maps:keys(GoodBucket), Hist),
    {GoodBucket, BadBucket}.

%%%-----------------------------------------------------------------------------
%%% @doc Check whether the range of RSSI values lie within acceptable bounds
%%%-----------------------------------------------------------------------------
-spec centrality_metrics(Hist :: blockchain_ledger_gateway_v2:histogram(),
                         Vars :: map()) -> {float(), float()}.
centrality_metrics(Hist, Vars) ->
    {GoodBucket, BadBucket} = split_hist(Hist, Vars),

    GoodBucketValues = maps:values(GoodBucket),
    BadBucketValues = maps:values(BadBucket),

    SG0 = lists:sum(GoodBucketValues),
    SB0 = lists:sum(BadBucketValues),

    case {SG0, SB0} of
        {SG, SB} when SG > 0 andalso SB > 0 ->
            MaxGood = lists:max(GoodBucketValues),
            MaxBad = lists:max(BadBucketValues),

            MeanGood = blockchain_utils:normalize_float(lists:sum(GoodBucketValues) / length(GoodBucketValues)),
            MeanBad = blockchain_utils:normalize_float(lists:sum(BadBucketValues) / length(BadBucketValues)),

            MaxMetric = blockchain_utils:normalize_float(MaxBad / MaxGood),
            MeanMetric = blockchain_utils:normalize_float(MeanBad / MeanGood),

            %% If either of these two become >= 1.0, we are certain that
            %% either the witnessing is too close or inconclusive at best.
            {MaxMetric, MeanMetric};
        {SG, 0} when SG > 0 ->
            %% nothing in bad bucket
            {0.0, 0.0};
        _ ->
            %% Everything else is considered bad
            {1.0, 1.0}
    end.

-spec is_legit_rssi_dominating(Witness :: blockchain_ledger_gateway_v2:gateway_witness()) -> boolean().
is_legit_rssi_dominating(Witness) ->
    try
        blockchain_ledger_gateway_v2:witness_hist(Witness)
    of
        Hist ->
            lists:sum(maps:values(maps:without([28], Hist))) > maps:get(28, Hist)
    catch
        error:no_histogram ->
            false
    end.

-spec assign_single_witness_prob(Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
assign_single_witness_prob(Witnesses) ->
    maps:map(fun(_WitnessPubkeyBin, Witness) ->
                     case is_legit_rssi_dominating(Witness) of
                         true ->
                             %% There is only a single witness with dominating legit RSSIs
                             1.0;
                         false ->
                             %% All bad RSSIs for this single witness
                             0.0
                     end
             end,
             Witnesses).

limit_witnesses(#{?poc_witness_consideration_limit := Limit}, RandState, Witnesses0) ->
    Witnesses = maps:to_list(Witnesses0),
    {RandState1, SubSet} = blockchain_utils:deterministic_subset(Limit, RandState, Witnesses),
    {RandState1, maps:from_list(SubSet)};
limit_witnesses(_Vars, RandState, Witnesses) ->
    {RandState, Witnesses}.
