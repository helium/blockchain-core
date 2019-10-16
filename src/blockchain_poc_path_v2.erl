%%%-------------------------------------------------------------------
%% Public
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_v2).

-export([
    build/6
]).

%% XXX: Maybe these need to be chain vars?
-define(POC_V4_EXCLUSION_CELLS, 10). %% exclude 10 grid cells for parent_res: 11
-define(POC_V4_PARENT_RES, 11). %% normalize to 11 res

-type path() :: [libp2p_crypto:pubkey_bin()].
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.


-spec build(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
            ActiveGateways :: blockchain_ledger_v1:active_gateways(),
            HeadBlockTime :: pos_integer(),
            Hash :: binary(),
            Limit :: pos_integer(),
            Vars :: map()) -> path().
build(TargetPubkeyBin, ActiveGateways, HeadBlockTime, Hash, Limit, Vars) ->
    TargetGwLoc = blockchain_ledger_gateway_v2:location(maps:get(TargetPubkeyBin, ActiveGateways)),
    Seed = seed(Hash),
    {RandVal, RandState} = rand:uniform_s(Seed),
    build_(TargetPubkeyBin,
           ActiveGateways,
           HeadBlockTime,
           Vars,
           RandVal,
           RandState,
           Limit,
           [TargetGwLoc],
           [TargetPubkeyBin]).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec build_(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
             ActiveGateways :: blockchain_ledger_v1:active_gateways(),
             HeadBlockTime :: pos_integer(),
             Vars :: map(),
             RandVal :: float(),
             RandState :: rand:state(),
             Limit :: pos_integer(),
             Indices :: [h3:h3_index()],
             Path :: path()) -> path().
build_(TargetPubkeyBin,
       ActiveGateways,
       HeadBlockTime,
       Vars,
       RandVal,
       RandState,
       Limit,
       Indices,
       Path) when length(Path) < Limit ->
    %% Try to find a next hop
    case next_hop(TargetPubkeyBin, ActiveGateways, HeadBlockTime, Vars, RandVal, Indices) of
        {error, no_witness} ->
            lists:reverse(Path);
        {ok, WitnessAddr} ->
            %% Try the last hop in the new path, basically flip so we search in two directions
            NextHopGw = maps:get(WitnessAddr, ActiveGateways),
            Index = blockchain_ledger_gateway_v2:location(NextHopGw),
            NewPath = [WitnessAddr | Path],
            {NewRandVal, NewRandState} = rand:uniform_s(RandState),
            build_(WitnessAddr,
                   ActiveGateways,
                   HeadBlockTime,
                   Vars,
                   NewRandVal,
                   NewRandState,
                   Limit,
                   [Index | Indices],
                   NewPath)
    end;
build_(_TargetPubkeyBin, _ActiveGateways, _HeadBlockTime, _Vars, _RandVal, _RandState, _Limit, _Indices, Path) ->
    lists:reverse(Path).

-spec next_hop(GatewayBin :: blockchain_ledger_gateway_v2:gateway(),
               ActiveGateways :: blockchain_ledger_v1:active_gateways(),
               HeadBlockTime :: pos_integer(),
               Vars :: map(),
               RandVal :: float(),
               Indices :: [h3:h3_index()]) -> {error, no_witness} | {ok, libp2p_crypto:pubkey_bin()}.
next_hop(GatewayBin, ActiveGateways, HeadBlockTime, Vars, RandVal, Indices) ->
    %% Get this gateway
    Gateway = maps:get(GatewayBin, ActiveGateways),

    case blockchain_ledger_gateway_v2:location(Gateway) of
        undefined ->
            {error, no_witness};
        GatewayLoc ->
            %% Get all the witnesses for this Gateway
            Witnesses = blockchain_ledger_gateway_v2:witnesses(Gateway),
            %% Filter witnesses
            FilteredWitnesses = filter_witnesses(GatewayLoc, Indices, Witnesses, ActiveGateways, Vars),
            %% Assign probabilities to filtered witnesses
            P1Map = rssi_probs(FilteredWitnesses),
            P2Map = time_probs(HeadBlockTime, FilteredWitnesses),
            P3Map = witness_count_probs(FilteredWitnesses),
            Probs = maps:map(fun(WitnessAddr, P2) ->
                                     P2 * maps:get(WitnessAddr, P1Map) * maps:get(WitnessAddr, P3Map)
                             end, P2Map),
            %% Scale probabilities assigned to filtered witnesses so they add up to 1 to do the selection
            SumProbs = lists:sum(maps:values(Probs)),
            ScaledProbs = maps:to_list(maps:map(fun(_WitnessAddr, P) ->
                                                        P / SumProbs
                                                end, Probs)),
            %% Pick one
            select_witness(ScaledProbs, RandVal)
    end.


-spec rssi_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
rssi_probs(Witnesses) when map_size(Witnesses) == 1 ->
    %% There is only a single witness, probabilitiy of picking it is 1
    maps:map(fun(_, _) -> 1.0 end, Witnesses);
rssi_probs(Witnesses) ->
    WitnessList = maps:to_list(Witnesses),
    lists:foldl(fun({WitnessAddr, Witness}, Acc) ->
                        RSSIs = blockchain_ledger_gateway_v2:witness_hist(Witness),
                        SumRSSI = lists:sum(maps:values(RSSIs)),
                        BadRSSI = maps:get(28, RSSIs, 0),

                        case {SumRSSI, BadRSSI} of
                            {0, _} ->
                                maps:put(WitnessAddr, 0.5, Acc);
                            {_S, 0} ->
                                maps:put(WitnessAddr, 1, Acc);
                            {S, S} ->
                                maps:put(WitnessAddr, 0.01, Acc);
                            {S, B} ->
                                maps:put(WitnessAddr, (1 - B/S), Acc)
                        end
                end, #{},
                WitnessList).


-spec time_probs(HeadBlockTime :: pos_integer(),
                 Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
time_probs(_, Witnesses) when map_size(Witnesses) == 1 ->
    %% There is only a single witness, probabilitiy of picking it is 1.0
    maps:map(fun(_, _) -> 1.0 end, Witnesses);
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

    DeltaSum = lists:sum(maps:values(Deltas)),

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

-spec witness_count_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
witness_count_probs(Witnesses) when map_size(Witnesses) == 1 ->
    %% only a single witness, probability = 1.0
    maps:map(fun(_, _) -> 1.0 end, Witnesses);
witness_count_probs(Witnesses) ->
    TotalRSSIs = maps:map(fun(_WitnessAddr, Witness) ->
                                  RSSIs = blockchain_ledger_gateway_v2:witness_hist(Witness),
                                  lists:sum(maps:values(RSSIs))
                          end,
                          Witnesses),

    maps:map(fun(WitnessAddr, _Witness) ->
                     case maps:get(WitnessAddr, TotalRSSIs) of
                         0 ->
                             %% No RSSIs at all, default to 1.0
                             1.0;
                         S ->
                             %% Scale and invert this prob
                             (1 - S/lists:sum(maps:values(TotalRSSIs)))
                     end
             end, Witnesses).

-spec select_witness([{libp2p_crypto:pubkey_bin(), float()}], float()) -> {error, no_witness} | {ok, libp2p_crypto:pubkey_bin()}.
select_witness([], _Rnd) ->
    {error, no_witness};
select_witness([{WitnessAddr, Prob}=_Head | _], Rnd) when Rnd - Prob < 0 ->
    {ok, WitnessAddr};
select_witness([{_WitnessAddr, Prob} | Tail], Rnd) ->
    select_witness(Tail, Rnd - Prob).

-spec filter_witnesses(GatewayLoc :: h3:h3_index(),
                       Indices :: [h3:h3_index()],
                       Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                       ActiveGateways :: blockchain_ledger_v1:active_gateways(),
                       Vars :: map()) -> blockchain_ledger_gateway_v2:witnesses().
filter_witnesses(GatewayLoc, Indices, Witnesses, ActiveGateways, Vars) ->

    ParentRes = maps:get(poc_v4_parent_res, Vars, ?POC_V4_PARENT_RES),
    ExclusionCells = maps:get(poc_v4_exclusion_cells, Vars, ?POC_V4_EXCLUSION_CELLS),

    GatewayParent = h3:parent(GatewayLoc, h3:get_resolution(GatewayLoc) - 1),
    ParentIndices = [h3:parent(Index, ParentRes) || Index <- Indices],
    maps:filter(fun(WitnessAddr, _Witness) ->
                        WitnessGw = maps:get(WitnessAddr, ActiveGateways),
                        WitnessLoc = blockchain_ledger_gateway_v2:location(WitnessGw),
                        WitnessParent = h3:parent(WitnessLoc, ParentRes),
                        %% Dont include any witness we've already added in indices
                        not(lists:member(WitnessLoc, Indices)) andalso
                        %% Don't include any witness whose parent is the same as the gateway we're looking at
                        (GatewayParent /= WitnessParent) andalso
                        %% Don't include any witness whose parent is too close to any of the indices we've already seen
                        check_witness_distance(WitnessParent, ParentIndices, ExclusionCells)
                end,
                Witnesses).

-spec seed(Hash :: binary()) -> rand:state().
seed(Hash) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Hash),
    rand:seed_s(exs1024s, {A, B, C}).

-spec check_witness_distance(WitnessParent :: h3:h3_index(),
                             ParentIndices :: [h3:h3_index()],
                             ExclusionCells :: pos_integer()) -> boolean().
check_witness_distance(WitnessParent, ParentIndices, ExclusionCells) ->
    not(lists:any(fun(ParentIndex) ->
                          h3:grid_distance(WitnessParent, ParentIndex) < ExclusionCells
                  end, ParentIndices)).
