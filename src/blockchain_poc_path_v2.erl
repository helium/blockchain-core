%%%-------------------------------------------------------------------
%% Public
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_v2).

-export([
    build/5
]).

%% XXX: Maybe these need to be chain vars?
-define(PROB, 0.01). %% probability that we pick a "bad" witness given that it got picked
-define(EX_DIST, 10). %% exclude 6 grid cells for res - 1
-define(PARENT_RES, 11). %% normalize to 11 res

-type path() :: [libp2p_crypto:pubkey_bin()].
-type prob_map() :: #{libp2p_crypto:pubkey_bin() => float()}.

-spec build(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
            ActiveGateways :: blockchain_ledger_v1:active_gateways(),
            HeadBlockTime :: pos_integer(),
            Hash :: binary(),
            Limit :: pos_integer()) -> path().
build(TargetPubkeyBin, ActiveGateways, HeadBlockTime, Hash, Limit) ->
    TargetGwLoc = blockchain_ledger_gateway_v2:location(maps:get(TargetPubkeyBin, ActiveGateways)),
    Seed = seed(Hash),
    {RandVal, RandState} = rand:uniform_s(Seed),
    build_(TargetPubkeyBin,
           ActiveGateways,
           HeadBlockTime,
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
             RandVal :: float(),
             RandState :: rand:state(),
             Limit :: pos_integer(),
             Indices :: [h3:h3_index()],
             Path :: path()) -> path().
build_(TargetPubkeyBin,
       ActiveGateways,
       HeadBlockTime,
       RandVal,
       RandState,
       Limit,
       Indices,
       Path) when length(Path) < Limit ->
    %% Try to find a next hop
    case next_hop(TargetPubkeyBin, ActiveGateways, HeadBlockTime, RandVal, Indices) of
        {error, no_witness} ->
            %% Try the last hotspot in the path if no witness found
            case next_hop(lists:last(Path), ActiveGateways, HeadBlockTime, RandVal, Indices) of
                {error, no_witness} ->
                    %% Stop
                    Path;
                {ok, WitnessAddr0} ->
                    %% Keep going
                    NextHopGw0 = maps:get(WitnessAddr0, ActiveGateways),
                    Index = blockchain_ledger_gateway_v2:location(NextHopGw0),
                    {NewRandVal0, NewRandState0} = rand:uniform_s(RandState),
                    build_(WitnessAddr0,
                           ActiveGateways,
                           HeadBlockTime,
                           NewRandVal0,
                           NewRandState0,
                           Limit,
                           [Index | Indices],
                           [WitnessAddr0 | Path])
            end;
        {ok, WitnessAddr} ->
            %% Try the last hop in the new path, basically flip so we search in two directions
            NextHopGw = maps:get(WitnessAddr, ActiveGateways),
            Index = blockchain_ledger_gateway_v2:location(NextHopGw),
            NewPath = [WitnessAddr | Path],
            {NewRandVal, NewRandState} = rand:uniform_s(RandState),
            build_(lists:last(NewPath),
                   ActiveGateways,
                   HeadBlockTime,
                   NewRandVal,
                   NewRandState,
                   Limit,
                   [Index | Indices],
                   lists:reverse(NewPath))
    end;
build_(_TargetPubkeyBin, _ActiveGateways, _HeadBlockTime, _RandVal, _RandState, _Limit, _Indices, Path) ->
    Path.

-spec next_hop(GatewayBin :: blockchain_ledger_gateway_v2:gateway(),
               ActiveGateways :: blockchain_ledger_v1:active_gateways(),
               HeadBlockTime :: pos_integer(),
               RandVal :: float(),
               Indices :: [h3:h3_index()]) -> {error, no_witness} | {ok, libp2p_crypto:pubkey_bin()}.
next_hop(GatewayBin, ActiveGateways, HeadBlockTime, RandVal, Indices) ->
    %% Get this gateway
    Gateway = maps:get(GatewayBin, ActiveGateways),

    case blockchain_ledger_gateway_v2:location(Gateway) of
        undefined ->
            {error, no_witness};
        GatewayLoc ->
            %% Get all the witnesses for this Gateway
            Witnesses = blockchain_ledger_gateway_v2:witnesses(Gateway),
            %% Filter witnesses
            FilteredWitnesses = filter_witnesses(GatewayLoc, Indices, Witnesses, ActiveGateways),
            %% Assign probabilities to filtered witnesses
            P1Map = bayes_probs(FilteredWitnesses),
            P2Map = time_probs(HeadBlockTime, FilteredWitnesses),
            Probs = maps:map(fun(WitnessAddr, P2) ->
                                     P2 * maps:get(WitnessAddr, P1Map)
                             end, P2Map),
            %% Scale probabilities assigned to filtered witnesses so they add up to 1 to do the selection
            SumProbs = lists:sum(maps:values(Probs)),
            ScaledProbs = maps:to_list(maps:map(fun(_WitnessAddr, P) ->
                                                        P / SumProbs
                                                end, Probs)),
            %% Pick one
            select_witness(ScaledProbs, RandVal)
    end.


-spec bayes_probs(Witnesses :: blockchain_ledger_gateway_v2:witnesses()) -> prob_map().
bayes_probs(Witnesses) when map_size(Witnesses) == 1 ->
    %% There is only a single witness, probabilitiy of picking it is 1
    maps:map(fun(_, _) -> 1.0 end, Witnesses);
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
                        maps:put(WitnessAddr, Prob, Acc)
                end,
                #{},
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
                       ActiveGateways :: blockchain_ledger_v1:active_gateways()) -> blockchain_ledger_gateway_v2:witnesses().
filter_witnesses(GatewayLoc, Indices, Witnesses, ActiveGateways) ->
    GatewayParent = h3:parent(GatewayLoc, h3:get_resolution(GatewayLoc) - 1),
    ParentIndices = [h3:parent(Index, ?PARENT_RES) || Index <- Indices],
    maps:filter(fun(WitnessAddr, _Witness) ->
                        WitnessGw = maps:get(WitnessAddr, ActiveGateways),
                        WitnessLoc = blockchain_ledger_gateway_v2:location(WitnessGw),
                        WitnessParent = h3:parent(WitnessLoc, ?PARENT_RES),
                        %% Dont include any witness we've already added in indices
                        not(lists:member(WitnessLoc, Indices)) andalso
                        %% Don't include any witness whose parent is the same as the gateway we're looking at
                        (GatewayParent /= WitnessParent) andalso
                        %% Don't include any witness whose parent is too close to any of the indices we've already seen
                        check_witness_distance(WitnessParent, ParentIndices)
                end,
                Witnesses).

-spec seed(Hash :: binary()) -> rand:state().
seed(Hash) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Hash),
    rand:seed_s(exs1024s, {A, B, C}).

-spec check_witness_distance(WitnessParent :: h3:h3_index(), ParentIndices :: [h3:h3_index()]) -> boolean().
check_witness_distance(WitnessParent, ParentIndices) ->
    not(lists:any(fun(ParentIndex) ->
                          h3:grid_distance(WitnessParent, ParentIndex) < ?EX_DIST
                  end, ParentIndices)).
