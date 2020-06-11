%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_path_v5 implementation.
%%%
%%% Builds on top of blockchain_poc_path_v4 implementation.
%%% TODO: Add documentation...
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_path_v5).

-export([
    build/5,
    validate/5
]).

-include("blockchain_utils.hrl").

-type path() :: [libp2p_crypto:pubkey_bin()].

%% @doc Build a path starting at `TargetPubkeyBin`.
-spec build(TargetPubkeyBin :: libp2p_crypto:pubkey_bin(),
            TargetRandState :: rand:state(),
            Ledger :: blockchain:ledger(),
            HeadBlockTime :: pos_integer(),
            Vars :: list()) -> path().
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
             Vars :: list(),
             RandState :: rand:state(),
             Indices :: [h3:h3_index()],
             Path :: path()) -> path().
build_(TargetPubkeyBin,
       Ledger,
       HeadBlockTime,
       Vars,
       RandState,
       Indices,
       Path) ->
    Limit = poc_path_limit(Vars),
    case length(Path) < Limit of
        true ->
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
        false ->
            lists:reverse(Path)
    end.

-spec next_hop(GatewayBin :: blockchain_ledger_gateway_v2:gateway(),
               Ledger :: blockchain:ledger(),
               HeadBlockTime :: pos_integer(),
               Vars :: list(),
               RandState :: rand:state(),
               Indices :: [h3:h3_index()]) -> {error, no_witness} |
                                              {error, all_witnesses_too_close} |
                                              {error, zero_weight} |
                                              {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
next_hop(GatewayBin, Ledger, _HeadBlockTime, Vars, RandState, Indices) ->
    %% Get gateway
    Gateway = find(GatewayBin, Ledger),
    case blockchain_ledger_gateway_v2:witnesses(Gateway) of
        W when map_size(W) == 0 ->
            {error, no_witness};
        Witnesses ->
            %% If this gateway has witnesses, it is implied that it's location cannot be undefined
            GatewayLoc = blockchain_ledger_gateway_v2:location(Gateway),
            %% Filter witnesses
            FilteredWitnesses = filter_witnesses(GatewayLoc, Indices, Witnesses, Ledger, Vars),

            case maps:size(FilteredWitnesses) of
                S when S > 0 ->
                    %% TODO: Once the witnesses have been filtered, assign equal probability to each of them
                    PWitness = #{},
                    PWitnessList = lists:keysort(1, maps:to_list(PWitness)),
                    %% Select witness using icdf
                    {RandVal, NewRandState} = rand:uniform_s(RandState),
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

-spec filter_witnesses(GatewayLoc :: h3:h3_index(),
                       Indices :: [h3:h3_index()],
                       Witnesses :: blockchain_ledger_gateway_v2:witnesses(),
                       Ledger :: blockchain:ledger(),
                       Vars :: list()) -> blockchain_ledger_gateway_v2:witnesses().
filter_witnesses(GatewayLoc, Indices, Witnesses, Ledger, Vars) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ParentRes = parent_res(Vars),
    ExclusionCells = exclusion_cells(Vars),
    GatewayParent = h3:parent(GatewayLoc, ParentRes),
    ParentIndices = [h3:parent(Index, ParentRes) || Index <- Indices],
    maps:filter(fun(WitnessPubkeyBin, _Witness) ->
                        WitnessGw = find(WitnessPubkeyBin, Ledger),
                        case is_witness_stale(WitnessGw, Height, Vars) of
                            true ->
                                false;
                            false ->
                                WitnessLoc = blockchain_ledger_gateway_v2:location(WitnessGw),
                                WitnessParent = h3:parent(WitnessLoc, ParentRes),
                                %% Dont include any witnesses in any parent cell we've already visited
                                not(lists:member(WitnessLoc, Indices)) andalso
                                %% Don't include any witness whose parent is the same as the gateway we're looking at
                                (GatewayParent /= WitnessParent) andalso
                                %% Don't include any witness whose parent is too close to any of the indices we've already seen
                                check_witness_distance(WitnessParent, ParentIndices, ExclusionCells) andalso
                                %% Don't include any witness who are too far from the current gateway
                                check_witness_too_far(WitnessLoc, GatewayLoc, Vars)
                                %% TODO: Add more specifically around the Witness metadata
                        end
                end,
                Witnesses).

-spec check_witness_too_far(WitnessLoc :: h3:h3_index(),
                            GatewayLoc :: h3:h3_index(),
                            Vars :: list()) -> boolean().
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

-spec is_witness_stale(Gateway :: blockchain_ledger_gateway_v2:gateway(),
                       Height :: pos_integer(),
                       Vars :: list()) -> boolean().
is_witness_stale(Gateway, Height, Vars) ->
    case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
        undefined ->
            %% No POC challenge, don't include
            true;
        C ->
            %% Check challenge age is recent depending on the set chain var
            (Height - C) >= challenge_age(Vars)
    end.

-spec parent_res(Vars :: list()) -> pos_integer().
parent_res(Vars) ->
    proplists:get_value(poc_v4_parent_res, Vars, undefined).

-spec exclusion_cells(Vars :: list()) -> pos_integer().
exclusion_cells(Vars) ->
    proplists:get_value(poc_v4_exclusion_cells, Vars, undefined).

-spec challenge_age(Vars :: list()) -> pos_integer().
challenge_age(Vars) ->
    proplists:get_value(poc_v4_target_challenge_age, Vars, undefined).

-spec poc_max_hop_cells(Vars :: list()) -> integer().
poc_max_hop_cells(Vars) ->
    proplists:get_value(poc_max_hop_cells, Vars, undefined).

-spec poc_path_limit(Vars :: list()) -> integer().
poc_path_limit(Vars) ->
    proplists:get_value(poc_path_limit, Vars, undefined).

%% ==================================================================
%% Helper Functions
%% ==================================================================

%% we assume that everything that has made it into build has already
%% been asserted, and thus the lookup will never fail. This function
%% in no way exists simply because
%% blockchain_ledger_v1:find_gateway_info is too much to type a bunch
%% of times.
-spec find(libp2p_crypto:pubkey_bin(), blockchain_ledger_v1:ledger()) -> blockchain_ledger_gateway_v2:gateway().
find(Addr, Ledger) ->
    {ok, Gw} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
    Gw.

%% @doc Validate the proof of coverage receipt path.
%%
%% The proof of coverage receipt path consists of a list of `poc path elements',
%% each of which corresponds to a layer of the onion-encrypted PoC packet. This
%% path element records who the layer was intended for, if we got a receipt from
%% them and any other peers that witnessed this packet but were unable to decrypt
%% it. This function verifies that all the receipts and witnesses appear in the
%% expected order, and satisfy all the validity checks.
%%
%% Because the first packet is delivered over p2p, it cannot have witnesses and
%% the receipt must indicate the origin was `p2p'. All subsequent packets must
%% have the receipt origin as `radio'. The final layer has the packet composed
%% entirely of padding, so there cannot be a valid receipt, but there can be
%% witnesses (as evidence that the final recipient transmitted it).
-spec validate(Txn :: blockchain_txn_poc_receipts_v2:txn_poc_receipts(),
               Path :: path(),
               LayerData :: [binary(), ...],
               LayerHashes :: [binary(), ...],
               OldLedger :: blockchain_ledger_v1:ledger()) -> ok | {error, atom()}.
validate(Txn, Path, LayerData, LayerHashes, OldLedger) ->
    TxnPath = blockchain_txn_poc_receipts_v2:path(Txn),
    TxnPathLength = length(TxnPath),
    RebuiltPathLength = length(Path),
    ZippedLayers = lists:zip(LayerData, LayerHashes),
    ZippedLayersLength = length(ZippedLayers),
    HexPOCID = blockchain_txn_poc_receipts_v2:hex_poc_id(Txn),
    lager:info([{poc_id, HexPOCID}], "starting poc receipt validation..."),

    case TxnPathLength == RebuiltPathLength of
        false ->
            HumanTxnPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(blockchain_poc_path_element_v2:challengee(E)))) || E <- TxnPath],
            HumanRebuiltPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(A))) || A <- Path],
            lager:error([{poc_id, HexPOCID}], "TxnPathLength: ~p, RebuiltPathLength: ~p", [TxnPathLength, RebuiltPathLength]),
            lager:error([{poc_id, HexPOCID}], "TxnPath: ~p", [HumanTxnPath]),
            lager:error([{poc_id, HexPOCID}], "RebuiltPath: ~p", [HumanRebuiltPath]),
            {error, path_length_mismatch};
        true ->
            %% Now check whether layers are of equal length
            case TxnPathLength == ZippedLayersLength of
                false ->
                    lager:error([{poc_id, HexPOCID}], "TxnPathLength: ~p, ZippedLayersLength: ~p", [TxnPathLength, ZippedLayersLength]),
                    {error, zip_layer_length_mismatch};
                true ->
                    Result = lists:foldl(
                               fun(_, {error, _} = Error) ->
                                       Error;
                                  ({Elem, Gateway, {LayerDatum, LayerHash}}, _Acc) ->
                                       case blockchain_poc_path_element_v2:challengee(Elem) == Gateway of
                                           true ->
                                               IsFirst = Elem == hd(blockchain_txn_poc_receipts_v2:path(Txn)),
                                               Receipt = blockchain_poc_path_element_v2:receipt(Elem),
                                               ExpectedOrigin = case IsFirst of
                                                                    true -> p2p;
                                                                    false -> radio
                                                                end,
                                               %% check the receipt
                                               case
                                                   Receipt == undefined orelse
                                                   (blockchain_poc_receipt_v2:is_valid(Receipt) andalso
                                                    blockchain_poc_receipt_v2:gateway(Receipt) == Gateway andalso
                                                    blockchain_poc_receipt_v2:data(Receipt) == LayerDatum andalso
                                                    blockchain_poc_receipt_v2:origin(Receipt) == ExpectedOrigin)
                                               of
                                                   true ->
                                                       %% ok the receipt looks good, check the witnesses
                                                       Witnesses = blockchain_poc_path_element_v2:witnesses(Elem),
                                                       case erlang:length(Witnesses) > 5 of
                                                           true ->
                                                               {error, too_many_witnesses};
                                                           false ->
                                                               %% check there are no duplicates in witnesses list
                                                               WitnessGateways = [blockchain_poc_witness_v2:gateway(W) || W <- Witnesses],
                                                               case length(WitnessGateways) == length(lists:usort(WitnessGateways)) of
                                                                   false ->
                                                                       {error, duplicate_witnesses};
                                                                   true ->
                                                                       blockchain_poc_witness_v2:check_witness_layerhash(Witnesses, Gateway, LayerHash, OldLedger)
                                                               end
                                                       end;
                                                   false ->
                                                       case Receipt == undefined of
                                                           true ->
                                                               lager:error([{poc_id, HexPOCID}],
                                                                           "Receipt undefined, ExpectedOrigin: ~p, LayerDatum: ~p, Gateway: ~p",
                                                                           [Receipt, ExpectedOrigin, LayerDatum, Gateway]);
                                                           false ->
                                                               lager:error([{poc_id, HexPOCID}],
                                                                           "Origin: ~p, ExpectedOrigin: ~p, Data: ~p, LayerDatum: ~p, ReceiptGateway: ~p, Gateway: ~p",
                                                                           [blockchain_poc_receipt_v2:origin(Receipt),
                                                                            ExpectedOrigin,
                                                                            blockchain_poc_receipt_v2:data(Receipt),
                                                                            LayerDatum,
                                                                            blockchain_poc_receipt_v2:gateway(Receipt),
                                                                            Gateway])
                                                       end,
                                                       {error, invalid_receipt}
                                               end;
                                           _ ->
                                               lager:error([{poc_id, HexPOCID}], "receipt not in order"),
                                               {error, receipt_not_in_order}
                                       end
                               end,
                               ok,
                               lists:zip3(TxnPath, Path, ZippedLayers)
                              ),
                    %% clean up ledger context
                    blockchain_ledger_v1:delete_context(OldLedger),
                    Result

            end
    end.
