%% == Blockchain Proof of Coverage Classification ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_classification).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-define(DB_FILE, "levels.db").
-define(HEIGHT_KEY, <<"height">>).
-define(MAX_WINDOW_SAMPLES, 11).

%% list of trustees
-type trustees() :: [libp2p_crypto:pubkey_bin()].

-record(som_evaluations, {
          windows :: blockchain_ledger_v1:windows(),
          init_trustees = [] :: trustees(),
          promoted_trustees = [] :: trustees()
         }).

-type evaluations() :: #som_evaluations{}.

-export([process_block/3,
         process_poc_txn/5,
         process_poc_txns/4,
         maybe_process_poc_txns/4,
         maybe_process_ass_loc_txns/2,
         process_ass_loc_txns/2,
         assign_scores/6,
         acc_scores/8,
         calculate_class/8,
         init_trustees/1,
         promoted_trustees/1,
         load_promoted_trustees/1]).

%%%-------------------------------------------------------------------
%%% Block processing
%%%-------------------------------------------------------------------
-spec process_block(Block :: blockchain_block:block(), Ledger :: blockchain_ledger_v1:ledger(), SomEval :: evaluations()) -> evaluations().
process_block(Block, Ledger, SomEval) ->
    DefaultCF = blockchain_ledger_v1:default_cf(Ledger),
    case blockchain_block:height(Block) of
        BlockHeight when BlockHeight >= 425525 ->
            %% Do block txn processing
            ok = maybe_process_poc_txns(Block, BlockHeight, Ledger, SomEval),
            ok = maybe_process_ass_loc_txns(Block, Ledger),

            %% If the last poc txn for a hotspot is extremely stale (governed by ?STALE_THRESHOLD) we reset its window
            ok = blockchain_ledger_som_v1:maybe_phase_out(BlockHeight, Ledger),

            %% Write the height for housekeeping
            ok = blockchain_ledger_som_v1:update_default(Ledger, DefaultCF, ?HEIGHT_KEY, <<BlockHeight:64/integer-unsigned-little>>),

            %% Do all the DB operations in this batch
            %%ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),

            %% Check if there are any promoted trustees
            NewPromotedTrustees = load_promoted_trustees(SomEval),
            NewSomEval = SomEval#som_evaluations{promoted_trustees=NewPromotedTrustees},

            %% Export plotting data
            %%ok = plot_every_500th_block(BlockHeight, NewState),

            %% XXX: Do this better
            %% persistent_term:put(?MODULE, NewState),
            NewSomEval;
        _ ->
            SomEval
    end.

-spec process_ass_loc_txns(AssLocTxns :: [blockchain_txn_assert_location_v1:txn_assert_location()],
                           Ledger :: blockchain_ledger_v1:ledger()) -> ok.
process_ass_loc_txns(AssLocTxns, Ledger) ->
    ok = lists:foreach(fun(Tx) ->
                               Hotspot = blockchain_txn_assert_location_v1:gateway(Tx),
                               ok = blockchain_ledger_som_v1:reset_window(Ledger, Hotspot)
                       end, AssLocTxns),
    ok.

-spec process_poc_txns(BlockHeight :: pos_integer(),
                       POCReceipts :: [blockchain_txn_poc_receipts_v1:txn_poc_receipts()],
                       Ledger :: blockchain_ledger_v1:ledger(),
                       SomEvaluations :: evaluations()) -> ok.
process_poc_txns(BlockHeight,
                 POCReceipts,
                 Ledger,
                 #som_evaluations{promoted_trustees=PromotedTrustees,
                     init_trustees=InitTrustees}) ->
    lists:foreach(fun(POC) ->
                          case process_poc_txn(BlockHeight, InitTrustees, PromotedTrustees, POC, Ledger) of
                              ok ->
                                  ok;
                              {ok, beacon} ->
                                  ok;
                              {ok, HotspotWindowUpdates} ->
                                  POCHash = blockchain_txn_poc_receipts_v1:hash(POC),
                                  blockchain_ledger_som_v1:update_windows(Ledger, BlockHeight, POCHash, HotspotWindowUpdates)
                          end
                  end, POCReceipts).

-spec process_poc_txn(BlockHeight :: pos_integer(),
                      InitTrustees :: trustees(),
                      PromotedTrustees :: trustees(),
                      Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                      Ledger :: blockchain_ledger_v1:ledger()) ->
    ok | {ok, [blockchain_ledger_som_v1:tagged_score()]} | {ok, beacon}.
process_poc_txn(BlockHeight, InitTrustees, PromotedTrustees, Txn, Ledger) ->
    case blockchain:config(?poc_version, Ledger) of
        {ok, V} when V >= 10 ->
            case get_channels(Txn) of
                {ok, Channels} ->
                    do_process_poc_txn(Txn, Channels, InitTrustees, PromotedTrustees, Ledger, BlockHeight);
                {error, _} ->
                    lager:error("Check this first, we'll ignore later or whatever..."),
                    ok
            end;
        _ ->
            %% This will go away with poc v10+
            case catch(calc_txn_channels(Txn, Ledger)) of
                {ok, Channels} ->
                    do_process_poc_txn(Txn, Channels, InitTrustees, PromotedTrustees, Ledger, BlockHeight);
                _ ->
                    ok
            end
    end.

do_process_poc_txn(Txn, Channels, InitTrustees, PromotedTrustees, Ledger, _BlockHeight) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    PathLength = length(Path),

    case PathLength of
        L when L > 1 ->
            case assign_scores(Path, Channels, L, InitTrustees, PromotedTrustees, Ledger) of
                [_ | _]=TaggedScores ->
                    %% We got some new tagged scores
                    %% lager:info("height: ~p, poc_hash: ~p, path: ~p, process_poc_txn: ~p",
                    %%            [BlockHeight,
                    %%             ?TO_B58(blockchain_txn_poc_receipts_v1:hash(Txn)),
                    %%             human_path(Path), human_scores(TaggedScores)]),
                    {ok, TaggedScores};
                [] ->
                    %% There were no new scores, do nothing
                    ok
            end;
        _ ->
            %% Ignore beaconing for now
            {ok, beacon}
    end.


-spec assign_scores(Path :: blockchain_poc_path_element_v1:poc_path(),
                    Frequencies :: [float(),...],
                    PathLength :: non_neg_integer(),
                    InitTrustees :: trustees(),
                    PromotedTrustees :: trustees(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> [blockchain_ledger_som_v1:tagged_score()].
assign_scores(Path, Frequencies, PathLength, InitTrustees, PromotedTrustees, Ledger) ->
    lists:foldl(fun({Index, Element}, Acc) ->
                        Hotspot = blockchain_poc_path_element_v1:challengee(Element),
                        IsTrusted = lists:member(Hotspot, InitTrustees ++ PromotedTrustees),
                        case {IsTrusted, Index} of
                            {true, 1} ->
                                %% first hotspot in path is trusted
                                %% look at second hotspot's receipt and witnesses
                                %% also check if the first hotspot is a witness of trusted hotspot's transmision
                                acc_scores(2, 1, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {true, PathLength} ->
                                %% last hotspot in path is trusted
                                %% look at second from last hotspot's receipt and witnesses
                                acc_scores(PathLength - 1, PathLength, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {true, I} ->
                                %% trusted hotspot in the middle of path
                                %% look at I+1 and I-1 hotspots' receipt and witnesses
                                Acc1 = acc_scores(I + 1, I, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc),
                                acc_scores(I - 1, I, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc1);
                            {false, 1} ->
                                %% look at second hotspot's receipt and witnesses
                                %% also check if the first hotspot is a witness of trusted hotspot's transmision
                                acc_scores(2, 1, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {false, PathLength} ->
                                %% look at second from last hotspot's receipt and witnesses
                                acc_scores(PathLength - 1, PathLength, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {false, I} ->
                                %% look at I+1 and I-1 hotspots' receipt and witnesses
                                Acc1 = acc_scores(I + 1, I, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc),
                                acc_scores(I - 1, I, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc1);
                            _ ->
                                %% trusted hotspot not in path
                                lager:info("Skipping acc_scores for some reason here: ~p", [{IsTrusted, Index}]),
                                Acc
                        end
                end,
                [], lists:zip(lists:seq(1, PathLength),Path)).


-spec acc_scores(Index :: pos_integer(),
                 TrustedIndex :: pos_integer(),
                 Path :: [blockchain_poc_path_element_v1:poc_path()],
                 Frequencies :: [float(), ...],
                 InitTrustees :: trustees(),
                 PromotedTrustees :: trustees(),
                 Ledger :: blockchain_ledger_v1:ledger(),
                 Acc :: [blockchain_ledger_som_v1:tagged_score()]) -> [blockchain_ledger_som_v1:tagged_score()].
acc_scores(Index, TrustedIndex, Path, Frequencies, InitTrustees, PromotedTrustees, Ledger, Acc) ->
    %% lager:info("frequency ~p Index ~p", [Frequencies, Index]),
    {PreviousElement, ReceiptFreq, WitnessFreq} =
    case Index of
        1 ->
            {undefined, 0.0, hd(Frequencies)};
        _ ->
            {lists:nth(Index - 1, Path), lists:nth(Index - 1, Frequencies), lists:nth(Index, Frequencies)}
    end,
    LookupElement = lists:nth(Index, Path),
    TrustedElement = lists:nth(TrustedIndex, Path),
    CheckHotspot = blockchain_poc_path_element_v1:challengee(LookupElement),
    case lists:member(CheckHotspot, InitTrustees) of
        false ->
            %% the hotspot we are calculating the score for is not trusted initially
            %% Use all known trustees to calculate its score
            Score = calculate_class(InitTrustees ++ PromotedTrustees,
                                    LookupElement,
                                    PreviousElement,
                                    TrustedElement,
                                    CheckHotspot,
                                    ReceiptFreq,
                                    WitnessFreq,
                                    Ledger),
            [{CheckHotspot, Score} | Acc];
        true ->
            %% the hotspot we're looking at is a member of initial trustees, do nothing
            Acc
    end.

-spec calculate_class(Trustees :: trustees(),
                      Element :: blockchain_poc_path_element_v1:poc_element(),
                      PreviousElement :: blockchain_poc_path_element_v1:poc_element(),
                      TrustedElement :: blockchain_poc_path_element_v1:poc_element(),
                      CheckHotspot :: libp2p_crypto:pubkey_bin(),
                      ReceiptFreq :: float(),
                      WitnessFreq :: float(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_ledger_v1:classification().
calculate_class(_Trustees, Element, PreviousElement, _TrustedElement, _CheckHotspot, ReceiptFreq,WitnessFreq, Ledger) ->
    _Receipt = valid_receipt(PreviousElement, Element, ReceiptFreq, Ledger),
    _Witnesses = valid_witnesses(Element, WitnessFreq, Ledger),

    Dst = blockchain_poc_path_element_v1:challengee(Element),

    DataPoints = blockchain_ledger_som_v1:retrieve_datapoints(Ledger, Dst),

    %% lager:info("Dst: ~p, DataPoints: ~p", [?TO_ANIMAL_NAME(Dst), DataPoints]),
    Data = blockchain_ledger_som_v1:calculate_som(Ledger, DataPoints, Dst),

    %% lager:info("~p", [Data]),
    {{T, _Td}, {F, _Fd}, {U, _Ud}} = Data,
    Sum = T+F+U,
    case Sum of
        S when S == 0 ->
            undefined;
        S when S =< ?MAX_WINDOW_SAMPLES ->
            undefined;
        S when S > ?MAX_WINDOW_SAMPLES ->
            Tper = T/S,
            %%_Fper = F/Total,
            %%_Uper = U/Total,
            %lager:info("Hotspot: ~p, Classification Results: ~p", [?TO_ANIMAL_NAME(Dst), {{T, Td}, {F, Fd}, {U, Ud}}]),
            case Tper of
                X when X > 0.65 ->
                    real;
                X when X =< 0.65 andalso X >= 0.5 ->
                    undefined;
                X when X < 0.5 ->
                    fake
            end
    end.

-spec load_promoted_trustees(Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_ledger_som_v1:trustees().
load_promoted_trustees(Ledger) ->
    Windows = blockchain_ledger_som_v1:windows(Ledger),

    lists:foldl(fun({H, Window}, Acc) ->
                        case blockchain_ledger_som_v1:is_promoted(Window) of
                            false -> Acc;
                            true -> [H | Acc]
                        end
                end,
                [], Windows).

-spec valid_receipt(PreviousElement :: undefined | blockchain_poc_path_element_v1:poc_element(),
                    Element :: blockchain_poc_path_element_v1:poc_element(),
                    Channel :: non_neg_integer(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> undefined | blockchain_poc_receipt_v1:poc_receipt().
valid_receipt(undefined, _Element, _Channel, _Ledger) ->
    %% first hop in the path, cannot be validated.
    undefined;
valid_receipt(PreviousElement, Element, Channel, Ledger) ->
    case blockchain_poc_path_element_v1:receipt(Element) of
        undefined ->
            %% nothing to validate
            undefined;
        Receipt ->
            {ok, Source} = blockchain_gateway_cache:get(blockchain_poc_path_element_v1:challengee(PreviousElement), Ledger),
            {ok, Destination} = blockchain_gateway_cache:get(blockchain_poc_path_element_v1:challengee(Element), Ledger),
            SourceLoc = blockchain_ledger_gateway_v2:location(Source),
            DestinationLoc = blockchain_ledger_gateway_v2:location(Destination),
            {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
            {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
            SourceParentIndex = h3:parent(SourceLoc, ParentRes),
            DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
            try h3:grid_distance(SourceParentIndex, DestinationParentIndex) >= ExclusionCells of
                true ->
                    RSSI = blockchain_poc_receipt_v1:signal(Receipt),
                    SNR = blockchain_poc_receipt_v1:snr(Receipt),
                    Freq = blockchain_poc_receipt_v1:frequency(Receipt),
                    MinRcvSig = blockchain_utils:min_rcv_sig(blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc, Freq)),
                    %% Expand of filtered and reason tags for SOM input vector later
                    Filtered = false,
                    Reason = undefined,
                    ok = blockchain_ledger_som_v1:update_datapoints(Source, Destination, RSSI, SNR, MinRcvSig, Filtered, Reason),
                    case RSSI < MinRcvSig of
                        false ->
                            %% RSSI is impossibly high discard this receipt
                            lager:debug("receipt ~p -> ~p rejected at height ~p for RSSI ~p above FSPL ~p with SNR ~p",
                                          [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                           ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                           element(2, blockchain_ledger_v1:current_height(Ledger)),
                                           RSSI, MinRcvSig, SNR]),
                            undefined;
                        true ->
                            case blockchain:config(?data_aggregation_version, Ledger) of
                                {ok, 2} ->
                                    {LowerBound, _} = calculate_rssi_bounds_from_snr(SNR),
                                    case RSSI >= LowerBound of
                                        true ->
                                            case blockchain_poc_receipt_v1:channel(Receipt) == Channel of
                                                true ->
                                                    lager:debug("receipt ok"),
                                                    Receipt;
                                                false ->
                                                    lager:debug("receipt ~p -> ~p rejected at height ~p for channel ~p /= ~p RSSI ~p SNR ~p",
                                                                  [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                                                   ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                   element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                   blockchain_poc_receipt_v1:channel(Receipt), Channel,
                                                                   RSSI, SNR]),
                                                    undefined
                                            end;
                                        false ->
                                            lager:debug("receipt ~p -> ~p rejected at height ~p for RSSI ~p below lower bound ~p with SNR ~p",
                                                          [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                                           ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                           element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                           RSSI, LowerBound, SNR]),
                                            undefined
                                    end;
                                _ ->
                                    %% SNR+Freq+Channels not collected, nothing else we can check
                                    Receipt
                            end
                    end;
                false ->
                    %% too close
                    undefined
            catch
                _:_ ->
                    %% pentagonal distortion
                    undefined
            end
    end.


-spec valid_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
                      Channel :: non_neg_integer(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_poc_witness_v1:poc_witnesses().
valid_witnesses(Element, Channel, Ledger) ->
    {ok, Source} = blockchain_gateway_cache:get(blockchain_poc_path_element_v1:challengee(Element), Ledger),

    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),

    lists:filter(fun(Witness) ->
                         {ok, Destination} = blockchain_gateway_cache:get(blockchain_poc_witness_v1:gateway(Witness), Ledger),
                         SourceLoc = blockchain_ledger_gateway_v2:location(Source),
                         DestinationLoc = blockchain_ledger_gateway_v2:location(Destination),
                         {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
                         {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
                         SourceParentIndex = h3:parent(SourceLoc, ParentRes),
                         DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
                         RSSI = blockchain_poc_witness_v1:signal(Witness),
                         SNR = blockchain_poc_witness_v1:snr(Witness),
                         Freq = blockchain_poc_witness_v1:frequency(Witness),
                         MinRcvSig = blockchain_utils:min_rcv_sig(blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc, Freq)),
                         %% Expand of filtered and reason tags for SOM input vector later
                         Filtered = false,
                         Reason = undefined,
                         ok = blockchain_ledger_som_v1:update_datapoints(Source, Destination, RSSI, SNR, MinRcvSig, Filtered, Reason),
                         try h3:grid_distance(SourceParentIndex, DestinationParentIndex) >= ExclusionCells of
                             true ->
                                 RSSI = blockchain_poc_witness_v1:signal(Witness),
                                 SNR = blockchain_poc_witness_v1:snr(Witness),
                                 Freq = blockchain_poc_witness_v1:frequency(Witness),
                                 MinRcvSig = blockchain_utils:min_rcv_sig(blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc, Freq)),

                                 case RSSI < MinRcvSig of
                                     false ->
                                         %% RSSI is impossibly high discard this witness
                                         lager:debug("witness ~p -> ~p rejected at height ~p for RSSI ~p above FSPL ~p with SNR ~p",
                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                        RSSI, MinRcvSig, SNR]),
                                         false;
                                     true ->
                                         case blockchain:config(?data_aggregation_version, Ledger) of
                                             {ok, 2} ->
                                                 {LowerBound, _} = calculate_rssi_bounds_from_snr(SNR),
                                                 case RSSI >= LowerBound of
                                                     true ->
                                                         case blockchain_poc_witness_v1:channel(Witness) == Channel of
                                                             true ->
                                                                 lager:debug("witness ok"),
                                                                 true;
                                                             false ->
                                                                 lager:debug("witness ~p -> ~p rejected at height ~p for channel ~p /= ~p RSSI ~p SNR ~p",
                                                                               [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                                ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                                element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                                blockchain_poc_witness_v1:channel(Witness), Channel,
                                                                                RSSI, SNR]),
                                                                 false
                                                         end;
                                                     false ->
                                                         lager:debug("witness ~p -> ~p rejected at height ~p for RSSI ~p below lower bound ~p with SNR ~p",
                                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                        RSSI, LowerBound, SNR]),
                                                         false
                                                 end;
                                             _ ->
                                                 %% SNR+Freq+Channels not collected, nothing else we can check
                                                 true
                                         end
                                 end;
                             false ->
                                 %% too close
                                 false
                         catch _:_ ->
                                   %% pentagonal distortion
                                   false
                         end
                 end, Witnesses).

-spec maybe_process_poc_txns(Block :: blockchain_block:block(),
                             BlockHeight :: pos_integer(),
                             Ledger :: blockchain_ledger_v1:ledger(),
                             Evaluations :: evaluations()) -> ok.
maybe_process_poc_txns(Block, BlockHeight, Ledger, Evaluations) ->
    HasPOCTxn = fun(T) -> blockchain_txn:type(T) == blockchain_txn_poc_receipts_v1 end,
    case blockchain_utils:find_txn(Block, HasPOCTxn) of
        [] ->
            ok;
        POCReceipts ->
            %% found poc transactions at this block height
            process_poc_txns(BlockHeight, POCReceipts, Ledger, Evaluations)
    end.

-spec maybe_process_ass_loc_txns(Ledger :: blockchain_ledger_v1:ledger(), Block :: blockchain_block:block()) -> ok.
maybe_process_ass_loc_txns(Ledger, Block) ->
    HasAssLocTxn = fun(T) -> blockchain_txn:type(T) == blockchain_txn_assert_location_v1 end,
    case blockchain_utils:find_txn(Block, HasAssLocTxn) of
        [] ->
            ok;
        AssLocTxns ->
            %% found assert location txns at this block height
            process_ass_loc_txns(Ledger, AssLocTxns)
    end.

calc_txn_channels(Txn, Ledger0) ->
    Chain = blockchain_worker:blockchain(),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger0),
    {ok, Ledger} = blockchain:ledger_at(Height - 1, Chain),
    Challenger = blockchain_txn_poc_receipts_v1:challenger(Txn),
    POCOnionKeyHash = blockchain_txn_poc_receipts_v1:onion_key_hash(Txn),
    %%POCID = blockchain_txn_poc_receipts_v1:poc_id(Txn),

    case blockchain_ledger_v1:find_poc(POCOnionKeyHash, Ledger) of
        {error, _Reason}=Error ->
            %% lager:warning([{poc_id, POCID}],
            %%               "poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p",
            %%               [POCOnionKeyHash, Reason]),
            Error;
        {ok, PoCs} ->
            Secret = blockchain_txn_poc_receipts_v1:secret(Txn),
            case blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret) of
                {error, _} ->
                    {error, poc_not_found};
                {ok, PoC} ->
                    case blockchain_gateway_cache:get(Challenger, Ledger) of
                        {error, _Reason}=Error ->
                            %% lager:warning([{poc_id, POCID}],
                            %%               "poc_receipts error find_gateway_info, challenger: ~p, reason: ~p",
                            %%               [Challenger, Reason]),
                            Error;
                        {ok, GwInfo} ->
                            LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
                            %% lager:info("gw last ~p ~p ~p", [LastChallenge, POCID, GwInfo]),
                            case blockchain:get_block(LastChallenge, Chain) of
                                {error, _Reason}=Error ->
                                    %% lager:warning([{poc_id, POCID}],
                                    %%               "poc_receipts error get_block, last_challenge: ~p, reason: ~p",
                                    %%               [LastChallenge, Reason]),
                                    Error;
                                {ok, Block1} ->
                                    Condition = case blockchain:config(?poc_version, Ledger) of
                                                    {ok, POCVersion} when POCVersion > 1 ->
                                                        fun(T) ->
                                                                blockchain_txn:type(T) == blockchain_txn_poc_request_v1 andalso
                                                                blockchain_txn_poc_request_v1:onion_key_hash(T) == POCOnionKeyHash andalso
                                                                blockchain_txn_poc_request_v1:block_hash(T) == blockchain_ledger_poc_v2:block_hash(PoC)
                                                        end;
                                                    _ ->
                                                        fun(T) ->
                                                                blockchain_txn:type(T) == blockchain_txn_poc_request_v1 andalso
                                                                blockchain_txn_poc_request_v1:onion_key_hash(T) == POCOnionKeyHash
                                                        end
                                                end,
                                    case lists:any(Condition, blockchain_block:transactions(Block1)) of
                                        false ->
                                            {error, onion_key_hash_mismatch};
                                        true ->
                                            %% Note there are 2 block hashes here; one is the block hash encoded into the original
                                            %% PoC request used to establish a lower bound on when that PoC request was made,
                                            %% and one is the block hash at which the PoC was absorbed onto the chain.
                                            %%
                                            %% The first, mediated via a chain var, is mixed with the ECDH derived key for each layer
                                            %% of the onion to ensure that nodes cannot decrypt the onion layer if they are not synced
                                            %% with the chain.
                                            %%
                                            %% The second of these is combined with the PoC secret to produce the combined entropy
                                            %% from both the chain and from the PoC requester.
                                            %%
                                            %% Keeping these distinct and using them for their intended purpose is important.
                                            PoCAbsorbedAtBlockHash  = blockchain_block:hash_block(Block1),
                                            Entropy = <<Secret/binary, PoCAbsorbedAtBlockHash/binary, Challenger/binary>>,
                                            {ok, OldLedger} = blockchain:ledger_at(blockchain_block:height(Block1), Chain),
                                            Vars = vars(OldLedger),
                                            Path = case blockchain:config(?poc_version, OldLedger) of
                                                       {ok, V} when V >= 8 ->
                                                           %% Targeting phase
                                                           %% Find the original target
                                                           {ok, {Target, TargetRandState}} = blockchain_poc_target_v3:target(Challenger, Entropy, OldLedger, Vars),
                                                           %% Path building phase
                                                           Time = blockchain_block:time(Block1),
                                                           RetB = blockchain_poc_path_v4:build(Target, TargetRandState, OldLedger, Time, Vars),
                                                           RetB;

                                                       {ok, V} when V >= 7 ->
                                                           %% If we make it to this point, we are bound to have a target.
                                                           {ok, Target} = blockchain_poc_target_v2:target_v2(Entropy, OldLedger, Vars),
                                                           Time = blockchain_block:time(Block1),
                                                           RetB = blockchain_poc_path_v3:build(Target, OldLedger, Time, Entropy, Vars),
                                                           RetB;

                                                       {ok, V} when V >= 4 ->
                                                           GatewayScoreMap = blockchain_utils:score_gateways(OldLedger),

                                                           Time = blockchain_block:time(Block1),
                                                           {ChallengerGw, _} = maps:get(Challenger, GatewayScoreMap),
                                                           ChallengerLoc = blockchain_ledger_gateway_v2:location(ChallengerGw),
                                                           {ok, OldHeight} = blockchain_ledger_v1:current_height(OldLedger),
                                                           GatewayScores = blockchain_poc_target_v2:filter(GatewayScoreMap, Challenger, ChallengerLoc, OldHeight, Vars),
                                                           %% If we make it to this point, we are bound to have a target.
                                                           {ok, Target} = blockchain_poc_target_v2:target(Entropy, GatewayScores, Vars),

                                                           RetB = case blockchain:config(?poc_typo_fixes, Ledger) of
                                                                      {ok, true} ->
                                                                          blockchain_poc_path_v2:build(Target, GatewayScores, Time, Entropy, Vars);
                                                                      _ ->
                                                                          blockchain_poc_path_v2:build(Target, GatewayScoreMap, Time, Entropy, Vars)
                                                                  end,
                                                           RetB;
                                                       _ ->
                                                           {Target, Gateways} = blockchain_poc_path:target(Entropy, OldLedger, Challenger),
                                                           {ok, P} = blockchain_poc_path:build(Entropy, Target, Gateways, LastChallenge, OldLedger),
                                                           P
                                                   end,
                                            N = erlang:length(Path),
                                            [<<_IV:16/integer-unsigned-little, _/binary>> | LayerData] = blockchain_txn_poc_receipts_v1:create_secret_hash(Entropy, N+1),
                                            {ok, lists:map(fun(Layer) ->
                                                                   <<IntData:16/integer-unsigned-little>> = Layer,
                                                                   lists:nth((IntData rem 8) + 1, [903.9, 904.1, 904.3, 904.5, 904.7, 904.9, 905.1, 905.3])
                                                           end, LayerData)}
                                    end
                            end
                    end
            end
    end.

vars(Ledger) ->
    blockchain_utils:vars_binary_keys_to_atoms(
      maps:from_list(blockchain_ledger_v1:snapshot_vars(Ledger))).

scale_unknown_snr(UnknownSNR) ->
    Diffs = lists:map(fun(K) -> {math:sqrt(math:pow(UnknownSNR - K, 2.0)), K} end, maps:keys(?SNR_CURVE)),
    {ScaleFactor, Key} = hd(lists:sort(Diffs)),
    {Low, High} = maps:get(Key, ?SNR_CURVE),
    {Low + (Low * ScaleFactor), High + (High * ScaleFactor)}.

calculate_rssi_bounds_from_snr(SNR) ->
    %% keef says rounding up hurts the least
    CeilSNR = ceil(SNR),
    case maps:get(CeilSNR, ?SNR_CURVE, undefined) of
        undefined ->
            scale_unknown_snr(CeilSNR);
        V ->
            V
    end.

-spec init_trustees(Evaluations :: #som_evaluations{}) -> trustees().
init_trustees(#som_evaluations{init_trustees=InitTrustees}) ->
    InitTrustees.

-spec promoted_trustees(Evaluations :: #som_evaluations{}) -> trustees().
promoted_trustees(#som_evaluations{promoted_trustees=PromotedTrustees}) ->
    PromotedTrustees.


-spec get_channels(Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts()) -> {ok, [non_neg_integer()]} | {error, any()}.
get_channels(Txn) ->
    Challenger = blockchain_txn_poc_receipts_v1:challenger(Txn),
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    Secret = blockchain_txn_poc_receipts_v1:secret(Txn),
    PathLength = length(Path),
    BlockHash = blockchain_txn_poc_receipts_v1:request_block_hash(Txn),
    case BlockHash of
        <<>> ->
            {error, request_block_hash_not_found};
        undefined ->
            {error, request_block_hash_not_found};
        BH ->
            Entropy1 = <<Secret/binary, BH/binary, Challenger/binary>>,
            [_ | LayerData1] = blockchain_txn_poc_receipts_v1:create_secret_hash(Entropy1, PathLength+1),
            Channels1 = lists:map(fun(Layer) ->
                                          <<IntData:16/integer-unsigned-little>> = Layer,
                                          IntData rem 8
                                  end, LayerData1),
            {ok, Channels1}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

create_export_import_test() ->
    {ok, Som} = som:new(10, 10, 3, false, #{classes => #{<<"1">> => 0.0, <<"0">> => 0.0}, custom_weighting => false}),
    {ok, Export} = som:export_json(Som),
    {ok, Import} = som:from_json(Export).

-endif.
