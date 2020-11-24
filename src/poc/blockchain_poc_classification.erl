%%%-------------------------------------------------------------------
%%% == Blockchain Proof of Coverage Classification ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_classification).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-define(MAX_WINDOW_SAMPLES, 19).

-export([process_poc_txn/4,
         process_assert_loc_txn/2,
         acc_scores/4,
         assign_scores/3,
         calculate_class/3,
         load_promoted_trustees/1]).

-spec process_assert_loc_txn(Txn :: blockchain_txn_assert_location_v1:txn_assert_location(),
                           Ledger :: blockchain_ledger_v1:ledger()) -> ok.
process_assert_loc_txn(Txn, Ledger) ->
    Hotspot = blockchain_txn_assert_location_v1:gateway(Txn),
    ok = blockchain_ledger_som_v1:reset_window(Ledger, Hotspot).

-spec process_poc_txn(BlockHeight :: pos_integer(),
                      Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                      Ledger :: blockchain_ledger_v1:ledger(),
                      POCHash :: binary()) -> ok.
process_poc_txn(BlockHeight, Txn, Ledger, POCHash) ->
    case do_process_poc_txn(Txn, Ledger) of
        ok ->
            ok;
        {ok, beacon} ->
            ok;
        {ok, HotspotWindowUpdates} ->
            blockchain_ledger_som_v1:update_windows(Ledger, BlockHeight, POCHash, HotspotWindowUpdates)
    end.

-spec do_process_poc_txn(Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                         Ledger :: blockchain_ledger_v1:ledger()) -> ok | {ok, [blockchain_ledger_som_v1:tagged_score()]} | {ok, beacon}.
do_process_poc_txn(Txn, Ledger) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    PathLength = length(Path),
    case PathLength of
        L when L > 1 ->
            case assign_scores(Path, L, Ledger) of
                [_ | _]=TaggedScores ->
                    %% lager:info("New scores: ~p", [TaggedScores]),
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
                    PathLength :: non_neg_integer(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> [blockchain_ledger_som_v1:tagged_score()].
assign_scores(Path, PathLength, Ledger) ->
    lists:foldl(fun(Index, Acc) ->
                        acc_scores(Index, Path, Ledger, Acc)
                end,
                [], lists:seq(1, PathLength)).


-spec acc_scores(Index :: pos_integer(),
                 Path :: [blockchain_poc_path_element_v1:poc_path()],
                 Ledger :: blockchain_ledger_v1:ledger(),
                 Acc :: [blockchain_ledger_som_v1:tagged_score()]) -> [blockchain_ledger_som_v1:tagged_score()].
acc_scores(Index, Path, Ledger, Acc) ->
    %% lager:info("frequency ~p Index ~p", [Frequencies, Index])
    PreviousElement = case Index of
        1 ->
            undefined;
        _ ->
            lists:nth(Index - 1, Path)
    end,
    LookupElement = lists:nth(Index, Path),
    CheckHotspot = blockchain_poc_path_element_v1:challengee(LookupElement),
    Score = calculate_class(LookupElement,
                            PreviousElement,
                            Ledger),
    [{CheckHotspot, Score} | Acc].

-spec calculate_class(Element :: blockchain_poc_path_element_v1:poc_element(),
                      PreviousElement :: blockchain_poc_path_element_v1:poc_element(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_ledger_som_v1:classification().
calculate_class(Element, PreviousElement, Ledger) ->
    process_receipt(PreviousElement, Element, Ledger),
    process_witness(Element, Ledger),
    Dst = blockchain_poc_path_element_v1:challengee(Element),
    DataPoints = blockchain_ledger_som_v1:retrieve_datapoints(Dst, Ledger),
    ok = blockchain_ledger_som_v1:update_bmus(Dst, DataPoints, Ledger),
    Data = blockchain_ledger_som_v1:calculate_bmus(Dst, Ledger),
    %% lager:info("Dst: ~p, DataPoints: ~p", [?TO_ANIMAL_NAME(Dst), DataPoints]),
    %%lager:info("~p Datapoints: ~p", [Dst, Data])
    {{T, _Td}, {F, _Fd}, {U, _Ud}} = Data,
    Sum = T+F+U,
    Result = case Sum of
        S when S == 0 ->
            {undefined, Data};
        S when S =< ?MAX_WINDOW_SAMPLES ->
            {undefined, Data};
        S when S > ?MAX_WINDOW_SAMPLES ->
            Tper = T/S,
            %%_Fper = F/Total,
            %%_Uper = U/Total,
            R = case Tper of
                X when X > 0.65 ->
                    {real, Data};
                X when X =< 0.65 andalso X >= 0.5 ->
                    {undefined, Data};
                X when X < 0.5 ->
                    {fake, Data}
            end,
            lager:info("~p | classification results: ~p", [?TO_ANIMAL_NAME(Dst), R]),
            R
    end,
    Result.

process_receipt(undefined, _Element, _Ledger) ->
    undefined;
process_receipt(PreviousElement, Element, Ledger) ->
    case blockchain_poc_path_element_v1:receipt(Element) of
        undefined ->
            undefined;
        Receipt ->
			SrcHotspot = blockchain_poc_path_element_v1:challengee(PreviousElement),
            {ok, Source} = blockchain_gateway_cache:get(SrcHotspot, Ledger),
            DstHotspot = blockchain_poc_path_element_v1:challengee(Element),
            {ok, Destination} = blockchain_gateway_cache:get(DstHotspot, Ledger),
            SourceLoc = blockchain_ledger_gateway_v2:location(Source),
            DestinationLoc = blockchain_ledger_gateway_v2:location(Destination),
            RSSI = blockchain_poc_receipt_v1:signal(Receipt),
            SNR = blockchain_poc_receipt_v1:snr(Receipt),
            Freq = blockchain_poc_receipt_v1:frequency(Receipt),
            MinRcvSig = blockchain_utils:min_rcv_sig(blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc, Freq)),
            Filtered = false,
            Reason = undefined,
            ok = blockchain_ledger_som_v1:update_datapoints(SrcHotspot, DstHotspot, RSSI, SNR, MinRcvSig, Filtered, Reason, Ledger)
    end.

process_witness(Element, Ledger) ->
    SrcHotspot = blockchain_poc_path_element_v1:challengee(Element),
    {ok, Source} = blockchain_gateway_cache:get(SrcHotspot, Ledger),
    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
    lists:foreach(fun(Witness) ->
                         DstHotspot = blockchain_poc_witness_v1:gateway(Witness),
                         {ok, Destination} = blockchain_gateway_cache:get(DstHotspot, Ledger),
                         SourceLoc = blockchain_ledger_gateway_v2:location(Source),
                         DestinationLoc = blockchain_ledger_gateway_v2:location(Destination),
                         MinRcvSig = blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc),
                         RSSI = blockchain_poc_witness_v1:signal(Witness),
                         SNR = blockchain_poc_witness_v1:snr(Witness),
                         Filtered = false,
                         Reason = undefined,
                         ok = blockchain_ledger_som_v1:update_datapoints(SrcHotspot, DstHotspot, RSSI, SNR, MinRcvSig, Filtered, Reason, Ledger)
                 end, Witnesses).


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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

create_export_import_test() ->
    {ok, Som} = som:new(10, 10, 3, false, #{classes => #{<<"1">> => 0.0, <<"0">> => 0.0}, custom_weighting => false}),
    {ok, Export} = som:export_json(Som),
    {ok, Import} = som:from_json(Export).

-endif.
