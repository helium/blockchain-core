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
         acc_scores/5,
         assign_scores/4,
         calculate_class/4,
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
    case do_process_poc_txn(BlockHeight, Txn, Ledger) of
        ok ->
            ok;
        {ok, beacon} ->
            ok;
        {ok, HotspotWindowUpdates} ->
            blockchain_ledger_som_v1:update_windows(Ledger, BlockHeight, POCHash, HotspotWindowUpdates)
    end.

-spec do_process_poc_txn(Height :: pos_integer(),
                         Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                         Ledger :: blockchain_ledger_v1:ledger()) -> ok | {ok, [blockchain_ledger_som_v1:tagged_score()]} | {ok, beacon}.
do_process_poc_txn(Height, Txn, Ledger) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    PathLength = length(Path),
    case PathLength of
        L when L > 1 ->
            case assign_scores(Height, Path, L, Ledger) of
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


-spec assign_scores(Height :: pos_integer(),
                    Path :: blockchain_poc_path_element_v1:poc_path(),
                    PathLength :: non_neg_integer(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> [blockchain_ledger_som_v1:tagged_score()].
assign_scores(Height, Path, PathLength, Ledger) ->
    lists:foldl(fun(Index, Acc) ->
                        acc_scores(Height, Index, Path, Ledger, Acc)
                end,
                [], lists:seq(1, PathLength)).


-spec acc_scores(Height :: pos_integer(),
        Index :: pos_integer(),
                 Path :: [blockchain_poc_path_element_v1:poc_path()],
                 Ledger :: blockchain_ledger_v1:ledger(),
                 Acc :: [blockchain_ledger_som_v1:tagged_score()]) -> [blockchain_ledger_som_v1:tagged_score()].
acc_scores(Height, Index, Path, Ledger, Acc) ->
    %% lager:info("frequency ~p Index ~p", [Frequencies, Index])
    PreviousElement = case Index of
        1 ->
            undefined;
        _ ->
            lists:nth(Index - 1, Path)
    end,
    LookupElement = lists:nth(Index, Path),
    CheckHotspot = blockchain_poc_path_element_v1:challengee(LookupElement),
    Score = calculate_class(Height,
                            LookupElement,
                            PreviousElement,
                            Ledger),
    [{CheckHotspot, Score} | Acc].

-spec calculate_class(Height :: pos_integer(),
                      Element :: blockchain_poc_path_element_v1:poc_element(),
                      PreviousElement :: blockchain_poc_path_element_v1:poc_element(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_ledger_som_v1:classification().
calculate_class(Height, Element, PreviousElement, Ledger) ->
    process_receipt(Height, PreviousElement, Element, Ledger),
    process_witness(Height, Element, Ledger),
    Dst = blockchain_poc_path_element_v1:challengee(Element),
    DataPoints = blockchain_ledger_som_v1:retrieve_datapoints(Dst, Ledger),
    ok = blockchain_ledger_som_v1:update_bmus(Dst, DataPoints, Ledger),
    Data = blockchain_ledger_som_v1:calculate_bmus(Dst, Ledger),
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
                X when X > 0.75 ->
                    {real, Data};
                X when X =< 0.75 andalso X >= 0.5 ->
                    {undefined, Data};
                X when X < 0.5 ->
                    {fake, Data}
            end,
            R
    end,
    Result.

process_receipt(_Height, undefined, _Element, _Ledger) ->
    undefined;
process_receipt(Height, PreviousElement, Element, Ledger) ->
    case blockchain_poc_path_element_v1:receipt(Element) of
        undefined ->
            undefined;
        Receipt ->
			SrcHotspot = blockchain_poc_path_element_v1:challengee(PreviousElement),
            {ok, Source} = blockchain_gateway_cache:get(SrcHotspot, Ledger),
            DstHotspot = blockchain_poc_path_element_v1:challengee(Element),
            {ok, Destination} = blockchain_gateway_cache:get(DstHotspot, Ledger),
            SourceLoc = blockchain_ledger_gateway_v3:location(Source),
            DestinationLoc = blockchain_ledger_gateway_v3:location(Destination),
            RSSI = blockchain_poc_receipt_v1:signal(Receipt),
            SNR = blockchain_poc_receipt_v1:snr(Receipt),
            Freq = blockchain_poc_receipt_v1:frequency(Receipt),
            MinRcvSig = blockchain_utils:min_rcv_sig(blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc, Freq)),
            Filtered = false,
            Reason = undefined,
            update_trust_scores(Height, SrcHotspot, Source, DstHotspot, Destination, RSSI, SNR, MinRcvSig, Filtered, Reason, Ledger),
            ok = blockchain_ledger_som_v1:update_datapoints(SrcHotspot, DstHotspot, RSSI, SNR, MinRcvSig, Filtered, Reason, Ledger)
    end.

process_witness(Height, Element, Ledger) ->
    SrcHotspot = blockchain_poc_path_element_v1:challengee(Element),
    {ok, Source} = blockchain_gateway_cache:get(SrcHotspot, Ledger),
    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
    lists:foreach(fun(Witness) ->
                         DstHotspot = blockchain_poc_witness_v1:gateway(Witness),
                         {ok, Destination} = blockchain_gateway_cache:get(DstHotspot, Ledger),
                         SourceLoc = blockchain_ledger_gateway_v3:location(Source),
                         DestinationLoc = blockchain_ledger_gateway_v3:location(Destination),
                         MinRcvSig = blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc),
                         RSSI = blockchain_poc_witness_v1:signal(Witness),
                         SNR = blockchain_poc_witness_v1:snr(Witness),
                         Filtered = false,
                         Reason = undefined,
                         update_trust_scores(Height, SrcHotspot, Source, DstHotspot, Destination, RSSI, SNR, MinRcvSig, Filtered, Reason, Ledger),
                         ok = blockchain_ledger_som_v1:update_datapoints(SrcHotspot, DstHotspot, RSSI, SNR, MinRcvSig, Filtered, Reason, Ledger)
                 end, Witnesses).

update_trust_scores(Height, SrcHotspot, Source, DstHotspot, Destination, RSSI, SNR, MinRcvSig, _Filtered, _Reason, Ledger) ->
    Value = case blockchain_ledger_som_v1:classify_sample(RSSI, SNR, MinRcvSig, Ledger) of
                {{_, _Dist}, <<"0">>} -> -1;
                {{_, _Dist}, <<"1">>} -> 1;
                {{_, _Dist}, <<"undefined">>} -> 0
            end,
    case {lists:member(libp2p_crypto:bin_to_b58(SrcHotspot), application:get_env(miner_pro, init_trustees, [])) orelse blockchain_ledger_gateway_v3:is_trusted(Source),
         lists:member(libp2p_crypto:bin_to_b58(DstHotspot), application:get_env(miner_pro, init_trustees, [])) orelse blockchain_ledger_gateway_v3:is_trusted(Destination)} of
        {false, false} ->
            %% neither side is trusted, do nothing
            ok;
        {true, false} ->
            %% destination was not trusted, update it
            blockchain_ledger_gateway_v3:add_trusted_poc_result(Height, Value, Destination);
        {false, true} ->
            %% source was not trusted, update it
            blockchain_ledger_gateway_v3:add_trusted_poc_result(Height, Value, Source);
        {true, true} when Value == 1 ->
            %% both sides were trusted and they did the thing, good job
            blockchain_ledger_gateway_v3:add_trusted_poc_result(Height, Value, Source),
            blockchain_ledger_gateway_v3:add_trusted_poc_result(Height, Value, Destination);
        {true, true} ->
            lager:warning("Trust propogation from ~p to ~p failed", [libp2p_crypto:bin_to_b58(SrcHotspot), libp2p_crypto:bin_to_b58(DstHotspot), RSSI, SNR, MinRcvSig])
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

create_export_import_test() ->
    {ok, Som} = som:new(10, 10, 3, false, #{classes => #{<<"1">> => 0.0, <<"0">> => 0.0}, custom_weighting => false}),
    {ok, Export} = som:export_json(Som),
    {ok, Import} = som:from_json(Export).

-endif.
