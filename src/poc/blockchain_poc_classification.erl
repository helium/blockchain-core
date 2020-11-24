%%%-------------------------------------------------------------------
%%% == Blockchain Proof of Coverage Classification ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_classification).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-define(MAX_WINDOW_SAMPLES, 11).

-export([process_poc_txn/4,
         process_assert_loc_txn/2,
         acc_scores/4,
         assign_scores/3,
         calculate_class/2,
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
            %POCHash = blockchain_txn_poc_receipts_v1:hash(POC),
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
    LookupElement = lists:nth(Index, Path),
    CheckHotspot = blockchain_poc_path_element_v1:challengee(LookupElement),
    Score = calculate_class(LookupElement,
                            Ledger),
    [{CheckHotspot, Score} | Acc].

-spec calculate_class(Element :: blockchain_poc_path_element_v1:poc_element(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_ledger_som_v1:classification().
calculate_class(Element, Ledger) ->
    Dst = blockchain_poc_path_element_v1:challengee(Element),

    DataPoints = blockchain_ledger_som_v1:retrieve_datapoints(Dst, Ledger),

    %% lager:info("Dst: ~p, DataPoints: ~p", [?TO_ANIMAL_NAME(Dst), DataPoints]),
    Data = blockchain_ledger_som_v1:calculate_som(DataPoints, Dst, Ledger),

    %% lager:info("~p", [Data]),
    {{T, _Td}, {F, _Fd}, {U, _Ud}} = Data,
    Sum = T+F+U,
    case Sum of
        S when S == 0 ->
            {undefined, Data};
        S when S =< ?MAX_WINDOW_SAMPLES ->
            {undefined, Data};
        S when S > ?MAX_WINDOW_SAMPLES ->
            Tper = T/S,
            %%_Fper = F/Total,
            %%_Uper = U/Total,
            Result = case Tper of
                X when X > 0.65 ->
                    {real, Data};
                X when X =< 0.65 andalso X >= 0.5 ->
                    {undefined, Data};
                X when X < 0.5 ->
                    {fake, Data}
            end,
            lager:info("~p | classification results: ~p", [?TO_ANIMAL_NAME(Dst), Result]),
            Result
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
