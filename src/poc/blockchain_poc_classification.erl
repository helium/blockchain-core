%%%-------------------------------------------------------------------
%%% == Blockchain Proof of Coverage Classification ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_classification).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-define(MAX_WINDOW_SAMPLES, 11).

%% list of trustees
-type trustees() :: [libp2p_crypto:pubkey_bin()].

-export([process_poc_txn/5,
         process_assert_loc_txn/2,
         acc_scores/6,
         assign_scores/5,
         calculate_class/3,
         load_promoted_trustees/1]).

-spec process_assert_loc_txn(Txn :: blockchain_txn_assert_location_v1:txn_assert_location(),
                           Ledger :: blockchain_ledger_v1:ledger()) -> ok.
process_assert_loc_txn(Txn, Ledger) ->
    Hotspot = blockchain_txn_assert_location_v1:gateway(Txn),
    ok = blockchain_ledger_som_v1:reset_window(Ledger, Hotspot).

-spec process_poc_txn(BlockHeight :: pos_integer(),
                      Evaluations :: blockchain_ledger_som_v1:evaluations(),
                      Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                      Ledger :: blockchain_ledger_v1:ledger(),
                      POCHash :: list()) ->
    ok | {ok, [blockchain_ledger_som_v1:tagged_score()]} | {ok, beacon}.
process_poc_txn(BlockHeight, Evaluations, Txn, Ledger, POCHash) ->
    InitTrustees = blockchain_ledger_som_v1:init_trustees(Evaluations),
    PromotedTrustees = blockchain_ledger_som_v1:promoted_trustees(Evaluations),
    case do_process_poc_txn(Txn, InitTrustees, PromotedTrustees, Ledger) of
        ok ->
            ok;
        {ok, beacon} ->
            ok;
        {ok, HotspotWindowUpdates} ->
            %POCHash = blockchain_txn_poc_receipts_v1:hash(POC),
            blockchain_ledger_som_v1:update_windows(Ledger, BlockHeight, POCHash, HotspotWindowUpdates)
    end.

-spec do_process_poc_txn(Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                         InitTrustees :: trustees(),
                         PromotedTrustees :: trustees(),
                         Ledger :: blockchain_ledger_v1:ledger()) -> ok | {ok, [blockchain_ledger_som_v1:tagged_score()]} | {ok, beacon}.
do_process_poc_txn(Txn, InitTrustees, PromotedTrustees, Ledger) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    PathLength = length(Path),
    case PathLength of
        L when L > 1 ->
            case assign_scores(Path, L, InitTrustees, PromotedTrustees, Ledger) of
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
                    InitTrustees :: trustees(),
                    PromotedTrustees :: trustees(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> [blockchain_ledger_som_v1:tagged_score()].
assign_scores(Path, PathLength, InitTrustees, PromotedTrustees, Ledger) ->
    lists:foldl(fun({Index, Element}, Acc) ->
                        Hotspot = blockchain_poc_path_element_v1:challengee(Element),
                        IsTrusted = lists:member(Hotspot, InitTrustees ++ PromotedTrustees),
                        case {IsTrusted, Index} of
                            {true, 1} ->
                                %% first hotspot in path is trusted
                                %% look at second hotspot's receipt and witnesses
                                %% also check if the first hotspot is a witness of trusted hotspot's transmision
                                acc_scores(2, Path, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {true, PathLength} ->
                                %% last hotspot in path is trusted
                                %% look at second from last hotspot's receipt and witnesses
                                acc_scores(PathLength - 1, Path, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {true, I} ->
                                %% trusted hotspot in the middle of path
                                %% look at I+1 and I-1 hotspots' receipt and witnesses
                                Acc1 = acc_scores(I + 1, Path, InitTrustees, PromotedTrustees, Ledger, Acc),
                                acc_scores(I - 1, Path, InitTrustees, PromotedTrustees, Ledger, Acc1);
                            {false, 1} ->
                                %% look at second hotspot's receipt and witnesses
                                %% also check if the first hotspot is a witness of trusted hotspot's transmision
                                acc_scores(2, Path, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {false, PathLength} ->
                                %% look at second from last hotspot's receipt and witnesses
                                acc_scores(PathLength - 1, Path, InitTrustees, PromotedTrustees, Ledger, Acc);
                            {false, I} ->
                                %% look at I+1 and I-1 hotspots' receipt and witnesses
                                Acc1 = acc_scores(I + 1, Path, InitTrustees, PromotedTrustees, Ledger, Acc),
                                acc_scores(I - 1, Path, InitTrustees, PromotedTrustees, Ledger, Acc1);
                            _ ->
                                %% trusted hotspot not in path
                                lager:info("Skipping acc_scores for some reason here: ~p", [{IsTrusted, Index}]),
                                Acc
                        end
                end,
                [], lists:zip(lists:seq(1, PathLength),Path)).


-spec acc_scores(Index :: pos_integer(),
                 Path :: [blockchain_poc_path_element_v1:poc_path()],
                 InitTrustees :: trustees(),
                 PromotedTrustees :: trustees(),
                 Ledger :: blockchain_ledger_v1:ledger(),
                 Acc :: [blockchain_ledger_som_v1:tagged_score()]) -> [blockchain_ledger_som_v1:tagged_score()].
acc_scores(Index, Path, InitTrustees, PromotedTrustees, Ledger, Acc) ->
    %% lager:info("frequency ~p Index ~p", [Frequencies, Index])
    LookupElement = lists:nth(Index, Path),
    CheckHotspot = blockchain_poc_path_element_v1:challengee(LookupElement),
    case lists:member(CheckHotspot, InitTrustees) of
        false ->
            %% the hotspot we are calculating the score for is not trusted initially
            %% Use all known trustees to calculate its score
            Score = calculate_class(InitTrustees ++ PromotedTrustees,
                                    LookupElement,
                                    Ledger),
            [{CheckHotspot, Score} | Acc];
        true ->
            %% the hotspot we're looking at is a member of initial trustees, do nothing
            Acc
    end.

-spec calculate_class(Trustees :: trustees(),
                      Element :: blockchain_poc_path_element_v1:poc_element(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_ledger_v1:classification().
calculate_class(_Trustees, Element, Ledger) ->
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

create_export_import_test() ->
    {ok, Som} = som:new(10, 10, 3, false, #{classes => #{<<"1">> => 0.0, <<"0">> => 0.0}, custom_weighting => false}),
    {ok, Export} = som:export_json(Som),
    {ok, Import} = som:from_json(Export).

-endif.
