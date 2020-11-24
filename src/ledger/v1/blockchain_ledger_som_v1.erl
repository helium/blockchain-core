%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger SOM ==
%% @end
%%%-------------------------------------------------------------------

-module(blockchain_ledger_som_v1).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(WINDOW_SIZE, 25).
-define(WINDOW_CAP, 500).
-define(SCORE_THRESHOLD, real).
-define(STALE_THRESHOLD, 60).
-define(MAX_NUM, 115792089237316195423570985008687907853269984665640564039457584007913129639935).

%% {hotspot, score_update}
-type tagged_score() :: {libp2p_crypto:pubkey_bin(), classification()}.
%% BMU calculation return type
-type bmu_results() :: {{non_neg_integer(), float()}, {non_neg_integer(), float()}, {non_neg_integer(), float()}}.
%% A type holding BMU data from one sample
-type bmu_data() :: {{{integer(), integer()}, float()}, atom()}.
%% List of BMU data
-type bmu_list() :: [bmu_data()].
%% list of trustees
-type trustees() :: [libp2p_crypto:pubkey_bin()].
%% Each window element is a block_height, poc_hash, score_update
-type window_element() :: {pos_integer(), blockchain_txn:hash(), classification()}.
%% List of window_elements
-type window() :: [window_element()].
%% List of windows, tagged via hotspot pubkey_bin
-type windows() :: [{libp2p_crypto:pubkey_bin(), window()}].
%% A class associated with hotspot trust
-type classification() :: {atom(), bmu_results()}.
-type evaluations() :: {trustees(), trustees()}.

-export([update_datapoints/8,
         update_bmus/3,
         update_windows/4,
         reset_window/2,
         windows/1,
         update_trustees/2,
         calculate_bmus/2,
         clear_som/1,
         clear_bmus/2,
         retrieve_som/1,
         retrieve_bmus/2,
         retrieve_datapoints/2,
         retrieve_trustees/1,
         init_trustees/1,
         promoted_trustees/1,
         is_promoted/1,
         maybe_phase_out/2,
         scores/1,
         hotspot_window/2]).
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------

%%%-------------------------------------------------------------------
%%% SOM Database functions
%%%-------------------------------------------------------------------

-spec update_datapoints(binary(), binary(), any(), any(), any(), atom(), atom(), Ledger :: blockchain_ledger_v1:ledger()) -> ok.
update_datapoints(Src, Dst, Rssi, Snr, Fspl, Filtered, Reason, Ledger) ->
    DatapointsCF = blockchain_ledger_v1:datapoints_cf(Ledger),
    BacklinksCF = blockchain_ledger_v1:backlinks_cf(Ledger),
    Key1 = <<Src/binary, Dst/binary>>,
    Key2 = <<Dst/binary, Src/binary>>,
    case blockchain_ledger_v1:cache_get(Ledger, DatapointsCF, Key1, []) of
        {ok, Bin} ->
            lager:info("Datapoints update: ~p", [Key1]),
            N = binary_to_term(Bin),
            Sample = {{Rssi, Snr, Fspl}, {Filtered, Reason}},
            ToInsert = term_to_binary([Sample | N]),
            ok = blockchain_ledger_v1:cache_put(Ledger, DatapointsCF, Key1, ToInsert),
            ok = blockchain_ledger_v1:cache_put(Ledger, BacklinksCF, Key2, ToInsert);
        not_found ->
            lager:info("Datapoints not found: ~p", [Key1]),
            Sample = {{Rssi, Snr, Fspl}, {Filtered, Reason}},
            ToInsert = term_to_binary([Sample]),
            ok = blockchain_ledger_v1:cache_put(Ledger, DatapointsCF, Key1, ToInsert),
            ok = blockchain_ledger_v1:cache_put(Ledger, BacklinksCF, Key2, ToInsert)
    end.

-spec retrieve_datapoints(binary(), Ledger :: blockchain_ledger_v1:ledger()) -> list().
retrieve_datapoints(Hotspot, Ledger) ->
    lists:flatten([blockchain_ledger_v1:witness_of(Hotspot, Ledger), blockchain_ledger_v1:witness_for(Hotspot, Ledger)]).

to_num(String) ->
    try list_to_float(String) of
        Res -> Res
    catch _:_ ->
              list_to_integer(String) * 1.0
    end.

shuffle(List) ->
    [X || {_,X} <- lists:sort([{rand:uniform(), N} || N <- List])].

init_som(Ledger) ->
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
        {ok, Bin} ->
            Serialized = binary_to_list(Bin),
            {ok, Som} = som:from_json(Serialized),
            Som;
        not_found ->
            PrivDir = code:priv_dir(blockchain_core),
            File = application:get_env(blockchain_core, aggregate_samples_file, "aggregate_samples.csv"),
            TrainingSetFile = PrivDir ++ "/" ++ File,
            {ok, IoDevice} = file:open(TrainingSetFile, [read]),
            Processor = fun({newline, ["pos"|_]}, Acc) ->
                                 %% ignore header
                                 Acc;
                           ({newline, [_Pos, Signal, SNR, FSPL, Class]}, Acc) ->
                                 [{[((to_num(Signal) - (-134))/(134)), ((to_num(SNR) - (-19))/(17 - (-19))), ((to_num(FSPL) - (-164))/(164))], list_to_binary(Class)} | Acc];
                            (_, Acc) ->
                                 Acc
                         end,
            {ok, ProcessedRows} = ecsv:process_csv_file_with(IoDevice, Processor, []),

            Samples = shuffle(ProcessedRows),

            {ok, SOM} = som:new(10, 10, 3, false, #{classes => #{<<"1">> => 0.0, <<"0">> => 0.0}, custom_weighting => false}),

            %% divide training and testing data
            {Supervised, Unsupervised} = lists:partition(fun(_) -> rand:uniform(100) < 90 end, shuffle(Samples)),
            {SupervisedSamples, SupervisedClasses} = lists:unzip(Supervised),

            %% Train the network through supervised learning
            som:train_random_supervised(SOM, SupervisedSamples, SupervisedClasses, 2000),

            %% Estimate trained map accuracy
            Matched = lists:foldl(fun({Sample, Class}, Acc) ->
                                          case som:winner_vals(SOM, Sample) of
                                              {_, Class} ->
                                                  Acc + 1;
                                              {_, <<"0">>} ->
                                                  %% false negative, leave this uncommented to make that not count as a fail
                                                  Acc + 1;
                                              _ ->
                                                  lager:info("mismatch ~p ~p ~p", [Sample, Class, som:winner_vals(SOM, Sample)]),
                                                  Acc
                                          end
                                  end, 0, Unsupervised),
            lager:info("matched ~p/~p => ~p%", [Matched, length(Unsupervised), Matched / length(Unsupervised) * 100]),

            {ok, Serialized} = som:export_json(SOM),
            blockchain_ledger_v1:cache_put(Ledger, SomCF, term_to_binary(global), Serialized),
            SOM
    end.

-spec calculate_bmus(binary(), Ledger :: blockchain_ledger_v1:ledger()) -> bmu_results().
calculate_bmus(Key, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, Key, []) of
        {ok, Bin} ->
            Bmus = binary_to_term(Bin),
            lager:info("Calculate BMUs for: ~p", [Key]),
            {{Reals, RDist}, {Fakes, FDist}, {Undefs, UDist}} = lists:foldl(fun({{_, Dist}, Class}, {{Rsum, RDsum}, {Fsum, FDsum}, {Usum, UDsum}}) -> case Class of
                                                                             <<"1">> -> {{Rsum + 1, RDsum + Dist}, {Fsum, FDsum}, {Usum, UDsum}};
                                                                             <<"0">> -> {{Rsum, RDsum}, {Fsum + 1, FDsum + Dist}, {Usum, UDsum}};
                                                                             <<"undefined">> -> {{Rsum, RDsum}, {Fsum, FDsum}, {Usum + 1, UDsum + Dist}}
                                                                         end
                               end, {{0,0},{0,0},{0,0}}, Bmus),
            case {Reals, Fakes, Undefs} of
                {R, F, U} when R == 0, F == 0, U == 0 ->
                    {{Reals, 0}, {Fakes, 0}, {0, 0}};
                {R, F, 0} when R > 0, F == 0 ->
                    {{R, RDist / R}, {0, 0}, {0, 0}};
                {R, F, 0} when F > 0, R == 0 ->
                    {{0, 0}, {F, FDist / F}, {0, 0}};
                {R, F, 0} when R > 0, F > 0 ->
                    {{Reals, RDist / Reals}, {Fakes, FDist / Fakes}, {0, 0}};
                {R, F, U} when R > 0, F > 0, U > 0 ->
                    {{R, RDist / R}, {F, FDist / F}, {U, UDist / U}}
            end;
        not_found ->
            lager:info("No BMUs to calculate for: ~p", [Key]),
            {{0,0.0},{0,0.0},{0,0.0}}
    end.

-spec update_bmus(binary(), [term()], Ledger :: blockchain_ledger_v1:ledger()) -> ok.
update_bmus(Key, Values, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, Key, []) of
        {ok, BmusBin} ->
            Bmus = binary_to_term(BmusBin),
            case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
                {ok, SomBin} ->
                    {ok, Som} = som:from_json(SomBin),
                    %% Calculate new BMUs with stored SOM
                    NewBmus = lists:map(fun({{Signal, Snr, Fspl}, _}) ->
                                     som:winner_vals(Som,
                                                     [float(((Signal - (-134))/(134))), float(((Snr - (-19))/(17 - (-19)))), float(((Fspl - (-164))/(164)))]) end,
                                        Values),
                    %% Append BMUs list
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary(lists:sublist(NewBmus ++ Bmus, ?WINDOW_CAP)));
                not_found ->
                    Som = init_som(Ledger),
                    NewBmus = lists:map(fun({{Signal, Snr, Fspl}, _}) ->
                                             som:winner_vals(Som,
                                                             [float(((Signal - (-134))/(134))), float(((Snr - (-19))/(17 - (-19)))), float(((Fspl - (-164))/(164)))]) end,
                                        Values),
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary(lists:sublist(NewBmus ++ Bmus, ?WINDOW_CAP)))
            end;
        not_found ->
            case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
                {ok, SomBin} ->
                    {ok, Som} = som:from_json(SomBin),
                    %% Calculate new BMUs with stored SOM
                    NewBmus = lists:map(fun({{Signal, Snr, Fspl}, _}) ->
                                     som:winner_vals(Som,
                                                     [float(((Signal - (-134))/(134))), float(((Snr - (-19))/(17 - (-19)))), float(((Fspl - (-164))/(164)))]) end,
                             Values),
                    %% Append BMUs list
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary(NewBmus));
                not_found ->
                    Som = init_som(Ledger),
                    NewBmus = lists:map(fun({{Signal, Snr, Fspl}, _}) ->
                                             som:winner_vals(Som,
                                                             [float(((Signal - (-134))/(134))), float(((Snr - (-19))/(17 - (-19)))), float(((Fspl - (-164))/(164)))]) end,
                                     Values),
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary(NewBmus))
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SOM DB API (internal/console)
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec retrieve_bmus(binary(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, bmu_list()} | {error, not_found}.
retrieve_bmus(A, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, <<A/binary>>, []) of
        {ok, Bin} ->
            %%lager:info("BMUs RETRIEVED FOR: ~p", [?TO_ANIMAL_NAME(A)]),
            N = binary_to_term(Bin),
            {ok, N};
        not_found ->
            lager:debug("Retrieve FAIL"),
            {error, not_found}
    end.

-spec clear_bmus(binary(), Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, not_found}.
clear_bmus(A, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, <<A/binary>>, []) of
        {ok, _Bin} ->
            %%lager:info("Clear BMUs for: ~p", [?TO_ANIMAL_NAME(A)]),
            blockchain_ledger_v1:cache_delete(Ledger, BmuCF, <<A/binary>>),
            ok;
        not_found ->
            lager:debug("Clear BMUs FAIL"),
            {error, not_found}
    end.

-spec retrieve_som(Ledger :: blockchain_ledger_v1:ledger()) -> {ok, term()} | {error, not_found}.
retrieve_som(Ledger) ->
     SomCF = blockchain_ledger_v1:som_cf(Ledger),
     case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
         {ok, Bin} ->
             lager:debug("SOM RETRIEVED"),
             N = binary_to_term(Bin),
             {ok, N};
         not_found ->
             lager:debug("Retrieve FAIL"),
             {error, not_found}
     end.

-spec clear_som(Ledger :: blockchain_ledger_v1:ledger()) -> {ok, list()} | {error, not_found}.
clear_som(Ledger) ->
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
        {ok, Bin} ->
            lager:info("Clear SOM"),
            blockchain_ledger_v1:cache_delete(Ledger, SomCF, term_to_binary(global)),
            {ok, Bin};
        not_found ->
            lager:debug("Clear SOM FAIL"),
            {error, not_found}
    end.

-spec init_trustees(Evaluations :: evaluations()) -> trustees().
init_trustees({InitTrustees, _}) ->
    InitTrustees.

-spec promoted_trustees(Evaluations :: evaluations()) -> trustees().
promoted_trustees({_, PromotedTrustees}) ->
    PromotedTrustees.


-spec update_trustees(Trustees :: trustees(), Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, not_found}.
update_trustees(Trustees, Ledger) ->
    TrusteesCF = blockchain_ledger_v1:trustees_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, TrusteesCF, term_to_binary(global), []) of
        {ok, Bin} ->
            N = binary_to_term(Bin),
            ToInsert = [Trustees | N],
            ok = blockchain_ledger_v1:cache_put(Ledger, TrusteesCF, term_to_binary(global), term_to_binary([ToInsert | N])),
            ok;
        not_found ->
            ok = blockchain_ledger_v1:cache_put(Ledger, TrusteesCF, term_to_binary(global), term_to_binary(Trustees)),
            {ok, new_list}
    end.

-spec retrieve_trustees(Ledger :: blockchain_ledger_v1:ledger()) -> {ok, [evaluations()]} | {error, not_found}.
retrieve_trustees(Ledger) ->
    TrusteesCF = blockchain_ledger_v1:trustees_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, TrusteesCF, term_to_binary(global), []) of
        {ok, Bin} ->
            N = binary_to_term(Bin),
            {ok, N};
        not_found ->
            {error, not_found}
    end.


%%%-------------------------------------------------------------------
%%% Window manipulation
%%%-------------------------------------------------------------------
-spec windows(Ledger :: blockchain_ledger_v1:ledger()) -> windows().
windows(Ledger) ->
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    blockchain_ledger_v1:cache_fold(Ledger, WindowsCF, fun({Hotspot, Res}, Acc) ->
                                          [{Hotspot, binary_to_term(Res)} | Acc] end,
               []).

-spec reset_window(Ledger :: blockchain_ledger_v1:ledger(),
                   Hotspot :: libp2p_crypto:pubkey_bin()) -> ok.
reset_window(Ledger, Hotspot) ->
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Hotspot, term_to_binary([])).

-spec slide_window(Hotspot :: libp2p_crypto:pubkey_bin(),
                   Window :: window(),
                   BlockHeight :: pos_integer(),
                   POCHash :: blockchain_txn:hash(),
                   ScoreUpdate :: tagged_score(),
                   Ledger :: blockchain_ledger_v1:ledger()) -> ok.
slide_window(Hotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger) ->
    %% slide window
    [_Head | Tail] = lists:reverse(Window),
    WindowElement = {BlockHeight, POCHash, ScoreUpdate},
    ToInsert = term_to_binary([WindowElement | lists:reverse(Tail)]),
    %% lager:info("sliding window, hotspot: ~p, popped: ~p, to_insert: ~p",
    %%            [?TO_ANIMAL_NAME(Hotspot), Head, WindowElement]),
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Hotspot, ToInsert).

-spec add_to_window(Hotspot :: libp2p_crypto:pubkey_bin(),
                    Window :: blockchain_ledger_v1:window(),
                    BlockHeight :: pos_integer(),
                    POCHash :: blockchain_txn:hash(),
                    ScoreUpdate :: tagged_score(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> ok.
add_to_window(Hotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger) ->
    WindowElement = {BlockHeight, POCHash, ScoreUpdate},
    ToInsert = term_to_binary([WindowElement | Window]),
    %% lager:info("adding to window, hotspot: ~p, to_insert: ~p",
    %%            [?TO_ANIMAL_NAME(Hotspot), WindowElement]),
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Hotspot, ToInsert).

-spec update_windows(Ledger :: blockchain_ledger_v1:ledger(),
                     BlockHeight :: pos_integer(),
                     POCHash :: blockchain_txn:hash(),
                     HotspotWindowUpdates :: [tagged_score()]) -> ok.
update_windows(Ledger,
               BlockHeight,
               POCHash,
               HotspotWindowUpdates) when length(HotspotWindowUpdates) > 0 ->

    ok = lists:foreach(fun({Hotspot, ScoreUpdate}) ->
                               case hotspot_window(Ledger, Hotspot) of
                                   [_ | _]=Window when length(Window) >= ?WINDOW_SIZE ->
                                       %% If this hotspot's score in the window is greater than
                                       %% score threshold, we consider it to be pretty reliable
                                       %% and give it a bigger window_size. Essentially implying that
                                       %% they get more breathing room now that they've crossed the threshold
                                       case window_score(Window) of
                                           %% Check if we hit the score threshold for this hotspot
                                           {C, _Data} when C == ?SCORE_THRESHOLD ->
                                               %% Check if we hit the window cap for this hotspot
                                               case length(Window) >= ?WINDOW_CAP of
                                                   true ->
                                                       %% slide because we hit the window cap
                                                       slide_window(Hotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger);
                                                   false ->
                                                       %% add as we hit the score cap but not the window cap
                                                       add_to_window(Hotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger)
                                               end;
                                           _ ->
                                               %% just slide
                                               slide_window(Hotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger)
                                       end;
                                   [_ | _]=Window ->
                                       add_to_window(Hotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger);
                                   [] ->
                                       %% first element
                                       WindowElement = {BlockHeight, POCHash, ScoreUpdate},
                                       ToInsert = term_to_binary([WindowElement]),
                                       %% lager:info("empty window, hotspot: ~p, to_insert: ~p",
                                       %%            [?TO_ANIMAL_NAME(Hotspot), WindowElement]),
                                       WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
                                       blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Hotspot, ToInsert)
                               end
                       end,
                       HotspotWindowUpdates);
update_windows( _, _, _, _) ->
    ok.

-spec is_promoted(Window :: window()) -> boolean().
is_promoted(Window) ->
    {LatestClass, _Data} = window_score(Window),
    LatestClass == ?SCORE_THRESHOLD.

-spec window_score(Window :: window()) -> classification().
window_score(Window) ->
    case Window of
        [] ->
            {undefined, undefined};
        [Head | _Tail] ->
            %%lager:info("Window Head ~p", [Head]),
            {_, _, C} = Head,
            C
    end.

-spec hotspot_window(Ledger :: blockchain_ledger_v1:ledger(),
                     Hotspot :: libp2p_crypto:pubkey_bin()) -> window().
hotspot_window(Ledger, Hotspot) ->
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, WindowsCF, Hotspot, []) of
        not_found ->
            [];
        {ok, BinWindow} ->
            binary_to_term(BinWindow);
        {error, _} ->
            []
    end.

-spec scores(Ledger :: blockchain_ledger_v1:ledger()) -> [tagged_score()].
scores(Ledger) ->
    Windows = windows(Ledger),
    lists:foldl(fun({Hotspot, Window}, Acc) ->
                                 Score = window_score(Window),
                                 [{Hotspot, Score} | Acc]
                         end, [], Windows).

-spec maybe_phase_out(BlockHeight :: pos_integer(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> ok.
maybe_phase_out(BlockHeight, Ledger) ->
    Windows = windows(Ledger),
    lists:foreach(fun({_Hotspot, []}) ->
                          ok;
                     ({Hotspot, [{LatestPOCTxnHeight, _, _} | _]}) ->
                          case (BlockHeight - LatestPOCTxnHeight) >= ?STALE_THRESHOLD of
                              false ->
                                  ok;
                              true ->
                                  reset_window(Ledger, Hotspot)
                          end
                  end, Windows).


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).



-endif.
