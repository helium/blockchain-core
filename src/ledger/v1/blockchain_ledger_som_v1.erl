%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger SOM ==
%% @end
%%%-------------------------------------------------------------------

-module(blockchain_ledger_som_v1).

-include("blockchain.hrl").
-include("blockchain_utils.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(WINDOW_PERIOD, 1400).
-define(MAX_WINDOW_LENGTH, 16000).
-define(WINDOW_SIZE, 25).
-define(WINDOW_CAP, 50).
-define(SCORE_THRESHOLD, <<"0">>).
-define(STALE_THRESHOLD, 20000).
-define(MAX_NUM, 115792089237316195423570985008687907853269984665640564039457584007913129639935).

%% {hotspot, score_update}
-type tagged_score() :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), bmu_data()}.
%% BMU calculation return type
-type bmu_results() :: {{non_neg_integer(), float()},
                        {non_neg_integer(), float()},
                        {non_neg_integer(), float()},
                        {non_neg_integer(), float()}}.
%% A type holding BMU data from one sample
-type bmu_data() :: {{{integer(), integer()}, float()}, binary()}.
%% List of BMU data
-type bmu_list() :: [bmu_data()].
%% list of trustees
-type trustees() :: [libp2p_crypto:pubkey_bin()].
%% Each window element is a block_height, poc_hash, score_update
-type window_element() :: {pos_integer(), blockchain_txn:hash(), bmu_data()}.
%% List of window_elements
-type window() :: [window_element()].
%% List of windows, tagged via hotspot pubkey_bin
-type windows() :: [{binary(), window()}].
%% A class associated with hotspot trust
%%-type classification() :: {atom(), bmu_data()}.
-type window_calculation() :: {float(), float(), float(), float(),
                               float(), float()}.
-type evaluations() :: {trustees(), trustees()}.

-export([update_datapoints/7,
         update_bmus/5,
         classify_sample/4,
         update_windows/4,
         reset_window/3,
         reset_window/2,
         windows/1,
         update_trustees/2,
         initialize_som/1,
         initialize_som/7,
         calculate_bmus/2,
         calculate_bmus/1,
         calculate_hotspot_bmus/2,
         calculate_hotspot_bmus/3,
         calculate_data_windows/2,
         clear_datapoints/3,
         clear_bmus/3,
         retrieve_som/1,
         retrieve_bmus/3,
         retrieve_datapoints/3,
         retrieve_trustees/1,
         init_trustees/1,
         promoted_trustees/1,
         is_promoted/1,
         maybe_phase_out/2,
         scores/1,
         hotspot_window/3]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------

%%%-------------------------------------------------------------------
%%% SOM Database functions
%%%-------------------------------------------------------------------
-spec update_datapoints(binary(), binary(), any(), any(), any(), any(), Ledger :: blockchain_ledger_v1:ledger()) -> ok.
update_datapoints(Src, Dst, Rssi, Snr, Fspl, Distance, Ledger) ->
    DatapointsCF = blockchain_ledger_v1:datapoints_cf(Ledger),
    %BacklinksCF = blockchain_ledger_v1:backlinks_cf(Ledger),
    Key1 = <<Dst/binary, Src/binary>>,
    %Key2 = <<Dst/binary, Src/binary>>,
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, DatapointsCF, Key1, []) of
        {ok, Bin} ->
            N = binary_to_term(Bin),
            Sample = {Height, Rssi, Snr, Fspl, Distance},
            Combined = [Sample | N],
            Clipped = lists:foldl(fun({H, R, S, F, D}, DAcc) ->
                                      case H < (Height - ?MAX_WINDOW_LENGTH) of
                                          true ->
                                              DAcc;
                                          false ->
                                              [{H, R, S, F, D} | DAcc]
                                      end
                                   end, [], Combined),
            ToInsert = term_to_binary(Clipped),
            ok = blockchain_ledger_v1:cache_put(Ledger, DatapointsCF, Key1, ToInsert);
        not_found ->
            BacklinksCF = blockchain_ledger_v1:backlinks_cf(Ledger),
            Sample = [{Height, Rssi, Snr, Fspl, Distance}],
            ToInsert = term_to_binary(Sample),
            ok = blockchain_ledger_v1:cache_put(Ledger, DatapointsCF, Key1, ToInsert),
            LastWindow = term_to_binary({Height + ?WINDOW_PERIOD * 4, {0.0, 0.0, 0.0}}),
            ok = blockchain_ledger_v1:cache_put(Ledger, BacklinksCF, Key1, LastWindow)
    end.

-spec clear_datapoints(binary(), binary(), Ledger :: blockchain_ledger_v1:ledger()) -> ok.
clear_datapoints(Dst, Src, Ledger) ->
    DatapointsCF = blockchain_ledger_v1:datapoints_cf(Ledger),
    BacklinksCF = blockchain_ledger_v1:backlinks_cf(Ledger),
    %BacklinksCF = blockchain_ledger_v1:backlinks_cf(Ledger),
    Key1 = <<Dst/binary, Src/binary>>,
    case blockchain_ledger_v1:cache_get(Ledger, DatapointsCF, Key1, []) of
        {ok, _Bin} ->
            ok = blockchain_ledger_v1:cache_delete(Ledger, DatapointsCF, Key1),
            ok = blockchain_ledger_v1:cache_delete(Ledger, BacklinksCF, Key1);
        not_found ->
            lager:info("No datapoints found for ~p => ~p when trying to delete", [?TO_ANIMAL_NAME(Src), ?TO_ANIMAL_NAME(Dst)]),
            ok
    end.

-spec retrieve_datapoints(binary(), binary(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, {list(), {float(), float(), float()}}} | {ok, active_window} | {error, atom()}.
retrieve_datapoints(Src, Dst, Ledger) ->
    DatapointsCF = blockchain_ledger_v1:datapoints_cf(Ledger),
    BacklinksCF = blockchain_ledger_v1:backlinks_cf(Ledger),
    Key = <<Dst/binary, Src/binary>>,
    case blockchain_ledger_v1:cache_get(Ledger, DatapointsCF, Key, []) of
        {ok, Bin} ->
            N = binary_to_term(Bin),
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            case blockchain_ledger_v1:cache_get(Ledger, BacklinksCF, Key, []) of
                {ok, Res} ->
                    {LastWindow, _Distribution} = binary_to_term(Res),
                    case LastWindow =< Height of
                        true ->
                            lager:info("HEIGHT: ~p | WINDOW EXPIRED ~p", [Height, LastWindow]),
                            NewDistribution = distribution(Dst, Ledger),
                            ok = blockchain_ledger_v1:cache_put(Ledger, BacklinksCF, Key, term_to_binary({Height + ?WINDOW_PERIOD, NewDistribution})),
                            lager:info("GOT DATA FOR ~p => ~p \n ~p",
                                       [?TO_ANIMAL_NAME(<<Src/binary>>),
                                        ?TO_ANIMAL_NAME(<<Dst/binary>>), N]),
                            {ok, {N, NewDistribution}};
                        false ->
                            {ok, active_window}
                    end;
                not_found ->
                    lager:info("Shit we should never get to here by this point..."),
                    {error, ohfuck}
               end;
        not_found ->
            {error, no_datapoints}
    end.

meanvar({RSum, SSum, Count, SetPoints}) ->
    {RMean, SMean} = {RSum/Count, SSum/Count},
    {Rssq, Sssq} = lists:foldl(fun({Rssi, Snr}, {RAcc, SAcc}) -> {(RAcc + math:pow((Rssi - RMean), 2)), (SAcc + math:pow((Snr - SMean), 2))} end, {0,0}, SetPoints),
    {Rvar, Svar} = {Rssq/Count, Sssq/Count},
    {RMean, Rvar, SMean, Svar}.

-spec distribution(Hotspot :: binary(), Ledger :: blockchain_ledger_v1:ledger()) -> {float(), float(), float()}.
distribution(Hotspot, Ledger) ->
    DatapointsCF = blockchain_ledger_v1:datapoints_cf(Ledger),
    CollectedDatapoints = blockchain_ledger_v1:cache_fold(Ledger, DatapointsCF, fun({<<D:33/binary, S:33/binary>>, Res}, Acc) ->
                                                                   case Hotspot of
                                                                       D ->
                                                                           [{Hotspot, binary_to_term(Res)} | Acc];
                                                                       S ->
                                                                           Acc;
                                                                       _ ->
                                                                           Acc
                                                                   end
                                                           end, []),

    AvgDatapoints = lists:foldl(fun({_Height, Rssi, Snr, _Fspl, _Distance},
                                 {Rssi4Acc, Snr4Acc, Count4Acc, Set4Acc}) ->
                                     {Rssi4Acc + Rssi, Snr4Acc + Snr, Count4Acc + 1, [{Rssi, Snr} | Set4Acc]}
                              end, {0,0,0, []}, CollectedDatapoints),
    {_RMean, _RVar, SMean, SVar} = meanvar(AvgDatapoints),
    {C, L, R, Count} = lists:foldl(fun({_Height, _Rssi, Snr, _Fspl, _Distance},
                                 {CAcc, LAcc, RAcc, CountAcc}) ->
                                     case Snr > (SMean - math:sqrt(SVar)) andalso Snr < (SMean + math:sqrt(SVar)) of
                                         true ->
                                             {CAcc + 1, LAcc, RAcc, CountAcc + 1};
                                         false ->
                                               case Snr >= (SMean + math:sqrt(SVar)) of
                                                   true ->
                                                       {CAcc, LAcc, RAcc+1, CountAcc + 1};
                                                   false -> % SNR =< (SMean + math:sqrt(SVar))
                                                       {CAcc, LAcc+1, RAcc, CountAcc + 1}
                                               end
                                     end
                              end, {0,0,0,0}, CollectedDatapoints),
    {C/Count, L/Count, R/Count}.




-spec calculate_data_windows(Datapoints :: list(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, window_calculation()}.
calculate_data_windows(Datapoints, _Ledger) ->
    WindowPoints4 = lists:foldl(fun({_Height, Rssi, Snr, _Fspl, _Distance},
                                 {Rssi4Acc, Snr4Acc, Count4Acc, Set4Acc}) ->
                                     {Rssi4Acc + Rssi, Snr4Acc + Snr, Count4Acc + 1, [{Rssi, Snr} | Set4Acc]}
                              end, {0,0,0, []}, Datapoints),
    [H|_T] = Datapoints,
    {_, _Rssi, _Snr, Fspl, Distance} = H,
    {RMean, RVar, SMean, SVar} = meanvar(WindowPoints4),
    {ok, {RMean, RVar, SMean, SVar,
     Fspl, Distance}}.

to_num(String) ->
    try list_to_float(String) of
        Res -> Res
    catch _:_ ->
              list_to_integer(String) * 1.0
    end.

init_som(Ledger) ->
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
        {ok, Bin} ->
            Serialized = binary_to_list(Bin),
            {ok, Som} = som:from_json(Serialized),
            Som;
        not_found ->
            %PrivDir = code:priv_dir(blockchain),
            %File = application:get_env(blockchain, aggregate_samples_file, "aggregate_samples_2.csv"),
            PrivDir = code:priv_dir(miner_pro),
            File = application:get_env(miner_pro, aggregate_samples_file, "aggregate_samples_3.csv"),
            TrainingSetFile = PrivDir ++ "/" ++ File,
            {ok, IoDevice} = file:open(TrainingSetFile, [read]),
            Processor = fun({newline, ["pos"|_]}, Acc) ->
                                 %% ignore header
                                 Acc;
                           ({newline, [_Pos,
                                       Dist, FSPL, Signal, Snr,
                                       FSnrPer, RSnrPer, LSnrPer, _FSigPer, _RSigPer, _LSigPer, Class]}, Acc) ->
                                 [{[to_num(Signal), to_num(Snr), to_num(FSnrPer), to_num(LSnrPer),
                                    to_num(RSnrPer), to_num(FSPL),
                                    to_num(Dist)], list_to_binary(Class)} | Acc];
                            (_, Acc) ->
                                 Acc
                         end,
            {ok, ProcessedRows} = ecsv:process_csv_file_with(IoDevice, Processor, []),
            {SupervisedSamples, SupervisedClasses} = lists:unzip(ProcessedRows),
            {ok, SOM} = som:new(20, 20, 7, true, #{classes => #{<<"0">> => 1.7, <<"1">> => 0.6, <<"2">> => 0.0},
                                            custom_weighting => false,
                                            sigma => 0.5,
                                            random_seed => [209,162,182,84,44,167,62,240,152,122,118,154,48,208,143,84,
                                                             186,211,219,113,71,108,171,185,51,159,124,176,167,192,23,245]}),
            %% Train the network through supervised learning
            som:train_random_supervised(SOM, SupervisedSamples, SupervisedClasses, 10000),
            {ok, Serialized} = som:export_json(SOM),
            blockchain_ledger_v1:cache_put(Ledger, SomCF, term_to_binary(global), Serialized),
            SOM
    end.

-spec calculate_hotspot_bmus(binary(), Ledger :: blockchain_ledger_v1:ledger(), Opts :: challengee | witness | combined) -> bmu_list().
calculate_hotspot_bmus(Hotspot, Ledger, Opts) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    case Opts of
        challengee ->
            blockchain_ledger_v1:cache_fold(Ledger, BmuCF, fun({<<S:33/binary, _:33/binary>>, Res}, Acc) ->
                                                                   case Hotspot of
                                                                       S ->
                                                                           [{Hotspot, binary_to_term(Res)} | Acc];
                                                                       _ ->
                                                                           Acc
                                                                   end
                                                           end, []);
        witness ->
            blockchain_ledger_v1:cache_fold(Ledger, BmuCF, fun({<<_:33/binary, D:33/binary>>, Res}, Acc) ->
                                                                   case Hotspot of
                                                                       D ->
                                                                           [{Hotspot, binary_to_term(Res)} | Acc];
                                                                       _ ->
                                                                           Acc
                                                                   end
                                                           end, []);
        combined ->
            blockchain_ledger_v1:cache_fold(Ledger, BmuCF, fun({<<S:33/binary, D:33/binary>>, Res}, Acc) ->
                                                                   case Hotspot of
                                                                       S ->
                                                                           [{Hotspot, binary_to_term(Res)} | Acc];
                                                                       D ->
                                                                           [{Hotspot, binary_to_term(Res)} | Acc];
                                                                       _ ->
                                                                           Acc
                                                                   end
                                                           end, [])
    end.

-spec calculate_bmus(Ledger :: blockchain_ledger_v1:ledger()) -> [{binary(), bmu_results()}].
calculate_bmus(Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),

    {SKeys, Keys} = blockchain_ledger_v1:cache_fold(Ledger, BmuCF, fun({<<S:33/binary, D:33/binary>>, _Res}, {SAcc, Acc}) ->
                                                                           {[S | SAcc], [{S, D} | Acc]} end, {[], []}),

    NonDupKeys = sets:to_list(sets:from_list(SKeys)),
    lists:foldl(fun(Key, Acc) ->
                        %% Return all BMUs for keys matching the challengee address
                        KeyMatchBmus = lists:foldl(fun({First, Second}, ResultAcc) ->
                                                           case Key == First of
                                                               true ->
                                                                   Lookup = <<First/binary, Second/binary>>,
                                                                  case blockchain_ledger_v1:cache_get(Ledger, BmuCF, Lookup, []) of
                                                                      {ok, Bin} ->
                                                                           Bmus = binary_to_term(Bin),
                                                                           Bmus ++ ResultAcc;
                                                                      not_found ->
                                                                          ResultAcc
                                                                  end;
                                                               false ->
                                                                  ResultAcc
                                                           end
                                                   end, [], Keys),
                        case KeyMatchBmus of
                            [] ->
                                lager:info("NO KEY MATCH BMUs FOUND FOR: ~p => ~p", [Key, ?TO_ANIMAL_NAME(Key)]),
                                %% For some reason we didn't match any keys here
                                Acc;
                            [_|_] ->
                                %% Calculate the average based on all the BMUs for the prior key results
                                Results = lists:foldl(fun({{_, Dist}, Class}, {{Rsum, RDsum}, {Fsum, FDsum}, {Msum, MDsum}, {Usum, UDsum}}) ->
                                                             case Class of
                                                                 <<"0">> -> {{Rsum + 1, RDsum + Dist}, {Fsum, FDsum}, {Msum, MDsum}, {Usum, UDsum}};
                                                                 <<"1">> -> {{Rsum, RDsum}, {Fsum + 1, FDsum + Dist}, {Msum, MDsum}, {Usum, UDsum}};
                                                                 <<"2">> -> {{Rsum, RDsum}, {Fsum, FDsum}, {Msum + 1, MDsum + Dist}, {Usum, UDsum}};
                                                                 <<"undefined">> -> {{Rsum, RDsum}, {Fsum, FDsum}, {Msum, MDsum}, {Usum + 1, UDsum + Dist}}
                                                             end
                                                     end, {{0,0},{0,0},{0,0},{0,0}}, KeyMatchBmus),
                                {{Reals, RDist}, {Fakes, FDist}, {Mids, FMids}, {Undefs, UDist}} = Results,
                                RAvg = case Reals of
                                           0 ->
                                               0;
                                           _ ->
                                               RDist/Reals
                                       end,
                                FAvg = case Fakes of
                                           0 ->
                                               0;
                                           _ ->
                                               FDist/Fakes
                                       end,
                                MAvg = case Mids of
                                           0 ->
                                               0;
                                           _ ->
                                               FMids/Mids
                                       end,
                                UAvg = case Undefs of
                                           0 ->
                                               0;
                                           _ ->
                                               UDist/Undefs
                                       end,
                                BmuDataAvgs = {{Reals, RAvg}, {Fakes, FAvg}, {Mids, MAvg}, {Undefs, UAvg}},
                                %% Add this challengee key and it's average result to list and move on to next one
                                [{Key, BmuDataAvgs} | Acc]
                        end
                end, [], NonDupKeys).


-spec calculate_bmus(binary(), Ledger :: blockchain_ledger_v1:ledger()) -> bmu_results().
calculate_bmus(Key, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, Key, []) of
        {ok, Bin} ->
            Bmus = binary_to_term(Bin),
            %% lager:info("Calculate BMUs for: ~p", [Key]),
            {{Reals, RDist}, {Fakes, FDist}, {Mids, FMids}, {Undefs, UDist}} = lists:foldl(fun({{_, Dist}, Class}, {{Rsum, RDsum}, {Fsum, FDsum}, {Msum, MDsum}, {Usum, UDsum}}) -> case Class of
                                                                             <<"0">> -> {{Rsum + 1, RDsum + Dist}, {Fsum, FDsum}, {Msum, MDsum}, {Usum, UDsum}};
                                                                             <<"1">> -> {{Rsum, RDsum}, {Fsum + 1, FDsum + Dist}, {Msum, MDsum}, {Usum, UDsum}};
                                                                             <<"2">> -> {{Rsum, RDsum}, {Fsum, FDsum}, {Msum + 1, MDsum + Dist}, {Usum, UDsum}};
                                                                             <<"undefined">> -> {{Rsum, RDsum}, {Fsum, FDsum}, {Msum, MDsum}, {Usum + 1, UDsum + Dist}}
                                                                         end
                               end, {{0,0},{0,0},{0,0},{0,0}}, Bmus),
            RAvg = case Reals of
                       0 ->
                           0;
                       _ ->
                           RDist/Reals
                   end,
            FAvg = case Fakes of
                       0 ->
                           0;
                       _ ->
                           FDist/Fakes
                   end,
            MAvg = case Mids of
                       0 ->
                           0;
                       _ ->
                           FMids/Mids
                   end,
            UAvg = case Undefs of
                       0 ->
                           0;
                       _ ->
                           UDist/Undefs
                   end,
            {{Reals, RAvg}, {Fakes, FAvg}, {Mids, MAvg}, {Undefs, UAvg}};
    not_found ->
            {{0,0.0},{0,0.0},{0,0.0},{0,0.0}}
    end.

-spec calculate_hotspot_bmus(binary(), Ledger :: blockchain_ledger_v1:ledger()) -> bmu_list().
calculate_hotspot_bmus(Hotspot, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    blockchain_ledger_v1:cache_fold(Ledger, BmuCF, fun({<<S:33/binary, _:33/binary>>, Res}, Acc) ->
                                                                   case Hotspot of
                                                                       S ->
                                                                           [{Hotspot, binary_to_term(Res)} | Acc];
                                                                       _ ->
                                                                           Acc
                                                                   end
                                                           end, []).

-spec update_bmus(binary(), binary(), Values :: window_calculation(), Distribution :: {float(), float(), float()}, Ledger :: blockchain_ledger_v1:ledger()) -> {ok, bmu_data()}.
update_bmus(Src, Dst, Values, Distribution, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    Key = <<Dst/binary, Src/binary>>,
    {CDist, LDist, RDist} = Distribution,
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, Key, []) of
        {ok, BmusBin} ->
            Bmus = binary_to_term(BmusBin),
            case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
                {ok, SomBin} ->
                    {ok, Som} = som:from_json(SomBin),
                    %% Calculate new BMUs with stored SOM
                    {Signal, _Sigvar, Snr, _Snrvar,
                     Fspl, Dist} = Values,
                    NewBmu = som:winner_vals(Som,
                                             [float((Signal - (-135))/(135)), float((Snr - (-19))/(17 - (-19))),
                                              float(CDist), float(LDist), float(RDist),
                                              float((Fspl - (-165))/(165)), float((Dist)/(3920))]),
                    lager:info("NORMALIZED BMU FOR ~p => ~p | ~p", [?TO_ANIMAL_NAME(Src), ?TO_ANIMAL_NAME(Dst), NewBmu]),
                    %% Append BMUs list
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary(lists:sublist([NewBmu | Bmus], ?WINDOW_CAP))),
                    {ok, NewBmu};
                not_found ->
                    Som = init_som(Ledger),
                    {Signal, _Sigvar, Snr, _Snrvar,
                     Fspl, Dist} = Values,
                    NewBmu = som:winner_vals(Som,
                                             [float((Signal - (-135))/(135)), float((Snr - (-19))/(17 - (-19))),
                                              float(CDist), float(LDist), float(RDist),
                                             float((Fspl - (-165))/(165)), float((Dist)/(3920))]),
                    lager:info("NORMALIZED BMU FOR ~p => ~p | ~p", [?TO_ANIMAL_NAME(Src), ?TO_ANIMAL_NAME(Dst), NewBmu]),
                    %% Append BMUs list
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary(lists:sublist([NewBmu | Bmus], ?WINDOW_CAP))),
                    {ok, NewBmu}
            end;
        not_found ->
            case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
                {ok, SomBin} ->
                    {ok, Som} = som:from_json(SomBin),
                    %% Calculate new BMUs with stored SOM
                    {Signal, _Sigvar, Snr, _Snrvar,
                     Fspl, Dist} = Values,
                    NewBmu = som:winner_vals(Som,
                                             [float((Signal - (-135))/(135)), float((Snr - (-19))/(17 - (-19))),
                                              float(CDist), float(LDist), float(RDist),
                                             float((Fspl - (-165))/(165)), float((Dist)/(3920))]),
                    lager:info("NORMALIZED BMU FOR ~p => ~p | ~p", [?TO_ANIMAL_NAME(Src), ?TO_ANIMAL_NAME(Dst), NewBmu]),
                    %% Append BMUs list
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary([NewBmu])),
                    {ok, NewBmu};
                not_found ->
                    Som = init_som(Ledger),
                    {Signal, _Sigvar, Snr, _Snrvar,
                     Fspl, Dist} = Values,
                    NewBmu = som:winner_vals(Som,
                                             [float((Signal - (-135))/(135)), float((Snr - (-19))/(17 - (-19))),
                                              float(CDist), float(LDist), float(RDist),
                                             float((Fspl - (-165))/(165)), float((Dist)/(3920))]),
                    lager:info("NORMALIZED BMU FOR ~p => ~p | ~p", [?TO_ANIMAL_NAME(Src), ?TO_ANIMAL_NAME(Dst), NewBmu]),
                    %% Append BMUs list
                    blockchain_ledger_v1:cache_put(Ledger, BmuCF, Key, term_to_binary([NewBmu])),
                    {ok, NewBmu}
            end
    end.

classify_sample(Signal, Snr, Fspl, Ledger) ->
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, SomCF, term_to_binary(global), []) of
        {ok, SomBin} ->
            {ok, Som} = som:from_json(SomBin),
            %% Calculate new BMUs with stored SOM
            som:winner_vals(Som,
                            [((float(Signal) - (1 * math:pow(10, -17)))/(0.001 - (1 * math:pow(10, -17)))),
                             ((float(Snr) - (-19))/(17 - (-19))),
                             ((float(Fspl) -  (3.981072 * math:pow(10, -20)))/(0.001 - (3.981072 * math:pow(10, -20))))]);
        not_found ->
            Som = init_som(Ledger),
            som:winner_vals(Som,
                            [((float(Signal) - (1 * math:pow(10, -17)))/(0.001 - (1 * math:pow(10, -17)))),
                             ((float(Snr) - (-19))/(17 - (-19))),
                             ((float(Fspl) -  (3.981072 * math:pow(10, -20)))/(0.001 - (3.981072 * math:pow(10, -20))))])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SOM DB API (internal/console)
%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec retrieve_bmus(binary(), binary(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, bmu_list()} | {error, not_found}.
retrieve_bmus(A, B, Ledger) ->
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, <<B/binary, A/binary>>, []) of
        {ok, Bin} ->
            lager:info("BMUs RETRIEVED FOR: ~p => ~p", [?TO_ANIMAL_NAME(A), ?TO_ANIMAL_NAME(B)]),
            N = binary_to_term(Bin),
            {ok, N};
        not_found ->
            lager:debug("Retrieve FAIL"),
            {error, not_found}
    end.

-spec clear_bmus(binary(), binary(), Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, not_found}.
clear_bmus(Src, Dst, Ledger) ->
    Key = <<Dst/binary, Src/binary>>,
    BmuCF = blockchain_ledger_v1:bmu_cf(Ledger),
    case blockchain_ledger_v1:cache_get(Ledger, BmuCF, Key, []) of
        {ok, _Bin} ->
            lager:info("BMUs CLEARED FOR: ~p => ~p", [?TO_ANIMAL_NAME(Src), ?TO_ANIMAL_NAME(Dst)]),
            blockchain_ledger_v1:cache_delete(Ledger, BmuCF, Key),
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
             {ok, Bin};
         not_found ->
             lager:debug("Retrieve FAIL"),
             {error, not_found}
     end.

-spec initialize_som(Ledger :: blockchain_ledger_v1:ledger(), Classes :: #{}, Sigma :: float(), Xdim :: non_neg_integer(), Ydim :: non_neg_integer(), InVec :: non_neg_integer(), CustomWeights :: boolean()) -> ok.
initialize_som(Ledger, Classes, Sigma, Xdim, Ydim, InVec, CustomWeights) ->
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    PrivDir = code:priv_dir(miner_pro),
    File = application:get_env(miner_pro, aggregate_samples_file, "aggregate_samples_3.csv"),
    TrainingSetFile = PrivDir ++ "/" ++ File,
    {ok, IoDevice} = file:open(TrainingSetFile, [read]),

    Processor = fun({newline, ["pos"|_]}, Acc) ->
                                 %% ignore header
                                 Acc;
                           ({newline, [_Pos,
                                       Dist, FSPL, Signal, Snr,
                                       FSnrPer, RSnrPer, LSnrPer, _FSigPer, _RSigPer, _LSigPer, Class]}, Acc) ->
                                 [{[to_num(Signal), to_num(Snr), to_num(FSnrPer), to_num(LSnrPer),
                                    to_num(RSnrPer), to_num(FSPL),
                                    to_num(Dist)], list_to_binary(Class)} | Acc];
                            (_, Acc) ->
                                 Acc
                         end,
    {ok, ProcessedRows} = ecsv:process_csv_file_with(IoDevice, Processor, []),
    {SupervisedSamples, SupervisedClasses} = lists:unzip(ProcessedRows),
    {ok, SOM} = som:new(Xdim, Ydim, InVec, true, #{classes => Classes,
                                    custom_weighting => CustomWeights,
                                    sigma => Sigma,
                                    random_seed => [209,162,182,84,44,167,62,240,152,122,118,154,48,208,143,84,
                                                     186,211,219,113,71,108,171,185,51,159,124,176,167,192,23,245]}),
    %% Train the network through supervised learning
    som:train_random_supervised(SOM, SupervisedSamples, SupervisedClasses, 10000),
    {ok, Serialized} = som:export_json(SOM),
    blockchain_ledger_v1:cache_put(Ledger, SomCF, term_to_binary(global), Serialized).


-spec initialize_som(Ledger :: blockchain_ledger_v1:ledger()) -> ok.
initialize_som(Ledger) ->
    SomCF = blockchain_ledger_v1:som_cf(Ledger),
    PrivDir = code:priv_dir(miner_pro),
    File = application:get_env(miner_pro, aggregate_samples_file, "aggregate_samples_3.csv"),
    TrainingSetFile = PrivDir ++ "/" ++ File,
    {ok, IoDevice} = file:open(TrainingSetFile, [read]),
    Processor = fun({newline, ["pos"|_]}, Acc) ->
                                 %% ignore header
                                 Acc;
                           ({newline, [_Pos,
                                       Dist, FSPL, Signal, Snr,
                                       FSnrPer, RSnrPer, LSnrPer, _FSigPer, _RSigPer, _LSigPer, Class]}, Acc) ->
                                 [{[to_num(Signal), to_num(Snr), to_num(FSnrPer), to_num(LSnrPer),
                                    to_num(RSnrPer), to_num(FSPL),
                                    to_num(Dist)], list_to_binary(Class)} | Acc];
                            (_, Acc) ->
                                 Acc
                         end,
            {ok, ProcessedRows} = ecsv:process_csv_file_with(IoDevice, Processor, []),
            {SupervisedSamples, SupervisedClasses} = lists:unzip(ProcessedRows),
            {ok, SOM} = som:new(20, 20, 7, true, #{classes => #{<<"0">> => 1.7, <<"1">> => 0.6, <<"2">> => 0.0},
                                            custom_weighting => false,
                                            sigma => 0.5,
                                            random_seed => [209,162,182,84,44,167,62,240,152,122,118,154,48,208,143,84,
                                                             186,211,219,113,71,108,171,185,51,159,124,176,167,192,23,245]}),

    %% Train the network through supervised learning
    som:train_random_supervised(SOM, SupervisedSamples, SupervisedClasses, 10000),
    {ok, Serialized} = som:export_json(SOM),
    blockchain_ledger_v1:cache_put(Ledger, SomCF, term_to_binary(global), Serialized).

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
    blockchain_ledger_v1:cache_fold(Ledger, WindowsCF, fun({HotspotKey, Res}, Acc) ->
                                          [{HotspotKey, binary_to_term(Res)} | Acc] end,
               []).

-spec reset_window(Ledger :: blockchain_ledger_v1:ledger(),
                   Hotspot :: libp2p_crypto:pubkey_bin()) -> ok.
reset_window(Ledger, Hotspot) ->
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    Keys = blockchain_ledger_v1:cache_fold(Ledger, WindowsCF, fun({<<D:33/binary, S:33/binary>>=Key, _Res}, Acc) ->
                                                               case Hotspot of
                                                                   S ->
                                                                       lager:info("Clear Challengee: ~p", [?TO_ANIMAL_NAME(Hotspot)]),
                                                                       [Key | Acc];
                                                                   D ->
                                                                       lager:info("Clear Witness: ~p", [?TO_ANIMAL_NAME(Hotspot)]),
                                                                       [Key | Acc];
                                                                   _ ->
                                                                       Acc
                                                               end
                                                       end,
               []),
    case Keys of
        [] ->
            ok;
        [_ | _] ->
            lists:foldl(fun(<<D:33/binary, S:33/binary>>=Key, _Acc) ->
                                ok = blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Key, term_to_binary([])),
                                ok = clear_datapoints(D, S, Ledger)
                        end, [], Keys),
            ok
    end.


-spec reset_window(Ledger :: blockchain_ledger_v1:ledger(),
                   SourceHotspot :: libp2p_crypto:pubkey_bin(),
                   DestHotspot :: libp2p_crypto:pubkey_bin()) -> ok.
reset_window(Ledger, SourceHotspot, DestHotspot) ->
    lager:info("Clear Window for ~p => ~p", [?TO_ANIMAL_NAME(<<SourceHotspot/binary>>), ?TO_ANIMAL_NAME(<<DestHotspot/binary>>)]),
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    Key = <<DestHotspot/binary, SourceHotspot/binary>>,
    ok = clear_datapoints(DestHotspot, SourceHotspot, Ledger),
    blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Key, term_to_binary([])).

-spec slide_window(SourceHotspot :: libp2p_crypto:pubkey_bin(),
                   DestHostpot :: libp2p_crypto:pubkey_bin(),
                   Window :: window(),
                   BlockHeight :: pos_integer(),
                   POCHash :: blockchain_txn:hash(),
                   ScoreUpdate :: tagged_score(),
                   Ledger :: blockchain_ledger_v1:ledger()) -> ok.
slide_window(SourceHotspot, DestHostpot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger) ->
    %% slide window
    [_Head | Tail] = lists:reverse(Window),
    WindowElement = {BlockHeight, POCHash, ScoreUpdate},
    ToInsert = term_to_binary([WindowElement | lists:reverse(Tail)]),
    %% lager:info("sliding window, hotspot: ~p, popped: ~p, to_insert: ~p",
    %%            [?TO_ANIMAL_NAME(Hotspot), Head, WindowElement]),
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    Key = <<DestHostpot/binary, SourceHotspot/binary>>,
    blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Key, ToInsert).

-spec add_to_window(SourceHotspot :: libp2p_crypto:pubkey_bin(),
                    DestHotspot :: libp2p_crypto:pubkey_bin(),
                    Window :: blockchain_ledger_v1:window(),
                    BlockHeight :: pos_integer(),
                    POCHash :: blockchain_txn:hash(),
                    ScoreUpdate :: tagged_score(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> ok.
add_to_window(SourceHotspot, DestHotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger) ->
    WindowElement = {BlockHeight, POCHash, ScoreUpdate},
    ToInsert = term_to_binary([WindowElement | Window]),
    %% lager:info("adding to window, hotspot: ~p, to_insert: ~p",
    %%            [?TO_ANIMAL_NAME(Hotspot), WindowElement]),
    Key = <<DestHotspot/binary, SourceHotspot/binary>>,
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Key, ToInsert).

-spec update_windows(Ledger :: blockchain_ledger_v1:ledger(),
                     BlockHeight :: pos_integer(),
                     POCHash :: blockchain_txn:hash(),
                     HotspotWindowUpdates :: [tagged_score()]) -> ok.
update_windows(Ledger,
               BlockHeight,
               POCHash,
               HotspotWindowUpdates) when length(HotspotWindowUpdates) > 0 ->
    ok = lists:foreach(fun({DestHotspot, SourceHotspot, ScoreUpdate}) ->
                               case hotspot_window(Ledger, DestHotspot, SourceHotspot) of
                                   [_ | _]=Window when length(Window) > ?WINDOW_CAP ->
                                       lager:info("SLIDING WINDOW FOR ~p => ~p", [?TO_ANIMAL_NAME(<<SourceHotspot/binary>>), ?TO_ANIMAL_NAME(<<DestHotspot/binary>>)]),
                                       slide_window(DestHotspot, SourceHotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger);
                                   [_ | _]=Window ->
                                        lager:info("ADDING TO WINDOW FOR ~p => ~p", [?TO_ANIMAL_NAME(<<SourceHotspot/binary>>), ?TO_ANIMAL_NAME(<<DestHotspot/binary>>)]),
                                        add_to_window(DestHotspot, SourceHotspot, Window, BlockHeight, POCHash, ScoreUpdate, Ledger);
                                   [] ->
                                       %% first element
                                       WindowElement = {BlockHeight, POCHash, ScoreUpdate},
                                       ToInsert = term_to_binary([WindowElement]),
                                       WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
                                       Key = <<DestHotspot/binary, SourceHotspot/binary>>,
%                                       Key = <<SourceHotspot/binary, DestHostpot/binary>>,
                                       blockchain_ledger_v1:cache_put(Ledger, WindowsCF, Key, ToInsert)
                               end
                       end,
                       HotspotWindowUpdates);
update_windows( _, _, _, _) ->
    ok.

-spec is_promoted(Window :: window()) -> boolean().
is_promoted(Window) ->
    {{{_, _}, _Dist}, LatestClass} = window_score(Window),
    LatestClass == ?SCORE_THRESHOLD.

-spec window_score(Window :: window()) -> bmu_data().
window_score(Window) ->
    case Window of
        [] ->
            {{{0,0}, 0.0}, <<"undefined">>};
        [Head | _Tail] ->
            {_Height, _Hash, Data} = Head,
            Data
    end.

-spec hotspot_window(Ledger :: blockchain_ledger_v1:ledger(),
                     SourceHotspot :: libp2p_crypto:pubkey_bin(),
                     DestHostpot :: libp2p_crypto:pubkey_bin()) -> window().
hotspot_window(Ledger, SourceHotspot, DestHotspot) ->
    WindowsCF = blockchain_ledger_v1:windows_cf(Ledger),
    Key = <<DestHotspot/binary, SourceHotspot/binary>>,
    case blockchain_ledger_v1:cache_get(Ledger, WindowsCF, Key, []) of
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
    lists:foldl(fun({Key, Window}, Acc) ->
                                 Score = window_score(Window),
                                 <<DestHotspot:33/binary, SourceHotspot:33/binary>> = Key,
                                 [{DestHotspot, SourceHotspot, Score} | Acc]
                         end, [], Windows).

-spec maybe_phase_out(BlockHeight :: pos_integer(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> ok.
maybe_phase_out(BlockHeight, Ledger) ->
    Windows = windows(Ledger),
    lists:foreach(fun({_SrcHotspot, _DstHotspot, []}) ->
                          ok;
                     ({SrcHotspot, DstHotspot, [{LatestPOCTxnHeight, _, _} | _]}) ->
                          case (BlockHeight - LatestPOCTxnHeight) >= ?STALE_THRESHOLD of
                              false ->
                                  ok;
                              true ->
                                  reset_window(Ledger, SrcHotspot, DstHotspot)
                          end
                  end, Windows).


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).



-endif.
