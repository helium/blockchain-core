-module(path_v4_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("include/blockchain_vars.hrl").

-import(eqc_utils,
        [ledger/1,
         dead_hotspots/0,
         find_challenger/2,
         ledger_vars/1,
         big_witness_hotspots/0,
         maybe_output_paths/3
        ]).

-export([prop_path_check/0, prop_path_top_200_witness_check/0]).

prop_path_check() ->
    ?FORALL({Hash, PathLimit, ChallengerIndex},
            {gen_hash(), gen_path_limit(), gen_challenger_index()},
            begin
                {ok, GWCache} = blockchain_gateway_cache:start_link(),
                Ledger = ledger(poc_v8_vars()),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                LedgerVars = ledger_vars(Ledger),

                %% use this to artificially constrain path limit for testing
                %% PathLimit = 5,

                %% use this to make timings more realistic for pi
                %% disks, set to 1 or 2
                application:set_env(blockchain, find_gateway_sim_delay, 0),

                %% Overwrite poc_path_limit for checking generated path limits
                Vars = maps:put(poc_path_limit, PathLimit, LedgerVars),

                %% Find some challenger
                {ChallengerPubkeyBin, _ChallengerLoc} = find_challenger(ChallengerIndex, ActiveGateways),

                {ok, {TargetPubkeyBin, TargetRandState}} = blockchain_poc_target_v3:target(ChallengerPubkeyBin, Hash, Ledger, Vars),
                {Time, Path} = timer:tc(fun() ->
                                                blockchain_poc_path_v4:build(TargetPubkeyBin,
                                                                             TargetRandState,
                                                                             Ledger,
                                                                             block_time(),
                                                                             Vars)
                                        end),

                %% change the default to true to collect and display
                %% time stats at each run
                case application:get_env(blockchain, collect_stats, false) of
                    true ->
                        Runs =
                            case get(runs) of
                                undefined ->
                                    put(runs, [Time]),
                                    [Time];
                                R ->
                                    R1 = [Time | R],
                                    put(runs, R1),
                                    R1
                            end,

                        L = length(Runs),
                        Avg = trunc(lists:sum(Runs) / L),
                        Med = case L < 5 of
                                  true -> no_med;
                                  _ -> lists:nth((L div 2), lists:sort(Runs))
                              end,
                        Max = lists:max(Runs),

                        io:fwrite(standard_error, "build took avg ~p med ~p max ~p time ~p us\n", [Avg, Med, Max, Time]);
                    false ->
                        ok
                end,
                PathLength = length(Path),

                ok = maybe_output_paths(TargetPubkeyBin, Path, Time),

                %% Checks:
                %% - honor path limit
                %% - atleast one element in path
                %% - target is always in path
                %% - we never go back to the same h3 index in path
                %% - check next hop is a witness of previous gateway
                C1 = PathLength =< PathLimit andalso PathLength >= 1,
                C2 = length(Path) == length(lists:usort(Path)),
                C3 = lists:member(TargetPubkeyBin, Path),
                C4 = check_path_h3_indices(Path, ActiveGateways),
                C5 = check_next_hop(Path, ActiveGateways),
                C6 = check_target_and_path_members_not_dead(TargetPubkeyBin, Path),

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),
                gen_server:stop(GWCache),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger),
                              blockchain_score_cache:stop(),
                              gen_server:stop(GWCache)
                          end,
                          %% TODO: split into multiple verifiers instead of a single consolidated one
                          conjunction([
                                       {verify_path_limit, C1},
                                       {verify_minimum_one_element, C2},
                                       {verify_target_in_path, C3},
                                       {verify_non_vw_path, C4},
                                       {verify_next_hop_is_witness, C5},
                                       {verify_not_dead, C6}
                                      ])
                         )
            end).

prop_path_top_200_witness_check() ->
    ?FORALL({Hash, PathLimit, ChallengerIndex, TargetPubkeyBin},
            {gen_hash(), gen_path_limit(), gen_challenger_index(), gen_big_witness_target()},
            begin
                {ok, GWCache} = blockchain_gateway_cache:start_link(),
                Ledger = ledger(poc_v8_vars()),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                LedgerVars = ledger_vars(Ledger),

                %% use this to artificially constrain path limit for testing
                %% PathLimit = 5,

                %% use this to make timings more realistic for pi
                %% disks, set to 1 or 2
                application:set_env(blockchain, find_gateway_sim_delay, 0),

                %% Overwrite poc_path_limit for checking generated path limits
                Vars = maps:put(poc_path_limit, PathLimit, LedgerVars),

                %% Find some challenger
                {ChallengerPubkeyBin, _ChallengerLoc} = find_challenger(ChallengerIndex, ActiveGateways),

                {ok, {_, TargetRandState}} = blockchain_poc_target_v3:target(ChallengerPubkeyBin, Hash, Ledger, Vars),
                {Time, Path} = timer:tc(fun() ->
                                                blockchain_poc_path_v4:build(TargetPubkeyBin,
                                                                             TargetRandState,
                                                                             Ledger,
                                                                             block_time(),
                                                                             Vars)
                                        end),

                %% change the default to true to collect and display
                %% time stats at each run
                case application:get_env(blockchain, collect_stats, false) of
                    true ->
                        Runs =
                            case get(runs) of
                                undefined ->
                                    put(runs, [Time]),
                                    [Time];
                                R ->
                                    R1 = [Time | R],
                                    put(runs, R1),
                                    R1
                            end,

                        L = length(Runs),
                        Avg = trunc(lists:sum(Runs) / L),
                        Med = case L < 5 of
                                  true -> no_med;
                                  _ -> lists:nth((L div 2), lists:sort(Runs))
                              end,
                        Max = lists:max(Runs),

                        io:fwrite(standard_error, "build took avg ~p med ~p max ~p time ~p us\n", [Avg, Med, Max, Time]);
                    false ->
                        ok
                end,
                PathLength = length(Path),

                ok = maybe_output_paths(TargetPubkeyBin, Path, Time),

                %% Checks:
                %% - honor path limit
                %% - atleast one element in path
                %% - target is always in path
                %% - we never go back to the same h3 index in path
                %% - check next hop is a witness of previous gateway
                C1 = PathLength =< PathLimit andalso PathLength >= 1,
                C2 = length(Path) == length(lists:usort(Path)),
                C3 = lists:member(TargetPubkeyBin, Path),
                C4 = check_path_h3_indices(Path, ActiveGateways),
                C5 = check_next_hop(Path, ActiveGateways),
                C6 = check_target_and_path_members_not_dead(TargetPubkeyBin, Path),

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),
                gen_server:stop(GWCache),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger),
                              blockchain_score_cache:stop(),
                              gen_server:stop(GWCache)
                          end,
                          %% TODO: split into multiple verifiers instead of a single consolidated one
                          conjunction([
                                       {verify_path_limit, C1},
                                       {verify_minimum_one_element, C2},
                                       {verify_target_in_path, C3},
                                       {verify_non_vw_path, C4},
                                       {verify_next_hop_is_witness, C5},
                                       {verify_not_dead, C6}
                                      ])
                         )
            end).

gen_big_witness_target() ->
    elements(big_witness_hotspots()).

gen_path_limit() ->
    elements([3, 4, 5, 6, 7]).

gen_hash() ->
    binary(32).

gen_challenger_index() ->
    ?SUCHTHAT(S, int(), S < 3024 andalso S > 0).

block_time() ->
    %% block time at height 194196
    1580943269.

check_path_h3_indices(Path, ActiveGateways) ->
    %% check every path member has a unique h3 index
    PathIndices = lists:foldl(fun(PubkeyBin, Acc) ->
                                      [blockchain_ledger_gateway_v2:location(maps:get(PubkeyBin, ActiveGateways)) | Acc]
                              end,
                              [],
                              Path),
    length(lists:usort(PathIndices)) == length(PathIndices).

check_next_hop([_H], _ActiveGateways) ->
    true;
check_next_hop([H | T], ActiveGateways) ->
    HGw = maps:get(H, ActiveGateways),
    case maps:is_key(hd(T), blockchain_ledger_gateway_v2:witnesses(HGw)) of
        true ->
            check_next_hop(T, ActiveGateways);
        false ->
            false
    end.

check_target_and_path_members_not_dead(TargetPubkeyBin, Path) ->
    DeadHotspots = dead_hotspots(),
    (not lists:member(TargetPubkeyBin, DeadHotspots) andalso
     lists:all(fun(P) ->
                       not lists:member(P, DeadHotspots)
               end,
               Path)
    ).

poc_v8_vars() ->
    #{?poc_version => 8,
      %% lower range for good rssi
      ?poc_good_bucket_low => -115,
      %% upper range for good rssi
      ?poc_good_bucket_high => -80,
      %% math:sqrt(3) * 9.4 * (2000-1) ~ 32.5km, where 9.4 = hex_edge_length
      ?poc_max_hop_cells => 2000,
      %% half random next hop selection
      ?poc_v4_randomness_wt => 0.5,
      %% other half based for next hop selection based on witness rssi centrality
      ?poc_centrality_wt => 0.5,
      %% zeroed this because half random + half centrality
      ?poc_v4_prob_rssi_wt => 0.0,
      %% zeroed this because half random + half centrality
      ?poc_v4_prob_time_wt => 0.0,
      %% zeroed this because half random + half centrality
      ?poc_v4_prob_count_wt => 0.0,
      ?poc_witness_consideration_limit => 20
     }.
