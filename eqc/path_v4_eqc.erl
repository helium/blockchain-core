-module(path_v4_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-import(eqc_utils, [name/1, ledger/1, dead_hotspots/0, find_challenger/2]).

-export([prop_path_check/0]).

prop_path_check() ->
    ?FORALL({Hash, PathLimit, ChallengerIndex},
            {gen_hash(), gen_path_limit(), gen_challenger_index()},
            begin
                Ledger = ledger(poc_v8_vars()),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                LedgerVars = blockchain_utils:vars_binary_keys_to_atoms(blockchain_ledger_v1:all_vars(Ledger)),

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

                PathLength = length(Path),

                B58Path = #{libp2p_crypto:bin_to_b58(TargetPubkeyBin) => [[libp2p_crypto:bin_to_b58(P) || P <- Path]]},
                HumanFullPath = #{name(TargetPubkeyBin) => [[name(P) || P <- Path]]},
                HumanPath = [name(P) || P <- Path],
                io:format("Time: ~p\t Path: ~p~n", [erlang:convert_time_unit(Time, microsecond, millisecond), HumanPath]),

                case length(Path) > 1 of
                    true ->
                        ok = file:write_file("/tmp/paths_js", io_lib:fwrite("~p.\n", [B58Path]), [append]),
                        ok = file:write_file("/tmp/paths_name_js", io_lib:fwrite("~p.\n", [HumanFullPath]), [append]),
                        ok = file:write_file("/tmp/paths_target", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append]);
                    false ->
                        ok = file:write_file("/tmp/paths_beacon", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append])
                end,

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

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger)
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
    #{poc_version => 8,
      %% lower range for good rssi
      poc_good_bucket_low => -115,
      %% upper range for good rssi
      poc_good_bucket_high => -80,
      %% math:sqrt(3) * 9.4 * (2000-1) ~ 32.5km, where 9.4 = hex_edge_length
      poc_max_hop_cells => 2000,
      %% half random next hop selection
      poc_v4_randomness_wt => 0.5,
      %% other half based for next hop selection based on witness rssi centrality
      poc_centrality_wt => 0.5,
      %% zeroed this because half random + half centrality
      poc_v4_prob_rssi_wt => 0.0,
      %% zeroed this because half random + half centrality
      poc_v4_prob_time_wt => 0.0,
      %% zeroed this because half random + half centrality
      poc_v4_prob_count_wt => 0.0
     }.
