-module(path_v3_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(eqc_utils,
        [
         ledger_vars/1
        ]).

-export([prop_path_check/0]).

prop_path_check() ->
    ?FORALL({Hash, PathLimit},
            {gen_hash(), gen_path_limit()},
            begin
                Ledger = ledger(),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                LedgerVars = ledger_vars(Ledger),
                %% Overwrite poc_path_limit for checking generated path limits
                Vars = maps:put(poc_path_limit, PathLimit, LedgerVars),
                {ok, GWCache} = blockchain_gateway_cache:start_link(),

                Check = case blockchain_poc_target_v2:target_v2(Hash, Ledger, Vars) of
                            {error, not_found} ->
                                %% TODO: Investigation pending
                                true;
                            {ok, TargetPubkeyBin} ->

                                {_Time, Path} = timer:tc(fun() ->
                                                                blockchain_poc_path_v3:build(TargetPubkeyBin,
                                                                                             Ledger,
                                                                                             block_time(),
                                                                                             Hash,
                                                                                             Vars)
                                                        end),

                                PathLength = length(Path),

                                %% B58Path = #{libp2p_crypto:bin_to_b58(TargetPubkeyBin) => [[libp2p_crypto:bin_to_b58(P) || P <- Path]]},
                                %% HumanPath = [name(P) || P <- Path],
                                %% io:format("Time: ~p\t Path: ~p~n", [erlang:convert_time_unit(Time, microsecond, millisecond), HumanPath]),

                                %% case length(Path) > 1 of
                                %%     true ->
                                %%         ok = file:write_file("/tmp/paths_js", io_lib:fwrite("~p.\n", [B58Path]), [append]),
                                %%         ok = file:write_file("/tmp/paths_target", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append]);
                                %%     false ->
                                %%         ok = file:write_file("/tmp/paths_beacon", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append])
                                %% end,

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
                                C1 andalso C2 andalso C3 andalso C3 andalso C4 andalso C5

                        end,

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),
                gen_server:stop(GWCache),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger)
                          end,
                          %% TODO: split into multiple verifiers instead of a single consolidated one
                          conjunction([{verify_path_construction, Check}]))
            end).

gen_path_limit() ->
    elements([3, 4, 5, 6, 7]).

gen_hash() ->
    binary(32).

ledger() ->
    %% Ledger at height: 194196
    %% ActiveGateway Count: 3023
    {ok, Dir} = file:get_cwd(),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    %% Path to static ledger tar
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    case filelib:is_file(LedgerTar) of
        true ->
            %% if we have already unpacked it, no need to do it again
            LedgerDB = filename:join([PrivDir, "ledger.db"]),
            case filelib:is_dir(LedgerDB) of
                true ->
                    ok;
                false ->
                    %% ledger tar file present, extract
                    ok = erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
            end;
        false ->
            %% ledger tar file not found, download & extract
            ok = ssl:start(),
            {ok, {{_, 200, "OK"}, _, Body}} = httpc:request("https://blockchain-core.s3-us-west-1.amazonaws.com/ledger.tar.gz"),
            ok = file:write_file(filename:join([PrivDir, "ledger.tar.gz"]), Body),
            ok = erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
    end,
    Ledger = blockchain_ledger_v1:new(PrivDir),
    %% if we haven't upgraded the ledger, upgrade it
    case blockchain_ledger_v1:get_hexes(Ledger) of
        {ok, _Hexes} ->
            Ledger;
        _ ->
            Ledger1 = blockchain_ledger_v1:new_context(Ledger),
            %% Ensure the ledger has the vars we're testing against
            blockchain_ledger_v1:vars(maps:merge(default_vars(), targeting_vars()), [], Ledger1),
            blockchain:bootstrap_hexes(Ledger1),
            blockchain_ledger_v1:commit_context(Ledger1),
            Ledger
    end.

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

%% name(PubkeyBin) ->
%%     {ok, Name} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(PubkeyBin)),
%%     Name.

targeting_vars() ->
    #{poc_v4_target_prob_score_wt => 0.0,
      poc_v4_target_prob_edge_wt => 0.0,
      poc_v5_target_prob_randomness_wt => 1.0,
      poc_v4_target_challenge_age => 300,
      poc_v4_target_exclusion_cells => 6000,
      poc_v4_target_score_curve => 2,
      poc_target_hex_parent_res => 5,
      poc_version => 7
     }.

default_vars() ->
    #{poc_v4_exclusion_cells => 10,
      poc_v4_parent_res => 11,
      poc_v4_prob_bad_rssi => 0.01,
      poc_v4_prob_count_wt => 0.3,
      poc_v4_prob_good_rssi => 1.0,
      poc_v4_prob_no_rssi => 0.5,
      poc_v4_prob_rssi_wt => 0.3,
      poc_v4_prob_time_wt => 0.3,
      poc_v4_randomness_wt => 0.1,
      poc_target_hex_parent_res => 5,
      poc_version => 7}.
