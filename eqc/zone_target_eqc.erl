-module(zone_target_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_target_check/0]).

prop_target_check() ->
    noshrink(
    ?FORALL({_Hash, ChallengerIndex},
            {gen_hash(), gen_challenger_index()},
            begin
                %% known problematic hash/index pairs
                Ledger = ledger(),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                Challenger = lists:nth(ChallengerIndex, maps:keys(ActiveGateways)),
                Hash = crypto:strong_rand_bytes(32),
                Vars = maps:merge(default_vars(), targeting_vars()),
                Check = case blockchain_ledger_gateway_v2:location(maps:get(Challenger, ActiveGateways)) of
                            undefined ->
                                true;
                            _ChallengerLoc ->
                                {_Time, {ok, TargetPubkeyBin}} =
                                    timer:tc(fun() ->
                                                      blockchain_poc_target_v2:target_v2(Hash, Ledger, Vars)
                                             end),
                                %% io:format("Time: ~p\t Target: ~p~n",
                                %%           [erlang:convert_time_unit(Time, microsecond, millisecond),
                                %%            element(2,
                                %%                    erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(TargetPubkeyBin)))
                                %%           ]),

                                %% {ok, TargetName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(TargetPubkeyBin)),
                                %% {ok, ChallengerName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(Challenger)),
                                %% {ok, TargetScore} = blockchain_ledger_v1:gateway_score(TargetPubkeyBin, Ledger),
                                %% TargetLoc = blockchain_ledger_gateway_v2:location(maps:get(TargetPubkeyBin, ActiveGateways)),
                                %% {ok, Dist} = vincenty:distance(h3:to_geo(TargetLoc), h3:to_geo(ChallengerLoc)),
                                %% ok = file:write_file("/tmp/targets", io_lib:fwrite("~p: ~p.\n", [TargetName, TargetScore]), [append]),
                                %% synthesize this from the location
                                %% ok = file:write_file("/tmp/zones", io_lib:fwrite("PickedZoneIndex: ~p.\n", [PickedZoneIndex]), [append]),
                                %% ok = file:write_file("/tmp/target_details",
                                %%                      io_lib:fwrite("~p, ~p, ~p, ~p, ~p, ~p\n",
                                %%                                    [ChallengerName, ChallengerLoc, TargetName, TargetLoc, TargetScore, Dist]),
                                %%                      [append]),
                                maps:is_key(TargetPubkeyBin, ActiveGateways)
                        end,

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger)
                          end,
                          conjunction([{verify_target_found, Check}])
                         )

            end)).

gen_hash() ->
    binary(32).

gen_challenger_index() ->
    ?SUCHTHAT(S, int(), S < 3024 andalso S > 0).

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
            blockchain_ledger_v1:vars(maps:merge(default_vars(), targeting_vars()), [], Ledger1),
            blockchain:bootstrap_hexes(Ledger1),
            blockchain_ledger_v1:commit_context(Ledger1),
            Ledger
    end.

targeting_vars() ->
    #{poc_v4_target_prob_score_wt => 0.0,
      poc_v4_target_prob_edge_wt => 0.0,
      poc_v5_target_prob_randomness_wt => 1.0,
      poc_v4_target_challenge_age => 300,
      poc_v4_target_exclusion_cells => 6000,
      poc_v4_target_score_curve => 2,
      poc_target_hex_parent_res => 5
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
