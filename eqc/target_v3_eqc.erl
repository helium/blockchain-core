-module(target_v3_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_target_check/0]).

prop_target_check() ->
    ?FORALL({Hash, ChallengerIndex}, {gen_hash(), gen_challenger_index()},
            begin
                Ledger = ledger(),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
                Challenger = lists:nth(ChallengerIndex, maps:keys(ActiveGateways)),
                Vars = #{},
                Check = case blockchain_ledger_gateway_v2:location(maps:get(Challenger, ActiveGateways)) of
                            undefined ->
                                true;
                            ChallengerLoc ->
                                GatewayScoreMap = blockchain_poc_zone_v2:pick(Hash, Ledger, Vars),

                                case blockchain_poc_target_v2:filter(GatewayScoreMap, Challenger, ChallengerLoc, Height, Vars) of
                                    M when map_size(M) == 0 ->
                                        %% NOTE: Not a single gateway in the zone satisfies target filter criteria,
                                        %% just say it's okay for now. But when doing poc_requests, we should
                                        %% check and re-pick a new zone probably.
                                        true;
                                    GatewayScores ->
                                        {Time, {ok, TargetPubkeyBin}} = timer:tc(fun() ->
                                                                                         blockchain_poc_target_v2:target(Hash, GatewayScores, Vars)
                                                                                 end),
                                        io:format("Time: ~p\t Target: ~p~n",
                                                  [Time, element(2,
                                                                 erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(TargetPubkeyBin)))
                                                  ]),

                                        {ok, TargetName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(TargetPubkeyBin)),
                                        {ok, ChallengerName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(Challenger)),
                                        {ok, TargetScore} = blockchain_ledger_v1:gateway_score(TargetPubkeyBin, Ledger),
                                        TargetLoc = blockchain_ledger_gateway_v2:location(maps:get(TargetPubkeyBin, ActiveGateways)),
                                        {ok, Dist} = vincenty:distance(h3:to_geo(TargetLoc), h3:to_geo(ChallengerLoc)),
                                        ok = file:write_file("/tmp/targets", io_lib:fwrite("~p: ~p.\n", [TargetName, TargetScore]), [append]),
                                        ok = file:write_file("/tmp/challenger_targets",
                                                             io_lib:fwrite("~p, ~p, ~p, ~p, ~p, ~p.\n",
                                                                           [ChallengerName, ChallengerLoc, TargetName, TargetLoc, TargetScore, Dist]),
                                                             [append]),
                                        maps:is_key(TargetPubkeyBin, ActiveGateways)
                                end
                        end,

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger)
                          end,
                          conjunction([{verify_target_found, Check}])
                         )

            end).

gen_hash() ->
    binary(32).

gen_challenger_index() ->
    ?SUCHTHAT(S, int(), S < 1088 andalso S > 0).

ledger() ->
    %% Ledger at height: 105719
    %% ActiveGateway Count: 1087
    {ok, Dir} = file:get_cwd(),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    %% Path to static ledger tar
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    case filelib:is_file(LedgerTar) of
        true ->
            %% ledger tar file present, extract
            ok = erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}]);
        false ->
            %% ledger tar file not found, download & extract
            ok = ssl:start(),
            {ok, {{_, 200, "OK"}, _, Body}} = httpc:request("https://blockchain-core.s3-us-west-1.amazonaws.com/ledger.tar.gz"),
            ok = file:write_file(filename:join([PrivDir, "ledger.tar.gz"]), Body),
            ok = erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
    end,
    blockchain_ledger_v1:new(PrivDir).
