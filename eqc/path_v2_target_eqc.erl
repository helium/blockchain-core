-module(path_v2_target_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_target_check/0]).

prop_target_check() ->
    ?FORALL({Hash, ChallengerIndex}, {gen_hash(), gen_challenger_index()},
            begin
                Ledger = ledger(),
                application:set_env(blockchain, test, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
                Challenger = lists:nth(ChallengerIndex, maps:keys(ActiveGateways)),

                GatewayScoreMap = maps:map(fun(Addr, Gateway) ->
                                                   {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, Gateway, Height, Ledger),
                                                   {Score, Gateway}
                                           end,
                                           ActiveGateways),

                GatewayScores = blockchain_poc_target_v2:filter(GatewayScoreMap, Challenger, Height, #{}),

                {Time, {ok, TargetPubkeyBin}} = timer:tc(fun() ->
                                                                 blockchain_poc_target_v2:target(Hash, GatewayScores)
                                                         end),
                %% io:format("Target: ~p, Time: ~p~n", [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(TargetPubkeyBin))), Time]),

                {ok, TargetName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(TargetPubkeyBin)),
                {ok, TargetScore} = blockchain_ledger_v1:gateway_score(TargetPubkeyBin, Ledger),
                ok = file:write_file("/tmp/targets", io_lib:fwrite("~p: ~p.\n", [TargetName, TargetScore]), [append]),

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger),
                              io:format("TargetPubkeyBin: ~p~n", [TargetPubkeyBin]),
                              io:format("Time: ~p~n", [Time])
                          end,
                          conjunction([{verify_target_found, maps:is_key(TargetPubkeyBin, ActiveGateways)}])
                         )
            end).

gen_hash() ->
    binary(32).

gen_challenger_index() ->
    ?SUCHTHAT(S, int(), S < 970 andalso S > 0).

ledger() ->
    %% Ledger at height: 104196
    %% ActiveGateway Count: 969
    {ok, Dir} = file:get_cwd(),
    PrivDir = filename:join([Dir, "priv"]),
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    ok = erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}]),
    blockchain_ledger_v1:new(PrivDir).
