-module(path_v2_target_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_target_check/0]).

prop_target_check() ->
    ?FORALL({Hash, ChallengerIndex}, {gen_hash(), gen_challenger_index()},
            begin
                Ledger = ledger(),
                ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
                Challenger = lists:nth(ChallengerIndex, maps:keys(ActiveGateways)),
                {ok, TargetPubkeyBin} = blockchain_poc_target_v2:target(Hash, Ledger, Challenger),

                {ok, TargetName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(TargetPubkeyBin)),
                {ok, TargetScore} = blockchain_ledger_v1:gateway_score(TargetPubkeyBin, Ledger),
                ok = file:write_file("/tmp/targets", io_lib:fwrite("~p: ~p.\n", [TargetName, TargetScore]), [append]),

                blockchain_ledger_v1:close(Ledger),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger),
                              io:format("TargetPubkeyBin: ~p~n", [TargetPubkeyBin])
                          end,
                          conjunction([{verify_target_found, maps:is_key(TargetPubkeyBin, ActiveGateways)}])
                         )
            end).

gen_hash() ->
    binary(32).

gen_challenger_index() ->
    ?SUCHTHAT(S, int(), S < 883 andalso S > 0).

ledger() ->
    %% Ledger at height: 101523
    %% ActiveGateway Count: 883
    {ok, Dir} = file:get_cwd(),
    PrivDir = filename:join([Dir, "priv"]),
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    ok = erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}]),
    blockchain_ledger_v1:new(PrivDir).
