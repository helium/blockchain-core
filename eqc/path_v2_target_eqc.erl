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
                blockchain_ledger_v1:close(Ledger),

                ?WHENFAIL(begin
                              io:format("TargetPubkeyBin: ~p~n", [TargetPubkeyBin])
                          end,
                          conjunction([{verify_target_found, maps:is_key(TargetPubkeyBin, ActiveGateways)}])
                         )
            end).

gen_hash() ->
    binary(32).

gen_challenger_index() ->
    ?SUCHTHAT(S, int(), S < 440 andalso S > 0).

ledger() ->
    {ok, BaseDir} = file:get_cwd(),
    blockchain_ledger_v1:new(filename:join(BaseDir, "eqc")).
