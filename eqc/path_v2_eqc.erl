-module(path_v2_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_path_check/0]).

prop_path_check() ->
    ?FORALL({Hash, ChallengerIndex, PathLimit},
            {gen_hash(), gen_challenger_index(), gen_path_limit()},
            begin
                Ledger = ledger(),
                {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
                ActiveGateways = filter_gateways(blockchain_ledger_v1:active_gateways(Ledger), Height),
                Challenger = lists:nth(ChallengerIndex, maps:keys(ActiveGateways)),
                {ok, TargetPubkeyBin} = blockchain_poc_target_v2:target(Hash, Ledger, Challenger),
                {_Time, Path} = timer:tc(fun() -> blockchain_poc_path_v2:build(TargetPubkeyBin,
                                                                               ActiveGateways,
                                                                               block_time(),
                                                                               Hash,
                                                                               PathLimit,
                                                                               #{})
                                         end),
                blockchain_ledger_v1:close(Ledger),

                PathLength = length(Path),

                B58Path = #{libp2p_crypto:bin_to_b58(TargetPubkeyBin) => [[libp2p_crypto:bin_to_b58(P) || P <- Path]]},

                case length(Path) > 1 of
                    true ->
                        file:write_file("/tmp/paths", io_lib:fwrite("~p.\n", [B58Path]), [append]);
                    false ->
                        ok
                end,


                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger),
                              io:format("Target: ~p~n", [TargetPubkeyBin]),
                              io:format("PathLimit: ~p~n", [PathLimit]),
                              io:format("Path: ~p~n", [Path])
                          end,
                          %% Checks:
                          %% - honor path limit
                          %% - atleast one element in path
                          %% - target is always in path
                          %% - we never go back to the same h3 index in path
                          %% - check next hop is an witness of previous gateway
                          conjunction([{verify_path_length, PathLength =< PathLimit andalso PathLength >= 1},
                                       {verify_path_uniqueness, length(Path) == length(lists:usort(Path))},
                                       {verify_target_membership, lists:member(TargetPubkeyBin, Path)},
                                       {verify_next_hops, check_next_hop(Path, ActiveGateways)}
                                      ]))

            end).

gen_path_limit() ->
    elements([3, 4, 5, 6, 7]).

gen_hash() ->
    binary(32).

gen_challenger_index() ->
    ?SUCHTHAT(S, int(), S < 440 andalso S > 0).

ledger() ->
    blockchain_ledger_v1:new("/var/data/test").

block_time() ->
    1571360971 * 1000000000.

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

filter_gateways(Gateways, Height) ->
    maps:filter(fun(_, Gateway) ->
                        case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
                            undefined ->
                                false;
                            C ->
                                (Height - C) < 90
                        end
                end,
                Gateways).
