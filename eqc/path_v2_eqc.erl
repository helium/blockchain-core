-module(path_v2_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_path_check/0]).

prop_path_check() ->
    ?FORALL({TargetPubkeyBin, Entropy, PathLimit},
            {gen_target(), gen_entropy(), gen_path_limit()},
            begin
                ActiveGateways = active_gateways(),
                Path = blockchain_poc_path_v2:build(TargetPubkeyBin,
                                                    ActiveGateways,
                                                    block_time(),
                                                    Entropy,
                                                    PathLimit,
                                                    #{}),
                PathLength = length(Path),

                B58Path = #{libp2p_crypto:bin_to_b58(TargetPubkeyBin) => [[libp2p_crypto:bin_to_b58(P) || P <- Path]]},

                case length(Path) > 1 of
                    true ->
                        file:write_file("/tmp/paths", io_lib:fwrite("~p.\n", [B58Path]), [append]);
                    false ->
                        ok
                end,


                ?WHENFAIL(begin
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

gen_entropy() ->
    binary(32).

gen_target() ->
    elements(maps:keys(active_gateways())).

gen_path_limit() ->
    elements([3, 4, 5, 6, 7]).

active_gateways() ->
    {ok, Dir} = file:get_cwd(),
    {ok, [AG]} = file:consult(filename:join([Dir,  "eqc", "active88127"])),
    AG.

block_time() ->
    1571266381 * 1000000000.

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
