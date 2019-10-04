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
                                                    PathLimit),
                PathLength = length(Path),
                Check = PathLength =< PathLimit andalso PathLength >= 1,
                ?WHENFAIL(begin
                              io:format("Target: ~p~n", [TargetPubkeyBin]),
                              io:format("PathLimit: ~p~n", [PathLimit]),
                              io:format("Path: ~p~n", [Path])
                          end,
                          conjunction([{verify_path, Check}]))

            end).

gen_entropy() ->
    binary(32).

gen_target() ->
    elements(maps:keys(active_gateways())).

gen_path_limit() ->
    elements([3, 4, 5, 6, 7]).

active_gateways() ->
    {ok, Dir} = file:get_cwd(),
    {ok, [AG]} = file:consult(filename:join([Dir,  "eqc", "active41829.txt"])),
    AG.

block_time() ->
    1568081668 * 1000000000.
