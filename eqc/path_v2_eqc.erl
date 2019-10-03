-module(path_v2_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_path_check/0]).

prop_path_check() ->
    ?FORALL({Hash, ChallengerIndex, PathLimit},
            {gen_hash(), gen_challenger_index(), gen_path_limit()},
            begin
                Ledger = ledger(),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),
                {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
                ActiveGateways = filter_gateways(blockchain_ledger_v1:active_gateways(Ledger), Height),
                Vars = #{poc_path_limit => PathLimit},

                {Challenger, ChallengerLoc} = find_challenger(ChallengerIndex, ActiveGateways),

                GatewayScoreMap = maps:map(fun(Addr, Gateway) ->
                                                    {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, Gateway, Height, Ledger),
                                                    {Gateway, Score}
                                            end,
                                            ActiveGateways),

                GatewayScores = blockchain_poc_target_v2:filter(GatewayScoreMap, Challenger, ChallengerLoc, Height, Vars),

                {ok, TargetPubkeyBin} = blockchain_poc_target_v2:target(Hash, GatewayScores, Vars),
                {Time, Path} = timer:tc(fun() ->
                                                blockchain_poc_path_v2:build(TargetPubkeyBin,
                                                                                ActiveGateways,
                                                                                block_time(),
                                                                                Hash,
                                                                                Vars)
                                        end),

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),

                PathLength = length(Path),

                B58Path = #{libp2p_crypto:bin_to_b58(TargetPubkeyBin) => [[libp2p_crypto:bin_to_b58(P) || P <- Path]]},
                HumanPath = [name(P) || P <- Path],

                case length(Path) > 1 of
                    true ->
                        ok = file:write_file("/tmp/paths_js", io_lib:fwrite("~p.\n", [B58Path]), [append]),
                        ok = file:write_file("/tmp/paths_target", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append]);
                    false ->
                        ok = file:write_file("/tmp/paths_beacon", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append])
                end,


                ?WHENFAIL(begin
                                blockchain_ledger_v1:close(Ledger),
                                io:format("Target: ~p~n", [TargetPubkeyBin]),
                                io:format("PathLimit: ~p~n", [PathLimit]),
                                io:format("Time: ~p, Path: ~p~n", [Time, HumanPath])
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

block_time() ->
    %% block time at height 105719
    1573151628 * 1000000000.

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

name(PubkeyBin) ->
    {ok, Name} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(PubkeyBin)),
    Name.

find_challenger(ChallengerIndex, ActiveGateways) ->
    find_challenger(ChallengerIndex, ActiveGateways, 0).

find_challenger(ChallengerIndex, ActiveGateways, Iteration) ->
    Idx = case abs(ChallengerIndex + Iteration) rem maps:size(ActiveGateways) of
              0 -> maps:size(ActiveGateways);
              N -> N
          end,
    Challenger = lists:nth(Idx, maps:keys(ActiveGateways)),
    case blockchain_ledger_gateway_v2:location(maps:get(Challenger, ActiveGateways)) of
        undefined ->
            find_challenger(ChallengerIndex, ActiveGateways, next_iteration(Iteration));
        ChallengerLoc ->
            {Challenger, ChallengerLoc}
    end.

next_iteration(0) -> 1;
next_iteration(N) when N > 0 ->
    N * -1;
next_iteration(N) ->
    (N * -1) + 1.
