-module(hex_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_hex_check/0]).

prop_hex_check() ->
    ?FORALL({Iterations, Hash}, {elements([10000]), binary(32)},
            begin
                Ledger = ledger(),
                application:set_env(blockchain, disable_score_cache, true),
                {ok, _Pid} = blockchain_score_cache:start_link(),

                %% Grab the list of parent hexes
                {ok, Hexes} = blockchain_ledger_v1:get_hexes(Ledger),
                HexList = maps:to_list(Hexes),
                %% ok = file:write_file("/tmp/hexes", io_lib:fwrite("~p\n", [HexList])),
                Population = lists:keysort(1, HexList),

                %% Use entropy to generate randval for running iterations
                Entropy = blockchain_utils:rand_state(Hash),

                %% Need this to match counters against assumptions
                CumulativePopulationList = cdf(Population),
                CumulativePopulationMap = maps:from_list(CumulativePopulationList),

                %% Intiial acc for the counter, each node starts with a 0 count
                InitAcc = maps:map(fun(_, _) -> 0 end, CumulativePopulationMap),

                %% Fname = "/tmp/zones_" ++ libp2p_crypto:bin_to_b58(Hash),

                %% Track all counts a node gets picked
                {Counter, _} = lists:foldl(fun(_I, {Acc, AccEntropy}) ->
                                              {RandVal, NewEntropy} = rand:uniform_s(AccEntropy),
                                              {ok, Node} = blockchain_utils:icdf_select(Population, RandVal),
                                              %% ok = file:write_file(Fname, io_lib:fwrite("~p\n", [Node]), [append]),
                                              {maps:update_with(Node, fun(X) -> X + 1 end, 1, Acc), NewEntropy}
                                      end,
                                      {InitAcc, Entropy},
                                      lists:seq(1, Iterations)),

                %% Check that it's roughly equal or more appropriately within some threshold (0.1 is good enough, probably).
                %% Fucking probabilities.
                CheckCounterLinesUp = lists:all(fun({Node, Count}) ->
                                                        abs(Count/Iterations - maps:get(Node, CumulativePopulationMap)) < 0.1
                                                end,
                                                maps:to_list(Counter)),

                blockchain_ledger_v1:close(Ledger),
                blockchain_score_cache:stop(),

                ?WHENFAIL(begin
                              blockchain_ledger_v1:close(Ledger),
                              io:format("Population: ~p~n", [Population]),
                              io:format("CDF: ~p~n", [CumulativePopulationList]),
                              io:format("Counter: ~p~n", [Counter])
                          end,
                          noshrink(conjunction(
                                     [{verify_population_exists, length(Population) > 0},
                                      {verify_unique_nodes, length(Population) == length(lists:usort(Population))},
                                      {verify_cdf, lists:sum([W || {_, W} <- CumulativePopulationList]) >= 0.99}, %% it's pretty much 1.0 but damn floats
                                      {verify_counts_line_up, CheckCounterLinesUp}
                                     ]
                                    )
                                  )
                         )
            end).

cdf(PopulationList) ->
    %% This takes the population and coverts it to a cumulative distribution.
    Sum = lists:sum([Weight || {_Node, Weight} <- PopulationList]),
    [{Node, blockchain_utils:normalize_float(Weight/Sum)} || {Node, Weight} <- PopulationList].

ledger() ->
    %% Ledger at height: 194196
    %% ActiveGateway Count: 3024
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
