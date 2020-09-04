-module(eqc_utils).

-export([find_challenger/2,
         dead_hotspots/0,
         ledger/1,
         name/1,
         maybe_output_paths/3,
         ledger_vars/1,
         big_witness_hotspots/0
        ]).

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

dead_hotspots() ->
    DeadHotspots = [
                    %% magic-carob-quail
                    "11DMUS9sEDngNh5RF1k4pb23Ucbb9fN9RwPDd1ZMAJJiCjvo3Xx",
                    %% delightful-stone-beetle
                    "11idAPnBHP3AM3SjkpFRGy7XLVv12oecTFLHGBMfmBSvzNY9CW8",
                    %% bitter-bronze-okapi
                    "11CAwto42LXquADz1asH4gWU6yUbSi1hATDijYm8ZM4Z7RtzeQK"
                   ],

    lists:map(fun(B58Addr) -> libp2p_crypto:b58_to_bin(B58Addr) end, DeadHotspots).

name(PubkeyBin) ->
    {ok, Name} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(PubkeyBin)),
    Name.

ledger(ExtraVars) ->
    %% Ledger at height: 481929
    %% ActiveGateway Count: 8000
    {ok, Dir} = file:get_cwd(),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    %% Path to static ledger tar
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    %% Extract ledger tar if required
    ok = extract_ledger_tar(PrivDir, LedgerTar),
    %% Get the ledger
    Ledger = blockchain_ledger_v1:new(PrivDir),
    %% Get current ledger vars
    LedgerVars = ledger_vars(Ledger),
    %% Ensure the ledger has the vars we're testing against
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    blockchain_ledger_v1:vars(maps:merge(LedgerVars, ExtraVars), [], Ledger1),
    %% If the hexes aren't on the ledger add them
    blockchain:bootstrap_hexes(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    Ledger.

ledger_vars(Ledger) ->
    blockchain_utils:vars_binary_keys_to_atoms(maps:from_list(blockchain_ledger_v1:snapshot_vars(Ledger))).

big_witness_hotspots() ->
    {ok, Dir} = file:get_cwd(),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    BigWitnessHotspotFile = filename:join([PrivDir, "big_witness_hotspots"]),
    {ok, LBin} = file:read_file(BigWitnessHotspotFile),
    binary_to_term(LBin).

extract_ledger_tar(PrivDir, LedgerTar) ->
    case filelib:is_file(LedgerTar) of
        true ->
            %% if we have already unpacked it, no need to do it again
            LedgerDB = filename:join([PrivDir, "ledger.db"]),
            case filelib:is_dir(LedgerDB) of
                true ->
                    ok;
                false ->
                    %% ledger tar file present, extract
                    erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
            end;
        false ->
            %% ledger tar file not found, download & extract
            ok = ssl:start(),
            {ok, {{_, 200, "OK"}, _, Body}} = httpc:request("https://blockchain-core.s3-us-west-1.amazonaws.com/ledger-481929.tar.gz"),
            ok = file:write_file(filename:join([PrivDir, "ledger.tar.gz"]), Body),
            erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
    end.

maybe_output_paths(TargetPubkeyBin, Path, Time) ->
    %% export EQC_DEBUG=1 when running eqc to output paths
    case os:getenv("EQC_DEBUG") of
        false ->
            ok;
        "1" ->
            B58Path = #{libp2p_crypto:bin_to_b58(TargetPubkeyBin) => [[libp2p_crypto:bin_to_b58(P) || P <- Path]]},
            HumanFullPath = #{name(TargetPubkeyBin) => [[name(P) || P <- Path]]},
            HumanPath = [name(P) || P <- Path],
            io:format("Time: ~p\t Path: ~p~n", [erlang:convert_time_unit(Time, microsecond, millisecond), HumanPath]),

            case length(Path) > 1 of
                true ->
                    ok = file:write_file("/tmp/paths_js", io_lib:fwrite("~p.\n", [B58Path]), [append]),
                    ok = file:write_file("/tmp/paths_name_js", io_lib:fwrite("~p.\n", [HumanFullPath]), [append]),
                    ok = file:write_file("/tmp/paths_target", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append]);
                false ->
                    ok = file:write_file("/tmp/paths_beacon", io_lib:fwrite("~p: ~p.\n", [name(TargetPubkeyBin), HumanPath]), [append])
            end;
        _ ->
            ok
    end.
