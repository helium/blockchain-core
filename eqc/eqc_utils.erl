-module(eqc_utils).

-export([find_challenger/2,
         current_vars/0,
         dead_hotspots/0,
         ledger/1,
         name/1
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
    %% Ledger at height: 194196
    %% ActiveGateway Count: 3023
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
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    %% Ensure the ledger has the vars we're testing against
    blockchain_ledger_v1:vars(maps:merge(current_vars(), ExtraVars), [], Ledger1),
    blockchain:bootstrap_hexes(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    Ledger.

current_vars() ->
    #{poc_challenge_sync_interval => 90,
      poc_target_hex_parent_res => 5,election_selection_pct => 20,
      poc_v4_target_prob_score_wt => 0.0,
      poc_v4_target_score_curve => 5,poc_challenge_interval => 60,
      poc_typo_fixes => true,poc_v4_prob_count_wt => 0.2,
      dc_percent => 0,poc_path_limit => 7,
      num_consensus_members => 16,consensus_percent => 0.1,
      h3_max_grid_distance => 120,poc_v4_prob_good_rssi => 1.0,
      poc_v4_target_prob_edge_wt => 0.0,alpha_decay => 0.0035,
      poc_v4_target_exclusion_cells => 6000,election_version => 2,
      predicate_callback_mod => miner,reward_version => 2,
      poc_challengees_percent => 0.35,
      poc_v4_prob_bad_rssi => 0.01,
      monthly_reward => 500000000000000,
      poc_witnesses_percent => 0.05,
      var_gw_inactivity_threshold => 600,max_staleness => 100000,
      min_score => 0.15,poc_v4_exclusion_cells => 8,
      vars_commit_delay => 1,chain_vars_version => 2,
      block_time => 60000,securities_percent => 0.35,
      beta_decay => 0.002,predicate_threshold => 0.95,
      block_version => v1,election_restart_interval => 5,
      batch_size => 2500,poc_v4_target_challenge_age => 300,
      poc_v4_prob_time_wt => 0.2,election_replacement_slope => 20,
      election_cluster_res => 8,election_removal_pct => 40,
      h3_neighbor_res => 12,poc_v4_randomness_wt => 0.4,
      election_interval => 30,
      poc_v5_target_prob_randomness_wt => 1.0,
      election_replacement_factor => 4,
      h3_exclusion_ring_dist => 6,poc_challengers_percent => 0.15,
      poc_v4_parent_res => 11,min_assert_h3_res => 12,
      poc_v4_prob_no_rssi => 0.5,poc_version => 7,
      dkg_curve => 'SS512',poc_v4_prob_rssi_wt => 0.2,
      predicate_callback_fun => version}.

