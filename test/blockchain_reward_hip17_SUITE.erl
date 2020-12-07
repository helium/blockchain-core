-module(blockchain_reward_hip17_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain_vars.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    no_var_test/1,
    with_hip15_vars_test/1,
    with_hip17_vars_test/1,
    comparison_test/1
]).

all() ->
    [
        no_var_test,
        with_hip15_vars_test,
        with_hip17_vars_test,
        comparison_test
    ].

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, StorePid} = blockchain_test_reward_store:start(),
    [{store, StorePid} | Config].

%%--------------------------------------------------------------------
%% TEST SUITE TEARDOWN
%%--------------------------------------------------------------------

end_per_suite(_Config) ->
    blockchain_test_reward_store:stop(),
    ok.

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),
    ok = application:set_env(blockchain, hip17_test_mode, true),

    ExtraVars =
        case TestCase of
            with_hip15_vars_test ->
                hip15_vars();
            with_hip17_vars_test ->
                maps:merge(hip15_vars(), hip17_vars());
            _ ->
                #{}
        end,


    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
    test_utils:init_chain_with_fixed_locations(Balance, {PrivKey, PubKey}, true, known_locations(), ExtraVars),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),

    %% Add hexes to the ledger
    LedgerC = blockchain_ledger_v1:new_context(Ledger),
    ok = blockchain:bootstrap_hexes(LedgerC),
    ok = blockchain_ledger_v1:bootstrap_h3dex(LedgerC),
    ok = blockchain_ledger_v1:commit_context(LedgerC),
    ok = blockchain_ledger_v1:compact(Ledger),

    Entries = blockchain_ledger_v1:entries(Ledger),
    _ = lists:foreach(
        fun(Entry) ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            0 = blockchain_ledger_entry_v1:nonce(Entry)
        end,
        maps:values(Entries)
    ),

    meck:new(blockchain_txn_rewards_v1, [passthrough]),
    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:new(blockchain_hex, [passthrough]),

    [
        {balance, Balance},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        {tc_name, TestCase},
        Keys
        | Config0
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_TestCase, Config) ->
    meck:unload(blockchain_txn_rewards_v1),
    meck:unload(blockchain_txn_poc_receipts_v1),
    meck:unload(blockchain_hex),
    meck:unload(),
    Sup = ?config(sup, Config),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

no_var_test(Config) ->
    Witnesses = [b, c, e, f, g],
    run_test(Witnesses, Config).

with_hip15_vars_test(Config) ->
    %% - We have gateways: [a, b, c, d, e, f, g, h, i, j, k]
    %% - We'll make a poc receipt txn by hand, without any validation
    %% - We'll also consider that all witnesses are legit (legit_witnesses)
    %% - The poc transaction will have path like so: a -> d -> h
    %% - For a -> d; [b, c, e, f, g] will all be witnesses, their rewards should get scaled
    %% - For d -> h; 0 witnesses
    Witnesses = [b, c, e, f, g],
    run_test(Witnesses, Config).

with_hip17_vars_test(Config) ->
    %% - We have gateways: [a, b, c, d, e, f, g, h, i, j, k]
    %% - We'll make a poc receipt txn by hand, without any validation
    %% - We'll also consider that all witnesses are legit (legit_witnesses)
    %% - The poc transaction will have path like so: a -> d -> h
    %% - For a -> d; [b, c, e, f, g] will all be witnesses, their rewards should get scaled
    %% - For d -> h; 0 witnesses
    Witnesses = [b, c, e, f, g],
    run_test(Witnesses, Config).

comparison_test(_Config) ->
    %% Aggregate rewards from no_var_test
    NoVarChallengeeRewards = blockchain_test_reward_store:fetch(no_var_test_challengee_rewards),
    NoVarWitnessRewards = blockchain_test_reward_store:fetch(no_var_test_witness_rewards),

    TotalNoVarChallengeeRewards = lists:sum(maps:values(NoVarChallengeeRewards)),
    TotalNoVarWitnessRewards = lists:sum(maps:values(NoVarWitnessRewards)),

    FractionalNoVarChallengeeRewards = maps:map(fun(_, V) -> V / TotalNoVarChallengeeRewards end, NoVarChallengeeRewards),
    FractionalNoVarWitnessRewards = maps:map(fun(_, V) -> V / TotalNoVarWitnessRewards end, NoVarWitnessRewards),

    %% Aggregate rewards from hip15 test
    Hip15ChallengeeRewards = blockchain_test_reward_store:fetch(with_hip15_vars_test_challengee_rewards),
    Hip15WitnessRewards = blockchain_test_reward_store:fetch(with_hip15_vars_test_witness_rewards),

    TotalHip15ChallengeeRewards = lists:sum(maps:values(Hip15ChallengeeRewards)),
    TotalHip15WitnessRewards = lists:sum(maps:values(Hip15WitnessRewards)),

    FractionalHip15ChallengeeRewards = maps:map(fun(_, V) -> V / TotalHip15ChallengeeRewards end, Hip15ChallengeeRewards),
    FractionalHip15WitnessRewards = maps:map(fun(_, V) -> V / TotalHip15WitnessRewards end, Hip15WitnessRewards),

    %% Aggregate rewards from hip17 test
    Hip17ChallengeeRewards = blockchain_test_reward_store:fetch(with_hip17_vars_test_challengee_rewards),
    Hip17WitnessRewards = blockchain_test_reward_store:fetch(with_hip17_vars_test_witness_rewards),

    TotalHip17ChallengeeRewards = lists:sum(maps:values(Hip17ChallengeeRewards)),
    TotalHip17WitnessRewards = lists:sum(maps:values(Hip17WitnessRewards)),

    FractionalHip17ChallengeeRewards = maps:map(fun(_, V) -> V / TotalHip17ChallengeeRewards end, Hip17ChallengeeRewards),
    FractionalHip17WitnessRewards = maps:map(fun(_, V) -> V / TotalHip17WitnessRewards end, Hip17WitnessRewards),

    ct:pal("NoVarChallengeeRewards: ~p", [NoVarChallengeeRewards]),
    ct:pal("FractionalNoVarChallengeeRewards: ~p", [FractionalNoVarChallengeeRewards]),

    ct:pal("NoVarWitnessRewards: ~p", [NoVarWitnessRewards]),
    ct:pal("FractionalNoVarWitnessRewards: ~p", [FractionalNoVarWitnessRewards]),

    ct:pal("Hip15ChallengeeRewards: ~p", [Hip15ChallengeeRewards]),
    ct:pal("FractionalHip15ChallengeeRewards: ~p", [FractionalHip15ChallengeeRewards]),

    ct:pal("Hip15WitnessRewards: ~p", [Hip15WitnessRewards]),
    ct:pal("FractionalHip15WitnessRewards: ~p", [FractionalHip15WitnessRewards]),

    ct:pal("Hip17ChallengeeRewards: ~p", [Hip17ChallengeeRewards]),
    ct:pal("FractionalHip17ChallengeeRewards: ~p", [FractionalHip17ChallengeeRewards]),

    ct:pal("Hip17WitnessRewards: ~p", [Hip17WitnessRewards]),
    ct:pal("FractionalHip17WitnessRewards: ~p", [FractionalHip17WitnessRewards]),

    %% TODO: Actually assert some invariant and compare the three results

    ok.

%%--------------------------------------------------------------------
%% HELPER
%%--------------------------------------------------------------------

run_test(Witnesses, Config) ->
    ct:pal("Config: ~p", [Config]),
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),
    TCName = ?config(tc_name, Config),
    Store = ?config(store, Config),
    ct:pal("store: ~p", [Store]),

    Ledger = blockchain:ledger(Chain),
    Vars = blockchain_ledger_v1:snapshot_vars(Ledger),
    ct:pal("Vars: ~p", [Vars]),

    AG = blockchain_ledger_v1:active_gateways(Ledger),

    GatewayAddrs = lists:sort(maps:keys(AG)),

    AllGws = [a, b, c, d, e, f, g, h, i, j, k],

    %% For crosscheck
    GatewayNameMap = lists:foldl(
        fun({Letter, A}, Acc) ->
            maps:put(blockchain_utils:addr2name(A), Letter, Acc)
        end,
        #{},
        lists:zip(AllGws, GatewayAddrs)
    ),

    %% For crosscheck
    GatewayLocMap = lists:foldl(
        fun(A, Acc) ->
            {ok, Gw} = blockchain_ledger_v1:find_gateway_info(A, Ledger),
            GwLoc = blockchain_ledger_gateway_v2:location(Gw),
            maps:put(blockchain_utils:addr2name(A), GwLoc, Acc)
        end,
        #{},
        GatewayAddrs
    ),

    %% For crosscheck
    GatewayLetterToAddrMap = lists:foldl(
        fun({Letter, A}, Acc) ->
            maps:put(Letter, A, Acc)
        end,
        #{},
        lists:zip(AllGws, GatewayAddrs)
    ),

    %% For crosscheck
    GatewayLetterLocMap = lists:foldl(
        fun({Letter, A}, Acc) ->
            {ok, Gw} = blockchain_ledger_v1:find_gateway_info(A, Ledger),
            GwLoc = blockchain_ledger_gateway_v2:location(Gw),
            maps:put(Letter, GwLoc, Acc)
        end,
        #{},
        lists:zip(AllGws, GatewayAddrs)
    ),

    Challenger = maps:get(k, GatewayLetterToAddrMap),

    GwA = maps:get(a, GatewayLetterToAddrMap),
    GwD = maps:get(d, GatewayLetterToAddrMap),
    GwH = maps:get(h, GatewayLetterToAddrMap),

    Rx1 = blockchain_poc_receipt_v1:new(
        GwA,
        1000,
        10,
        "first_rx",
        p2p
    ),
    Rx2 = blockchain_poc_receipt_v1:new(
        GwD,
        1000,
        10,
        "second_rx",
        radio
    ),
    Rx3 = blockchain_poc_receipt_v1:new(
        GwH,
        1000,
        10,
        "third_rx",
        radio
    ),

    ct:pal("Rx1: ~p", [Rx1]),
    ct:pal("Rx2: ~p", [Rx2]),
    ct:pal("Rx3: ~p", [Rx3]),

    ConstructedWitnesses = lists:foldl(
        fun(W, Acc) ->
            WitnessGw = maps:get(W, GatewayLetterToAddrMap),
            Witness = blockchain_poc_witness_v1:new(
                WitnessGw,
                1001,
                10,
                crypto:strong_rand_bytes(32),
                9.8,
                915.2,
                10,
                <<"data_rate">>
            ),
            [Witness | Acc]
        end,
        [],
        Witnesses
    ),

    ct:pal("ConstructedWitnesses: ~p", [ConstructedWitnesses]),

    %% We'll consider all the witnesses to be "good quality" for the sake of testing
    meck:expect(
        blockchain_txn_rewards_v1,
        legit_witnesses,
        fun(_, _, _, _, _, _) ->
            ConstructedWitnesses
        end
    ),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, valid_witnesses, fun(_, _, _) ->
        ConstructedWitnesses
    end),
    meck:expect(blockchain_txn_poc_receipts_v1, good_quality_witnesses, fun(_, _) ->
        ConstructedWitnesses
    end),
    meck:expect(blockchain_txn_poc_receipts_v1, get_channels, fun(_, _) ->
        {ok, lists:seq(1, 11)}
    end),
    meck:expect(blockchain_hex, destroy_memoization, fun() ->
        true
    end),

    P1 = blockchain_poc_path_element_v1:new(GwA, Rx1, []),
    P2 = blockchain_poc_path_element_v1:new(GwD, Rx2, ConstructedWitnesses),
    P3 = blockchain_poc_path_element_v1:new(GwH, Rx3, []),

    ct:pal("P1: ~p", [P1]),
    ct:pal("P2: ~p", [P2]),
    ct:pal("P3: ~p", [P3]),

    Txn = blockchain_txn_poc_receipts_v1:new(
        Challenger,
        <<"secret">>,
        <<"onion_key_hash">>,
        <<"block_hash">>,
        [P1, P2, P3]
    ),
    ct:pal("Txn: ~p", [Txn]),

    %% Construct a block for the poc receipt txn WITHOUT validation
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [Txn], #{}, false),
    ct:pal("Block2: ~p", [Block2]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    %% Empty block
    {ok, Block3} = test_utils:create_block(ConsensusMembers, []),
    ct:pal("Block3: ~p", [Block3]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:swarm()),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    %% Calculate rewards by hand
    Start = 1,
    End = 3,
    {ok, Rewards} = blockchain_txn_rewards_v1:calculate_rewards(Start, End, Chain),

    ChallengeesRewards = lists:filter(
        fun(R) ->
            blockchain_txn_reward_v1:type(R) == poc_challengees
        end,
        Rewards
    ),

    WitnessRewards = lists:filter(
        fun(R) ->
            blockchain_txn_reward_v1:type(R) == poc_witnesses
        end,
        Rewards
    ),

    ChallengeesRewardsMap =
        lists:foldl(
            fun(R, Acc) ->
                maps:put(
                    maps:get(
                        blockchain_utils:addr2name(blockchain_txn_reward_v1:gateway(R)),
                        GatewayNameMap
                    ),
                    blockchain_txn_reward_v1:amount(R),
                    Acc
                )
            end,
            #{},
            ChallengeesRewards
        ),

    WitnessRewardsMap =
        lists:foldl(
            fun(R, Acc) ->
                maps:put(
                    maps:get(
                        blockchain_utils:addr2name(blockchain_txn_reward_v1:gateway(R)),
                        GatewayNameMap
                    ),
                    blockchain_txn_reward_v1:amount(R),
                    Acc
                )
            end,
            #{},
            WitnessRewards
        ),

    %% Theoretically, gateways J, K should have higher witness rewards than B, C, E, F, G, I
    ct:pal("GatewayNameMap: ~p", [GatewayNameMap]),
    ct:pal("GatewayLocMap: ~p", [GatewayLocMap]),
    ct:pal("GatewayLetterLocMap: ~p", [GatewayLetterLocMap]),
    ct:pal("ChallengeesRewardsMap: ~p", [ChallengeesRewardsMap]),
    ct:pal("WitnessRewardsMap: ~p", [WitnessRewardsMap]),

    ok = blockchain_test_reward_store:insert(
        list_to_atom(atom_to_list(TCName) ++ "_witness_rewards"),
        WitnessRewardsMap
    ),
    ok = blockchain_test_reward_store:insert(
        list_to_atom(atom_to_list(TCName) ++ "_challengee_rewards"),
        ChallengeesRewardsMap
    ),

    ok.

hip15_vars() ->
    #{
        %% configured on chain
        ?poc_version => 9,
        ?reward_version => 5,
        %% new hip15 vars for testing
        ?poc_reward_decay_rate => 0.8,
        ?witness_redundancy => 4
    }.

hip17_vars() ->
    #{
        ?hip17_res_0 => <<"2,100000,100000">>,
        ?hip17_res_1 => <<"2,100000,100000">>,
        ?hip17_res_2 => <<"2,100000,100000">>,
        ?hip17_res_3 => <<"2,100000,100000">>,
        ?hip17_res_4 => <<"1,250,800">>,
        ?hip17_res_5 => <<"1,100,400">>,
        ?hip17_res_6 => <<"1,25,100">>,
        ?hip17_res_7 => <<"2,5,20">>,
        ?hip17_res_8 => <<"2,1,4">>,
        ?hip17_res_9 => <<"2,1,2">>,
        ?hip17_res_10 => <<"2,1,1">>,
        ?hip17_res_11 => <<"2,100000,100000">>,
        ?hip17_res_12 => <<"2,100000,100000">>,
        ?density_tgt_res => 8
    }.

known_locations() ->

    %% NameToPubkeyBin =
    %% #{
    %%   "rare-amethyst-reindeer" => libp2p_crypto:b58_to_bin("112VfPXs1WTRjp24WkbbjQmbFNhb5ptot7gpGwJULm4SRiacjuvW"),
    %%   "cold-canvas-duck" => libp2p_crypto:b58_to_bin("112ciDxDUBwJZs5YjjPWJWKGwGtUtdJdxSgDAYJPhu9fHw4sgeQy"),
    %%   "melted-tangelo-aphid" => libp2p_crypto:b58_to_bin("112eNuzPYSeeo3tqNDidr2gPysz7QtLkePkY5Yn1V7ddNPUDN6p5"),
    %%   "early-lime-rat" => libp2p_crypto:b58_to_bin("112Xr4ZtiNbeh8wfiWTYfeo7KwBbXwvx5F2LPdNTC8wp8q4EQCAm"),
    %%   "flat-lilac-shrimp" => libp2p_crypto:b58_to_bin("112euXBKmLzUAfyi7FaYRxRpcH5RmfPKprV3qEyHCTt8nqwyVFYo"),
    %%   "harsh-sandstone-stork" => libp2p_crypto:b58_to_bin("11UFysjjP9W8S7ZV54iK7L6HpkxkHrSPRm4rKkWq22cStYYhDhM"),
    %%   "pet-pewter-lobster" => libp2p_crypto:b58_to_bin("112LYrRkJX32jVNsuAzt9kDqrddXqWrwpG8N5QX2hELvzf8JJZbw"),
    %%   "amateur-tan-monkey" => libp2p_crypto:b58_to_bin("112bQKSN3TiaYMrsjNKGZotd14QPi7DB37FeV88rmVMgP4MgTK9q"),
    %%   "clean-wooden-zebra" => libp2p_crypto:b58_to_bin("112AT5baYcYG6yHchYa9xnkqNJ4cXbgxCh8i8nvnfZeewAHJ8zKc"),
    %%   "odd-champagne-nuthatch" => libp2p_crypto:b58_to_bin("112fDV4b5FqSgcnu3F592RauuNo5HkuPzfETM7WJ9AfCdaCo9sLk"),
    %%   "abundant-grape-butterfly" => libp2p_crypto:b58_to_bin("112pdh3waHFbu3XqtCWwbw9xEtYtUEvbqzgSVbEoENBRQznj9Tuy")
    %%  },

    %% Locs =
    %% #{
    %%   "rare-amethyst-reindeer" => 631786582666296319,
    %%   "cold-canvas-duck" => 631786582410363903,
    %%   "melted-tangelo-aphid" => 631786582655116287,
    %%   "early-lime-rat" => 631786582659491327,
    %%   "flat-lilac-shrimp" => 631786581941828607,
    %%   "harsh-sandstone-stork" => 631786581946850303,
    %%   "pet-pewter-lobster" => 631786581906280959,
    %%   "amateur-tan-monkey" => 631786581937244159,
    %%   "clean-wooden-zebra" => 631786581846989823,
    %%   "odd-champagne-nuthatch" => 631786581944091647,
    %%   "abundant-grape-butterfly" => 631786582694056959
    %%  },

    %% [631786581906280959,
    %%  631786582666296319,
    %%  631786582410363903,
    %%  631786582694056959,
    %%  631786582655116287,
    %%  631786582659491327,
    %%  631786581941828607,
    %%  631786581937244159,
    %%  631786581946850303,
    %%  631786581846989823,
    %%  631786581944091647],


    [631211351866199551,
     631211351866199551,
     631211351866084351,
     631211351866223615,
     631211351866300415,
     631211351866239999,
     631211351866165759,
     631211351866165247,
     631211351866289663,
     631211351865407487,
     631211351865991679].
