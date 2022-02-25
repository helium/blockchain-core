%%
%% This is primarily a static test
%%
%% For reference:
%% --------------------------------------------------------------------------------------------------
%% fake_name                    real_name                        letter     location            scale
%% --------------------------------------------------------------------------------------------------
%% tall-azure-whale             striped-umber-copperhead         e          631210990515645439  0.25
%% basic-seaweed-walrun         fresh-honey-lizard               b          631210990515609087  0.25
%% brief-saffron-cricket        round-fiery-platypus             k          631210990516667903  0.5
%% mini-eggshell-panther        quick-admiral-gecko              j          631210990528935935  0.2
%% silly-azure-chicken          curly-ivory-alligator            d          631210990528385535  0.2
%% daring-watermelon-sloth      skinny-amber-condor              c          631210990528546815  0.2
%% keen-ultraviolet-guppy       fantastic-blue-lemur             i          631210990529462783  0.2
%% sharp-green-swift            crazy-silver-platypus            a          631210990529337343  0.2
%% bitter-saffron-ferret        icy-paisley-lizard               g          631210990524024831  0.33
%% creamy-porcelain-poodled     able-mocha-rhino                 h          631210990524753919  0.33
%% blurry-raising-wolf          puny-bubblegum-shell             f          631210990525267455  0.33
%%
%% Essentially we will check:
%%
%% - test_between_vars:
%%   - Do the rewards follow: no_var > hip15_var >= hip17_var.
%%
%% - test_between_challengees:
%%   - Paths tested: [e, h, j], [k, h, j], [g, h, j]
%%
%%   - For path: [e, h, j]
%%      - e: 0.25, h: 0.33, ignore j
%%      - e's witnesses [a, b, c, f] should get scaled using 0.25
%%      - h's witnesses [i] should get scaled using 0.33
%%
%%   - For path: [k, h, j]
%%      - k: 0.5, h: 0.33, ignore j
%%      - k's witnesses [a, b, c, f] should get scaled using 0.5
%%      - h's witnesses [i] should get scaled using 0.33
%%
%%   - For path: [g, h, j]
%%      - g: 0.33, h: 0.33, ignore j
%%      - g's witnesses [a, b, c, f] should get scaled using 0.33
%%      - h's witnesses [i] should get scaled using 0.33
%%
%% - test between different beaconers
%%   - For each test there will be two beaconers
%%      - comparison between beacons in the same test:
%%          - j (0.2) and e (0.25) are beaconers:
%%              - j's witnesses should have less rewards than e's witnesses
%%          - k (0.5) and e (0.25) are beaconers
%%              - k's witnesses should have higher rewards than e's witnesses
%%          - g (0.33) and e (0.25) are beaconers
%%              - g's witnesses should have higher rewards than e's witnesses
%%
%%      - comparison between beacon tests
%%          - k's witnesses should have higher rewards than g's witnesses
%%          - j's witnesses should have lower rewards than g's witnesses
%%

-module(blockchain_reward_hip17_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain_vars.hrl").

-export([
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    no_var_test/1,
    only_hip15_vars_test/1,
    hip15_17_vars_test/1,
    comparison_test/1,
    path_ehj/1,
    path_khj/1,
    path_ghj/1,
    compare_challengee_test/1,
    beaconer_j/1,
    beaconer_k/1,
    beaconer_g/1,
    compare_beacon_test/1
]).

all_var_tests() ->
    [
        no_var_test,
        only_hip15_vars_test,
        hip15_17_vars_test,
        comparison_test
    ].

all_challengee_tests() ->
    [
        path_ehj,
        path_khj,
        path_ghj,
        compare_challengee_test
    ].

all_beacon_tests() ->
    [
        beaconer_j,
        beaconer_k,
        beaconer_g,
        compare_beacon_test
    ].

all() ->
    [{group, test_between_vars},
     {group, test_between_challengees},
     {group, test_beacon_only}
    ].

groups() ->
    [{test_between_vars,
      [],
      all_var_tests()
     },
     {test_between_challengees,
      [],
      all_challengee_tests()
     },
     {test_beacon_only,
      [],
      all_beacon_tests()
     }].

%%--------------------------------------------------------------------
%% TEST GROUP SETUP
%%--------------------------------------------------------------------
init_per_group(_, Config) ->
    {ok, StorePid} = blockchain_test_reward_store:start(),
    [{store, StorePid} | Config].

%%--------------------------------------------------------------------
%% TEST GROUP TEARDOWN
%%--------------------------------------------------------------------
end_per_group(_, _Config) ->
    ok = blockchain_test_reward_store:stop(),
    ok.

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------

init_per_suite(Config) ->

    GenesisKeys = static_keys(),
    GenesisMembers = [
        {
            libp2p_crypto:pubkey_to_bin(PubKey),
            {PubKey, PrivKey, libp2p_crypto:mk_sig_fun(PrivKey)}
        } || #{public := PubKey, secret := PrivKey} <- GenesisKeys
    ],
    Locations = known_locations(),

     [{genesis_members, GenesisMembers},
     {locations, Locations} | Config].

%%--------------------------------------------------------------------
%% TEST SUITE TEARDOWN
%%--------------------------------------------------------------------

end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),
    ok = application:set_env(blockchain, hip17_test_mode, true),
    application:ensure_all_started(lager),

    BothHip15And17Vars = maps:merge(hip15_vars(), hip17_vars()),

    ExtraVars =
        case TestCase of
            only_hip15_vars_test ->
                hip15_vars();
            hip15_17_vars_test ->
                BothHip15And17Vars;
            path_ehj ->
                BothHip15And17Vars;
            path_ghj ->
                BothHip15And17Vars;
            path_khj ->
                BothHip15And17Vars;
            beaconer_j ->
                BothHip15And17Vars;
            beaconer_g ->
                BothHip15And17Vars;
            beaconer_k ->
                BothHip15And17Vars;
            _ ->
                #{}
        end,

    GenesisMembers = ?config(genesis_members, Config),
    Locations = ?config(locations, Config),

    {ok, _GenesisBlock, ConsensusMembers, Keys} =
    test_utils:init_chain_with_fixed_locations(Balance, GenesisMembers, Locations, ExtraVars),

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
    meck:new(blockchain_txn_poc_request_v1, [passthrough]),

    case TestCase of
        hip15_17_vars_test ->
            lists:foreach(fun(Loc) ->
                                  Scale = blockchain_hex:scale(Loc, Ledger),
                                  ct:pal("Loc: ~p, Scale: ~p", [Loc, Scale])
                          end,
                          known_locations());
        _ ->
            ok
    end,

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
        {tc_name, TestCase},
        Keys
        | Config0
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_TestCase, Config) ->
    meck:unload(blockchain_txn_rewards_v1),
    meck:unload(blockchain_txn_poc_request_v1),
    meck:unload(blockchain_txn_poc_receipts_v1),
    meck:unload(),
    Sup = ?config(sup, Config),
    BaseDir = ?config(base_dir, Config),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    test_utils:cleanup_tmp_dir(BaseDir),
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

no_var_test(Config) ->
    Witnesses = [b, c, e, f, g],
    run_vars_test(Witnesses, Config).

only_hip15_vars_test(Config) ->
    %% - We have gateways: [a, b, c, d, e, f, g, h, i, j, k]
    %% - We'll make a poc receipt txn by hand, without any validation
    %% - We'll also consider that all witnesses are legit (legit_witnesses)
    %% - The poc transaction will have path like so: a -> d -> h
    %% - For a -> d; [b, c, e, f, g] will all be witnesses, their rewards should get scaled
    %% - For d -> h; 0 witnesses
    Witnesses = [b, c, e, f, g],
    run_vars_test(Witnesses, Config).

hip15_17_vars_test(Config) ->
    %% - We have gateways: [a, b, c, d, e, f, g, h, i, j, k]
    %% - We'll make a poc receipt txn by hand, without any validation
    %% - We'll also consider that all witnesses are legit (legit_witnesses)
    %% - The poc transaction will have path like so: a -> d -> h
    %% - For a -> d; [b, c, e, f, g] will all be witnesses, their rewards should get scaled
    %% - For d -> h; 0 witnesses
    Witnesses = [b, c, e, f, g],
    run_vars_test(Witnesses, Config).

comparison_test(_Config) ->
    %% Aggregate rewards from no_var_test
    NoVarChallengeeRewards = blockchain_test_reward_store:fetch(no_var_test_challengee_rewards),
    NoVarWitnessRewards = blockchain_test_reward_store:fetch(no_var_test_witness_rewards),

    TotalNoVarChallengeeRewards = lists:sum(maps:values(NoVarChallengeeRewards)),
    TotalNoVarWitnessRewards = lists:sum(maps:values(NoVarWitnessRewards)),

    FractionalNoVarChallengeeRewards = maps:map(fun(_, V) -> V / TotalNoVarChallengeeRewards end, NoVarChallengeeRewards),
    FractionalNoVarWitnessRewards = maps:map(fun(_, V) -> V / TotalNoVarWitnessRewards end, NoVarWitnessRewards),

    %% Aggregate rewards from hip15 test
    Hip15ChallengeeRewards = blockchain_test_reward_store:fetch(only_hip15_vars_test_challengee_rewards),
    Hip15WitnessRewards = blockchain_test_reward_store:fetch(only_hip15_vars_test_witness_rewards),

    TotalHip15ChallengeeRewards = lists:sum(maps:values(Hip15ChallengeeRewards)),
    TotalHip15WitnessRewards = lists:sum(maps:values(Hip15WitnessRewards)),

    FractionalHip15ChallengeeRewards = maps:map(fun(_, V) -> V / TotalHip15ChallengeeRewards end, Hip15ChallengeeRewards),
    FractionalHip15WitnessRewards = maps:map(fun(_, V) -> V / TotalHip15WitnessRewards end, Hip15WitnessRewards),

    %% Aggregate rewards from hip17 test
    Hip17ChallengeeRewards = blockchain_test_reward_store:fetch(hip15_17_vars_test_challengee_rewards),
    Hip17WitnessRewards = blockchain_test_reward_store:fetch(hip15_17_vars_test_witness_rewards),

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


    %% challengee rewards for no_vars >= only_hip15_vars >= hip15_17_vars
    Challengees = maps:keys(NoVarChallengeeRewards),

    true = lists:all(fun(Challengee) ->
                             NoVarReward = maps:get(Challengee, NoVarChallengeeRewards),
                             Hip15Reward = maps:get(Challengee, Hip15ChallengeeRewards),
                             Hip17Reward = maps:get(Challengee, Hip17ChallengeeRewards),
                             (NoVarReward >= Hip15Reward) andalso (Hip15Reward >= Hip17Reward)
                     end, Challengees),

    ok.


path_ehj(Config) ->
    run_challengees_test(d, e, h, j, [a, b, c, f], [i], Config).

path_khj(Config) ->
    run_challengees_test(d, k, h, j, [a, b, c, f], [i], Config).

path_ghj(Config) ->
    run_challengees_test(d, g, h, j, [a, b, c, f], [i], Config).

beaconer_j(Config) ->
    _RunConfig = #{challenger => d,
                   beaconer1 => j,
                   beaconer1_witnesses => [a, b],
                   beaconer2 => e,
                   beaconer2_witnesses => [c, f]},

    run_beacon_test(d, j, [a, b], e, [c, f], Config).

beaconer_k(Config) ->
    run_beacon_test(d, k, [a, b], e, [c, f], Config).

beaconer_g(Config) ->
    run_beacon_test(d, g, [a, b], e, [c, f], Config).

compare_challengee_test(_Config) ->
    ChallengeeE = maps:get(e, blockchain_test_reward_store:fetch(path_ehj_challengee_rewards)),
    ChallengeeG = maps:get(g, blockchain_test_reward_store:fetch(path_ghj_challengee_rewards)),
    ChallengeeK = maps:get(k, blockchain_test_reward_store:fetch(path_khj_challengee_rewards)),

    ct:pal("ChallengeeE: ~p, ChallengeeG: ~p, ChallengeeK: ~p",
           [ChallengeeE, ChallengeeG, ChallengeeK]),

    ChallengeeChecks = ((ChallengeeK > ChallengeeG) andalso (ChallengeeK > ChallengeeE) andalso (ChallengeeG > ChallengeeE)),

    true = ChallengeeChecks,

    %% for path e, h, j
    %% i gets higher scaled reward
    WitnessA_PathEHJ = maps:get(a, blockchain_test_reward_store:fetch(path_ehj_witness_rewards)),
    WitnessB_PathEHJ = maps:get(b, blockchain_test_reward_store:fetch(path_ehj_witness_rewards)),
    WitnessC_PathEHJ = maps:get(c, blockchain_test_reward_store:fetch(path_ehj_witness_rewards)),
    WitnessF_PathEHJ = maps:get(f, blockchain_test_reward_store:fetch(path_ehj_witness_rewards)),
    WitnessI_PathEHJ = maps:get(i, blockchain_test_reward_store:fetch(path_ehj_witness_rewards)),

    true = (WitnessA_PathEHJ == WitnessB_PathEHJ) andalso
    (WitnessC_PathEHJ == WitnessF_PathEHJ) andalso
    (WitnessA_PathEHJ == WitnessC_PathEHJ) andalso
    (WitnessI_PathEHJ > WitnessA_PathEHJ),

    %% for path k, h, j
    %% i gets lower scaled reward
    WitnessA_PathKHJ = maps:get(a, blockchain_test_reward_store:fetch(path_khj_witness_rewards)),
    WitnessB_PathKHJ = maps:get(b, blockchain_test_reward_store:fetch(path_khj_witness_rewards)),
    WitnessC_PathKHJ = maps:get(c, blockchain_test_reward_store:fetch(path_khj_witness_rewards)),
    WitnessF_PathKHJ = maps:get(f, blockchain_test_reward_store:fetch(path_khj_witness_rewards)),
    WitnessI_PathKHJ = maps:get(i, blockchain_test_reward_store:fetch(path_khj_witness_rewards)),

    true = (WitnessA_PathKHJ == WitnessB_PathKHJ) andalso
    (WitnessC_PathKHJ == WitnessF_PathKHJ) andalso
    (WitnessA_PathKHJ == WitnessC_PathKHJ) andalso
    (WitnessI_PathKHJ < WitnessA_PathKHJ),

    %% for path g, h, j
    %% i gets equal scaled reward
    WitnessA_PathGHJ = maps:get(a, blockchain_test_reward_store:fetch(path_ghj_witness_rewards)),
    WitnessB_PathGHJ = maps:get(b, blockchain_test_reward_store:fetch(path_ghj_witness_rewards)),
    WitnessC_PathGHJ = maps:get(c, blockchain_test_reward_store:fetch(path_ghj_witness_rewards)),
    WitnessF_PathGHJ = maps:get(f, blockchain_test_reward_store:fetch(path_ghj_witness_rewards)),
    WitnessI_PathGHJ = maps:get(i, blockchain_test_reward_store:fetch(path_ghj_witness_rewards)),

    true = (WitnessA_PathGHJ == WitnessB_PathGHJ) andalso
    (WitnessC_PathGHJ == WitnessF_PathGHJ) andalso
    (WitnessA_PathGHJ == WitnessC_PathGHJ) andalso
    (WitnessI_PathGHJ == WitnessA_PathGHJ),

    ok.

compare_beacon_test(_Config) ->
    %% - test between different beaconers
    %%   - For each test there will be two beaconers
    %%      - comparison between beacons in the same test:
    %%          - j (0.2) and e (0.25) are beaconers:
    %%              - j's witnesses should have less rewards than e's witnesses
    %%          - k (0.5) and e (0.25) are beaconers
    %%              - k's witnesses should have higher rewards than e's witnesses
    %%          - g (0.33) and e (0.25) are beaconers
    %%              - g's witnesses should have higher rewards than e's witnesses
    %%
    %%      - comparison between beacon tests
    %%          - k's witnesses should have higher rewards than g's witnesses
    %%          - j's witnesses should have lower rewards than g's witnesses
    BeaconerE_J = maps:get(e, blockchain_test_reward_store:fetch(beaconer_j_challengee_rewards)),
    BeaconerE_K = maps:get(e, blockchain_test_reward_store:fetch(beaconer_k_challengee_rewards)),
    BeaconerE_G = maps:get(e, blockchain_test_reward_store:fetch(beaconer_g_challengee_rewards)),

    BeaconerJ = maps:get(j, blockchain_test_reward_store:fetch(beaconer_j_challengee_rewards)),
    BeaconerK = maps:get(k, blockchain_test_reward_store:fetch(beaconer_k_challengee_rewards)),
    BeaconerG = maps:get(g, blockchain_test_reward_store:fetch(beaconer_g_challengee_rewards)),

    ct:pal("BeaconerJ: ~p, BeaconerG: ~p, BeaconerK: ~p",
           [BeaconerJ, BeaconerG, BeaconerK]),

    BeaconerChecks = ((BeaconerJ < BeaconerE_J) andalso
                      (BeaconerK > BeaconerE_K) andalso
                      (BeaconerG > BeaconerE_G)),

    true = BeaconerChecks,

    WitnessA_J = maps:get(a, blockchain_test_reward_store:fetch(beaconer_j_witness_rewards)),
    WitnessB_J = maps:get(b, blockchain_test_reward_store:fetch(beaconer_j_witness_rewards)),
    WitnessC_JE = maps:get(c, blockchain_test_reward_store:fetch(beaconer_j_witness_rewards)),
    WitnessF_JE = maps:get(f, blockchain_test_reward_store:fetch(beaconer_j_witness_rewards)),

    true = ((WitnessA_J == WitnessB_J) andalso
            (WitnessC_JE == WitnessF_JE) andalso
            (WitnessA_J < WitnessC_JE)),

    WitnessA_K = maps:get(a, blockchain_test_reward_store:fetch(beaconer_k_witness_rewards)),
    WitnessB_K = maps:get(b, blockchain_test_reward_store:fetch(beaconer_k_witness_rewards)),
    WitnessC_KE = maps:get(c, blockchain_test_reward_store:fetch(beaconer_k_witness_rewards)),
    WitnessF_KE = maps:get(f, blockchain_test_reward_store:fetch(beaconer_k_witness_rewards)),

    true = ((WitnessA_K == WitnessB_K) andalso
            (WitnessC_KE == WitnessF_KE) andalso
            (WitnessA_K > WitnessC_KE)),

    WitnessA_G = maps:get(a, blockchain_test_reward_store:fetch(beaconer_g_witness_rewards)),
    WitnessB_G = maps:get(b, blockchain_test_reward_store:fetch(beaconer_g_witness_rewards)),
    WitnessC_GE = maps:get(c, blockchain_test_reward_store:fetch(beaconer_g_witness_rewards)),
    WitnessF_GE = maps:get(f, blockchain_test_reward_store:fetch(beaconer_g_witness_rewards)),

    true = ((WitnessA_G == WitnessB_G) andalso
            (WitnessC_GE == WitnessF_GE) andalso
            (WitnessA_G > WitnessC_GE)),

    true = ((WitnessA_K > WitnessA_G) andalso
            (WitnessA_J < WitnessA_G)),

    ok.

%%--------------------------------------------------------------------
%% HELPER
%%--------------------------------------------------------------------
run_challengees_test(Constructor,
                     Elem1,
                     Elem2,
                     Elem3,
                     Layer1Witnesses,
                     Layer2Witnesses,
                     Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    TCName = ?config(tc_name, Config),
    #{ gateway_names := GatewayNameMap, gateway_letter_to_addr := GatewayLetterToAddrMap } = cross_check_maps(Config),

    Challenger = maps:get(Constructor, GatewayLetterToAddrMap),
    {_, {_, _, ChallengerSigFun}} = lists:keyfind(Challenger, 1, GenesisMembers),

    GwElem1 = maps:get(Elem1, GatewayLetterToAddrMap),
    GwElem2 = maps:get(Elem2, GatewayLetterToAddrMap),
    GwElem3 = maps:get(Elem3, GatewayLetterToAddrMap),

    Rx1 = blockchain_poc_receipt_v1:new(
        GwElem1,
        1000,
        10,
        <<"first_rx">>,
        p2p
    ),
    Rx2 = blockchain_poc_receipt_v1:new(
        GwElem2,
        1000,
        10,
        <<"second_rx">>,
        radio
    ),
    Rx3 = blockchain_poc_receipt_v1:new(
        GwElem3,
        1000,
        10,
        <<"third_rx">>,
        radio
    ),

    ConstructedWitnesses1 = lists:foldl(
        fun(W, Acc) ->
            WitnessGw = maps:get(W, GatewayLetterToAddrMap),
            Witness = blockchain_poc_witness_v1:new(
                WitnessGw,
                1001,
                10,
                crypto:strong_rand_bytes(32),
                9,
                915,
                10,
                "data_rate"
            ),
            [Witness | Acc]
        end,
        [],
        Layer1Witnesses
    ),

    ConstructedWitnesses2 = lists:foldl(
        fun(W, Acc) ->
            WitnessGw = maps:get(W, GatewayLetterToAddrMap),
            Witness = blockchain_poc_witness_v1:new(
                WitnessGw,
                1001,
                10,
                crypto:strong_rand_bytes(32),
                9,
                915,
                10,
                "data_rate"
            ),
            [Witness | Acc]
        end,
        [],
        Layer2Witnesses
    ),

    %% Construct poc receipt txn
    P1 = blockchain_poc_path_element_v1:new(GwElem1, Rx1, ConstructedWitnesses1),
    P2 = blockchain_poc_path_element_v1:new(GwElem2, Rx2, ConstructedWitnesses2),
    P3 = blockchain_poc_path_element_v1:new(GwElem3, Rx3, []),

    ct:pal("P1: ~p", [P1]),
    ct:pal("P2: ~p", [P2]),
    ct:pal("P3: ~p", [P3]),

    %% We'll consider all the witnesses to be "good quality" for the sake of testing
    meck:expect(blockchain_txn_poc_request_v1, is_valid, fun(_, _) ->
        ok
    end),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, get_channels, fun(_, _) ->
        {ok, lists:seq(1, 11)}
    end),
    meck:expect(blockchain_txn_poc_receipts_v1, good_quality_witnesses,
                fun
                    (E, _) when E == P1 ->
                        ConstructedWitnesses1;
                    (E, _) when E == P2 ->
                        ConstructedWitnesses2;
                    (_, _) ->
                        []
                end),
    meck:expect(
        blockchain_txn_rewards_v1,
        legit_witnesses,
        fun
            (_, _, _, E, _, _) when E == P1 ->
                ConstructedWitnesses1;
            (_, _, _, E, _, _) when E == P2 ->
                ConstructedWitnesses2;
            (_, _, _, _, _, _) ->
                []
        end
    ),
    meck:expect(blockchain_txn_poc_receipts_v1, valid_witnesses,
                fun
                    (E, _, _) when E == P1 ->
                        ConstructedWitnesses1;
                    (E, _, _) when E == P2 ->
                        ConstructedWitnesses2;
                    (_, _, _) ->
                        []
                end),


    Secret = crypto:strong_rand_bytes(32),
    OnionKeyHash = crypto:strong_rand_bytes(32),
    BlockHash = crypto:strong_rand_bytes(32),

    RTxn0 = blockchain_txn_poc_request_v1:new(Challenger,
                                              Secret,
                                              OnionKeyHash,
                                              BlockHash,
                                              10),
    RTxn = blockchain_txn_poc_request_v1:sign(RTxn0, ChallengerSigFun),
    ct:pal("RTxn: ~p", [RTxn]),
    %% Construct a block for the poc request txn
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [RTxn], #{}, false),
    ct:pal("Block2: ~p", [Block2]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    Txn0 = blockchain_txn_poc_receipts_v1:new(
        Challenger,
        Secret,
        OnionKeyHash,
        BlockHash,
        [P1, P2, P3]
    ),
    Txn = blockchain_txn_poc_receipts_v1:sign(Txn0, ChallengerSigFun),
    ct:pal("Txn: ~p", [Txn]),

    %% Construct a block for the poc receipt txn WITHOUT validation
    {ok, Block3} = test_utils:create_block(ConsensusMembers, [Txn], #{}, false),
    ct:pal("Block3: ~p", [Block3]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    %% Empty block
    {ok, Block4} = test_utils:create_block(ConsensusMembers, []),
    ct:pal("Block4: ~p", [Block4]),
    _ = blockchain_gossip_handler:add_block(Block4, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 4}, blockchain:height(Chain)),

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


run_vars_test(Witnesses, Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    TCName = ?config(tc_name, Config),
    #{ gateway_names := GatewayNameMap, gateway_letter_to_addr := GatewayLetterToAddrMap } = cross_check_maps(Config),

    Challenger = maps:get(k, GatewayLetterToAddrMap),
    {_, {_, _, ChallengerSigFun}} = lists:keyfind(Challenger, 1, GenesisMembers),

    GwA = maps:get(a, GatewayLetterToAddrMap),
    GwD = maps:get(d, GatewayLetterToAddrMap),
    GwI = maps:get(i, GatewayLetterToAddrMap),

    Rx1 = blockchain_poc_receipt_v1:new(
        GwA,
        1000,
        10,
        <<"first_rx">>,
        p2p
    ),
    Rx2 = blockchain_poc_receipt_v1:new(
        GwD,
        1000,
        10,
        <<"second_rx">>,
        radio
    ),
    Rx3 = blockchain_poc_receipt_v1:new(
        GwI,
        1000,
        10,
        <<"third_rx">>,
        radio
    ),

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
                "data_rate"
            ),
            [Witness | Acc]
        end,
        [],
        Witnesses
    ),

    %% We'll consider all the witnesses to be "good quality" for the sake of testing
    meck:expect(
        blockchain_txn_rewards_v1,
        legit_witnesses,
        fun(_, _, _, _, _, _) ->
            ConstructedWitnesses
        end
    ),
    meck:expect(blockchain_txn_poc_request_v1, is_valid, fun(_, _) ->
        ok
    end),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_, _) -> ok end),
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
    %% meck:expect(blockchain_hex, destroy_memoization, fun() ->
    %%     true
    %% end),

    Secret = crypto:strong_rand_bytes(32),
    OnionKeyHash = crypto:strong_rand_bytes(32),
    BlockHash = crypto:strong_rand_bytes(32),

    RTxn0 = blockchain_txn_poc_request_v1:new(Challenger,
                                              Secret,
                                              OnionKeyHash,
                                              BlockHash,
                                              10),
    RTxn = blockchain_txn_poc_request_v1:sign(RTxn0, ChallengerSigFun),
    ct:pal("RTxn: ~p", [RTxn]),
    %% Construct a block for the poc request txn
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [RTxn], #{}, false),
    ct:pal("Block2: ~p", [Block2]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    %% Construct poc receipt txn
    P1 = blockchain_poc_path_element_v1:new(GwA, Rx1, []),
    P2 = blockchain_poc_path_element_v1:new(GwD, Rx2, ConstructedWitnesses),
    P3 = blockchain_poc_path_element_v1:new(GwI, Rx3, []),

    ct:pal("P1: ~p", [P1]),
    ct:pal("P2: ~p", [P2]),
    ct:pal("P3: ~p", [P3]),

    Txn0 = blockchain_txn_poc_receipts_v1:new(
        Challenger,
        Secret,
        OnionKeyHash,
        BlockHash,
        [P1, P2, P3]
    ),
    Txn = blockchain_txn_poc_receipts_v1:sign(Txn0, ChallengerSigFun),
    ct:pal("Txn: ~p", [Txn]),

    %% Construct a block for the poc receipt txn WITHOUT validation
    {ok, Block3} = test_utils:create_block(ConsensusMembers, [Txn], #{}, false),
    ct:pal("Block3: ~p", [Block3]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    %% Empty block
    {ok, Block4} = test_utils:create_block(ConsensusMembers, []),
    ct:pal("Block4: ~p", [Block4]),
    _ = blockchain_gossip_handler:add_block(Block4, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 4}, blockchain:height(Chain)),

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

cross_check_maps(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),
    AG = blockchain_ledger_v1:active_gateways(Ledger),
    GatewayAddrs = lists:sort(maps:keys(AG)),
    AllGws = [a, b, c, d, e, f, g, h, i, j, k],
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

    #{
        gateway_names => GatewayNameMap,
        gateway_locs => GatewayLocMap,
        gateway_letter_to_addr => GatewayLetterToAddrMap,
        gateway_letter_to_loc => GatewayLetterLocMap
    }.


hip15_vars() ->
    #{
        %% configured on chain
        ?poc_version => 10,
        ?reward_version => 5,
        %% new hip15 vars for testing
        ?poc_reward_decay_rate => 0.8,
        ?witness_redundancy => 4
    }.

run_beacon_test(Challenger0,
                FirstBeaconer,
                FirstBeaconerWitnesses,
                SecondBeaconer,
                SecondBeaconerWitnesses,
                Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    TCName = ?config(tc_name, Config),
    #{ gateway_names := GatewayNameMap,
       gateway_letter_to_addr := GatewayLetterToAddrMap } = cross_check_maps(Config),

    Challenger = maps:get(Challenger0, GatewayLetterToAddrMap),
    {_, {_, _, ChallengerSigFun}} = lists:keyfind(Challenger, 1, GenesisMembers),

    %% First beaconer (challengee)
    GwFirstBeaconer = maps:get(FirstBeaconer, GatewayLetterToAddrMap),

    %% Receipt for first beaconer
    Rx1 = blockchain_poc_receipt_v1:new(
        GwFirstBeaconer,
        1000,
        10,
        <<"first_rx">>,
        p2p
    ),

    %% Witnesses for first beaconer
    ConstructedWitnesses1 = lists:foldl(
        fun(W, Acc) ->
            WitnessGw = maps:get(W, GatewayLetterToAddrMap),
            Witness = blockchain_poc_witness_v1:new(
                WitnessGw,
                1001,
                10,
                crypto:strong_rand_bytes(32),
                9.800000190734863,
                915.2000122070313,
                10,
                "data_rate"
            ),
            [Witness | Acc]
        end,
        [],
        FirstBeaconerWitnesses
    ),

    %% Second beaconer (challengee)
    GwSecondBeaconer = maps:get(SecondBeaconer, GatewayLetterToAddrMap),

    %% Rceipt for second beaconer
    Rx2 = blockchain_poc_receipt_v1:new(
        GwSecondBeaconer,
        1000,
        10,
        <<"first_rx">>,
        p2p
    ),

    %% Witnesses for second beaconer
    ConstructedWitnesses2 = lists:foldl(
        fun(W, Acc) ->
            WitnessGw = maps:get(W, GatewayLetterToAddrMap),
            Witness = blockchain_poc_witness_v1:new(
                WitnessGw,
                1001,
                10,
                crypto:strong_rand_bytes(32),
                9.800000190734863,
                915.2000122070313,
                10,
                "data_rate"
            ),
            [Witness | Acc]
        end,
        [],
        SecondBeaconerWitnesses
    ),

    %% poc request for first beacon
    Secret = crypto:strong_rand_bytes(32),
    OnionKeyHash = crypto:strong_rand_bytes(32),
    BlockHash = crypto:strong_rand_bytes(32),

    FirstRequestTxn = blockchain_txn_poc_request_v1:new(Challenger,
                                                        Secret,
                                                        OnionKeyHash,
                                                        BlockHash,
                                                        10),
    SignedFirstRequestTxn = blockchain_txn_poc_request_v1:sign(FirstRequestTxn, ChallengerSigFun),
    ct:pal("SignedFirstRequestTxn: ~p", [SignedFirstRequestTxn]),

    %% Construct first poc receipt txn
    P1 = blockchain_poc_path_element_v1:new(GwFirstBeaconer, Rx1, ConstructedWitnesses1),
    ct:pal("P1: ~p", [P1]),
    FirstPocTxn = blockchain_txn_poc_receipts_v1:new(
                    Challenger,
                    Secret,
                    OnionKeyHash,
                    BlockHash,
                    [P1]
                   ),
    SignedFirstPocTxn = blockchain_txn_poc_receipts_v1:sign(FirstPocTxn, ChallengerSigFun),
    ct:pal("SignedFirstPocTxn: ~p", [SignedFirstPocTxn]),

    %% Second poc request and receipt txn
    Secret2 = crypto:strong_rand_bytes(32),
    OnionKeyHash2 = crypto:strong_rand_bytes(32),
    BlockHash2 = crypto:strong_rand_bytes(32),

    SecondRequestTxn = blockchain_txn_poc_request_v1:new(Challenger,
                                                         Secret2,
                                                         OnionKeyHash2,
                                                         BlockHash2,
                                                         10),
    SignedSecondRequestTxn = blockchain_txn_poc_request_v1:sign(SecondRequestTxn, ChallengerSigFun),
    ct:pal("SignedSecondRequestTxn: ~p", [SignedSecondRequestTxn]),

    %% Construct second poc receipt txn
    P2 = blockchain_poc_path_element_v1:new(GwSecondBeaconer, Rx2, ConstructedWitnesses2),
    ct:pal("P2: ~p", [P2]),
    SecondPocTxn = blockchain_txn_poc_receipts_v1:new(
                    Challenger,
                    Secret2,
                    OnionKeyHash2,
                    BlockHash2,
                    [P2]
                   ),
    SignedSecondPocTxn = blockchain_txn_poc_receipts_v1:sign(SecondPocTxn, ChallengerSigFun),
    ct:pal("SignedSecondPocTxn: ~p", [SignedSecondPocTxn]),


    %% We'll consider all the witnesses to be "good quality" for the sake of testing
    meck:expect(
        blockchain_txn_rewards_v1,
        legit_witnesses,
        fun
            (_, _, _, E, _, _) when E == P1 ->
                ConstructedWitnesses1;
            (_, _, _, E, _, _) when E == P2 ->
                ConstructedWitnesses2;
            (_, _, _, _, _, _) ->
                []
        end
    ),
    meck:expect(blockchain_txn_poc_receipts_v1, good_quality_witnesses,
                fun
                    (E, _) when E == P1 ->
                        ConstructedWitnesses1;
                    (E, _) when E == P2 ->
                        ConstructedWitnesses2;
                    (_, _) ->
                        []
                end),
    meck:expect(blockchain_txn_poc_receipts_v1, valid_witnesses,
                fun
                    (E, _, _) when E == P1 ->
                        ConstructedWitnesses1;
                    (E, _, _) when E == P2 ->
                        ConstructedWitnesses2;
                    (_, _, _) ->
                        []
                end),
    meck:expect(blockchain_txn_poc_request_v1, is_valid, fun(_, _) ->
        ok
    end),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, get_channels, fun(_, _) ->
        {ok, lists:seq(1, 11)}
    end),

    %% Construct a block for the first poc request txn
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [SignedFirstRequestTxn], #{}, false),
    ct:pal("Block2: ~p", [Block2]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    %% Construct a block for the first poc receipt txn
    {ok, Block3} = test_utils:create_block(ConsensusMembers, [SignedFirstPocTxn], #{}, false),
    ct:pal("Block3: ~p", [Block3]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    %% Construct a block for the second poc request txn
    {ok, Block4} = test_utils:create_block(ConsensusMembers, [SignedSecondRequestTxn], #{}, false),
    ct:pal("Block4: ~p", [Block4]),
    _ = blockchain_gossip_handler:add_block(Block4, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 4}, blockchain:height(Chain)),

    %% Construct a block for the second poc receipt txn
    {ok, Block5} = test_utils:create_block(ConsensusMembers, [SignedSecondPocTxn]),
    ct:pal("Block5: ~p", [Block5]),
    _ = blockchain_gossip_handler:add_block(Block5, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, 5}, blockchain:height(Chain)),

    %% Calculate rewards by hand
    Start = 1,
    End = 5,
    {ok, Rewards} = blockchain_txn_rewards_v1:calculate_rewards(Start, End, Chain),

    ct:pal("Rewards: ~p", [Rewards]),
    ct:pal("TotalRewards: ~p", [lists:sum([blockchain_txn_reward_v1:amount(R) || R <- Rewards])]),

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
        ?density_tgt_res => 4,
        ?hip17_interactivity_blocks => 1200 * 3,
        ?poc_challengees_percent => 0.0531,
        ?poc_witnesses_percent => 0.2124,
        ?consensus_percent => 0.06,
        ?dc_percent => 0.325,
        ?poc_challengers_percent => 0.0095,
        ?securities_percent => 0.34
    }.

known_locations() ->
    [631210990515645439,
     631210990515609087,
     631210990516667903,
     631210990528935935,
     631210990528385535,
     631210990528546815,
     631210990529462783,
     631210990529337343,
     631210990524024831,
     631210990524753919,
     631210990525267455
    ].

static_keys() ->
    %% These static keys are only generated to ensure we get consistent results
    %% between successive test runs
    <<MainNet_ECC_COMPACT:8/integer>> = <<0:4, 0:4>>,
    %% Each entry in this list is a libp2p_crypto "swarm_key" blob.
    [ libp2p_crypto:keys_from_bin(Blob) || Blob <-
        [
          <<MainNet_ECC_COMPACT,156,174,164,201,194,229,247,253,190,55,
            192,6,178,54,77,8,107,207,119,165,225,56,57,77,56,93,18,7,204,3,87,19,
            %% Public point
            4,135,35,145,156,56,241,143,56,203,33,59,137,227,51,220,158,75,110,92,
            79,47,197,189,61,2,166,40,158,85,94,187,210,23,124,197,126,82,136,156,
            224,77,29,244,7,181,54,27,193,238,247,20,220,223,82,172,29,184,166,244,
            80,180,158,234,200>>,

          <<MainNet_ECC_COMPACT,74,195,136,8,5,64,160,74,239,70,204,88,28,125,218,
            23,158,45,29,211,216,145,16,44,78,66,232,65,60,96,37,195,
            %% Public point
            4,20,71,124,234,252,184,6,227,161,188,190,47,137,191,118,55,90,107,76,
            18,110,125,250,200,219,154,35,120,32,13,85,162,82,177,64,62,209,191,108,
            219,71,95,159,45,165,110,57,225,131,208,229,15,227,239,68,150,156,254,
            184,111,119,196,72,157>>,

          <<MainNet_ECC_COMPACT,225,210,133,73,145,176,53,145,226,86,23,195,148,
            179,16,42,71,247,197,165,158,144,151,227,103,187,209,110,2,16,100,99,
            %% Public point
            4,217,189,89,225,39,191,180,16,213,28,126,134,2,140,86,174,237,57,197,
            104,123,176,138,216,163,253,140,2,4,159,237,17,100,96,191,118,251,152,
            42,105,120,182,220,31,13,120,76,56,254,170,50,153,63,47,84,160,68,36,
            156,45,187,209,160,81>>,

          <<MainNet_ECC_COMPACT,77,233,24,124,91,70,54,186,28,49,82,177,176,200,8,
            68,211,204,31,128,74,142,24,118,112,207,51,86,10,74,155,139,
            %% Public point
            4,215,157,27,147,66,217,10,140,181,194,91,108,130,23,111,221,203,186,
            194,157,244,168,53,45,184,162,228,141,214,155,106,122,38,162,55,138,90,
            19,132,142,109,40,39,237,77,117,34,14,160,114,41,62,104,54,56,240,115,
            124,9,53,189,251,42,56>>,

          <<MainNet_ECC_COMPACT,241,178,164,75,59,239,80,187,86,100,0,137,105,108,
            64,161,36,76,103,226,66,241,42,114,119,131,125,203,205,2,213,21,
            %% Public point
            4,85,63,44,227,26,250,122,155,247,250,201,91,215,217,210,181,152,209,
            90,103,54,116,57,145,2,191,107,135,227,150,188,139,106,164,125,200,121,
            31,25,3,125,231,89,189,212,151,43,237,167,194,1,145,180,132,128,149,
            213,142,55,113,43,57,129,192>>,

          <<MainNet_ECC_COMPACT,35,37,85,183,224,18,148,11,77,133,138,152,248,222,
            56,7,8,70,251,212,124,223,107,122,184,18,15,60,254,173,172,18,
            %% Public point
            4,33,183,166,74,151,68,17,14,58,106,91,30,171,149,116,42,54,136,187,6,
            135,149,78,44,132,144,224,168,180,185,5,210,8,205,10,58,37,17,206,158,
            32,200,182,231,53,43,66,110,7,107,125,127,244,91,98,235,213,107,130,
            177,229,189,26,225>>,

          <<MainNet_ECC_COMPACT,248,151,148,106,71,235,30,171,175,38,61,208,228,
            196,194,195,249,95,180,188,95,132,216,225,68,184,114,177,226,242,21,60,
            %% Public point
            4,197,206,105,167,43,204,77,56,215,206,79,130,83,194,243,95,100,232,161,
            135,166,145,34,142,103,155,65,147,209,189,13,145,120,0,190,129,210,122,
            118,155,125,166,201,50,78,61,217,80,236,99,106,75,181,30,27,230,173,173,
            102,84,25,102,28,126>>,

          <<MainNet_ECC_COMPACT,208,185,203,219,249,155,80,235,230,243,229,64,55,
            110,230,34,135,106,11,22,26,202,149,11,135,154,242,158,9,77,136,242,
            %% Public point
            4,7,94,141,107,189,125,163,87,153,196,200,77,40,78,50,238,22,1,154,70,
            45,135,148,16,46,120,188,198,164,147,250,255,19,149,195,194,244,103,75,
            60,21,25,210,102,160,221,218,228,176,202,38,39,78,110,184,59,52,196,95,
            173,105,168,140,210>>,

          <<MainNet_ECC_COMPACT,180,123,47,194,133,184,161,103,6,218,189,247,36,
            157,70,102,114,5,199,223,38,24,244,74,248,111,229,69,30,232,234,205,
            %% Public point
            4,161,224,241,247,215,177,248,246,170,70,111,93,20,168,111,142,225,183,
            129,50,237,242,215,38,34,0,224,216,228,118,55,26,68,26,209,120,63,198,
            91,107,11,223,80,59,88,34,239,206,159,182,46,177,249,154,53,8,38,195,
            129,102,176,32,85,201>>,

          <<MainNet_ECC_COMPACT,155,29,3,116,69,125,244,121,73,22,179,49,210,8,187,
            245,179,70,6,58,3,60,12,136,25,186,144,133,58,236,232,160,
            %% Public point
            4,181,132,129,89,193,104,228,73,203,137,46,161,153,156,56,205,253,243,
            206,109,218,93,13,9,77,222,143,147,148,135,39,15,122,11,108,114,34,18,
            8,83,94,141,159,31,164,88,209,127,192,66,45,132,137,86,40,57,122,188,
            185,86,99,180,161,224>>,

          <<MainNet_ECC_COMPACT,32,249,3,92,124,248,0,146,225,224,17,2,87,54,7,126,
            165,185,28,110,196,141,58,35,250,244,162,224,158,40,0,28,
            %% Public point
            4,156,114,72,115,65,211,12,113,160,134,127,252,250,62,10,149,32,182,30,
            19,158,41,162,182,224,15,48,57,27,13,50,200,108,73,193,233,44,47,102,
            122,37,188,76,253,248,32,143,166,59,54,47,46,239,151,157,211,72,75,185,
            81,203,125,20,50>>
      ]
    ].
