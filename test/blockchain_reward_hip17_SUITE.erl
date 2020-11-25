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

    ExtraVars =
        case TestCase of
            no_var_test ->
                #{};
            with_hip15_vars_test ->
                hip15_vars();
            with_hip17_vars_test ->
                maps:merge(hip15_vars(), hip17_vars())
        end,

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
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
    %% TODO: compare the witness and challengee rewards between no_var, with_hip15_vars_test and with_hip17_vars_test

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
    GatewayLetterToAddrMap = lists:foldl(
        fun({Letter, A}, Acc) ->
            maps:put(Letter, A, Acc)
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
    ct:pal("Gateways: ~p", [GatewayNameMap]),
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
