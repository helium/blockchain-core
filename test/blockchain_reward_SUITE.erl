-module(blockchain_reward_SUITE).

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
    hip15_test/1,
    non_hip15_test/1
]).

all() ->
    [
        hip15_test,
        non_hip15_test
    ].

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    %% Using this simple gen_server to store test results for comparison in end_per_suite
    blockchain_test_reward_store:start_link(),
    Config.

%%--------------------------------------------------------------------
%% TEST SUITE TEARDOWN
%%--------------------------------------------------------------------

end_per_suite(Config) ->
    ct:pal("Config: ~p", [Config]),
    State = blockchain_test_reward_store:state(),
    ct:pal("State: ~p", [State]),
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
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars =
        case TestCase of
            hip15_test ->
                #{
                    %% configured on chain
                    ?poc_version => 9,
                    ?reward_version => 5,
                    %% new vars for testing
                    ?poc_reward_decay_rate => 0.8,
                    ?witness_redundancy => 4
                };
            _ ->
                #{}
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

end_per_testcase(_TestCase, _Config) ->
    meck:unload(blockchain_txn_rewards_v1),
    meck:unload(blockchain_txn_poc_receipts_v1),
    meck:unload(),
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

non_hip15_test(Config) ->
    run_test(Config).

hip15_test(Config) ->
    %% - We have gateways: [A, B, C, D, E, F, G, H, I, J, K]
    %% - We'll make a poc receipt txn by hand, without any validation
    %% - We'll also consider that all witnesses are legit (legit_witnesses)
    %% - The poc transaction will have path like so: A -> D -> H
    %% - For A -> D; J, K will be the only witnesses; no scaling should happen
    %% - For D -> H; B, C, E, F, G, I will all be witnesses, their rewards should get scaled
    run_test(Config).

%%--------------------------------------------------------------------
%% HELPER
%%--------------------------------------------------------------------

run_test(Config) ->
    ct:pal("Config: ~p", [Config]),
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),
    TCName = ?config(tc_name, Config),

    Ledger = blockchain:ledger(Chain),
    Vars = blockchain_ledger_v1:snapshot_vars(Ledger),
    ct:pal("Vars: ~p", [Vars]),

    AG = blockchain_ledger_v1:active_gateways(Ledger),

    GatewayAddrs =
        [
            GwA,
            GwB,
            GwC,
            GwD,
            GwE,
            GwF,
            GwG,
            GwH,
            GwI,
            GwJ,
            GwK
        ] = lists:sort(maps:keys(AG)),

    %% For crosscheck
    GatewayNameMap = lists:foldl(
        fun({Letter, A}, Acc) ->
            maps:put(blockchain_utils:addr2name(A), Letter, Acc)
        end,
        #{},
        lists:zip([a, b, c, d, e, f, g, h, i, j, k], GatewayAddrs)
    ),

    Rx1 = blockchain_poc_receipt_v1:new(GwA, 1000, 10, "first_rx", p2p),
    Rx2 = blockchain_poc_receipt_v1:new(GwD, 1000, 10, "second_rx", radio),
    Rx3 = blockchain_poc_receipt_v1:new(GwH, 1000, 10, "third_rx", radio),

    ct:pal("Rx1: ~p", [Rx1]),
    ct:pal("Rx2: ~p", [Rx2]),
    ct:pal("Rx3: ~p", [Rx3]),

    W1 = blockchain_poc_witness_v1:new(GwJ, 1001, 10, <<"hash_w1">>, 9.8, 915.2, 1, <<"dr">>),
    W2 = blockchain_poc_witness_v1:new(GwK, 1001, 11, <<"hash_w2">>, 9.8, 915.2, 2, <<"dr">>),

    ct:pal("W1: ~p", [W1]),
    ct:pal("W2: ~p", [W2]),

    W3 = blockchain_poc_witness_v1:new(GwB, 1001, 10, <<"hash_w3">>, 9.8, 915.2, 3, <<"dr">>),
    W4 = blockchain_poc_witness_v1:new(GwC, 1002, 11, <<"hash_w4">>, 9.8, 915.2, 4, <<"dr">>),
    W5 = blockchain_poc_witness_v1:new(GwD, 1003, 12, <<"hash_w5">>, 9.8, 915.2, 5, <<"dr">>),
    W6 = blockchain_poc_witness_v1:new(GwE, 1004, 13, <<"hash_w6">>, 9.8, 915.2, 6, <<"dr">>),
    W7 = blockchain_poc_witness_v1:new(GwF, 1005, 14, <<"hash_w7">>, 9.8, 915.2, 7, <<"dr">>),
    W8 = blockchain_poc_witness_v1:new(GwG, 1006, 15, <<"hash_w8">>, 9.8, 915.2, 8, <<"dr">>),
    W9 = blockchain_poc_witness_v1:new(GwI, 1007, 16, <<"hash_w9">>, 9.8, 915.2, 9, <<"dr">>),

    %% We'll consider all the witnesses to be "good quality" for the sake of testing
    Witnesses = [W1, W2, W3, W4, W5, W6, W7, W8, W9],
    meck:expect(
        blockchain_txn_rewards_v1,
        legit_witnesses,
        fun(_, _, _, _, _, _) ->
            Witnesses
        end
    ),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, valid_witnesses, fun(_, _, _) -> Witnesses end),
    meck:expect(blockchain_txn_poc_receipts_v1, good_quality_witnesses, fun(_, _) -> Witnesses end),
    meck:expect(blockchain_txn_poc_receipts_v1, get_channels, fun(_, _) ->
        {ok, lists:seq(1, 11)}
    end),

    ct:pal("W3: ~p", [W3]),
    ct:pal("W4: ~p", [W4]),
    ct:pal("W5: ~p", [W5]),
    ct:pal("W6: ~p", [W6]),
    ct:pal("W7: ~p", [W7]),
    ct:pal("W8: ~p", [W8]),
    ct:pal("W9: ~p", [W9]),

    P1 = blockchain_poc_path_element_v1:new(GwA, Rx1, []),
    P2 = blockchain_poc_path_element_v1:new(GwD, Rx2, [W1, W2]),
    P3 = blockchain_poc_path_element_v1:new(GwH, Rx3, [W3, W4, W5, W6, W7, W8, W9]),

    ct:pal("P1: ~p", [P1]),
    ct:pal("P2: ~p", [P2]),
    ct:pal("P3: ~p", [P3]),

    Txn = blockchain_txn_poc_receipts_v1:new(
        <<"challenger">>,
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
                    blockchain_utils:addr2name(blockchain_txn_reward_v1:gateway(R)),
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
                    blockchain_utils:addr2name(blockchain_txn_reward_v1:gateway(R)),
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
        list_to_atom(atom_to_list(TCName) ++ "_witness"),
        WitnessRewardsMap
    ),

    ok = blockchain_test_reward_store:insert(
        list_to_atom(atom_to_list(TCName) ++ "_challengee"),
        ChallengeesRewardsMap
    ),

    ok.
