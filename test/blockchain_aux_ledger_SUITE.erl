-module(blockchain_aux_ledger_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_txn_rewards_v2_pb.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    bootstrap_test/1,
    alter_var_test/1,
    aux_rewards_test/1,
    aux_rewards_v2_test/1
]).

all() ->
    [
        bootstrap_test,
        alter_var_test,
        aux_rewards_test,
        aux_rewards_v2_test
    ].

%%--------------------------------------------------------------------
%% test case setup
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    BaseDir = ?config(base_dir, Config0),
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),

    ExtraVars = extra_vars(TestCase),

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

    [
        {balance, Balance},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {ledger, Ledger},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        {base_dir, BaseDir},
        Keys
        | Config0
    ].

%%--------------------------------------------------------------------
%% test case teardown
%%--------------------------------------------------------------------

end_per_testcase(_, Config) ->
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
%% test cases
%%--------------------------------------------------------------------

bootstrap_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    Ledger = ?config(ledger, Config),
    AuxLedger0 = blockchain_aux_ledger_v1:bootstrap(
        filename:join([BaseDir, "bootstrap_test.db"]),
        Ledger
    ),
    AuxLedger = blockchain_ledger_v1:mode(aux, AuxLedger0),

    aux = blockchain_ledger_v1:mode(AuxLedger),

    Entries = blockchain_ledger_v1:entries(Ledger),
    AuxEntries = blockchain_ledger_v1:entries(AuxLedger),

    %% the entries should be the same
    true = lists:sort(maps:to_list(Entries)) == lists:sort(maps:to_list(AuxEntries)),

    ok.

alter_var_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    Ledger = ?config(ledger, Config),
    AuxLedger0 = blockchain_aux_ledger_v1:bootstrap(
        filename:join([BaseDir, "alter_var_test.db"]),
        Ledger
    ),
    AuxLedger = blockchain_ledger_v1:mode(aux, AuxLedger0),

    aux = blockchain_ledger_v1:mode(AuxLedger),

    Vars = blockchain_ledger_v1:snapshot_vars(Ledger),
    AuxVars = blockchain_ledger_v1:snapshot_vars(AuxLedger),

    %% the vars should be equal
    true = lists:sort(Vars) == lists:sort(AuxVars),

    {ok, MonthlyReward} = blockchain_ledger_v1:config(?monthly_reward, Ledger),

    AlteredMonthlyReward = MonthlyReward * 100,

    AlterVars = #{monthly_reward => AlteredMonthlyReward},

    ok = blockchain_aux_ledger_v1:set_vars(AlterVars, AuxLedger),

    {ok, AuxMonthlyReward} = blockchain_ledger_v1:config(?monthly_reward, AuxLedger),

    true = AuxMonthlyReward == AlteredMonthlyReward,

    ok.

aux_rewards_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    Ledger = ?config(ledger, Config),
    AuxLedger0 = blockchain_aux_ledger_v1:bootstrap(
        filename:join([BaseDir, "bootstrap_test.db"]),
        Ledger
    ),
    AuxLedger = blockchain_ledger_v1:mode(aux, AuxLedger0),

    %% construct some yolo rewards, two versions, real and aux
    Reward = 1000000000,
    Height = 10,
    AuxMultiplier = 100,
    AuxReward = Reward * AuxMultiplier,
    Owner = <<"o1">>,

    GW1 = <<"g1">>,
    GW2 = <<"g2">>,

    Rewards = [
        blockchain_txn_reward_v1:new(Owner, undefined, Reward, securities),
        blockchain_txn_reward_v1:new(Owner, GW1, Reward, consensus),
        blockchain_txn_reward_v1:new(Owner, GW2, Reward, consensus),
        blockchain_txn_reward_v1:new(Owner, GW1, Reward, poc_challengees),
        blockchain_txn_reward_v1:new(Owner, GW2, Reward, poc_witnesses),
        blockchain_txn_reward_v1:new(Owner, GW2, Reward, poc_challengers)
    ],
    AuxRewards = [
        blockchain_txn_reward_v1:new(Owner, undefined, AuxReward, securities),
        blockchain_txn_reward_v1:new(Owner, GW1, AuxReward, consensus),
        blockchain_txn_reward_v1:new(Owner, GW2, AuxReward, consensus),
        blockchain_txn_reward_v1:new(Owner, GW1, AuxReward, poc_challengees),
        blockchain_txn_reward_v1:new(Owner, GW2, AuxReward, poc_witnesses),
        blockchain_txn_reward_v1:new(Owner, GW2, AuxReward, poc_challengers)
    ],

    %% set both sets of rewards for aux ledger
    ok = blockchain_aux_ledger_v1:set_rewards(Height, Rewards, AuxRewards, AuxLedger),

    %% check that we get the correct sets of rewards
    {ok, {Rewards, AuxRewards}} = blockchain_aux_ledger_v1:get_rewards_at(Height, AuxLedger),

    %% check that all the aux rewards are as expected
    ExpectedAuxRewards = #{Height => {Rewards, AuxRewards}},
    AllAuxRewards = blockchain_aux_ledger_v1:get_rewards(AuxLedger),
    true = lists:sort(maps:to_list(ExpectedAuxRewards)) == lists:sort(maps:to_list(AllAuxRewards)),

    %% check that the diff has the right account rewards
    Diff = blockchain_aux_ledger_v1:diff_rewards(AuxLedger),
    ct:pal("Diff: ~p", [Diff]),

    true = check_bal_diff(Owner, AuxMultiplier, Height, Diff),
    true = check_bal_diff(GW1, AuxMultiplier, Height, Diff),
    true = check_bal_diff(GW2, AuxMultiplier, Height, Diff),

    ok.

aux_rewards_v2_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    Ledger = ?config(ledger, Config),
    AuxLedger0 = blockchain_aux_ledger_v1:bootstrap(
        filename:join([BaseDir, "bootstrap_test.db"]),
        Ledger
    ),
    AuxLedger = blockchain_ledger_v1:mode(aux, AuxLedger0),

    %% construct some yolo rewards, two versions, real and aux
    Height = 10,
    AuxMultiplier = 100,
    O1 = <<"o1">>,
    O2 = <<"o2">>,
    O3 = <<"o3">>,
    R1 = 1000000000,
    R2 = 2000000000,
    R3 = 3000000000,

    %% We'll only push the internal rewards to aux ledger instead of wrapping them in a transaction
    Rewards = [
        #blockchain_txn_reward_v2_pb{account = O1, amount = R1},
        #blockchain_txn_reward_v2_pb{account = O2, amount = R2},
        #blockchain_txn_reward_v2_pb{account = O3, amount = R3}
    ],

    AuxRewards = [
        #blockchain_txn_reward_v2_pb{account = O1, amount = R1 * AuxMultiplier},
        #blockchain_txn_reward_v2_pb{account = O2, amount = R2 * AuxMultiplier},
        #blockchain_txn_reward_v2_pb{account = O3, amount = R3 * AuxMultiplier}
    ],

    %% set both sets of rewards for aux ledger
    ok = blockchain_aux_ledger_v1:set_rewards(Height, Rewards, AuxRewards, AuxLedger),

    %% check that we get the correct sets of rewards
    {ok, {Rewards, AuxRewards}} = blockchain_aux_ledger_v1:get_rewards_at(Height, AuxLedger),

    %% check that all the aux rewards are as expected
    ExpectedAuxRewards = #{Height => {Rewards, AuxRewards}},
    AllAuxRewards = blockchain_aux_ledger_v1:get_rewards(AuxLedger),

    ct:pal("ExpectedAuxRewards: ~p", [ExpectedAuxRewards]),
    ct:pal("AllAuxRewards: ~p", [AllAuxRewards]),

    true = lists:sort(maps:to_list(ExpectedAuxRewards)) == lists:sort(maps:to_list(AllAuxRewards)),

    %% check that the diff has the right account rewards
    Diff = blockchain_aux_ledger_v1:diff_rewards(AuxLedger),
    ct:pal("Diff: ~p", [Diff]),

    true = check_bal_diff(O1, AuxMultiplier, Height, Diff),
    true = check_bal_diff(O2, AuxMultiplier, Height, Diff),
    true = check_bal_diff(O3, AuxMultiplier, Height, Diff),

    ok.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

extra_vars(aux_rewards_v2_test) ->
    #{rewards_txn_version => 2};
extra_vars(_) ->
    #{}.

check_bal_diff(Owner, AuxMultiplier, Height, Diff) ->
    {O1_R, O1_AR} = maps:get(Owner, maps:get(Height, Diff)),
    Amount_O1_R = maps:get(amount, O1_R),
    Amount_O1_AR = maps:get(amount, O1_AR),
    Amount_O1_AR == AuxMultiplier * Amount_O1_R.
