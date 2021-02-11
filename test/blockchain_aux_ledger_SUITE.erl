-module(blockchain_aux_ledger_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    bootstrap_test/1,
    alter_var_test/1,
    aux_rewards_test/1
]).

all() ->
    [
        bootstrap_test,
        alter_var_test,
        aux_rewards_test
    ].

%%--------------------------------------------------------------------
%% test case setup
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    BaseDir = ?config(base_dir, Config0),
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),

    ExtraVars = #{},

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
    AuxLedger0 = blockchain_ledger_v1:bootstrap_aux(
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
    AuxLedger0 = blockchain_ledger_v1:bootstrap_aux(
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

    AlterVars = #{<<"monthly_reward">> => AlteredMonthlyReward},

    ok = blockchain_ledger_v1:set_aux_vars(AlterVars, AuxLedger),

    {ok, AuxMonthlyReward} = blockchain_ledger_v1:config(?monthly_reward, AuxLedger),

    true = AuxMonthlyReward == AlteredMonthlyReward,

    ok.

aux_rewards_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    Ledger = ?config(ledger, Config),
    AuxLedger0 = blockchain_ledger_v1:bootstrap_aux(
        filename:join([BaseDir, "bootstrap_test.db"]),
        Ledger
    ),
    AuxLedger = blockchain_ledger_v1:mode(aux, AuxLedger0),

    %% construct some yolo rewards, two versions, real and aux
    Reward = 1000000000,
    Height = 10,
    Rewards = [blockchain_txn_reward_v1:new(<<"owner">>, <<"gateway">>, Reward, securities)],
    AuxRewards = [blockchain_txn_reward_v1:new(<<"owner">>, <<"gateway">>, Reward * 100, securities)],

    %% set both sets of rewards for aux ledger
    ok = blockchain_ledger_v1:set_aux_rewards(Height, Rewards, AuxRewards, AuxLedger),

    %% compare
    {ok, {Rewards, AuxRewards}} = blockchain_ledger_v1:get_aux_rewards(Height, AuxLedger),

    ok.
