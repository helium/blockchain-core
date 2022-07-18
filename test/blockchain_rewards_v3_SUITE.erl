-module(blockchain_rewards_v3_SUITE).

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
    basic_test/1
]).

all() ->
    [
        basic_test
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

    HNTBal = 50000,
    HSTBal = 10000,
    MobileBal = 1000,
    IOTBal = 100,

    Config1 = [
        {hnt_bal, HNTBal},
        {hst_bal, HSTBal},
        {mobile_bal, MobileBal},
        {iot_bal, IOTBal}
        | Config0
    ],

    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config1)),

    ExtraVars = extra_vars(TestCase),
    TokenAllocations = token_allocations(TestCase, Config1),

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain_with_opts(
            #{
                balance =>
                    HNTBal,
                sec_balance =>
                    HSTBal,
                keys =>
                    {PrivKey, PubKey},
                in_consensus =>
                    false,
                have_init_dc =>
                    true,
                extra_vars =>
                    ExtraVars,
                token_allocations =>
                    TokenAllocations
            }
        ),

    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    Swarm = blockchain_swarm:tid(),
    N = length(ConsensusMembers),

    {EntryMod, _} = blockchain_ledger_v1:versioned_entry_mod_and_entries_cf(Ledger),

    [
        {hnt_bal, HNTBal},
        {hst_bal, HSTBal},
        {mobile_bal, MobileBal},
        {iot_bal, IOTBal},
        {entry_mod, EntryMod},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        Keys
        | Config1
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_TestCase, Config) ->
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

basic_test(Config) ->
    %% {NetworkPriv, _} = ?config(master_key, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),
    ConsensusMembers = ?config(consensus_members, Config),

    {ok, CGPercent} = blockchain:config(?consensus_percent, Ledger),
    {ok, SecPercent} = blockchain:config(?securities_percent, Ledger),
    TreasuryPercent = 1 - (CGPercent + SecPercent),

    %% There should be a treasury_pubkey_bin var set on ledger
    {ok, TPubkeyBin} = blockchain:config(?treasury_pubkey_bin, Ledger),

    %% Chain should be at height 1
    {ok, 1} = blockchain:height(Chain),

    %% Treasury should not have any rewards right now
    {error, address_entry_not_found} = blockchain_ledger_v1:find_entry(TPubkeyBin, Ledger),

    %% Add some blocks
    lists:foreach(
        fun(_) ->
            {ok, B} = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:tid())
        end,
        lists:seq(1, 29)
    ),

    %% Chain height should be 30 at this point
    {ok, 30} = blockchain:height(Chain),

    Start = 1,
    End = 30,

    %% Ensure that RewardsMD has the necessary keys
    {ok, RewardsMD} = blockchain_txn_rewards_v3:calculate_rewards_md(Start, End, Chain),
    [consensus_rewards, securities_rewards, treasury_rewards] = lists:sort(maps:keys(RewardsMD)),

    %% Ensure that the calculated rewards are within acceptable range
    TotalCGRewards = lists:sum(maps:values(maps:get(consensus_rewards, RewardsMD))),
    TotalSecRewards = lists:sum(maps:values(maps:get(securities_rewards, RewardsMD))),
    TotalTreasuryRewards = lists:sum(maps:values(maps:get(treasury_rewards, RewardsMD))),
    TotalRewards = TotalCGRewards + TotalSecRewards + TotalTreasuryRewards,
    CalcCGPercent = TotalCGRewards / TotalRewards,
    CalcSecPercent = TotalSecRewards / TotalRewards,
    CalcTreasuryPercent = TotalTreasuryRewards / TotalRewards,
    AcceptableDelta = 0.0000000001,
    ?assert(abs(CalcCGPercent - CGPercent) =< AcceptableDelta),
    ?assert(abs(CalcSecPercent - SecPercent) =< AcceptableDelta),
    ?assert(abs(CalcTreasuryPercent - TreasuryPercent) =< AcceptableDelta),

    %% Construct a rewards_v3 txn
    {ok, Rewards} = blockchain_txn_rewards_v3:calculate_rewards(Start, End, Chain),

    T = blockchain_txn_rewards_v3:new(Start, End, Rewards),
    %% NOTE: Signing rewards txn is pointless
    ok = blockchain_txn:is_valid(T, Chain),

    {ok, Block31} = test_utils:create_block(ConsensusMembers, [T]),
    _ = blockchain_gossip_handler:add_block(Block31, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block31)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block31}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 31}, blockchain:height(Chain)),
    ?assertEqual({ok, Block31}, blockchain:get_block(31, Chain)),

    %% There should be a treasury entry on ledger now
    {ok, TreasuryEntry} = blockchain_ledger_v1:find_entry(TPubkeyBin, Ledger),
    ?assertEqual(TotalTreasuryRewards, blockchain_ledger_entry_v2:balance(TreasuryEntry)),

    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
extra_vars(TestCase) ->
    #{secret := _TSec, public := TPub} = libp2p_crypto:generate_keys(ed25519),
    TPubkeyBin = libp2p_crypto:pubkey_to_bin(TPub),
    ExistingVars = on_chain_vars(TestCase),
    NewVars = #{
        ?allowed_num_reward_server_keys => 1,
        ?token_version => 2,
        ?subnetwork_reward_per_block_limit => 10,
        ?reward_version => 7,
        ?treasury_pubkey_bin => TPubkeyBin
    },
    maps:merge(ExistingVars, NewVars).

on_chain_vars(_TestCase) ->
    #{
        ?monthly_reward => 250000000000000,
        ?securities_percent => 0.33,
        ?consensus_percent => 0.06,
        ?election_interval => 30,
        ?election_restart_interval => 5,
        ?block_time => 60000,
        ?net_emissions_max_rate => 3424000000,
        ?election_version => 6
    }.

token_allocations(_, Config) ->
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    MobileBal = ?config(mobile_bal, Config),
    IOTBal = ?config(iot_bal, Config),
    #{hnt => HNTBal, hst => HSTBal, mobile => MobileBal, iot => IOTBal}.
