-module(blockchain_subnetwork_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    add_subnetwork_test/1,
    failing_subnetwork_test/1
]).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%%   Running tests for this suite
%% @end
%%--------------------------------------------------------------------
all() ->
    [
        add_subnetwork_test,
        failing_subnetwork_test
    ].

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

end_per_testcase(_, Config) ->
    Sup = ?config(sup, Config),
    meck:unload(),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    test_utils:cleanup_tmp_dir(?config(base_dir, Config)),
    {comment, done}.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

add_subnetwork_test(Config) ->
    {NetworkPriv, _} = ?config(master_key, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Generate a random subnetwork signer
    [{SubnetworkPubkeyBin, {_SubnetworkPub, _SubnetworkPriv, SubnetworkSigFun}}] = test_utils:generate_keys(
        1
    ),

    %% Generate a random reward server
    [{RewardServerPubkeyBin, _}] = test_utils:generate_keys(1),

    NetworkSigfun = libp2p_crypto:mk_sig_fun(NetworkPriv),

    TT = mobile,
    Premine = 5000,
    T = blockchain_txn_add_subnetwork_v1:new(
        TT, SubnetworkPubkeyBin, [RewardServerPubkeyBin], Premine
    ),
    ST0 = blockchain_txn_add_subnetwork_v1:sign_subnetwork(T, SubnetworkSigFun),
    ST = blockchain_txn_add_subnetwork_v1:sign(ST0, NetworkSigfun),

    IsValid = blockchain_txn:is_valid(ST, Chain),
    ?assertEqual(ok, IsValid),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [ST]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    LedgerSubnetworks = blockchain_ledger_v1:subnetworks_v1(Ledger),
    LedgerSubnetwork = maps:get(mobile, LedgerSubnetworks),
    ?assertEqual(mobile, blockchain_ledger_subnetwork_v1:type(LedgerSubnetwork)),
    ?assertEqual(Premine, blockchain_ledger_subnetwork_v1:token_treasury(LedgerSubnetwork)),
    ?assertEqual([RewardServerPubkeyBin], blockchain_ledger_subnetwork_v1:reward_server_keys(LedgerSubnetwork)),
    ?assertEqual(0, blockchain_ledger_subnetwork_v1:hnt_treasury(LedgerSubnetwork)),
    ?assertEqual(0, blockchain_ledger_subnetwork_v1:nonce(LedgerSubnetwork)),

    ok.

failing_subnetwork_test(Config) ->
    {NetworkPriv, _} = ?config(master_key, Config),
    Chain = ?config(chain, Config),

    %% Generate a random subnetwork signer
    [{SubnetworkPubkeyBin, {_SubnetworkPub, _SubnetworkPriv, SubnetworkSigFun}}] = test_utils:generate_keys(
        1
    ),

    %% Generate a random reward server
    [{RewardServerPubkeyBin, _}] = test_utils:generate_keys(1),

    NetworkSigfun = libp2p_crypto:mk_sig_fun(NetworkPriv),

    ct:pal("subnetwork_sigfun: ~p", [SubnetworkSigFun]),
    ct:pal("network_sigfun: ~p", [NetworkSigfun]),

    TT = hst,
    Premine = 5000,
    T = blockchain_txn_add_subnetwork_v1:new(
        TT, SubnetworkPubkeyBin, [RewardServerPubkeyBin], Premine
    ),
    ST0 = blockchain_txn_add_subnetwork_v1:sign_subnetwork(T, SubnetworkSigFun),
    ST = blockchain_txn_add_subnetwork_v1:sign(ST0, NetworkSigfun),

    IsValid = blockchain_txn:is_valid(ST, Chain),
    ?assertEqual({error, invalid_token_hst}, IsValid),

    TT2 = hnt,
    T2 = blockchain_txn_add_subnetwork_v1:new(
        TT2, SubnetworkPubkeyBin, [RewardServerPubkeyBin], Premine
    ),
    ST1 = blockchain_txn_add_subnetwork_v1:sign_subnetwork(T2, SubnetworkSigFun),
    ST2 = blockchain_txn_add_subnetwork_v1:sign(ST1, NetworkSigfun),

    IsValid2 = blockchain_txn:is_valid(ST2, Chain),
    ?assertEqual({error, invalid_token_hnt}, IsValid2),

    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

extra_vars(_) ->
    #{?allowed_num_reward_server_keys => 1,
      ?subnetwork_reward_per_block_limit => 100}.

token_allocations(_, Config) ->
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    MobileBal = ?config(mobile_bal, Config),
    IOTBal = ?config(iot_bal, Config),
    #{hnt => HNTBal, hst => HSTBal, mobile => MobileBal, iot => IOTBal}.
