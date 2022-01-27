-module(blockchain_simple_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_txn_token_burn_v1_pb.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v1_pb.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-define(TEST_LOCATION, 631210968840687103).

-export([
    basic_test/1,
    reload_test/1,
    restart_test/1,
    htlc_payee_redeem_test/1,
    htlc_payer_redeem_test/1,
    poc_request_test/1,
    bogus_coinbase_test/1,
    bogus_coinbase_with_good_payment_test/1,
    export_test/1,
    delayed_ledger_test/1,
    fees_since_test/1,
    security_token_test/1,
    routing_test/1,
    block_save_failed_test/1,
    absorb_failed_test/1,
    missing_last_block_test/1,
    epoch_reward_test/1,
    net_emissions_reward_test/1,
    election_test/1,
    election_v3_test/1,
    election_v4_test/1,
    dataonly_gw_election_v4_test/1,
    light_gw_election_v4_test/1,
    election_v5_test/1,
    election_v6_test/1,
    chain_vars_test/1,
    chain_vars_set_unset_test/1,
    token_burn_test/1,
    payer_test/1,
    poc_sync_interval_test/1,
    zero_payment_v1_test/1,
    zero_amt_htlc_create_test/1,
    negative_payment_v1_test/1,
    negative_amt_htlc_create_test/1,
    update_gateway_oui_test/1,
    max_subnet_test/1,
    replay_oui_test/1,
    failed_txn_error_handling/1,
    genesis_no_var_validation_stay_invalid_test/1,
    genesis_no_var_validation_make_valid_test/1
]).

-import(blockchain_utils, [normalize_float/1]).

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
        basic_test,
        reload_test,
        restart_test,
        htlc_payee_redeem_test,
        htlc_payer_redeem_test,
        poc_request_test,
        bogus_coinbase_test,
        bogus_coinbase_with_good_payment_test,
        export_test,
        delayed_ledger_test,
        fees_since_test,
        security_token_test,
        routing_test,
        block_save_failed_test,
        absorb_failed_test,
        missing_last_block_test,
        epoch_reward_test,
        net_emissions_reward_test,
        election_test,
        election_v3_test,
        election_v4_test,
        dataonly_gw_election_v4_test,
        light_gw_election_v4_test,
        chain_vars_test,
        chain_vars_set_unset_test,
        token_burn_test,
        payer_test,
        poc_sync_interval_test,
        zero_payment_v1_test,
        zero_amt_htlc_create_test,
        negative_payment_v1_test,
        negative_amt_htlc_create_test,
        update_gateway_oui_test,
        max_subnet_test,
        replay_oui_test,
        failed_txn_error_handling,
        genesis_no_var_validation_stay_invalid_test,
        genesis_no_var_validation_make_valid_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),
    %% two tests rely on the swarm not being in the consensus group, so exclude them here
    ExtraVars = case TestCase of
                    election_v3_test ->
                        #{election_version => 3,
                          election_bba_penalty => 0.01,
                          election_seen_penalty => 0.03};
                    X when X == election_v4_test;
                           X == dataonly_gw_election_v4_test;
                           X == light_gw_election_v4_test ->
                        #{election_version => 4,
                          election_bba_penalty => 0.01,
                          election_seen_penalty => 0.03};
                    election_v5_test ->
                        #{election_version => 5,
                          validator_minimum_stake => ?bones(10000),
                          validator_liveness_grace_period => 50,
                          validator_liveness_interval => 200,
                          validator_penalty_filter => 10.0,
                          dkg_penalty => 1.0,
                          penalty_history_limit => 100,
                          election_bba_penalty => 0.01,
                          election_seen_penalty => 0.03};
                    election_v6_test ->
                        #{election_version => 6,
                          validator_minimum_stake => ?bones(10000),
                          validator_liveness_grace_period => 50,
                          validator_liveness_interval => 200,
                          validator_penalty_filter => 10.0,
                          dkg_penalty => 1.0,
                          penalty_history_limit => 100,
                          election_bba_penalty => 0.01,
                          election_seen_penalty => 0.03};
                    genesis_no_var_validation_stay_invalid_test ->
                        %% Intentionally supply an incorrect (out-of-bound) chain variable here
                        #{election_version => 10000};
                    genesis_no_var_validation_make_valid_test ->
                        %% Intentionally supply an incorrect (out-of-bound) chain variable here
                        #{election_version => 10000, vars_commit_delay => 1};
                    net_emissions_reward_test ->
                        #{?net_emissions_enabled => true,
                          ?rewards_txn_version => 2,
                          ?reward_version => 5,
                          ?monthly_reward => 10000,
                          ?net_emissions_max_rate => 40000};
                    _ ->
                        #{allow_zero_amount => false,
                          max_open_sc => 2,
                          min_expire_within => 10,
                          max_xor_filter_size => 1024*100,
                          max_xor_filter_num => 5,
                          max_subnet_size => 65536,
                          min_subnet_size => 8,
                          max_subnet_num => 20}
                end,

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain_with_opts(
            #{
                balance =>
                    Balance,
                keys =>
                    {PrivKey, PubKey},
                in_consensus =>
                    not lists:member(TestCase, [bogus_coinbase_test, bogus_coinbase_with_good_payment_test]),
                have_init_dc =>
                    true,
                extra_vars =>
                    ExtraVars
            }
        ),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    _ = lists:foreach(fun(Entry) ->
        Balance = blockchain_ledger_entry_v1:balance(Entry),
        0 = blockchain_ledger_entry_v1:nonce(Entry)
    end, maps:values(Entries)),

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
        Keys
        | Config0
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

basic_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

reload_test(Config) ->
    BaseDir = ?config(base_dir, Config),

    Balance = 5000,
    ConsensusMembers = ?config(consensus_members, Config),
    Sup = ?config(sup, Config),
    Opts = ?config(opts, Config),
    Chain0 = ?config(chain, Config),
    N0 = ?config(n, Config),

    % Add some blocks
    lists:foreach(
        fun(_) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(Block, Chain0, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 10)
    ),
    ?assertEqual({ok, 11}, blockchain:height(Chain0)),

    %% Kill this blockchain sup
    OldSwarm = blockchain_swarm:swarm(),
    Worker = whereis(blockchain_worker),
    ok = gen_server:stop(Sup),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Sup) end),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(OldSwarm) end),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Worker) end),

    %% TODO: why is this required?
    blockchain:close(Chain0),

    {InitialVars, _Config} = blockchain_ct_utils:create_vars(#{num_consensus_members => N0}),

    % Create new genesis block
    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance + 1)
                     || {Addr, _} <- ConsensusMembers],
    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new([Addr || {Addr, _} <- ConsensusMembers], <<"proof">>, 1, 0),
    Txs = InitialVars ++ GenPaymentTxs ++ [GenConsensusGroupTx],
    NewGenBlock = blockchain_block:new_genesis_block(Txs),
    GenDir = BaseDir ++ "2",  %% create a second/alternative base dir
    File = filename:join(GenDir, "genesis"),
    ok = test_utils:atomic_save(File, blockchain_block:serialize(NewGenBlock)),

    ct:pal("new start"),

    {ok, Sup1} = blockchain_sup:start_link([{update_dir, GenDir}|Opts]),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    Chain = blockchain_worker:blockchain(),
    {ok, HeadBlock} = blockchain:head_block(Chain),
    ?assertEqual(blockchain_block:hash_block(NewGenBlock), blockchain_block:hash_block(HeadBlock)),
    ?assertEqual(NewGenBlock, HeadBlock),
    ?assertEqual({ok, blockchain_block:hash_block(NewGenBlock)}, blockchain:genesis_hash(Chain)),
    ?assertEqual({ok, NewGenBlock}, blockchain:genesis_block(Chain)),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    true = erlang:exit(Sup1, normal),
    ok.

restart_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    GenDir = BaseDir ++ "2",  %% create a second/alternative base dir

    ConsensusMembers = ?config(consensus_members, Config),
    Sup = ?config(sup, Config),
    Opts = ?config(opts, Config),
    Chain0 = ?config(chain, Config),
    {ok, GenBlock} = blockchain:head_block(Chain0),

    % Add some blocks
    [LastBlock| _Blocks] = lists:foldl(
        fun(_, Acc) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(Block, Chain0, self(), blockchain_swarm:swarm()),
            timer:sleep(100),
            [Block|Acc]
        end,
        [],
        lists:seq(1, 10)
    ),
    ?assertEqual({ok, 11}, blockchain:height(Chain0)),

    %% Kill this blockchain sup
    OldSwarm = blockchain_swarm:swarm(),
    ok = gen_server:stop(Sup),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Sup) end),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(OldSwarm) end),

    %% TODO: why is this required?
    blockchain:close(Chain0),

    % Restart with an empty 'GenDir'
    {ok, Sup1} = blockchain_sup:start_link([{update_dir, GenDir}|Opts]),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    Chain = blockchain_worker:blockchain(),
    {ok, HeadBlock} = blockchain:head_block(Chain),
    ?assertEqual(blockchain_block:hash_block(LastBlock), blockchain_block:hash_block(HeadBlock)),
    ?assertEqual({ok, LastBlock}, blockchain:head_block(Chain)),
    ?assertEqual({ok, blockchain_block:hash_block(GenBlock)}, blockchain:genesis_hash(Chain)),
    ?assertEqual({ok, GenBlock}, blockchain:genesis_block(Chain)),
    ?assertEqual({ok, 11}, blockchain:height(Chain)),

   %% Kill this blockchain sup
    OldSwarm2 = blockchain_swarm:swarm(),
    ok = gen_server:stop(Sup1),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Sup1) end),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(OldSwarm2) end),

    blockchain:close(Chain),

    % Restart with the existing genesis block in 'GenDir'
    ok = filelib:ensure_dir(filename:join([GenDir, "genesis"])),
    ok = file:write_file(filename:join([GenDir, "genesis"]), blockchain_block:serialize(GenBlock)),

    {ok, Sup2} = blockchain_sup:start_link([{update_dir, GenDir}|Opts]),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    Chain1 = blockchain_worker:blockchain(),
    {ok, HeadBlock1} = blockchain:head_block(Chain1),
    ?assertEqual(blockchain_block:hash_block(LastBlock), blockchain_block:hash_block(HeadBlock1)),
    ?assertEqual({ok, LastBlock}, blockchain:head_block(Chain1)),
    ?assertEqual({ok, blockchain_block:hash_block(GenBlock)}, blockchain:genesis_hash(Chain1)),
    ?assertEqual({ok, GenBlock}, blockchain:genesis_block(Chain1)),
    ?assertEqual({ok, 11}, blockchain:height(Chain1)),

    true = erlang:exit(Sup2, normal),
    ok.


htlc_payee_redeem_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    PubKey = ?config(pubkey, Config),
    PrivKey = ?config(privkey, Config),
    Chain = ?config(chain, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    % Create a Payee
    #{public := PayeePubKey, secret := PayeePrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PayeePubKey),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(33),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    CreateTx = blockchain_txn_create_htlc_v1:new(Payer, Payee, HTLCAddress, Hashlock, 3, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedCreateTx = blockchain_txn_create_htlc_v1:sign(CreateTx, SigFun),

    %% confirm the txns passes validations
    ?assertEqual(ok, blockchain_txn_create_htlc_v1:is_valid(SignedCreateTx, Chain)),

    % send some money to the payee so they have enough to pay the fee for redeeming
    Tx = blockchain_txn_payment_v1:new(Payer, Payee, 100, 2),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),

    %% check both txns are valid, in context
    ?assertMatch({_, []}, blockchain_txn:validate([SignedCreateTx, SignedTx], Chain)),

    %% these transactions depend on each other, but they should be able to exist in the same block
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedCreateTx, SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block), HeadHash),
    ?assertEqual({ok, Block}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    % Check that the Payer balance has been reduced by 2500
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    ?assertEqual(Balance - 2600, blockchain_ledger_entry_v1:balance(NewEntry0)),

    % Check that the HLTC address exists and has the correct balance, hashlock and timelock
    % NewHTLC0 = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain:ledger(Chain)),
    {ok, NewHTLC0} = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain:ledger(Chain)),
    ?assertEqual(2500, blockchain_ledger_htlc_v1:balance(NewHTLC0)),
    ?assertEqual(Hashlock, blockchain_ledger_htlc_v1:hashlock(NewHTLC0)),
    ?assertEqual(3, blockchain_ledger_htlc_v1:timelock(NewHTLC0)),

    % Try and redeem
    RedeemSigFun = libp2p_crypto:mk_sig_fun(PayeePrivKey),
    RedeemTx = blockchain_txn_redeem_htlc_v1:new(Payee, HTLCAddress, <<"sharkfed">>),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx, RedeemSigFun),
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check that the second block with the Redeem TX was mined properly
    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block2), HeadHash2),
    ?assertEqual({ok, Block2}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    % Check that the Payee now owns 2500
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payee, blockchain:ledger(Chain)),
    ?assertEqual(2600, blockchain_ledger_entry_v1:balance(NewEntry1)),

    % confirm the replay of the previously absorbed txn fails validations
    % as we are reusing the same nonce
    ?assertEqual({error,{bad_nonce,{create_htlc,1,2}}}, blockchain_txn_create_htlc_v1:is_valid(SignedCreateTx, Chain)),
    ok.

htlc_payer_redeem_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    PubKey = ?config(pubkey, Config),
    PrivKey = ?config(privkey, Config),
    Chain = ?config(chain, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(33),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    CreateTx = blockchain_txn_create_htlc_v1:new(Payer, Payer, HTLCAddress, Hashlock, 3, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedCreateTx = blockchain_txn_create_htlc_v1:sign(CreateTx, SigFun),

    %% confirm the txns passes validations
    ?assertEqual(ok, blockchain_txn_create_htlc_v1:is_valid(SignedCreateTx, Chain)),

      {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedCreateTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block), HeadHash),
    ?assertEqual({ok, Block}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    % Check that the Payer balance has been reduced by 2500
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),

    % Check that the HLTC address exists and has the correct balance, hashlock and timelock
    % NewHTLC0 = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain:ledger(Chain)),
    {ok, NewHTLC0} = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain:ledger(Chain)),
    ?assertEqual(2500, blockchain_ledger_htlc_v1:balance(NewHTLC0)),
    ?assertEqual(Hashlock, blockchain_ledger_htlc_v1:hashlock(NewHTLC0)),
    ?assertEqual(3, blockchain_ledger_htlc_v1:timelock(NewHTLC0)),

    % Mine another couple of blocks
    {ok, Block2} = test_utils:create_block(ConsensusMembers, []),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),
    {ok, Block3} = test_utils:create_block(ConsensusMembers, []),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:swarm()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check we are at height 4
    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block3), HeadHash2),
    ?assertEqual({ok, Block3}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 4}, blockchain:height(Chain)),

    ?assertEqual({ok, 4}, blockchain_ledger_v1:current_height(blockchain:ledger(Chain))),

    % Try and redeem
    RedeemTx = blockchain_txn_redeem_htlc_v1:new(Payer, HTLCAddress, <<"sharkfed">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx, SigFun),
    {ok, Block4} = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    _ = blockchain_gossip_handler:add_block(Block4, Chain, self(), blockchain_swarm:swarm()),

    % Check that the Payer now owns 5000 again
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    ?assertEqual(5000, blockchain_ledger_entry_v1:balance(NewEntry1)),

    % confirm the replay of the previously absorbed txn fails validations
    % as we are reusing the same nonce
    ?assertEqual({error,{bad_nonce,{create_htlc,1,1}}}, blockchain_txn_create_htlc_v1:is_valid(SignedCreateTx, Chain)),

    ok.

poc_request_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    PubKey = ?config(pubkey, Config),
    PrivKey = ?config(privkey, Config),
    Owner = libp2p_crypto:pubkey_to_bin(PubKey),
    Chain = ?config(chain, Config),
    Balance = ?config(balance, Config),

    Ledger = blockchain:ledger(Chain),
    Rate = 100000000,
    {Priv, _} = ?config(master_key, Config),

    % fake an oracle price and a burn price, these figures are not representative
    % TODO: setup actual onchain oracle prices, get rid of these mecks
    OP = 1000,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),

    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance0 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry0),

    %% NOTE: the token burn exchange rate block is not required for most of this test to run
    %% it should be removed but the POC is using it atm - needs refactored
    Vars = #{token_burn_exchange_rate => Rate},
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),
    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    % Step 2: Token burn txn should pass now
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    BurnTx0 = blockchain_txn_token_burn_v1:new(Owner, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, OwnerSigFun),
    {ok, Block23} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 23}, blockchain:height(Chain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Owner, Ledger),
    ?assertEqual(Balance - 10, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance1 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry1),
    ?assertEqual(10 * OP + DCBalance0, DCBalance1),

    % Create a Gateway
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),


    % Add a Gateway
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),
      {ok, Block24} = test_utils:create_block(ConsensusMembers, [SignedGatewayAddGatewayTx]),
    _ = blockchain_gossip_handler:add_block(Block24, Chain, self(), blockchain_swarm:swarm()),

    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block24), HeadHash),
    ?assertEqual({ok, Block24}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 24}, blockchain:height(Chain)),

    % Check that the Gateway is there
    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),
    ?assertEqual(Owner, blockchain_ledger_gateway_v2:owner_address(GwInfo)),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, ?TEST_LOCATION, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),

    {ok, Block25} = test_utils:create_block(ConsensusMembers, [SignedAssertLocationTx]),
    ok = blockchain_gossip_handler:add_block(Block25, Chain, self(), blockchain_swarm:swarm()),
    timer:sleep(500),

    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block25), HeadHash2),
    ?assertEqual({ok, Block25}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 25}, blockchain:height(Chain)),

    % Create the PoC challenge request txn
    Keys0 = libp2p_crypto:generate_keys(ecc_compact),
    Secret0 = libp2p_crypto:keys_to_bin(Keys0),
    #{public := OnionCompactKey0} = Keys0,
    SecretHash0 = crypto:hash(sha256, Secret0),
    OnionKeyHash0 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey0)),
    PoCReqTxn0 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash0, OnionKeyHash0, blockchain_block:hash_block(Block2), 1),
    SignedPoCReqTxn0 = blockchain_txn_poc_request_v1:sign(PoCReqTxn0, GatewaySigFun),
    {ok, Block26} = test_utils:create_block(ConsensusMembers, [SignedPoCReqTxn0]),
    _ = blockchain_gossip_handler:add_block(Block26, Chain, self(), blockchain_swarm:swarm()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 26} =:= blockchain:height(Chain) end),

    Ledger = blockchain:ledger(Chain),
    {ok, HeadHash3} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block26), HeadHash3),
    ?assertEqual({ok, Block26}, blockchain:get_block(HeadHash3, Chain)),
    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GwInfo2} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(26, blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo2)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GwInfo2)),

    % Check that the PoC info
    {ok, [PoC]} = blockchain_ledger_v1:find_pocs(OnionKeyHash0, Ledger),
    ?assertEqual(SecretHash0, blockchain_ledger_poc_v2:secret_hash(PoC)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_poc_v2:onion_key_hash(PoC)),
    ?assertEqual(Gateway, blockchain_ledger_poc_v2:challenger(PoC)),

    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_Txn, _Chain) -> ok end),

    PoCReceiptsTxn = blockchain_txn_poc_receipts_v1:new(Gateway, Secret0, OnionKeyHash0, []),
    SignedPoCReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(PoCReceiptsTxn, GatewaySigFun),
    {ok, Block27} = test_utils:create_block(ConsensusMembers, [SignedPoCReceiptsTxn]),
    _ = blockchain_gossip_handler:add_block(Block27, Chain, self(), blockchain_swarm:swarm()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 27} =:= blockchain:height(Chain) end),

    Block62 = lists:foldl(
        fun(_, _) ->
            {ok, B} = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:swarm()),
            timer:sleep(10),
            B
        end,
        <<>>,
        lists:seq(1, 35)
    ),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 62} =:= blockchain:height(Chain) end),

    % Create another PoC challenge request txn
    Keys1 = libp2p_crypto:generate_keys(ecc_compact),
    Secret1 = libp2p_crypto:keys_to_bin(Keys1),
    #{public := OnionCompactKey1} = Keys1,
    SecretHash1 = crypto:hash(sha256, Secret1),
    OnionKeyHash1 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey1)),
    PoCReqTxn1 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash1, OnionKeyHash1, blockchain_block:hash_block(Block62), 1),
    SignedPoCReqTxn1 = blockchain_txn_poc_request_v1:sign(PoCReqTxn1, GatewaySigFun),
    {ok, Block63} = test_utils:create_block(ConsensusMembers, [SignedPoCReqTxn1]),
    _ = blockchain_gossip_handler:add_block(Block63, Chain, self(), blockchain_swarm:swarm()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 63} =:= blockchain:height(Chain) end),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_pocs(OnionKeyHash0, Ledger)),
    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GwInfo3} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(63, blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo3)),
    ?assertEqual(OnionKeyHash1, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GwInfo3)),

    ?assert(meck:validate(blockchain_txn_poc_receipts_v1)),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_txn_poc_receipts_v1),
    meck:unload(blockchain_ledger_v1),
    ok.

bogus_coinbase_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    [{FirstMemberAddr, _} | _] = ConsensusMembers,
    Chain = ?config(chain, Config),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    %% Lets give the first member a bunch of coinbase tokens
    BogusCoinbaseTxn = blockchain_txn_coinbase_v1:new(FirstMemberAddr, 999999),

    %% This should error out cuz this is an invalid txn
    {error, {invalid_txns, [{BogusCoinbaseTxn, _InvalidReason}]}} = test_utils:create_block(ConsensusMembers, [BogusCoinbaseTxn]),

    %% Check that the chain didn't grow
    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    ok.

bogus_coinbase_with_good_payment_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    [{FirstMemberAddr, _} | _] = ConsensusMembers,
    Chain = ?config(chain, Config),

    %% Lets give the first member a bunch of coinbase tokens
    BogusCoinbaseTxn = blockchain_txn_coinbase_v1:new(FirstMemberAddr, 999999),

    %% Create a good payment transaction as well
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedGoodPaymentTxn = blockchain_txn_payment_v1:sign(Tx, SigFun),

    %% This should error out cuz this is an invalid txn
    {error, {invalid_txns, [{BogusCoinbaseTxn, _InvalidReason}]}} = test_utils:create_block(ConsensusMembers,
                                                                          [BogusCoinbaseTxn, SignedGoodPaymentTxn]),
    %% Check that the chain didnt' grow
    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    ok.

export_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    GenesisMembers = ?config(genesis_members, Config),
    Balance = ?config(balance, Config),
    [_,
     {Payer1, {PayerPubKey1, PayerPrivKey1, _}},
     {Payer2, {_, PayerPrivKey2, _}},
     {Payer3, {_, PayerPrivKey3, _}}
     | _] = ConsensusMembers,
    Amount = 2500,
    Fee = 0,
    N = length(ConsensusMembers),
    Chain = ?config(chain, Config),
    N = ?config(n, Config),

    Ledger = blockchain:ledger(Chain),

    % fake an oracle price and a burn price, these figures are not representative
    % TODO: setup actual onchain oracle prices, get rid of these mecks
    OP = 1000,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),

    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 20)
    ),

    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    Owner = libp2p_crypto:pubkey_to_bin(PayerPubKey1),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey1),

    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance0 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry0),

    % Step 2: Token burn txn should pass now
    BurnTx0 = blockchain_txn_token_burn_v1:new(Owner, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, OwnerSigFun),
    {ok, Block22} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block22, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block22)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block22}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 22}, blockchain:height(Chain)),
    ?assertEqual({ok, Block22}, blockchain:get_block(22, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Owner, Ledger),
    ?assertEqual(Balance - 10, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance1 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry1),
    ?assertEqual(10 * OP + DCBalance0, DCBalance1),

    % Create a Gateway
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),

    % Add a Gateway
    AddGatewayTx0 = blockchain_txn_add_gateway_v1:new(Owner, Gateway),
    AddGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx0, blockchain_txn_add_gateway_v1:calculate_fee(AddGatewayTx0, Chain)),
    AddGatewayTx = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx1, blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGatewayTx1, Chain)),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, ?TEST_LOCATION, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),

    %% adding the gateway and asserting a location depend on each other, but they should be able to appear in the same block
    PaymentTxn1 = test_utils:create_payment_transaction(Payer1, PayerPrivKey1, Amount, 2, blockchain_swarm:pubkey_bin()),
    PaymentTxn2 = test_utils:create_payment_transaction(Payer2, PayerPrivKey2, Amount, 1, blockchain_swarm:pubkey_bin()),
    PaymentTxn3 = test_utils:create_payment_transaction(Payer3, PayerPrivKey3, Amount, 1, blockchain_swarm:pubkey_bin()),
    Txns0 = [SignedAssertLocationTx, PaymentTxn2, SignedGatewayAddGatewayTx, PaymentTxn1, PaymentTxn3],
    Txns1 = lists:sort(fun blockchain_txn:sort/2, Txns0),
    {ok, Block23} = test_utils:create_block(ConsensusMembers, Txns1),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 23}, blockchain:height(Chain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, Chain)),
    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),

    ?assertEqual(Owner, blockchain_ledger_gateway_v2:owner_address(GwInfo)),

    timer:sleep(500),

    [{securities, Securities},
     {accounts, Accounts},
     {gateways, Gateways},
     {chain_vars, _ChainVars},
     {dcs, DCs}
    ] = blockchain_ledger_exporter_v1:export(blockchain:ledger(Chain)),

    ct:pal("gateways ~p", [Gateways]),

    %% check DC balance for Payer1
    Payer1Addr = libp2p_crypto:bin_to_b58(Payer1),
    [[{address, _}, {dc_balance, Payer1DCBalance0}]] =
        [DC || [{address, Addr}, {dc_balance, _}]=DC <- DCs, Addr =:= Payer1Addr],
    ?assertEqual(Payer1DCBalance0, DCBalance0 + (OP * 10 - 2)),

    %% we added this after we add all of the existing gateways in the
    %% genesis block with nonce 0.  we filter those out to make sure
    %% we're still getting the txn that we're looking for
    Gateways1 = [G || [_, _, _, {nonce, Nonce}] = G <- Gateways, Nonce == 1],

    ?assertEqual([[{gateway_address, libp2p_crypto:pubkey_to_b58(GatewayPubKey)},
                   {owner_address, libp2p_crypto:pubkey_to_b58(PayerPubKey1)},
                   {location,?TEST_LOCATION},
                   {nonce,1}]], Gateways1),

    FilteredExportedAccounts = lists:foldl(fun(Account, Acc) ->
                                                   AccountAddress = proplists:get_value(address, Account),
                                                   case libp2p_crypto:bin_to_b58(Payer1) == AccountAddress orelse
                                                        libp2p_crypto:bin_to_b58(Payer2) == AccountAddress orelse
                                                        libp2p_crypto:bin_to_b58(Payer3) == AccountAddress
                                                   of
                                                       true -> [Account | Acc];
                                                       false -> Acc
                                                   end
                                           end, [], Accounts),
    lists:all(fun(Account) ->
                      (Balance - Amount - Fee) == proplists:get_value(balance, Account)
              end, FilteredExportedAccounts),

    %% check all genesis members are included in the exported securities
    ?assertEqual(length(Securities), length(GenesisMembers)),

    %% check security balance for each member
    lists:all(fun(Security) ->
                      Token = proplists:get_value(token, Security),
                      Token == Balance
              end, Securities),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ok.


delayed_ledger_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    Balance = ?config(balance, Config),

    Ledger = blockchain:ledger(Chain),
    ?assertEqual({ok, 1}, blockchain_ledger_v1:current_height(Ledger)),

    DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
    ?assertEqual({ok, 1}, blockchain_ledger_v1:current_height(DelayedLedger)),

        % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Payee = blockchain_swarm:pubkey_bin(),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    lists:foreach(
        fun(X) ->
            Tx = blockchain_txn_payment_v1:new(Payer, Payee, 1, X),
            SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
            {ok, B} = test_utils:create_block(ConsensusMembers, [SignedTx]),
            _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 100)
    ),

    % Check heights of Ledger and delayed ledger should be 50 block behind
    ?assertEqual({ok, 101}, blockchain_ledger_v1:current_height(Ledger)),
    ?assertEqual({ok, 51}, blockchain_ledger_v1:current_height(DelayedLedger)),

    % Check balances of payer and payee in edger
    {ok, Entry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 100, blockchain_ledger_entry_v1:balance(Entry1)),

    {ok, Entry2} = blockchain_ledger_v1:find_entry(Payee, Ledger),
    ?assertEqual(Balance + 100, blockchain_ledger_entry_v1:balance(Entry2)),

    % Check balances of payer and payee in  delayed ledger
    {ok, Entry3} = blockchain_ledger_v1:find_entry(Payer, DelayedLedger),
    ?assertEqual(Balance - 50, blockchain_ledger_entry_v1:balance(Entry3)),

    {ok, Entry4} = blockchain_ledger_v1:find_entry(Payee, DelayedLedger),
    ?assertEqual(Balance + 50, blockchain_ledger_entry_v1:balance(Entry4)),

    % Same as above except receting context/cache
    {ok, Entry1} = blockchain_ledger_v1:find_entry(Payer, blockchain_ledger_v1:new_context(Ledger)),
    ?assertEqual(Balance - 100, blockchain_ledger_entry_v1:balance(Entry1)),

    {ok, Entry2} = blockchain_ledger_v1:find_entry(Payee, blockchain_ledger_v1:new_context(Ledger)),
    ?assertEqual(Balance + 100, blockchain_ledger_entry_v1:balance(Entry2)),

    {ok, Entry3} = blockchain_ledger_v1:find_entry(Payer, blockchain_ledger_v1:new_context(DelayedLedger)),
    ?assertEqual(Balance - 50, blockchain_ledger_entry_v1:balance(Entry3)),

    {ok, Entry4} = blockchain_ledger_v1:find_entry(Payee, blockchain_ledger_v1:new_context(DelayedLedger)),
    ?assertEqual(Balance + 50, blockchain_ledger_entry_v1:balance(Entry4)),

    % We should not allow to query prior delayed ledger and obviously neither in the futur
    ?assertEqual({error, height_too_old}, blockchain:ledger_at(50, Chain)),
    ?assertEqual({error, invalid_height}, blockchain:ledger_at(107, Chain)),

    % Now lets go forward a block & check balances again
    {ok, LedgerAt} = blockchain:ledger_at(100, Chain),

    {ok, Entry5} = blockchain_ledger_v1:find_entry(Payer, LedgerAt),
    ?assertEqual(Balance - 99, blockchain_ledger_entry_v1:balance(Entry5)),

    {ok, Entry6} = blockchain_ledger_v1:find_entry(Payee, LedgerAt),
    ?assertEqual(Balance + 99, blockchain_ledger_entry_v1:balance(Entry6)),

     % Check balances of payer and payee in  delayed ledger again making sure context did not explode
    {ok, Entry3} = blockchain_ledger_v1:find_entry(Payer, DelayedLedger),
    ?assertEqual(Balance - 50, blockchain_ledger_entry_v1:balance(Entry3)),

    {ok, Entry4} = blockchain_ledger_v1:find_entry(Payee, DelayedLedger),
    ?assertEqual(Balance + 50, blockchain_ledger_entry_v1:balance(Entry4)),
    ok.

fees_since_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Payee = blockchain_swarm:pubkey_bin(),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    %% a lotta mecking going on
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:new(blockchain_txn_payment_v1, [passthrough]),

    meck:expect(blockchain_ledger_v1, check_dc_balance, fun(_, _, _) -> ok end),
    meck:expect(blockchain_ledger_v1, check_dc_or_hnt_balance, fun(_, _, _, _) -> ok end),
    meck:expect(blockchain_ledger_v1, debit_fee, fun(_, _, _, _, _, _) -> ok end),

    meck:expect(blockchain_txn_payment_v1, calculate_fee, fun(_, _) -> 10 end),
    % Add 100 txns with 1 fee each
    lists:foreach(
        fun(X) ->
            Tx = blockchain_txn_payment_v1:new(Payer, Payee, 1, X),
            ExpectedFeeSize = blockchain_txn_payment_v1:calculate_fee(Tx, Chain),  %% works out at 3 dc
            Tx1 = Tx#blockchain_txn_payment_v1_pb{fee=ExpectedFeeSize},
            SignedTx = blockchain_txn_payment_v1:sign(Tx1, SigFun),
            {ok, B} = test_utils:create_block(ConsensusMembers, [SignedTx]),
            _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 100)
    ),

    ?assertEqual({error, bad_height}, blockchain:fees_since(100000, Chain)),
    ?assertEqual({error, bad_height}, blockchain:fees_since(1, Chain)),
    ?assertEqual({ok, 100*10}, blockchain:fees_since(2, Chain)),
    ?assert(meck:validate(blockchain_ledger_v1)),
    ?assert(meck:validate(blockchain_txn_payment_v1)),
    meck:unload(blockchain_ledger_v1),
    meck:unload(blockchain_txn_payment_v1).

security_token_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_security_exchange_v1:new(Payer, Recipient, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_security_exchange_v1:sign(Tx, SigFun),
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),
    {ok, NewEntry0} = blockchain_ledger_v1:find_security_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_security_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_security_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500, blockchain_ledger_security_entry_v1:balance(NewEntry1)),
    ok.

routing_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    Swarm = ?config(swarm, Config),
    Ledger = blockchain:ledger(Chain),

    [_, {Payer, {_, PayerPrivKey, _}}, {Router1, {_, RouterPrivKey, _}}|_] = ConsensusMembers,
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    RouterSigFun = libp2p_crypto:mk_sig_fun(RouterPrivKey),

    meck:new(blockchain_txn_oui_v1, [no_link, passthrough]),
    %% an OUI has a default staking fee of 1, the old version of the test was defaulting this to zero
    %% and then mecking out the is_valid to prevent it being rejected due to expected /= presented fee
    %% since we can no longer override the default fee, now have to meck out the check_db & debit fee functions instead
    %% as the account does not have any credits, but the mecking of the is_valid can be removed
    meck:expect(blockchain_ledger_v1, check_dc_or_hnt_balance, fun(_, _, _, _) -> ok end),
    meck:expect(blockchain_ledger_v1, debit_fee, fun(_, _, _, _, _, _) -> ok end),

    OUI1 = 1,
    Addresses0 = [libp2p_swarm:pubkey_bin(Swarm), Router1],
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn0 = blockchain_txn_oui_v1:new(OUI1, Payer, Addresses0, Filter, 8),
    SignedOUITxn0 = blockchain_txn_oui_v1:sign(OUITxn0, SigFun),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    {ok, Block0} = test_utils:create_block(ConsensusMembers, [SignedOUITxn0]),
    _ = blockchain_gossip_handler:add_block(Block0, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 2} == blockchain:height(Chain) end),

    Routing0 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses0, Filter,
                                                <<0:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(8)):23/integer-unsigned-big>>, 0),
    ?assertEqual({ok, Routing0}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    #{public := NewPubKey, secret := _PrivKey} = libp2p_crypto:generate_keys(ed25519),
    Addresses1 = [libp2p_crypto:pubkey_to_bin(NewPubKey)],
    OUITxn2 = blockchain_txn_routing_v1:update_router_addresses(OUI1, Payer, Addresses1, 1),
    SignedOUITxn2 = blockchain_txn_routing_v1:sign(OUITxn2, SigFun),
    {ok, Block1} = test_utils:create_block(ConsensusMembers, [SignedOUITxn2]),
    _ = blockchain_gossip_handler:add_block(Block1, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 3} == blockchain:height(Chain) end),

    Routing1 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses1, Filter,
                                                <<0:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(8)):23/integer-unsigned-big>>, 1),
    ?assertEqual({ok, Routing1}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    OUITxn3 = blockchain_txn_routing_v1:request_subnet(OUI1, Payer, 32, 2),
    SignedOUITxn3 = blockchain_txn_routing_v1:sign(OUITxn3, SigFun),
    {Filter2, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn4 = blockchain_txn_routing_v1:update_xor(OUI1, Payer, 0, Filter2, 3),
    SignedOUITxn4 = blockchain_txn_routing_v1:sign(OUITxn4, SigFun),
    {Filter2a, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn4a = blockchain_txn_routing_v1:new_xor(OUI1, Payer, Filter2a, 4),
    SignedOUITxn4a = blockchain_txn_routing_v1:sign(OUITxn4a, SigFun),

    {ok, Block2} = test_utils:create_block(ConsensusMembers, [SignedOUITxn3, SignedOUITxn4, SignedOUITxn4a]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 4} == blockchain:height(Chain) end),

    {ok, Routing2} = blockchain_ledger_v1:find_routing(OUI1, Ledger),
    %Routing2 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses1, Filter2, <<0,0,0,127,255,254>>, 3),
    ?assertEqual([<<0:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(8)):23/integer-unsigned-big>>,
                  <<32:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(32)):23/integer-unsigned-big>>],
                 blockchain_ledger_routing_v1:subnets(Routing2)),
    ?assertEqual([Filter2, Filter2a], blockchain_ledger_routing_v1:filters(Routing2)),
    ?assertEqual(4, blockchain_ledger_routing_v1:nonce(Routing2)),

    %% test updating with invalid filter
    Filter3 = <<"lolsdf">>,
    OUITxn5 = blockchain_txn_routing_v1:update_xor(OUI1, Payer, 0, Filter3, 5),
    SignedOUITxn5 = blockchain_txn_routing_v1:sign(OUITxn5, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn5]),

    Filter4 = crypto:strong_rand_bytes(1024*1024),
    OUITxn6 = blockchain_txn_routing_v1:update_xor(OUI1, Payer, 0, Filter4, 5),
    SignedOUITxn6 = blockchain_txn_routing_v1:sign(OUITxn6, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn6]),

    OUITxn6a = blockchain_txn_routing_v1:update_xor(OUI1, Payer, 2, Filter, 5),
    SignedOUITxn6a = blockchain_txn_routing_v1:sign(OUITxn6a, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn6a]),

    OUITxn6b = blockchain_txn_routing_v1:update_xor(OUI1, Payer, 5, Filter, 5),
    SignedOUITxn6b = blockchain_txn_routing_v1:sign(OUITxn6b, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn6b]),

    %% test invalid subnet_size
    OUITxn7 = blockchain_txn_routing_v1:request_subnet(OUI1, Payer, 31, 5),
    SignedOUITxn7 = blockchain_txn_routing_v1:sign(OUITxn7, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn7]),
    OUITxn8 = blockchain_txn_routing_v1:request_subnet(OUI1, Payer, 2, 5),
    SignedOUITxn8 = blockchain_txn_routing_v1:sign(OUITxn8, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn8]),
    OUITxn9 = blockchain_txn_routing_v1:request_subnet(OUI1, Payer, 4294967296, 5),
    SignedOUITxn9 = blockchain_txn_routing_v1:sign(OUITxn9, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn9]),

    %% test adding an invalid xor
    OUITxn10 = blockchain_txn_routing_v1:new_xor(OUI1, Payer, Filter3, 5),
    SignedOUITxn10 = blockchain_txn_routing_v1:sign(OUITxn10, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn10]),
    OUITxn11 = blockchain_txn_routing_v1:new_xor(OUI1, Payer, Filter4, 5),
    SignedOUITxn11 = blockchain_txn_routing_v1:sign(OUITxn11, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn11]),

    %% fill up the xor list for this node
    {Filter2b, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn4b = blockchain_txn_routing_v1:new_xor(OUI1, Payer, Filter2b, 5),
    SignedOUITxn4b = blockchain_txn_routing_v1:sign(OUITxn4b, SigFun),

    {Filter2c, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn4c = blockchain_txn_routing_v1:new_xor(OUI1, Payer, Filter2c, 6),
    SignedOUITxn4c = blockchain_txn_routing_v1:sign(OUITxn4c, SigFun),

    {Filter2d, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn4d = blockchain_txn_routing_v1:new_xor(OUI1, Payer, Filter2d, 7),
    SignedOUITxn4d = blockchain_txn_routing_v1:sign(OUITxn4d, SigFun),

    {ok, Block3} = test_utils:create_block(ConsensusMembers, [SignedOUITxn4b, SignedOUITxn4c, SignedOUITxn4d]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 5} == blockchain:height(Chain) end),

    {ok, Routing3} = blockchain_ledger_v1:find_routing(OUI1, Ledger),
    %Routing2 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses1, Filter2, <<0,0,0,127,255,254>>, 3),
    ?assertEqual([<<0:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(8)):23/integer-unsigned-big>>,
                  <<32:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(32)):23/integer-unsigned-big>>],
                 blockchain_ledger_routing_v1:subnets(Routing3)),
    ?assertEqual([Filter2, Filter2a, Filter2b, Filter2c, Filter2d], blockchain_ledger_routing_v1:filters(Routing3)),
    ?assertEqual(7, blockchain_ledger_routing_v1:nonce(Routing3)),

    %% no more filters can be added
    {Filter2e, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn4e = blockchain_txn_routing_v1:new_xor(OUI1, Payer, Filter2e, 8),
    SignedOUITxn4e = blockchain_txn_routing_v1:sign(OUITxn4e, SigFun),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn4e]),

    OUI2 = 2,
    OUITxn01 = blockchain_txn_oui_v1:new(OUI2, Payer, Addresses0, Filter, 8),
    SignedOUITxn01 = blockchain_txn_oui_v1:sign(OUITxn01, SigFun),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_routing(OUI2, Ledger)),

    {ok, Block4} = test_utils:create_block(ConsensusMembers, [SignedOUITxn01]),
    _ = blockchain_gossip_handler:add_block(Block4, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 6} == blockchain:height(Chain) end),

    Routing4 = blockchain_ledger_routing_v1:new(OUI2, Payer, Addresses0, Filter, <<0,0,4,127,255,254>>, 0),
    ?assertEqual({ok, Routing4}, blockchain_ledger_v1:find_routing(OUI2, Ledger)),

    %% Test if router (in addresses can modify filters)
    {FilterX, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn02 = blockchain_txn_routing_v1:new_xor(OUI2, Router1, FilterX, 1),
    SignedOUITxn02 = blockchain_txn_routing_v1:sign(OUITxn02, RouterSigFun),
    {ok, Block5} = test_utils:create_block(ConsensusMembers, [SignedOUITxn02]),
    _ = blockchain_gossip_handler:add_block(Block5, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 7} == blockchain:height(Chain) end),

    {ok, Routing5} = blockchain_ledger_v1:find_routing(OUI2, Ledger),
    ?assertEqual([Filter, FilterX], blockchain_ledger_routing_v1:filters(Routing5)),

    %% Test if OUI owner can modify filters
    {FilterY, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn03 = blockchain_txn_routing_v1:new_xor(OUI2, Payer, FilterY, 2),
    SignedOUITxn03 = blockchain_txn_routing_v1:sign(OUITxn03, SigFun),
    {ok, Block6} = test_utils:create_block(ConsensusMembers, [SignedOUITxn03]),
    _ = blockchain_gossip_handler:add_block(Block6, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 8} == blockchain:height(Chain) end),

    {ok, Routing6} = blockchain_ledger_v1:find_routing(OUI2, Ledger),
    ?assertEqual([Filter, FilterX, FilterY], blockchain_ledger_routing_v1:filters(Routing6)),

    %% Negative test
    #{secret := NegPivKey, public := NegPubKey} = libp2p_crypto:generate_keys(ecc_compact),
    NegPubKeyBin = libp2p_crypto:pubkey_to_bin(NegPubKey),
    NegSigFun = libp2p_crypto:mk_sig_fun(NegPivKey),
    {FilterZ, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn04 = blockchain_txn_routing_v1:new_xor(OUI2, NegPubKeyBin, FilterZ, 3),
    SignedOUITxn04 = blockchain_txn_routing_v1:sign(OUITxn04, NegSigFun),
    {error, _} = test_utils:create_block(ConsensusMembers, [SignedOUITxn04]),

    ?assert(meck:validate(blockchain_txn_oui_v1)),
    meck:unload(blockchain_txn_oui_v1),
    ok.

max_subnet_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    Swarm = ?config(swarm, Config),
    Ledger = blockchain:ledger(Chain),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    meck:new(blockchain_txn_oui_v1, [no_link, passthrough]),
    %% an OUI has a default staking fee of 1, the old version of the test was defaulting this to zero
    %% and then mecking out the is_valid to prevent it being rejected due to expected /= presented fee
    %% since we can no longer override the default fee, now have to meck out the check_db & debit fee functions instead
    %% as the account does not have any credits, but the mecking of the is_valid can be removed
    meck:expect(blockchain_ledger_v1, check_dc_or_hnt_balance, fun(_, _, _, _) -> ok end),
    meck:expect(blockchain_ledger_v1, debit_fee, fun(_, _, _, _, _, _) -> ok end),

    OUI1 = 1,
    Addresses0 = [libp2p_swarm:pubkey_bin(Swarm)],
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn0 = blockchain_txn_oui_v1:new(OUI1, Payer, Addresses0, Filter, 8),
    SignedOUITxn0 = blockchain_txn_oui_v1:sign(OUITxn0, SigFun),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    {ok, Block0} = test_utils:create_block(ConsensusMembers, [SignedOUITxn0]),
    _ = blockchain_gossip_handler:add_block(Block0, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 2} == blockchain:height(Chain) end),

    Routing0 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses0, Filter,
                                                <<0:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(8)):23/integer-unsigned-big>>, 0),
    ?assertEqual({ok, Routing0}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    #{public := NewPubKey, secret := _PrivKey} = libp2p_crypto:generate_keys(ed25519),
    Addresses1 = [libp2p_crypto:pubkey_to_bin(NewPubKey)],
    OUITxn2 = blockchain_txn_routing_v1:update_router_addresses(OUI1, Payer, Addresses1, 1),
    SignedOUITxn2 = blockchain_txn_routing_v1:sign(OUITxn2, SigFun),
    {ok, Block1} = test_utils:create_block(ConsensusMembers, [SignedOUITxn2]),
    _ = blockchain_gossip_handler:add_block(Block1, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 3} == blockchain:height(Chain) end),

    Routing1 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses1, Filter,
                                                <<0:25/integer-unsigned-big, (blockchain_ledger_routing_v1:subnet_size_to_mask(8)):23/integer-unsigned-big>>, 1),
    ?assertEqual({ok, Routing1}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    RoutingTxns = lists:foldl(fun(I, Acc) ->
                                      T = blockchain_txn_routing_v1:request_subnet(OUI1, Payer, 32, I),
                                      ST = blockchain_txn_routing_v1:sign(T, SigFun),
                                      [ST | Acc]
                              end,
                              [],
                              lists:seq(2, 20)),

    RoutingTxn21 = blockchain_txn_routing_v1:sign(blockchain_txn_routing_v1:request_subnet(OUI1, Payer, 32, 21), SigFun),

    {error, {invalid_txns, [{RoutingTxn21, _InvalidReason}]}} = test_utils:create_block(ConsensusMembers, RoutingTxns ++ [RoutingTxn21]),

    ?assert(meck:validate(blockchain_txn_oui_v1)),
    meck:unload(blockchain_txn_oui_v1),

    ok.

block_save_failed_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    {ok, OldHeight} = blockchain:height(Chain),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, save_block, fun(_, _) -> erlang:error(boom) end),
    {ok, OldHeight} = blockchain:height(Chain),
    blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
    meck:unload(blockchain),
    blockchain_lock:release(),
    ok = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
    {ok, NewHeight} = blockchain:height(Chain),
    ?assert(NewHeight == OldHeight + 1),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

absorb_failed_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, save_block, fun(B, C) ->
        meck:passthrough([B, C]),
        ct:pal("BOOM"),
        erlang:error(boom)
    end),
    blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
    meck:unload(blockchain),
    Ledger = blockchain:ledger(Chain),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, 1}, blockchain_ledger_v1:current_height(Ledger)),
    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    ct:pal("Try to re-add block 1 will cause the mismatch"),
    ok = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),
    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain_ledger_v1:current_height(Ledger) end),

    ct:pal("Try to re-add block 2"),
    ok = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),
    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain_ledger_v1:current_height(Ledger) end),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

missing_last_block_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, save_block, fun(_B, _C) ->
            %% just don't save it
            ok
    end),
    blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
    ?assert(meck:validate(blockchain)),
    meck:unload(blockchain),

    Ledger = blockchain:ledger(Chain),
    {ok, GenesisBlock} = blockchain:genesis_block(Chain),

    ?assertEqual({ok, blockchain_block:hash_block(GenesisBlock)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, GenesisBlock}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 2}, blockchain_ledger_v1:current_height(Ledger)),
    ?assertEqual({error, not_found}, blockchain:get_block(2, Chain)),

    ct:pal("Try to re-add block 1 will cause the mismatch"),
    ok = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),
    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain_ledger_v1:current_height(Ledger) end),

    ct:pal("Try to re-add block 2"),
    ok = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),
    ok = test_utils:wait_until(fun() -> {ok, 2} =:= blockchain_ledger_v1:current_height(Ledger) end),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

epoch_reward_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    [_, {PubKeyBin, {_, _PrivKey, _}}|_] = ConsensusMembers,

    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_Txn, _Chain) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_Txn, _Chain) -> ok end),

    meck:new(blockchain_txn_consensus_group_v1, [passthrough]),
    meck:expect(blockchain_txn_consensus_group_v1, is_valid, fun(_Txn, _Chain) -> ok end),

    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, find_gateway_info, fun(Address, _Ledger) ->
        {ok, blockchain_ledger_gateway_v2:new(Address, 12, full)}
    end),

    % Add few empty blocks to fake epoch
    Start = 1,
    End = 30,
    _Blocks = lists:reverse(lists:foldl(
        fun(X, Acc) ->
            Txns = case X =:= 15 of
                false ->
                    [];
                true ->
                    POCReceiptTxn = blockchain_txn_poc_receipts_v1:new(PubKeyBin, <<"Secret">>, <<"OnionKeyHash">>, []),
                    [POCReceiptTxn]
            end,
            {ok, B} = test_utils:create_block(ConsensusMembers, Txns),
            _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:swarm()),
            [B|Acc]
        end,
        [],
        lists:seq(1, End+2)
    )),
    {ok, Rewards} = blockchain_txn_rewards_v1:calculate_rewards(Start, End, Chain),
    ct:pal("rewards ~p", [Rewards]),
    Tx = blockchain_txn_rewards_v1:new(Start, End, Rewards),
    {ok, B} = test_utils:create_block(ConsensusMembers, [Tx]),
    _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:swarm()),

    Ledger = blockchain:ledger(Chain),
    {ok, Entry} = blockchain_ledger_v1:find_entry(PubKeyBin, Ledger),

    % 2604167 (poc_challengers) + 552399 (securities) + 248016 (consensus group) + 5000 (initial balance)
    ?assertEqual(34045820296, blockchain_ledger_entry_v1:balance(Entry)),

    ?assert(meck:validate(blockchain_txn_poc_receipts_v1)),
    meck:unload(blockchain_txn_poc_receipts_v1),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain_txn_consensus_group_v1)),
    meck:unload(blockchain_txn_consensus_group_v1).

net_emissions_reward_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    [_, {PubKeyBin, {_, PrivKey, _}}|_] = ConsensusMembers,

    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_Txn, _Chain) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_Txn, _Chain) -> ok end),

    meck:new(blockchain_txn_consensus_group_v1, [passthrough]),
    meck:expect(blockchain_txn_consensus_group_v1, is_valid, fun(_Txn, _Chain) -> ok end),

    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, find_gateway_info, fun(Address, _Ledger) ->
        {ok, blockchain_ledger_gateway_v2:new(Address, 12, full)}
    end),

    % Add few empty blocks to fake epoch
    Start = 1,
    End = 31,
    _Blocks = lists:reverse(lists:foldl(
        fun(X, Acc) ->
            Txns = case X =:= 15 of
                false ->
                    [];
                true ->
                    POCReceiptTxn = blockchain_txn_poc_receipts_v1:new(PubKeyBin, <<"Secret">>, <<"OnionKeyHash">>, []),
                    [POCReceiptTxn]
            end,
            {ok, B} = test_utils:create_block(ConsensusMembers, Txns),
            _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:swarm()),
            [B|Acc]
        end,
        [],
        lists:seq(1, 30)
    )),
    Ledger = blockchain:ledger(Chain),

    ?assertEqual({ok, 0}, blockchain_ledger_v1:net_overage(Ledger)),

    commit(fun(L) ->
                   ok = blockchain_ledger_v1:add_hnt_burned(20000, L)
           end, Ledger),

    ?assertEqual({ok, 20000}, blockchain_ledger_v1:hnt_burned(Ledger)),

    {ok, Rewards} = blockchain_txn_rewards_v2:calculate_rewards(Start, End, Chain),
    ct:pal("rewards ~p", [Rewards]),
    Tx = blockchain_txn_rewards_v2:new(Start, End, Rewards),
    {ok, B} = test_utils:create_block(ConsensusMembers, [Tx]),
    ct:pal("circ = ~p", [blockchain_ledger_v1:query_circulating_hnt(Ledger)]),

    ?assertEqual(55000, blockchain_ledger_v1:query_circulating_hnt(Ledger)),
    _ = blockchain_gossip_handler:add_block(B, Chain, self(), blockchain_swarm:swarm()),

    ct:pal("circ = ~p", [blockchain_ledger_v1:query_circulating_hnt(Ledger)]),

    %% why is this not 75001??
    ?assertEqual(66999, blockchain_ledger_v1:query_circulating_hnt(Ledger)),
    ?assertEqual({ok, 0}, blockchain_ledger_v1:net_overage(Ledger)),
    ?assertEqual({ok, 0}, blockchain_ledger_v1:hnt_burned(Ledger)),

    Start2 = 33,
    End2 = 62,

    lists:foldl(
      fun(_, Acc) ->
              Txns = [],
              {ok, Bl} = test_utils:create_block(ConsensusMembers, Txns),
              _ = blockchain_gossip_handler:add_block(Bl, Chain, self(), blockchain_swarm:swarm()),
              Acc
        end,
        [],
        lists:seq(1, 30)),

    commit(fun(L) ->
                   ok = blockchain_ledger_v1:add_hnt_burned(80000, L)
           end, Ledger),

    {ok, Rewards2} = blockchain_txn_rewards_v2:calculate_rewards(Start2, End2, Chain),
    Tx2 = blockchain_txn_rewards_v2:new(Start2, End2, Rewards2),
    {ok, B2} = test_utils:create_block(ConsensusMembers, [Tx2]),
    _ = blockchain_gossip_handler:add_block(B2, Chain, self(), blockchain_swarm:swarm()),


    ?assertEqual({ok, 0}, blockchain_ledger_v1:hnt_burned(Ledger)),
    ?assertEqual({ok, 40000}, blockchain_ledger_v1:net_overage(Ledger)),
    ?assertEqual(84999, blockchain_ledger_v1:query_circulating_hnt(Ledger)),

    Start3 = 64,
    End3 = 93,
    lists:foldl(
      fun(_, Acc) ->
              Txns = [],
              {ok, Bl} = test_utils:create_block(ConsensusMembers, Txns),
              _ = blockchain_gossip_handler:add_block(Bl, Chain, self(), blockchain_swarm:swarm()),
              Acc
        end,
        [],
        lists:seq(1, 30)),

    ?assertEqual({ok, 0}, blockchain_ledger_v1:hnt_burned(Ledger)),

    {ok, Rewards3} = blockchain_txn_rewards_v2:calculate_rewards(Start3, End3, blockchain:ledger(Ledger, Chain)),
    Tx3 = blockchain_txn_rewards_v2:new(Start3, End3, Rewards3),
    {ok, B3} = test_utils:create_block(ConsensusMembers, [Tx3]),
    _ = blockchain_gossip_handler:add_block(B3, Chain, self(), blockchain_swarm:swarm()),


    ?assertEqual(102999, blockchain_ledger_v1:query_circulating_hnt(Ledger)),
    ?assertEqual({ok, 0}, blockchain_ledger_v1:hnt_burned(Ledger)),
    ?assertEqual({ok, 0}, blockchain_ledger_v1:net_overage(Ledger)),

    %% Burn 3000 with token burn and check it appears on the ledger
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    TBTxn = blockchain_txn_token_burn_v1:sign(
              blockchain_txn_token_burn_v1:new(PubKeyBin, 3000, 1),
              SigFun),

    {ok, B4} = test_utils:create_block(ConsensusMembers, [TBTxn], #{}, false),
    _ = blockchain_gossip_handler:add_block(B4, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, 3000}, blockchain_ledger_v1:hnt_burned(Ledger)),
    ?assertEqual({ok, 0}, blockchain_ledger_v1:net_overage(Ledger)),

    ?assert(meck:validate(blockchain_txn_poc_receipts_v1)),
    meck:unload(blockchain_txn_poc_receipts_v1),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain_txn_consensus_group_v1)),
    meck:unload(blockchain_txn_consensus_group_v1).

commit(Fun, Ledger) ->
    L = blockchain_ledger_v1:new_context(Ledger),
    Fun(L),
    blockchain_ledger_v1:commit_context(L).

election_test(Config) ->
    %% ConsensusMembers = ?config(consensus_members, Config),
    GenesisMembers = ?config(genesis_members, Config),
    %% Chain = ?config(chain, Config),
    Chain = blockchain_worker:blockchain(),
    _Swarm = ?config(swarm, Config),
    N = 7,

    %% make sure our generated alpha & beta values are the same each time
    rand:seed(exs1024s, {1, 2, 234098723564079}),
    Ledger = blockchain:ledger(Chain),

    %% add random alpha and beta to gateways
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    [begin
         {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger1),
         Alpha = (rand:uniform() * 10.0) + 1.0,
         Beta = (rand:uniform() * 10.0) + 1.0,
         I2 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(Alpha, Beta, 1, I),
         blockchain_ledger_v1:update_gateway(I2, Addr, Ledger1)
     end
     || {Addr, _} <- GenesisMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% generate new group of the same length
    New =  blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 =  blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 1000),

    ct:pal("new ~p new1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    ?assertNotEqual(OldGroup, New),
    ?assertNotEqual(OldGroup, New1),

    %% confirm that they're sorted by score
    Scored =
        [begin
             {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
             {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, I, 1, Ledger),
             {Score, Addr}
         end
         || Addr <- New],

    ct:pal("scored ~p", [Scored]),

    %% no dupes
    ?assertEqual(lists:usort(Scored), lists:sort(Scored)),

    %% TODO: add better tests here

    ?assertEqual(1, length(New -- OldGroup)),

    ok.

election_v3_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    GenesisMembers = ?config(genesis_members, Config),
    %% Chain = ?config(chain, Config),
    Chain = blockchain_worker:blockchain(),
    N = 7,

    %% make sure our generated alpha & beta values are the same each time
    rand:seed(exs1024s, {1, 2, 234098723564079}),
    Ledger = blockchain:ledger(Chain),

    %% add random alpha and beta to gateways
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    %% give good, equal scores to everyone
    [begin
         {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger1),
         Alpha = 20.0,
         Beta = 1.0,
         I2 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(Alpha, Beta, 1, I),
         blockchain_ledger_v1:update_gateway(I2, Addr, Ledger1)
     end
     || {Addr, _} <- GenesisMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    %% we need to add some blocks here.   they have to have seen
    %% values and bbas.
    %% index 5 will be entirely absent
    %% index 6 will be talking (seen) but missing from bbas (maybe
    %% byzantine, maybe just missing/slow on too many packets to
    %% finish anything)
    %% index 7 will be bba-present, but only partially seen, and
    %% should not be penalized

    %% it's possible to test unseen but bba-present here, but that seems impossible?

    SeenA = maps:from_list([{I, case I of 5 -> false; 7 -> true; _ -> true end}
                            || I <- lists:seq(1, N)]),
    SeenB = maps:from_list([{I, case I of 5 -> false; 7 -> false; _ -> true end}
                            || I <- lists:seq(1, N)]),
    Seen0 = lists:duplicate(4, SeenA) ++ lists:duplicate(2, SeenB),

    BBA0 = maps:from_list([{I, case I of 5 -> false; 6 -> false; _ -> true end}
                          || I <- lists:seq(1, N)]),

    {_, Seen} =
        lists:foldl(fun(S, {I, Acc})->
                            V = blockchain_utils:map_to_bitvector(S),
                            {I + 1, [{I, V} | Acc]}
                    end,
                    {1, []},
                    Seen0),
    BBA = blockchain_utils:map_to_bitvector(BBA0),

    %% maybe these should vary more?

    BlockCt = 50,

    lists:foreach(
      fun(_) ->
              {ok, Block} = test_utils:create_block(ConsensusMembers, [], #{seen_votes => Seen,
                                                                      bba_completion => BBA}),
              _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
      end,
      lists:seq(1, BlockCt)
    ),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% generate new group of the same length
    New = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 1000),

    ct:pal("new ~p new1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    ?assertNotEqual(OldGroup, New),
    ?assertNotEqual(OldGroup, New1),
    ?assertNotEqual(New, New1),

    %% confirm that they're sorted by score
    Scored =
        [begin
             {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
             {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, I, 1, Ledger),
             {Score, Addr}
         end
         || Addr <- New],

    ScoredOldGroup =
        [begin
             {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
             %% this is at the wrong res but it shouldn't matter?
             Loc = blockchain_ledger_gateway_v2:location(I),
             {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, I, 1, Ledger),
             {Score, Loc, Addr}
         end
         || Addr <- OldGroup],


    ct:pal("scored ~p", [Scored]),

    %% no dupes
    ?assertEqual(lists:usort(Scored), lists:sort(Scored)),

    ?assertEqual(1, length(New -- OldGroup)),

    Adjusted = blockchain_election:adjust_old_group(ScoredOldGroup, Ledger),

    ct:pal("adjusted ~p", [Adjusted]),

    {ControlScore, _, _} = lists:nth(1, Adjusted),
    {FiveScore, _, _} = lists:nth(5, Adjusted),
    {SixScore, _, _} = lists:nth(6, Adjusted),
    {SevenScore, _, _} = lists:nth(7, Adjusted),

    {ok, BBAPenalty} = blockchain_ledger_v1:config(?election_bba_penalty, Ledger),
    {ok, SeenPenalty} = blockchain_ledger_v1:config(?election_seen_penalty, Ledger),

    %% five should have taken both hits
    FiveTarget = normalize_float(ControlScore - normalize_float((BlockCt * BBAPenalty + BlockCt * SeenPenalty))),
    ?assertEqual(FiveTarget, FiveScore),

    %% six should have taken only the BBA hit
    SixTarget = normalize_float(ControlScore - (BlockCt * BBAPenalty)),
    ?assertEqual(SixTarget, SixScore),

    %% seven should not have been penalized
    ?assertEqual(ControlScore, SevenScore),
    ok.

election_v4_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    GenesisMembers = ?config(genesis_members, Config),
    %% Chain = ?config(chain, Config),
    Chain = blockchain_worker:blockchain(),
    N = 7,

    %% make sure our generated alpha & beta values are the same each time
    rand:seed(exs1024s, {1, 2, 234098723564079}),
    Ledger = blockchain:ledger(Chain),

    %% add random alpha and beta to gateways
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    [begin
         {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger1),
         Alpha = 1.0 + rand:uniform(20),
         Beta = 1.0 + rand:uniform(4),
         I2 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(Alpha, Beta, 1, I),
         blockchain_ledger_v1:update_gateway(I2, Addr, Ledger1)
     end
     || {Addr, _} <- GenesisMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    %% we need to add some blocks here.   they have to have seen
    %% values and bbas.
    %% index 5 will be entirely absent
    %% index 6 will be talking (seen) but missing from bbas (maybe
    %% byzantine, maybe just missing/slow on too many packets to
    %% finish anything)
    %% index 7 will be bba-present, but only partially seen, and
    %% should not be penalized

    %% it's possible to test unseen but bba-present here, but that seems impossible?

    SeenA = maps:from_list([{I, case I of 5 -> false; 7 -> true; _ -> true end}
                            || I <- lists:seq(1, N)]),
    SeenB = maps:from_list([{I, case I of 5 -> false; 7 -> false; _ -> true end}
                            || I <- lists:seq(1, N)]),
    Seen0 = lists:duplicate(4, SeenA) ++ lists:duplicate(2, SeenB),

    BBA0 = maps:from_list([{I, case I of 5 -> false; 6 -> false; _ -> true end}
                          || I <- lists:seq(1, N)]),

    {_, Seen} =
        lists:foldl(fun(S, {I, Acc})->
                            V = blockchain_utils:map_to_bitvector(S),
                            {I + 1, [{I, V} | Acc]}
                    end,
                    {1, []},
                    Seen0),
    BBA = blockchain_utils:map_to_bitvector(BBA0),

    %% maybe these should vary more?

    BlockCt = 50,

    lists:foreach(
      fun(_) ->
              {ok, Block} = test_utils:create_block(ConsensusMembers, [], #{seen_votes => Seen,
                                                                      bba_completion => BBA}),
              _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
      end,
      lists:seq(1, BlockCt)
    ),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% generate new group of the same length
    New = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 1000),

    ct:pal("new ~p new1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    ?assertNotEqual(OldGroup, New),
    ?assertNotEqual(OldGroup, New1),
    ?assertNotEqual(New, New1),

    Scored =
        [begin
             {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
             {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, I, 1, Ledger),
             {Score, Addr}
         end
         || Addr <- New],

    %% it should be really unlikely that they're sorted by score now
    ?assertNotEqual(lists:reverse(lists:sort(Scored)), Scored),

    ScoredOldGroup =
        [begin
             {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
             %% this is at the wrong res but it shouldn't matter?
             Loc = blockchain_ledger_gateway_v2:location(I),
             {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, I, 1, Ledger),
             {Score, Loc, Addr}
         end
         || Addr <- OldGroup],


    ct:pal("scored ~p", [Scored]),

    %% no dupes
    ?assertEqual(lists:usort(Scored), lists:sort(Scored)),

    ?assertEqual(1, length(New -- OldGroup)),

    Adjusted = blockchain_election:adjust_old_group(ScoredOldGroup, Ledger),

    ct:pal("adjusted ~p", [Adjusted]),

    {FiveScore, _, _} = lists:nth(5, Adjusted),
    {SixScore, _, _} = lists:nth(6, Adjusted),
    {SevenScore, _, _} = lists:nth(7, Adjusted),

    {ok, BBAPenalty} = blockchain_ledger_v1:config(?election_bba_penalty, Ledger),
    {ok, SeenPenalty} = blockchain_ledger_v1:config(?election_seen_penalty, Ledger),

    %% five should have taken both hits
    FiveTarget = normalize_float(element(1, lists:nth(5, ScoredOldGroup)) -
                                     normalize_float((BlockCt * BBAPenalty + BlockCt * SeenPenalty))),
    ?assertEqual(FiveTarget, FiveScore),

    %% six should have taken only the BBA hit
    SixTarget = normalize_float(element(1, lists:nth(6, ScoredOldGroup))
                                - (BlockCt * BBAPenalty)),
    ?assertEqual(SixTarget, SixScore),

    %% seven should not have been penalized
    ?assertEqual(element(1, lists:nth(7, ScoredOldGroup)), SevenScore),

    ok.

light_gw_election_v4_test(Config) ->
    %% this reusues the election v4 test but modifies it so that before the new election
    %% all the GWs are updated to be light mode
    %% this means they should be exlcuded from becoming part of the new group
    %% as the test updates all the GWs the new group ends up being same as the old group
    ConsensusMembers = ?config(consensus_members, Config),
    GenesisMembers = ?config(genesis_members, Config),
    %% Chain = ?config(chain, Config),
    Chain = blockchain_worker:blockchain(),
    N = 7,

    %% make sure our generated alpha & beta values are the same each time
    rand:seed(exs1024s, {1, 2, 234098723564079}),
    Ledger = blockchain:ledger(Chain),

    %% add random alpha and beta to gateways
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    [begin
         {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger1),
         Alpha = 1.0 + rand:uniform(20),
         Beta = 1.0 + rand:uniform(4),
         I2 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(Alpha, Beta, 1, I),
         I3 = blockchain_ledger_gateway_v2:mode(light, I2),
         blockchain_ledger_v1:update_gateway(I3, Addr, Ledger1)
     end
     || {Addr, _} <- GenesisMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    %% we need to add some blocks here.   they have to have seen
    %% values and bbas.
    %% index 5 will be entirely absent
    %% index 6 will be talking (seen) but missing from bbas (maybe
    %% byzantine, maybe just missing/slow on too many packets to
    %% finish anything)
    %% index 7 will be bba-present, but only partially seen, and
    %% should not be penalized

    %% it's possible to test unseen but bba-present here, but that seems impossible?

    SeenA = maps:from_list([{I, case I of 5 -> false; 7 -> true; _ -> true end}
                            || I <- lists:seq(1, N)]),
    SeenB = maps:from_list([{I, case I of 5 -> false; 7 -> false; _ -> true end}
                            || I <- lists:seq(1, N)]),
    Seen0 = lists:duplicate(4, SeenA) ++ lists:duplicate(2, SeenB),

    BBA0 = maps:from_list([{I, case I of 5 -> false; 6 -> false; _ -> true end}
                          || I <- lists:seq(1, N)]),

    {_, Seen} =
        lists:foldl(fun(S, {I, Acc})->
                            V = blockchain_utils:map_to_bitvector(S),
                            {I + 1, [{I, V} | Acc]}
                    end,
                    {1, []},
                    Seen0),
    BBA = blockchain_utils:map_to_bitvector(BBA0),

    %% maybe these should vary more?

    BlockCt = 50,

    lists:foreach(
      fun(_) ->
              {ok, Block} = test_utils:create_block(ConsensusMembers, [], #{seen_votes => Seen,
                                                                      bba_completion => BBA}),
              _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
      end,
      lists:seq(1, BlockCt)
    ),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% update all of the GWs to light modes
    %% this should block them from becoming part of any new group
    %% confirm they are not elected
    %% the new group should be same as the old group as there are no valid new members
    Ledger2 = blockchain_ledger_v1:new_context(Ledger),
    {_, _SubGenesisMembers} = lists:split(2, GenesisMembers),

    [begin
         {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger2),
         I2 = blockchain_ledger_gateway_v2:mode(light, I),
         blockchain_ledger_v1:update_gateway(I2, Addr, Ledger2)
     end
     || {Addr, _} <- GenesisMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger2),

    %% generate new group of the same length
    New = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 1000),

    ct:pal("new ~p ~nnew1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    ?assertEqual(OldGroup, New),
    ?assertEqual(OldGroup, New1),
    ?assertEqual(New, New1),


    ok.

dataonly_gw_election_v4_test(Config) ->
    %% this reusues the election v4 test but modifies it so that before the new election
    %% all the GWs are updated to be dataonly mode
    %% this means they should be excluded from becoming part of the new group
    %% as the test updates all the GWs the new group ends up being same as the old group
    ConsensusMembers = ?config(consensus_members, Config),
    GenesisMembers = ?config(genesis_members, Config),
    %% Chain = ?config(chain, Config),
    Chain = blockchain_worker:blockchain(),
    N = 7,

    %% make sure our generated alpha & beta values are the same each time
    rand:seed(exs1024s, {1, 2, 234098723564079}),
    Ledger = blockchain:ledger(Chain),

    %% add random alpha and beta to gateways
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    [begin
         {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger1),
         Alpha = 1.0 + rand:uniform(20),
         Beta = 1.0 + rand:uniform(4),
         I2 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(Alpha, Beta, 1, I),
         I3 = blockchain_ledger_gateway_v2:mode(dataonly, I2),
         blockchain_ledger_v1:update_gateway(I3, Addr, Ledger1)
     end
     || {Addr, _} <- GenesisMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    %% we need to add some blocks here.   they have to have seen
    %% values and bbas.
    %% index 5 will be entirely absent
    %% index 6 will be talking (seen) but missing from bbas (maybe
    %% byzantine, maybe just missing/slow on too many packets to
    %% finish anything)
    %% index 7 will be bba-present, but only partially seen, and
    %% should not be penalized

    %% it's possible to test unseen but bba-present here, but that seems impossible?

    SeenA = maps:from_list([{I, case I of 5 -> false; 7 -> true; _ -> true end}
                            || I <- lists:seq(1, N)]),
    SeenB = maps:from_list([{I, case I of 5 -> false; 7 -> false; _ -> true end}
                            || I <- lists:seq(1, N)]),
    Seen0 = lists:duplicate(4, SeenA) ++ lists:duplicate(2, SeenB),

    BBA0 = maps:from_list([{I, case I of 5 -> false; 6 -> false; _ -> true end}
                          || I <- lists:seq(1, N)]),

    {_, Seen} =
        lists:foldl(fun(S, {I, Acc})->
                            V = blockchain_utils:map_to_bitvector(S),
                            {I + 1, [{I, V} | Acc]}
                    end,
                    {1, []},
                    Seen0),
    BBA = blockchain_utils:map_to_bitvector(BBA0),

    %% maybe these should vary more?

    BlockCt = 50,

    lists:foreach(
      fun(_) ->
              {ok, Block} = test_utils:create_block(ConsensusMembers, [], #{seen_votes => Seen,
                                                                      bba_completion => BBA}),
              _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
      end,
      lists:seq(1, BlockCt)
    ),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% update all of the GWs to dataonly mode
    %% this should block them from becoming part of any new group
    %% confirm they are not elected
    %% the new group should be same as the old group as there are no valid new members
    Ledger2 = blockchain_ledger_v1:new_context(Ledger),
    {_, _SubGenesisMembers} = lists:split(2, GenesisMembers),

    [begin
         {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger2),
         I2 = blockchain_ledger_gateway_v2:mode(dataonly, I),
         blockchain_ledger_v1:update_gateway(I2, Addr, Ledger2)
     end
     || {Addr, _} <- GenesisMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger2),

    %% generate new group of the same length
    New = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 1000),

    ct:pal("new ~p ~nnew1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    ?assertEqual(OldGroup, New),
    ?assertEqual(OldGroup, New1),
    ?assertEqual(New, New1),


    ok.

election_v5_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    %% GenesisMembers = ?config(genesis_members, Config),
    %% Chain = ?config(chain, Config),
    Chain = blockchain_worker:blockchain(),
    N = 7,

    %% make sure our generated alpha & beta values are the same each time
    rand:seed(exs1024s, {1, 2, 234098723564079}),
    Ledger = blockchain:ledger(Chain),

    %% we need to add some blocks here.   they have to have seen
    %% values and bbas.
    %% index 5 will be entirely absent
    %% index 6 will be talking (seen) but missing from bbas (maybe
    %% byzantine, maybe just missing/slow on too many packets to
    %% finish anything)
    %% index 7 will be bba-present, but only partially seen, and
    %% should not be penalized

    %% it's possible to test unseen but bba-present here, but that seems impossible?

    SeenA = maps:from_list([{I, case I of 5 -> false; 7 -> true; _ -> true end}
                            || I <- lists:seq(1, N)]),
    SeenB = maps:from_list([{I, case I of 5 -> false; 7 -> false; _ -> true end}
                            || I <- lists:seq(1, N)]),
    Seen0 = lists:duplicate(4, SeenA) ++ lists:duplicate(2, SeenB),

    BBA0 = maps:from_list([{I, case I of 5 -> false; 6 -> false; _ -> true end}
                          || I <- lists:seq(1, N)]),

    {_, Seen} =
        lists:foldl(fun(S, {I, Acc})->
                            V = blockchain_utils:map_to_bitvector(S),
                            {I + 1, [{I, V} | Acc]}
                    end,
                    {1, []},
                    Seen0),
    BBA = blockchain_utils:map_to_bitvector(BBA0),

    %% maybe these should vary more?

    BlockCt0 = 50,
    BlockCt = 49, % different offset

    lists:foreach(
      fun(_) ->
              {ok, Block} = test_utils:create_block(ConsensusMembers, [], #{seen_votes => Seen,
                                                                      bba_completion => BBA}),
              _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
      end,
      lists:seq(1, BlockCt0)
    ),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% generate new group of the same length
    New = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 2000),

    ct:pal("new ~p new1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    ?assertNotEqual(OldGroup, New),
    ?assertNotEqual(OldGroup, New1),
    ?assertNotEqual(New, New1),

    %% no dupes
    ?assertEqual(lists:usort(New), lists:sort(New)),
    ?assertEqual(lists:usort(New1), lists:sort(New1)),


    ct:pal("diff ~p", [New -- OldGroup]),

    ?assertEqual(2, length(New -- OldGroup)),
    ?assertEqual(3, length(New1 -- OldGroup)),

    OldGroupVals =
        [begin
             {val_v1, 1.0, 1, Addr}
         end
         || Addr <- OldGroup],

    Adjusted = blockchain_election:adjust_old_group_v2(OldGroupVals, Ledger),

    ct:pal("adjusted ~p", [Adjusted]),

    {_, FiveScore} = lists:nth(5, Adjusted),
    {_, SixScore} = lists:nth(6, Adjusted),
    {_, SevenScore} = lists:nth(7, Adjusted),

    {ok, BBAPenalty} = blockchain_ledger_v1:config(?election_bba_penalty, Ledger),
    {ok, SeenPenalty} = blockchain_ledger_v1:config(?election_seen_penalty, Ledger),

    %% five should have taken both hits
    FiveTarget = normalize_float(element(2, lists:nth(5, OldGroupVals)) +
                                     normalize_float((BlockCt * BBAPenalty) + (BlockCt * SeenPenalty))),
    ?assert(FiveTarget > FiveScore),
    ?assertEqual(2.4896087646484375, FiveScore),

    %% six should have taken only the BBA hit
    %% move to static targets here because this is no longer easy to calculate
    SixTarget = normalize_float(element(2, lists:nth(6, OldGroupVals))
                                + (BlockCt * BBAPenalty)),
    ?assert(SixTarget > SixScore),
    ?assertEqual(1.372406005859375, SixScore),

    %% seven should not have been penalized
    ?assertEqual(element(2, lists:nth(7, OldGroupVals)), SevenScore),

    ok.

election_v6_test(Config) ->
    BaseDir = ?config(base_dir, Config),

    ConsensusMembers = ?config(consensus_members, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = blockchain_worker:blockchain(),
    N = 7,

    %% make sure our generated alpha & beta values are the same each time
    rand:seed(exs1024s, {1, 2, 234098723564079}),
    Ledger = blockchain:ledger(Chain),

    %% we need to add some blocks here.   they have to have seen
    %% values and bbas.
    %% index 5 will be entirely absent
    %% index 6 will be talking (seen) but missing from bbas (maybe
    %% byzantine, maybe just missing/slow on too many packets to
    %% finish anything)
    %% index 7 will be bba-present, but only partially seen, and
    %% should not be penalized

    %% it's possible to test unseen but bba-present here, but that seems impossible?

    SeenA = maps:from_list([{I, case I of 5 -> false; 7 -> true; _ -> true end}
                            || I <- lists:seq(1, N)]),
    SeenB = maps:from_list([{I, case I of 5 -> false; 7 -> false; _ -> true end}
                            || I <- lists:seq(1, N)]),
    Seen0 = lists:duplicate(4, SeenA) ++ lists:duplicate(2, SeenB),

    BBA0 = maps:from_list([{I, case I of 5 -> false; 6 -> false; _ -> true end}
                          || I <- lists:seq(1, N)]),

    {_, Seen} =
        lists:foldl(fun(S, {I, Acc})->
                            V = blockchain_utils:map_to_bitvector(S),
                            {I + 1, [{I, V} | Acc]}
                    end,
                    {1, []},
                    Seen0),
    BBA = blockchain_utils:map_to_bitvector(BBA0),

    %% maybe these should vary more?

    BlockCt0 = 50,
    BlockCt = 49, % different offset

    lists:foreach(
      fun(_) ->
              {ok, Block} = test_utils:create_block(ConsensusMembers, [], #{seen_votes => Seen,
                                                                            bba_completion => BBA}),
              _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
      end,
      lists:seq(1, BlockCt0)
    ),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% generate new group of the same length
    New = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 = blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 2000),

    ct:pal("new ~p new1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    ?assertNotEqual(OldGroup, New),
    ?assertNotEqual(OldGroup, New1),
    ?assertNotEqual(New, New1),

    %% no dupes
    ?assertEqual(lists:usort(New), lists:sort(New)),
    ?assertEqual(lists:usort(New1), lists:sort(New1)),

    ?assertEqual(2, length(New -- OldGroup)),
    ?assertEqual(3, length(New1 -- OldGroup)),

    OldGroupVals =
        [begin
             {val_v1, 1.0, 1, Addr}
         end
         || Addr <- OldGroup],

    Adjusted = blockchain_election:adjust_old_group_v2(OldGroupVals, Ledger),

    ct:pal("adjusted ~p", [Adjusted]),

    {_, FiveScore} = lists:nth(5, Adjusted),
    {_, SixScore} = lists:nth(6, Adjusted),
    {_, SevenScore} = lists:nth(7, Adjusted),

    {ok, BBAPenalty} = blockchain_ledger_v1:config(?election_bba_penalty, Ledger),
    {ok, SeenPenalty} = blockchain_ledger_v1:config(?election_seen_penalty, Ledger),

    %% five should have taken both hits
    FiveTarget = normalize_float(element(2, lists:nth(5, OldGroupVals)) +
                                     normalize_float((BlockCt * BBAPenalty) + (BlockCt * SeenPenalty))),
    ?assert(FiveTarget > FiveScore),
    ?assertEqual(2.4896087646484375, FiveScore),

    %% six should have taken only the BBA hit
    %% move to static targets here because this is no longer easy to calculate
    SixTarget = normalize_float(element(2, lists:nth(6, OldGroupVals))
                                + (BlockCt * BBAPenalty)),
    ?assert(SixTarget > SixScore),
    ?assertEqual(1.372406005859375, SixScore),

    %% seven should not have been penalized
    ?assertEqual(element(2, lists:nth(7, OldGroupVals)), SevenScore),

    ok.

chain_vars_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    {Priv, _} = ?config(master_key, Config),

    Ledger = blockchain:ledger(Chain),

    Vars = #{garbage_value => 2},

    ct:pal("priv_key ~p", [Priv]),

    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),

    {ok, InitBlock} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    _ = blockchain_gossip_handler:add_block(InitBlock, Chain, self(), blockchain_swarm:swarm()),

    {ok, Delay} = blockchain:config(?vars_commit_delay, Ledger),
    ct:pal("commit delay ~p", [Delay]),
    %% Add some blocks,
    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
                {ok, Height} = blockchain:height(Chain),
                case blockchain:config(garbage_value, Ledger) of % ignore "?"
                    {error, not_found} when Height < (Delay + 1) ->
                        ok;
                    {ok, 2} when Height >= (Delay + 1) ->
                        ok;
                    Res ->
                        throw({error, {chain_var_wrong_height, Res, Height}})
                end
        end,
        lists:seq(1, 15)
    ),
    ?assertEqual({ok, 17}, blockchain:height(Chain)),
    ok.

chain_vars_set_unset_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    {Priv, _} = ?config(master_key, Config),

    Ledger = blockchain:ledger(Chain),

    Vars = #{garbage_value => 2},

    ct:pal("priv_key ~p", [Priv]),

    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),

    {ok, InitBlock} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    _ = blockchain_gossip_handler:add_block(InitBlock, Chain, self(), blockchain_swarm:swarm()),

    {ok, Delay} = blockchain:config(?vars_commit_delay, Ledger),
    ct:pal("commit delay ~p", [Delay]),
    %% Add some blocks,
    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
                {ok, Height} = blockchain:height(Chain),
                case blockchain:config(garbage_value, Ledger) of % ignore "?"
                    {error, not_found} when Height < (Delay + 1) ->
                        ok;
                    {ok, 2} when Height >= (Delay + 1) ->
                        ok;
                    Res ->
                        throw({error, {chain_var_wrong_height, Res, Height}})
                end
        end,
        lists:seq(1, 15)
    ),
    ?assertEqual({ok, 17}, blockchain:height(Chain)),
    {ok, Height} = blockchain:height(Chain),
    ct:pal("Height ~p", [Height]),

    UnsetVarTxn = blockchain_txn_vars_v1:new(#{}, 4, #{unsets => [garbage_value]}),
    UnsetProof = blockchain_txn_vars_v1:create_proof(Priv, UnsetVarTxn),
    UnsetVarTxn1 = blockchain_txn_vars_v1:proof(UnsetVarTxn, UnsetProof),

    {ok, NewBlock} = test_utils:create_block(ConsensusMembers, [UnsetVarTxn1]),
    _ = blockchain_gossip_handler:add_block(NewBlock, Chain, self(), blockchain_swarm:swarm()),
    %% Add some blocks,
    lists:foreach(
        fun(_) ->
                {ok, Block1} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block1, Chain, self(), blockchain_swarm:swarm()),
                {ok, Height1} = blockchain:height(Chain),
                ct:pal("Height1 ~p", [Height1]),
                case blockchain:config(garbage_value, Ledger) of % ignore "?"
                    {ok, 2} when Height1 < (Height + Delay + 1) ->
                        ok;
                    {error, not_found} when Height1 >= (Height + Delay) ->
                        ok;
                    Res ->
                        throw({error, {chain_var_wrong_height, Res, Height1, Height + Delay + 1}})
                end
        end,
        lists:seq(1, 15)
    ),
    ?assertEqual({ok, 33}, blockchain:height(Chain)),

    ok.

token_burn_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Ledger = blockchain:ledger(Chain),

    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    DCBalance0 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry0),

    %% Sanity check
    ?assertEqual(Balance, DCBalance0, "The same init balance is used for DC and HNT."),

    % Step 1: Simple payment txn with no fees
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    Tx0 = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 1),
    SignedTx0 = blockchain_txn_payment_v1:sign(Tx0, SigFun),
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [SignedTx0]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry1)),

    % Step 2: Token burn txn (without an oracle price set) should fail and stay at same block
    BurnTx0 = blockchain_txn_token_burn_v1:new(Payer, 10, 2),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, SigFun),
    {error, {invalid_txns, [{SignedBurnTx0, _InvalidReason}]}} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),
    ?assertEqual({error, not_found}, blockchain_ledger_v1:token_burn_exchange_rate(Ledger)),

    % Step 3: fake an oracle price and a burn price, these figures are not representative
    % TODO: setup actual onchain oracle prices, get rid of these mecks
    OP = 1000,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),

    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    % Step 4: Retry token burn txn should pass now
    {ok, Block23} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 23}, blockchain:height(Chain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, Chain)),
    {ok, NewEntry2} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500 - 10, blockchain_ledger_entry_v1:balance(NewEntry2)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    DCBalance1 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry1),
    ?assertEqual(10 * OP + DCBalance0, DCBalance1),

    % Step 5: Try payment txn with fee this time
    Tx1 = blockchain_txn_payment_v1:new(Payer, Recipient, 500, 3),
    SignedTx1 = blockchain_txn_payment_v1:sign(Tx1, SigFun),
    {ok, Block24} = test_utils:create_block(ConsensusMembers, [SignedTx1]),
    _ = blockchain_gossip_handler:add_block(Block24, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block24)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block24}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 24}, blockchain:height(Chain)),
    ?assertEqual({ok, Block24}, blockchain:get_block(24, Chain)),
    {ok, NewEntry3} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500 + 500, blockchain_ledger_entry_v1:balance(NewEntry3)),
    {ok, NewEntry4} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500 - 10 - 500, blockchain_ledger_entry_v1:balance(NewEntry4)),
    TxnFee = 0, %% default fee will be zero
    {ok, DCEntry2} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    DCBalance2 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry2),
    ?assertEqual((10 * OP + DCBalance0) - TxnFee, DCBalance2),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),

    ok.

payer_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),
    Swarm = ?config(swarm, Config),

    [_, {Payer, {_, PayerPrivKey, _}}, {Owner, {_, OwnerPrivKey, _}}|_] = ConsensusMembers,
    PayerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),
    Ledger = blockchain:ledger(Chain),

    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    DCBalance0 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry0),

    %% Sanity check
    ?assertEqual(Balance, DCBalance0, "The same init balance is used for DC and HNT."),

    % Step 3: fake an oracle price and a burn price, these figures are not representative
    % TODO: setup actual onchain oracle prices, get rid of these mecks
    OP = 1000,
    DefaultTxnFee = 0,
    DefaultStakingFee = 1, %% NOTE not all txns have a default staking fee of 1, some are 0, be aware
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),
    meck:expect(blockchain_ledger_v1, config, fun(sc_version, _) -> {ok, 2};
                                              (A, B) -> blockchain_ledger_v1_meck_original:config(A, B) end),

    Blocks = lists:map(
               fun(_) ->
                       {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                       _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
                       Block
               end,
               lists:seq(1, 20)),
    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    BurnTx0 = blockchain_txn_token_burn_v1:new(Payer, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, PayerSigFun),
    {ok, Block22} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block22, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block22)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block22}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 22}, blockchain:height(Chain)),
    ?assertEqual({ok, Block22}, blockchain:get_block(22, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 10, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    ?assertEqual(10*OP + DCBalance0, blockchain_ledger_data_credits_entry_v1:balance(DCEntry1)),

    % Step 3: Add OUI, gateway, assert_location and let payer pay for it
    OUI1 = 1,
    Addresses0 = [libp2p_swarm:pubkey_bin(Swarm)],
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn0 = blockchain_txn_oui_v1:new(OUI1, Owner, Addresses0, Filter, 8, Payer),
    SignedOUITxn0 = blockchain_txn_oui_v1:sign(OUITxn0, OwnerSigFun),
    SignedOUITxn1 = blockchain_txn_oui_v1:sign_payer(SignedOUITxn0, PayerSigFun),


    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),

    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway, Payer),
    SignedAddGatewayTx0 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedAddGatewayTx1 = blockchain_txn_add_gateway_v1:sign_request(SignedAddGatewayTx0, GatewaySigFun),
    SignedAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedAddGatewayTx1, PayerSigFun),

    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, Payer, ?TEST_LOCATION, 1),
    SignedAssertLocationTx0 = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx1 = blockchain_txn_assert_location_v1:sign(SignedAssertLocationTx0, OwnerSigFun),
    SignedAssertLocationTx2 = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx1, PayerSigFun),

    {ok, Block23} = test_utils:create_block(ConsensusMembers, [SignedOUITxn1, SignedAddGatewayTx2, SignedAssertLocationTx2]),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 23}, blockchain:height(Chain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, Chain)),
    {ok, DCEntry2} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    ?assertEqual((10*OP-((DefaultTxnFee + DefaultStakingFee)*3) + DCBalance0), blockchain_ledger_data_credits_entry_v1:balance(DCEntry2)),


    Routing = blockchain_ledger_routing_v1:new(1, Owner, Addresses0, Filter, <<0,0,0,127,255,254>>, 0),
    ?assertEqual({ok, Routing}, blockchain_ledger_v1:find_routing(1, Ledger)),

    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),
    ?assertEqual(Owner, blockchain_ledger_gateway_v2:owner_address(GwInfo)),
    ?assertEqual(?TEST_LOCATION, blockchain_ledger_gateway_v2:location(GwInfo)),

    %% try resyncing the ledger
    {ok, NewChain} = blockchain:reset_ledger(Chain),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(NewChain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(NewChain)),
    ?assertEqual({ok, 23}, blockchain:height(NewChain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, NewChain)),
    {ok, DCEntry2} = blockchain_ledger_v1:find_dc_entry(Payer, blockchain:ledger(NewChain)),
    ?assertEqual((10*OP-((DefaultTxnFee + DefaultStakingFee )*3)) + DCBalance0, blockchain_ledger_data_credits_entry_v1:balance(DCEntry2)),

    blockchain:delete_block(Block22, NewChain),

    ?assertEqual({error, {missing_block, blockchain_block:hash_block(Block22), blockchain_block:height(Block22)}}, blockchain:reset_ledger(NewChain)),

    {ok, NewerChain} = blockchain:reset_ledger(19, NewChain),

    ?assertEqual(blockchain:head_hash(NewerChain), {ok, blockchain_block:hash_block(lists:nth(18, Blocks))}),
    ?assertEqual({ok, 19}, blockchain:height(NewerChain)),
    ?assertEqual({ok, lists:nth(18, Blocks)}, blockchain:head_block(NewerChain)),
    ?assertEqual({ok, lists:nth(18, Blocks)}, blockchain:get_block(19, NewerChain)),

    blockchain:add_blocks(lists:sublist(Blocks, 19, 2) ++ [Block22, Block23], NewerChain),

    %% ct:pal("block 22 ~p 23 ~p", [blockchain_block:hash_block(Block22), blockchain_block:hash_block(Block23)]),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(NewerChain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(NewerChain)),
    ?assertEqual({ok, 23}, blockchain:height(NewerChain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, NewerChain)),
    {ok, DCEntry2} = blockchain_ledger_v1:find_dc_entry(Payer, blockchain:ledger(NewerChain)),
    ?assertEqual((10*OP-((DefaultTxnFee + DefaultStakingFee )*3)) + DCBalance0, blockchain_ledger_data_credits_entry_v1:balance(DCEntry2)),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ok.

poc_sync_interval_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    PubKey = ?config(pubkey, Config),
    PrivKey = ?config(privkey, Config),
    Owner = libp2p_crypto:pubkey_to_bin(PubKey),
    Chain = ?config(chain, Config),
    Balance = ?config(balance, Config),
    Ledger = blockchain:ledger(Chain),
    {Priv, _} = ?config(master_key, Config),

    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance0 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry0),

    %% Sanity check
    ?assertEqual(Balance, DCBalance0, "The same init balance is used for DC and HNT."),

    % Step 3: fake an oracle price and a burn price, these figures are not representative
    % TODO: setup actual onchain oracle prices, get rid of these mecks
    OP = 1000,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),

    ok = dump_empty_blocks(Chain, ConsensusMembers, 20),

    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    BurnTx0 = blockchain_txn_token_burn_v1:new(Owner, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, OwnerSigFun),
    {ok, Block22} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block22, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block22)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block22}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 22}, blockchain:height(Chain)),
    ?assertEqual({ok, Block22}, blockchain:get_block(22, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Owner, Ledger),
    ?assertEqual(Balance - 10, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance1 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry1),
    ?assertEqual(10 * OP + DCBalance0, DCBalance1),

    %% Get gateways before adding a new gateway
    ActiveGateways0 = blockchain_ledger_v1:active_gateways(blockchain:ledger(Chain)),

    %% Add gateway with a location
    {Gateway, GatewaySigFun} = create_gateway(),
    SignedAddGatewayTx = fake_add_gateway(Owner, OwnerSigFun, Gateway, GatewaySigFun),
    SignedAssertLocTx = fake_assert_location(Owner, OwnerSigFun, Gateway, GatewaySigFun, ?TEST_LOCATION),
    Txns = [SignedAddGatewayTx, SignedAssertLocTx],

    %% Put the txns in a block
    {ok, Block23} = test_utils:create_block(ConsensusMembers, Txns),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), blockchain_swarm:swarm()),

    %% Check chain moved ahead
    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block23), HeadHash),
    ?assertEqual({ok, Block23}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 23}, blockchain:height(Chain)),

    %% Check that gateway made in, there should be 1 new
    ActiveGateways = blockchain_ledger_v1:active_gateways(blockchain:ledger(Chain)),
    {ok, AddedGw} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(1, maps:size(ActiveGateways) - maps:size(ActiveGateways0)),

    %% Check that the add gateway is eligible for POC before chain vars kick in
    ?assertEqual(true, blockchain_poc_path:check_sync(AddedGw, Ledger)),

    %% Prep chain vars
    POCVersion = 2,
    POCChallengeSyncInterval = 10,
    VarTxn2 = fake_var_txn(Priv,
                           3,
                           #{poc_version => POCVersion,
                             poc_challenge_sync_interval => POCChallengeSyncInterval}
                          ),

    %% Forge a chain var block
    {ok, Block24} = test_utils:create_block(ConsensusMembers, [VarTxn2]),
    _ = blockchain_gossip_handler:add_block(Block24, Chain, self(), blockchain_swarm:swarm()),

    %% Wait for things to settle
    timer:sleep(500),

    %% Dump some more blocks
    ok = dump_empty_blocks(Chain, ConsensusMembers, 20),

    %% Chain vars should have kicked in by now
    ?assertEqual({ok, POCVersion},
                 blockchain_ledger_v1:config(?poc_version, Ledger)),
    ?assertEqual({ok, POCChallengeSyncInterval},
                 blockchain_ledger_v1:config(?poc_challenge_sync_interval, Ledger)),

    %% Chain should have moved further up
    ?assertEqual({ok, 44}, blockchain:height(Chain)),

    %% Added gateway's poc eligibility should no longer be valid
    ?assertEqual(false, blockchain_poc_path:check_sync(AddedGw, Ledger)),

    %% Fake a poc_request
    {ok, BlockHash} = blockchain:head_hash(Chain),
    POCReqTxn = fake_poc_request(Gateway, GatewaySigFun, BlockHash),
    {ok, Block45} = test_utils:create_block(ConsensusMembers, [POCReqTxn]),
    _ = blockchain_gossip_handler:add_block(Block45, Chain, self(), blockchain_swarm:swarm()),

    %% Dump a couple more blocks
    ok = dump_empty_blocks(Chain, ConsensusMembers, 2),

    %% Check last_poc_challenge for added gateway is updated
    {ok, AddedGw2} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    LastPOCChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(AddedGw2),
    ?assert(LastPOCChallenge > 44),

    %% Added gateway's poc eligibility should become valid
    {ok, AddedGw3} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(true, blockchain_poc_path:check_sync(AddedGw3, Ledger)),

    %% Dump even more blocks to make it ineligible again
    ok = dump_empty_blocks(Chain, ConsensusMembers, 20),
    {ok, AddedGw4} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(false, blockchain_poc_path:check_sync(AddedGw4, Ledger)),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ok.

zero_payment_v1_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 0,
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, Amount, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),

    %% TODO: update when dc stuff with better test_util add_block lands
    {[], [{SignedTx, _InvalidReason}]} = blockchain_txn:validate([SignedTx], Chain),

    ok.

negative_payment_v1_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = -100,
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, Amount, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),

    %% TODO: update when dc stuff with better test_util add_block lands
    {[], [{SignedTx, _InvalidReason}]} = blockchain_txn:validate([SignedTx], Chain),

    ok.

zero_amt_htlc_create_test(Config) ->
    PubKey = ?config(pubkey, Config),
    PrivKey = ?config(privkey, Config),
    Chain = ?config(chain, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    % Create a Payee
    #{public := PayeePubKey, secret := _PayeePrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PayeePubKey),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(33),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    Amount = 0,
    CreateTx = blockchain_txn_create_htlc_v1:new(Payer, Payee, HTLCAddress, Hashlock, 3, Amount, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedCreateTx = blockchain_txn_create_htlc_v1:sign(CreateTx, SigFun),

    %% TODO: update when dc stuff with better test_util add_block lands
    {[], [{SignedCreateTx, _InvalidReason}]} = blockchain_txn:validate([SignedCreateTx], Chain),
    ok.

negative_amt_htlc_create_test(Config) ->
    PubKey = ?config(pubkey, Config),
    PrivKey = ?config(privkey, Config),
    Chain = ?config(chain, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    % Create a Payee
    #{public := PayeePubKey, secret := _PayeePrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PayeePubKey),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(33),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    Amount = -100,
    CreateTx = blockchain_txn_create_htlc_v1:new(Payer, Payee, HTLCAddress, Hashlock, 3, Amount, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedCreateTx = blockchain_txn_create_htlc_v1:sign(CreateTx, SigFun),

    %% TODO: update when dc stuff with better test_util add_block lands
    {[], [{SignedCreateTx, _InvalidReason}]} = blockchain_txn:validate([SignedCreateTx], Chain),
    ok.

update_gateway_oui_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    Ledger = blockchain:ledger(Chain),

    [_, {Owner, {_, OwnerPrivKey, _}}|_] = ConsensusMembers,
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    % Step 3: fake an oracle price and a burn price, these figures are not representative
    % TODO: setup actual onchain oracle prices, get rid of these mecks
    OP = 1000,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),

    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 20)
    ),
    ok = test_utils:wait_until(fun() -> {ok, 21} == blockchain:height(Chain) end),
    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    % Step 2: Burn some token to get some DCs
    BurnTx0 = blockchain_txn_token_burn_v1:new(Owner, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, OwnerSigFun),

    {ok, Block22} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block22, Chain, self(), blockchain_swarm:swarm()),
    ok = test_utils:wait_until(fun() -> {ok, 22} == blockchain:height(Chain) end),

    % Step 3: Create a Gateway and OUI
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    AddGatewayTxn = blockchain_txn_add_gateway_v1:new(Owner, Gateway),
    SignedOwnerAddGatewayTxn = blockchain_txn_add_gateway_v1:sign(AddGatewayTxn, OwnerSigFun),
    SignedGatewayAddGatewayTxn = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTxn, GatewaySigFun),
    OUI1 = 1,
    Addresses = [libp2p_swarm:pubkey_bin(Swarm)],
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn = blockchain_txn_oui_v1:new(OUI1, Owner, Addresses, Filter, 8),
    SignedOUITxn = blockchain_txn_oui_v1:sign(OUITxn, OwnerSigFun),
    {ok, Block23} = test_utils:create_block(ConsensusMembers, [SignedGatewayAddGatewayTxn, SignedOUITxn]),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), blockchain_swarm:swarm()),
    ok = test_utils:wait_until(fun() -> {ok, 23} == blockchain:height(Chain) end),

    %% Step 3: Check initial gateway info
    {ok, GwInfo0} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(undefined, blockchain_ledger_gateway_v2:oui(GwInfo0)),
    ?assertEqual(0, blockchain_ledger_gateway_v2:nonce(GwInfo0)),

    % Step 4: Updating a Gateway's OUI

    UpdateGatewayOUITxn = blockchain_txn_update_gateway_oui_v1:new(Gateway, OUI1, 1),
    SignedUpdateGatewayOUITxn = blockchain_txn_update_gateway_oui_v1:oui_owner_sign(blockchain_txn_update_gateway_oui_v1:gateway_owner_sign(UpdateGatewayOUITxn, OwnerSigFun), OwnerSigFun),
    {ok, Block24} = test_utils:create_block(ConsensusMembers, [SignedUpdateGatewayOUITxn]),
    _ = blockchain_gossip_handler:add_block(Block24, Chain, self(), blockchain_swarm:swarm()),
    ok = test_utils:wait_until(fun() -> {ok, 24} == blockchain:height(Chain) end),
    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),

    ?assertEqual(OUI1, blockchain_ledger_gateway_v2:oui(GwInfo)),
    ?assertEqual(1, blockchain_ledger_gateway_v2:nonce(GwInfo)),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ok.

replay_oui_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    Ledger = blockchain:ledger(Chain),

    [_, {Owner, {_, OwnerPrivKey, _}}|_] = ConsensusMembers,
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    % Step 3: fake an oracle price and a burn price, these figures are not representative
    % TODO: setup actual onchain oracle prices, get rid of these mecks
    OP = 1000,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),

    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), Swarm)
        end,
        lists:seq(1, 20)
    ),
    ok = test_utils:wait_until(fun() -> {ok, 21} == blockchain:height(Chain) end),
    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    % Step 2: Burn some token to get some DCs
    BurnTx0 = blockchain_txn_token_burn_v1:new(Owner, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, OwnerSigFun),

    {ok, Block22} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block22, Chain, self(), Swarm),
    ok = test_utils:wait_until(fun() -> {ok, 22} == blockchain:height(Chain) end),

    %% construct oui txn
    Addresses = [libp2p_swarm:pubkey_bin(Swarm)],
    {Filter, _} = xor16:to_bin(xor16:new([0], fun xxhash:hash64/1)),
    OUITxn1 = blockchain_txn_oui_v1:new(0, Owner, Addresses, Filter, 8),
    SignedOUITxn1 = blockchain_txn_oui_v1:sign(OUITxn1, OwnerSigFun),

    %% mine the oui txn
    {ok, Block23} = test_utils:create_block(ConsensusMembers, [SignedOUITxn1]),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), Swarm),

    %% wait 5 blocks, no good reason
    ok = lists:foreach(
           fun(_) ->
                   {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                   _ = blockchain_gossip_handler:add_block(Block, Chain, self(), Swarm)
           end,
           lists:seq(1, 5)
          ),
    ok = test_utils:wait_until(fun() -> {ok, 28} == blockchain:height(Chain) end),

    %% construct second oui txn
    OUITxn2 = blockchain_txn_oui_v1:new(0, Owner, Addresses, Filter, 8),
    SignedOUITxn2 = blockchain_txn_oui_v1:sign(OUITxn2, OwnerSigFun),

    %% mine second oui txn
    {ok, Block29} = test_utils:create_block(ConsensusMembers, [SignedOUITxn2]),
    _ = blockchain_gossip_handler:add_block(Block29, Chain, self(), Swarm),

    %% wait 5 blocks, no good reason
    ok = lists:foreach(
           fun(_) ->
                   {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                   _ = blockchain_gossip_handler:add_block(Block, Chain, self(), Swarm)
           end,
           lists:seq(1, 5)
          ),
    ok = test_utils:wait_until(fun() -> {ok, 34} == blockchain:height(Chain) end),

    ?assertEqual({ok, 2}, blockchain_ledger_v1:get_oui_counter(blockchain:ledger(Chain))),

    %% construct third oui txn, current oui counter is 2
    OUITxn3 = blockchain_txn_oui_v1:new(2, Owner, Addresses, Filter, 8),
    SignedOUITxn3 = blockchain_txn_oui_v1:sign(OUITxn3, OwnerSigFun),

    %% mine third oui txn
    {ok, Block35} = test_utils:create_block(ConsensusMembers, [SignedOUITxn3]),
    _ = blockchain_gossip_handler:add_block(Block35, Chain, self(), Swarm),

    %% wait 5 blocks, no good reason
    ok = lists:foreach(
           fun(_) ->
                   {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                   _ = blockchain_gossip_handler:add_block(Block, Chain, self(), Swarm)
           end,
           lists:seq(1, 5)
          ),
    ok = test_utils:wait_until(fun() -> {ok, 40} == blockchain:height(Chain) end),

    ?assertEqual({ok, 3}, blockchain_ledger_v1:get_oui_counter(blockchain:ledger(Chain))),

    %% mine third oui txn again, this should fail
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn3]),

    %% mine first oui txn again
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn1]),

    %% mine second oui txn again
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn2]),

    %% construct fourth and fifth oui txn, current oui counter is 3
    OUITxn4 = blockchain_txn_oui_v1:new(3, Owner, Addresses, Filter, 8),
    SignedOUITxn4 = blockchain_txn_oui_v1:sign(OUITxn4, OwnerSigFun),

    OUITxn5 = blockchain_txn_oui_v1:new(3, Owner, Addresses, Filter, 8),
    SignedOUITxn5 = blockchain_txn_oui_v1:sign(OUITxn5, OwnerSigFun),

    %% try a txn with a future oui
    OUITxn6 = blockchain_txn_oui_v1:new(4, Owner, Addresses, Filter, 8),
    SignedOUITxn6 = blockchain_txn_oui_v1:sign(OUITxn6, OwnerSigFun),

    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn6]),

    %% mine fourth and fifth oui txn
    {ok, Block40} = test_utils:create_block(ConsensusMembers, [SignedOUITxn4, SignedOUITxn5]),
    _ = blockchain_gossip_handler:add_block(Block40, Chain, self(), Swarm),

    ?assertEqual({ok, 5}, blockchain_ledger_v1:get_oui_counter(blockchain:ledger(Chain))),

    %% mine third oui txn again, this should fail
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn3]),

    %% mine first oui txn again
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn1]),

    %% mine second oui txn again
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn2]),

    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn4]),
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [SignedOUITxn5]),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ok.


failed_txn_error_handling(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    PubKey = ?config(pubkey, Config),
    PrivKey = ?config(privkey, Config),
    Owner = libp2p_crypto:pubkey_to_bin(PubKey),
    Chain = ?config(chain, Config),
    Balance = ?config(balance, Config),

    Ledger = blockchain:ledger(Chain),
    Rate = 100000000,
    {Priv, _} = ?config(master_key, Config),

    %% for this test use a poc receipt txn and push it through the validations
    %% we want it to be a valid txn which passed all validations and, hence all this initial boilerplate
    %% the txn will subsequently be handled by blockchain_utils:pmap/2
    %% pmap function will be mecked to return an error, simulating a crash and other error msgs
    %% the test will exercise the error handling in blockchain_txn:seperate_res/4
    %% and confirm that all expected and unexpected error msgs are handled

    % fake an oracle price and a burn price, these figures are not representative
    OP = 1000,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OP} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OP]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OP} end),

    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance0 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry0),

    %% Sanity check
    ?assertEqual(Balance, DCBalance0, "The same init balance is used for DC and HNT."),

    %% NOTE: the token burn exchange rate block is not required for most of this test to run
    %% it should be removed but the POC is using it atm - needs refactored
    Vars = #{token_burn_exchange_rate => Rate},
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),
    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual({ok, OP}, blockchain_ledger_v1:current_oracle_price(Ledger)),

    % Step 2: Token burn txn should pass now
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    BurnTx0 = blockchain_txn_token_burn_v1:new(Owner, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, OwnerSigFun),
    {ok, Block23} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Block23, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 23}, blockchain:height(Chain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Owner, Ledger),
    ?assertEqual(Balance - 10, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    DCBalance1 = blockchain_ledger_data_credits_entry_v1:balance(DCEntry1),
    ?assertEqual(10 * OP + DCBalance0, DCBalance1),

    % Create a Gateway
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),

    % Add a Gateway
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),
      {ok, Block24} = test_utils:create_block(ConsensusMembers, [SignedGatewayAddGatewayTx]),
    _ = blockchain_gossip_handler:add_block(Block24, Chain, self(), blockchain_swarm:swarm()),

    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block24), HeadHash),
    ?assertEqual({ok, Block24}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 24}, blockchain:height(Chain)),

    % Check that the Gateway is there
    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),
    ?assertEqual(Owner, blockchain_ledger_gateway_v2:owner_address(GwInfo)),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, ?TEST_LOCATION, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),

    {ok, Block25} = test_utils:create_block(ConsensusMembers, [SignedAssertLocationTx]),
    ok = blockchain_gossip_handler:add_block(Block25, Chain, self(), blockchain_swarm:swarm()),
    timer:sleep(500),

    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block25), HeadHash2),
    ?assertEqual({ok, Block25}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 25}, blockchain:height(Chain)),

    % Create the PoC challenge request txn
    Keys0 = libp2p_crypto:generate_keys(ecc_compact),
    Secret0 = libp2p_crypto:keys_to_bin(Keys0),
    #{public := OnionCompactKey0} = Keys0,
    SecretHash0 = crypto:hash(sha256, Secret0),
    OnionKeyHash0 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey0)),
    PoCReqTxn0 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash0, OnionKeyHash0, blockchain_block:hash_block(Block2), 1),
    SignedPoCReqTxn0 = blockchain_txn_poc_request_v1:sign(PoCReqTxn0, GatewaySigFun),

    %%
    %% core test exercise start from here
    %%
    meck:new(blockchain_utils, [passthrough]),

    %% meck out the pmap function to simulate it returning an error msg in the format {error, Reason}
    meck:expect(blockchain_utils, pmap, fun(_, _) -> [{SignedPoCReqTxn0, {error, txn_fail_reason}}] end),
    {[], [{SignedPoCReqTxn0, txn_fail_reason}]} = blockchain_txn:validate([SignedPoCReqTxn0], Chain),

    %% meck out the pmap function to simulate it returning an error msg in the format {error, {Reason, Details}}
    meck:expect(blockchain_utils, pmap, fun(_, _) -> [{SignedPoCReqTxn0, {error, {bad_nonce, {nonce_type, cur_nonce, ledger_nonce}}}}] end),
    {[], [{SignedPoCReqTxn0, bad_nonce}]} = blockchain_txn:validate([SignedPoCReqTxn0], Chain),

    %% meck out the pmap function to simulate it returning an exit crash msg with body formatted as an atom
    meck:expect(blockchain_utils, pmap, fun(_, _) -> [{SignedPoCReqTxn0, {'EXIT', exit_reason}}] end),
    {[], [{SignedPoCReqTxn0, exit_reason}]} = blockchain_txn:validate([SignedPoCReqTxn0], Chain),

    %% meck out the pmap function to simulate it returning an exit crash msg with body formatted as {{Error, Reason, Stack}
    meck:expect(blockchain_utils, pmap, fun(_, _) -> [{SignedPoCReqTxn0, {'EXIT', {{badmatch,{error, crash_in_pmap}}, []}}}] end),
    {[], [{SignedPoCReqTxn0, crash_in_pmap}]} = blockchain_txn:validate([SignedPoCReqTxn0], Chain),

    %% meck out the pmap function to simulate it returning an error msg in an unexpected format {error, unexpected_reason, unexpected_details}
    %% this will default to a generic error msg
    meck:expect(blockchain_utils, pmap, fun(_, _) -> [{SignedPoCReqTxn0, {crash, something_unexpected}}] end),
    {[], [{SignedPoCReqTxn0, txn_failed}]} = blockchain_txn:validate([SignedPoCReqTxn0], Chain),

    ?assert(meck:validate(blockchain_utils)),
    meck:unload(blockchain_utils),

    %% confirm the txn passes validation with the mecks removed
    {[SignedPoCReqTxn0], []} = blockchain_txn:validate([SignedPoCReqTxn0], Chain),

    ok.

genesis_no_var_validation_stay_invalid_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    {Priv, _} = ?config(master_key, Config),
    Chain = ?config(chain, Config),
    ct:pal("Chain ht: ~p", [blockchain:height(Chain)]),

    Ledger = blockchain:ledger(Chain),
    {ok, EV} = blockchain:config(election_version, Ledger),
    ct:pal("election_version: ~p", [EV]),
    ?assertEqual(10000, EV),
    ct:pal("Ledger ht: ~p", [blockchain_ledger_v1:current_height(Ledger)]),

    %% Attempt to change election_version to another incorrect value
    %% This will be a non-genesis block so we expect the validation to fail now
    Vars = #{election_version => 10001},
    ct:pal("priv_key ~p", [Priv]),
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),
    %% This var txn should blow up
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    ok.

genesis_no_var_validation_make_valid_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    {Priv, _} = ?config(master_key, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),
    ct:pal("Chain ht: ~p", [blockchain:height(Chain)]),
    ct:pal("Ledger ht: ~p", [blockchain_ledger_v1:current_height(Ledger)]),

    {ok, 10000} = blockchain:config(election_version, Ledger),

    %% Supply a valid election version
    Vars = #{election_version => 5},
    ct:pal("priv_key ~p", [Priv]),
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),

    %% This should succeed
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    ct:pal("Block2: ~p", [Block2]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:swarm()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),

    {ok, Delay} = blockchain:config(?vars_commit_delay, Ledger),
    ct:pal("commit delay ~p", [Delay]),

    %% Add some blocks, and check that election_version is set to 5 after delay is reached
    ok = lists:foreach(
           fun(_) ->
                   {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                   _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
                   {ok, Height} = blockchain:height(Chain),
                   case blockchain:config(election_version, Ledger) of
                       {ok, 10000} when Height < (Delay + 1) ->
                           ok;
                       {ok, 5} when Height >= (Delay + 1) ->
                           ok;
                       Res ->
                           throw({error, {chain_var_wrong_height, Res, Height}})
                   end
           end,
           lists:seq(1, 5)
          ),
    ?assertEqual({ok, 7}, blockchain:height(Chain)),
    ?assertEqual({ok, 5}, blockchain:config(election_version, Ledger)),

    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

create_gateway() ->
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    {Gateway, GatewaySigFun}.

fake_add_gateway(Owner, OwnerSigFun, Gateway, GatewaySigFun) ->
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun).

fake_assert_location(Owner, OwnerSigFun, Gateway, GatewaySigFun, Loc) ->
    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, Loc, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun).

fake_var_txn(Priv, Nonce, Vars) ->
    VarTxn = blockchain_txn_vars_v1:new(Vars, Nonce),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    blockchain_txn_vars_v1:proof(VarTxn, Proof).

dump_empty_blocks(Chain, ConsensusMembers, Count) ->
    lists:foreach(
        fun(_) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm())
        end,
        lists:seq(1, Count)
    ).

fake_poc_request(Gateway, GatewaySigFun, BlockHash) ->
    Keys0 = libp2p_crypto:generate_keys(ecc_compact),
    Secret0 = libp2p_crypto:keys_to_bin(Keys0),
    #{public := OnionCompactKey0} = Keys0,
    SecretHash0 = crypto:hash(sha256, Secret0),
    OnionKeyHash0 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey0)),
    PoCReqTxn0 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash0, OnionKeyHash0, BlockHash, 1),
    blockchain_txn_poc_request_v1:sign(PoCReqTxn0, GatewaySigFun).
