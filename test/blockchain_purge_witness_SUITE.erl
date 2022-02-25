-module(blockchain_purge_witness_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_txn_fees.hrl").

-define(TEST_LOCATION, 631210968840687103).
-define(TEST_LOCATION2, 631210968910285823).

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    purge_witness_test/1
]).

all() -> [
   %% purge_witness_test
].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config0) ->
    Config = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config0),
    BaseDir = ?config(base_dir, Config),
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars0 = #{
      poc_version => 2,
      poc_challenge_sync_interval => 10,
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 25,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 0,
      price_oracle_price_scan_max => 50,
      txn_fees => true,
      txn_fee_multiplier => 5000,
      max_payments => 10
    },


    Balance = 50000 * ?BONES_PER_HNT,
    BlocksN = 50,

    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} =
            test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars0),
    Chain = blockchain_worker:blockchain(),

    _Blocks0 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],


    {ExpectedPrices, Txns} = lists:unzip(make_oracle_txns(1, OracleKeys, 50)),

    {ok, PriceBlock} = test_utils:create_block(ConsensusMembers, Txns),
    blockchain:add_block(PriceBlock, Chain),

    _Blocks1 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],

    Ledger = blockchain:ledger(Chain),

    ct:pal("expected prices: ~p", [ExpectedPrices]),
    ct:pal("current oracle price: ~p", [median(ExpectedPrices)]),
    ?assertEqual({ok, median(ExpectedPrices)},
                    blockchain_ledger_v1:current_oracle_price(Ledger)),
    ?assertEqual({ok, lists:sort(ExpectedPrices)}, get_prices(
                    blockchain_ledger_v1:current_oracle_price_list(Ledger))),


    Ledger = blockchain:ledger(Chain),

    [_, {Payer, {_, PayerPrivKey, _}}, {Owner, {_, OwnerPrivKey, _}}|_] = ConsensusMembers,
    PayerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerOpenHNTBal =  blockchain_ledger_entry_v1:balance(NewEntry0),
    ct:pal("payer opening HNT balance: ~p", [PayerOpenHNTBal]),
    [{sup, Sup},
     {balance, Balance},
     {payer, Payer},
     {payer_sig_fun, PayerSigFun},
     {owner, Owner},
     {owner_sig_fun, OwnerSigFun},
     {ledger, Ledger},
     {chain, Chain},
     {consensus_members, ConsensusMembers},
     {payer_opening_hnt_bal, PayerOpenHNTBal} | Config ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    catch gen_server:stop(blockchain_sup),
    ok.

%%----------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

purge_witness_test(Config) ->
    %% Add 4 gateways and assert their locations
    %% start POC 1 , where GW1 = challenger, GW2 = Challengee, GW3 = Witness
    %% confirm challengee GW contains witness report from GW3
    %% Reassert GW3
    %% start POC 2 , where GW1 = challenger, GW2 = Challengee, GW4 = Witness
    %% confirm challengee GW is purged of witness report from GW3
    %% and contains witness report from GW4
    %% NOTE: purging occurs upon witness add hence the need for the second POC

    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),

    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),

    %% get the current height
    {ok, CurHeight} = blockchain:height(Chain),

    %%
    %% Top up the payer account so it can do whats needed
    %% this account will be used to fund the various txns
    %%

    BurnTx0 = blockchain_txn_token_burn_v1:new(Payer, Balance div 2, 1),
    %% get the fees for this txn
    BurnTxFee = blockchain_txn_token_burn_v1:calculate_fee(BurnTx0, Chain),
    ct:pal("Token burn txn fee ~p, staking fee ~p, total: ~p", [BurnTxFee, 'NA', BurnTxFee ]),

    %% get the payers HNT bal pre the burn
    {ok, PayerPreBurnEntry} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerPreBurnHNTBal =  blockchain_ledger_entry_v1:balance(PayerPreBurnEntry),

    %% set the fees on the base txn and then sign the various txns
    BurnTx1 = blockchain_txn_token_burn_v1:fee(BurnTx0, BurnTxFee),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx1, PayerSigFun),

    ?assertMatch(ok, blockchain_txn_token_burn_v1:is_valid(SignedBurnTx0, Chain)),
    {ok, BurnBlock} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    %% add the block
    blockchain:add_block(BurnBlock, Chain),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 1} =:= blockchain:height(Chain) end),

    %% confirm DC balances are credited
    {ok, PayerPostBurnEntry} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerPostBurnHNTBal =  blockchain_ledger_entry_v1:balance(PayerPostBurnEntry),
    ct:pal("Payer pre burn hnt bal: ~p, post burn hnt bal: ~p",[PayerPreBurnHNTBal, PayerPostBurnHNTBal]),

    %% create an owner for the gateways
    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Owner = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %%
    %% add gateway 1
    %%
    #{public := GW1PubKey, secret := GW1PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    GW1 = libp2p_crypto:pubkey_to_bin(GW1PubKey),
    GW1SigFun = libp2p_crypto:mk_sig_fun(GW1PrivKey),
    %% add gateway base txn
    AddGW1Tx0 = blockchain_txn_add_gateway_v1:new(Owner, GW1, Payer),
    %% get the fees for this txn
    AddGW1TxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddGW1Tx0, Chain),
    AddGW1StFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGW1Tx0, Chain),

    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGW1TxFee, AddGW1StFee, AddGW1TxFee + AddGW1StFee]),
    %% set the fees on the base txn and then sign the various txns
    AddGW1Tx1 = blockchain_txn_add_gateway_v1:fee(AddGW1Tx0, AddGW1TxFee),
    AddGW1Tx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGW1Tx1, AddGW1StFee),
    SignedOwnerAddGW1Tx2 = blockchain_txn_add_gateway_v1:sign(AddGW1Tx2, OwnerSigFun),
    SignedGatewayAddGW1Tx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGW1Tx2, GW1SigFun),
    SignedPayerAddGW1Tx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGW1Tx2, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGW1Tx2, Chain)),

    {ok, AddGW1Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGW1Tx2]),
    %% add the block
    _ = blockchain:add_block(AddGW1Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 2} =:= blockchain:height(Chain) end),

    %%
    %% Assert gateway 1
    %%
    AssertLocationGW1Tx = blockchain_txn_assert_location_v1:new(GW1, Owner, Payer, ?TEST_LOCATION, 1),
    AssertLocationGW1TxFee = blockchain_txn_assert_location_v1:calculate_fee(AssertLocationGW1Tx, Chain),
    AssertLocationGW1StFee = blockchain_txn_assert_location_v1:calculate_staking_fee(AssertLocationGW1Tx, Chain),
    AssertLocationGW1Tx1 = blockchain_txn_assert_location_v1:fee(AssertLocationGW1Tx, AssertLocationGW1TxFee),
    AssertLocationGW1Tx2 = blockchain_txn_assert_location_v1:staking_fee(AssertLocationGW1Tx1, AssertLocationGW1StFee),
    PartialAssertLocationGW1Txn = blockchain_txn_assert_location_v1:sign_request(AssertLocationGW1Tx2, GW1SigFun),
    SignedAssertLocationGW1Tx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationGW1Txn, OwnerSigFun),
    SignedPayerAssertLocationGW1Tx = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationGW1Tx, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationGW1Tx, Chain)),

    {ok, AssertLocationGW1Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationGW1Tx]),
    %% add the block
    _ = blockchain:add_block(AssertLocationGW1Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 3} =:= blockchain:height(Chain) end),


    %%
    %% add gateway 2
    %%
    #{public := GW2PubKey, secret := GW2PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    GW2 = libp2p_crypto:pubkey_to_bin(GW2PubKey),
    GW2SigFun = libp2p_crypto:mk_sig_fun(GW2PrivKey),
    %% add gateway base txn
    AddGW2Tx0 = blockchain_txn_add_gateway_v1:new(Owner, GW2, Payer),
    %% get the fees for this txn
    AddGW2TxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddGW2Tx0, Chain),
    AddGW2StFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGW2Tx0, Chain),

    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGW2TxFee, AddGW2StFee, AddGW2TxFee + AddGW2StFee]),
    %% set the fees on the base txn and then sign the various txns
    AddGW2Tx1 = blockchain_txn_add_gateway_v1:fee(AddGW2Tx0, AddGW2TxFee),
    AddGW2Tx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGW2Tx1, AddGW2StFee),
    SignedOwnerAddGW2Tx2 = blockchain_txn_add_gateway_v1:sign(AddGW2Tx2, OwnerSigFun),
    SignedGatewayAddGW2Tx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGW2Tx2, GW2SigFun),
    SignedPayerAddGW2Tx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGW2Tx2, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGW2Tx2, Chain)),

    {ok, AddGW2Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGW2Tx2]),
    %% add the block
    _ = blockchain:add_block(AddGW2Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 4} =:= blockchain:height(Chain) end),

    %%
    %% Assert gateway 2
    %%
    AssertLocationGW2Tx = blockchain_txn_assert_location_v1:new(GW2, Owner, Payer, ?TEST_LOCATION, 1),
    AssertLocationGW2TxFee = blockchain_txn_assert_location_v1:calculate_fee(AssertLocationGW2Tx, Chain),
    AssertLocationGW2StFee = blockchain_txn_assert_location_v1:calculate_staking_fee(AssertLocationGW2Tx, Chain),
    AssertLocationGW2Tx1 = blockchain_txn_assert_location_v1:fee(AssertLocationGW2Tx, AssertLocationGW2TxFee),
    AssertLocationGW2Tx2 = blockchain_txn_assert_location_v1:staking_fee(AssertLocationGW2Tx1, AssertLocationGW2StFee),
    PartialAssertLocationGW2Txn = blockchain_txn_assert_location_v1:sign_request(AssertLocationGW2Tx2, GW2SigFun),
    SignedAssertLocationGW2Tx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationGW2Txn, OwnerSigFun),
    SignedPayerAssertLocationGW2Tx = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationGW2Tx, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationGW2Tx, Chain)),

    {ok, AssertLocationGW2Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationGW2Tx]),
    %% add the block
    _ = blockchain:add_block(AssertLocationGW2Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 5} =:= blockchain:height(Chain) end),

    %%
    %% add gateway 3
    %%
    #{public := GW3PubKey, secret := GW3PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    GW3 = libp2p_crypto:pubkey_to_bin(GW3PubKey),
    GW3SigFun = libp2p_crypto:mk_sig_fun(GW3PrivKey),
    %% add gateway base txn
    AddGW3Tx0 = blockchain_txn_add_gateway_v1:new(Owner, GW3, Payer),
    %% get the fees for this txn
    AddGW3TxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddGW3Tx0, Chain),
    AddGW3StFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGW3Tx0, Chain),

    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGW3TxFee, AddGW3StFee, AddGW3TxFee + AddGW3StFee]),
    %% set the fees on the base txn and then sign the various txns
    AddGW3Tx1 = blockchain_txn_add_gateway_v1:fee(AddGW3Tx0, AddGW3TxFee),
    AddGW3Tx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGW3Tx1, AddGW3StFee),
    SignedOwnerAddGW3Tx2 = blockchain_txn_add_gateway_v1:sign(AddGW3Tx2, OwnerSigFun),
    SignedGatewayAddGW3Tx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGW3Tx2, GW3SigFun),
    SignedPayerAddGW3Tx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGW3Tx2, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGW3Tx2, Chain)),

    {ok, AddGW3Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGW3Tx2]),
    %% add the block
    _ = blockchain:add_block(AddGW3Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 6} =:= blockchain:height(Chain) end),

    %%
    %% Assert gateway 3
    %%
    AssertLocationGW3Tx = blockchain_txn_assert_location_v1:new(GW3, Owner, Payer, ?TEST_LOCATION, 1),
    AssertLocationGW3TxFee = blockchain_txn_assert_location_v1:calculate_fee(AssertLocationGW3Tx, Chain),
    AssertLocationGW3StFee = blockchain_txn_assert_location_v1:calculate_staking_fee(AssertLocationGW3Tx, Chain),
    AssertLocationGW3Tx1 = blockchain_txn_assert_location_v1:fee(AssertLocationGW3Tx, AssertLocationGW3TxFee),
    AssertLocationGW3Tx2 = blockchain_txn_assert_location_v1:staking_fee(AssertLocationGW3Tx1, AssertLocationGW3StFee),
    PartialAssertLocationGW3Txn = blockchain_txn_assert_location_v1:sign_request(AssertLocationGW3Tx2, GW3SigFun),
    SignedAssertLocationGW3Tx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationGW3Txn, OwnerSigFun),
    SignedPayerAssertLocationGW3Tx = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationGW3Tx, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationGW3Tx, Chain)),

    {ok, AssertLocationGW3Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationGW3Tx]),
    %% add the block
    _ = blockchain:add_block(AssertLocationGW3Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 7} =:= blockchain:height(Chain) end),

    %%
    %% add gateway 4
    %%
    #{public := GW4PubKey, secret := GW4PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    GW4 = libp2p_crypto:pubkey_to_bin(GW4PubKey),
    GW4SigFun = libp2p_crypto:mk_sig_fun(GW4PrivKey),
    %% add gateway base txn
    AddGW4Tx0 = blockchain_txn_add_gateway_v1:new(Owner, GW4, Payer),
    %% get the fees for this txn
    AddGW4TxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddGW4Tx0, Chain),
    AddGW4StFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGW4Tx0, Chain),

    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGW4TxFee, AddGW4StFee, AddGW4TxFee + AddGW4StFee]),
    %% set the fees on the base txn and then sign the various txns
    AddGW4Tx1 = blockchain_txn_add_gateway_v1:fee(AddGW4Tx0, AddGW4TxFee),
    AddGW4Tx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGW4Tx1, AddGW4StFee),
    SignedOwnerAddGW4Tx2 = blockchain_txn_add_gateway_v1:sign(AddGW4Tx2, OwnerSigFun),
    SignedGatewayAddGW4Tx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGW4Tx2, GW4SigFun),
    SignedPayerAddGW4Tx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGW4Tx2, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGW4Tx2, Chain)),

    {ok, AddGW4Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGW4Tx2]),
    %% add the block
    _ = blockchain:add_block(AddGW4Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 8} =:= blockchain:height(Chain) end),

    %%
    %% Assert gateway 4
    %%
    AssertLocationGW4Tx = blockchain_txn_assert_location_v1:new(GW4, Owner, Payer, ?TEST_LOCATION, 1),
    AssertLocationGW4TxFee = blockchain_txn_assert_location_v1:calculate_fee(AssertLocationGW4Tx, Chain),
    AssertLocationGW4StFee = blockchain_txn_assert_location_v1:calculate_staking_fee(AssertLocationGW4Tx, Chain),
    AssertLocationGW4Tx1 = blockchain_txn_assert_location_v1:fee(AssertLocationGW4Tx, AssertLocationGW4TxFee),
    AssertLocationGW4Tx2 = blockchain_txn_assert_location_v1:staking_fee(AssertLocationGW4Tx1, AssertLocationGW4StFee),
    PartialAssertLocationGW4Txn = blockchain_txn_assert_location_v1:sign_request(AssertLocationGW4Tx2, GW4SigFun),
    SignedAssertLocationGW4Tx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationGW4Txn, OwnerSigFun),
    SignedPayerAssertLocationGW4Tx = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationGW4Tx, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationGW4Tx, Chain)),

    {ok, AssertLocationGW4Block} = test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationGW4Tx]),
    %% add the block
    _ = blockchain:add_block(AssertLocationGW4Block, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 9} =:= blockchain:height(Chain) end),


    %%
    %% Start POC 1
    %% GW1 = challenger, GW2 = Challengee, GW3 = Witness
    %%

    %% have to meck out some stuff
    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_Txn, _Chain) -> ok end),

    POC1Keys = libp2p_crypto:generate_keys(ecc_compact),
    POC1Secret = libp2p_crypto:keys_to_bin(POC1Keys),
    #{public := POC1OnionCompactKey} = POC1Keys,
    POC1SecretHash = crypto:hash(sha256, POC1Secret),
    POC1OnionKeyHash = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(POC1OnionCompactKey)),
    POC1ReqTxn0 = blockchain_txn_poc_request_v1:new(GW1, POC1SecretHash, POC1OnionKeyHash, blockchain_block:hash_block(BurnBlock), 2),
    SignedPOC1ReqTxn0 = blockchain_txn_poc_request_v1:sign(POC1ReqTxn0, GW1SigFun),
    ?assertEqual(ok, blockchain_txn_poc_request_v1:is_valid(SignedPOC1ReqTxn0, Chain)),

    {ok, POC1ReqBlock} = test_utils:create_block(ConsensusMembers, [SignedPOC1ReqTxn0]),
    _ = blockchain_gossip_handler:add_block(POC1ReqBlock, Chain, self(), blockchain_swarm:tid()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 10} =:= blockchain:height(Chain) end),

    Ledger = blockchain:ledger(Chain),
    {ok, HeadHash3} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(POC1ReqBlock), HeadHash3),
    ?assertEqual({ok, POC1ReqBlock}, blockchain:get_block(HeadHash3, Chain)),

    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GW1Info} = blockchain_ledger_v1:find_gateway_info(GW1, Ledger),
    ?assertEqual(CurHeight + 10, blockchain_ledger_gateway_v2:last_poc_challenge(GW1Info)),
    ?assertEqual(POC1OnionKeyHash, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GW1Info)),
    ?assertEqual(0, maps:size(blockchain_ledger_gateway_v2:witnesses(GW1Info))),

    %%
    %% Make GW2 the challengee, GW3 a witness
    %%
    %% meck out some stuff which gets in the way of testing
    {ok, GW2Info} = blockchain_ledger_v1:find_gateway_info(GW2, Ledger),
    meck:new(blockchain_poc_path, [passthrough]),
    meck:expect(blockchain_poc_path, target, fun(_, _, _) -> {GW2, GW2Info} end),
    meck:expect(blockchain_poc_path, build, fun(_, _, _, _, _) -> {ok, [GW2]} end),

    POC1Rx1 = blockchain_poc_receipt_v1:new(
        GW2,
        1000,
        10,
        "first_rx",
        p2p
    ),
    SignedPOC1Rx1 = blockchain_poc_receipt_v1:sign(POC1Rx1, GW2SigFun),

    POC1Witness = blockchain_poc_witness_v1:new(
        GW3,
        1001,
        10,
        crypto:strong_rand_bytes(32),
        9.8,
        915.2,
        10,
        <<"data_rate">>
    ),
    SignedPOC1Witness = blockchain_poc_witness_v1:sign(POC1Witness, GW3SigFun),

    POC1Path = blockchain_poc_path_element_v1:new(GW2, SignedPOC1Rx1, [SignedPOC1Witness]),
    ct:pal("POC1Path: ~p", [POC1Path]),

    POC1ReceiptsTxn = blockchain_txn_poc_receipts_v1:new(
        GW1,
        POC1Secret,
        POC1OnionKeyHash,
        [POC1Path]
    ),
    SignedPOC1ReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(POC1ReceiptsTxn, GW1SigFun),
    ?assertEqual(true, blockchain_poc_witness_v1:is_valid(SignedPOC1Witness, Ledger)),
    ?assertEqual(true, blockchain_poc_receipt_v1:is_valid(SignedPOC1Rx1, Ledger)),
    ?assertEqual(ok, blockchain_txn_poc_receipts_v1:is_valid(SignedPOC1ReceiptsTxn, Chain)),

    %% add the receipts txn
    {ok, AddPOC1ReceiptsBlock} = test_utils:create_block(ConsensusMembers, [SignedPOC1ReceiptsTxn]),
    %% add the block
    _ = blockchain:add_block(AddPOC1ReceiptsBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 11} =:= blockchain:height(Chain) end),

    %% confirm the challengee GW2, now contains a single witness report and from GW3
    {ok, GW2InfoB} = blockchain_ledger_v1:find_gateway_info(GW2, Ledger),
    GW2WitnessesPOC1 = blockchain_ledger_gateway_v2:witnesses(GW2InfoB),
    ct:pal("GW2WitnessesPOC1: ~p", [GW2WitnessesPOC1]),
    ?assertEqual(1, maps:size(GW2WitnessesPOC1)),
    ?assertNotEqual(not_found, maps:get(GW3, GW2WitnessesPOC1, not_found)),


    %%
    %% Re Assert gateway 3
    %% GW2 has a witness report from GW3, it is now stale
    %% and should be purged on next witnesses read of GW2
    %%
    AssertLocationGW3TxB = blockchain_txn_assert_location_v1:new(GW3, Owner, Payer, ?TEST_LOCATION2, 2),
    AssertLocationGW3TxFeeB = blockchain_txn_assert_location_v1:calculate_fee(AssertLocationGW3TxB, Chain),
    AssertLocationGW3StFeeB = blockchain_txn_assert_location_v1:calculate_staking_fee(AssertLocationGW3TxB, Chain),
    AssertLocationGW3Tx1B = blockchain_txn_assert_location_v1:fee(AssertLocationGW3TxB, AssertLocationGW3TxFeeB),
    AssertLocationGW3Tx2B = blockchain_txn_assert_location_v1:staking_fee(AssertLocationGW3Tx1B, AssertLocationGW3StFeeB),
    PartialAssertLocationGW3TxnB = blockchain_txn_assert_location_v1:sign_request(AssertLocationGW3Tx2B, GW3SigFun),
    SignedAssertLocationGW3TxB = blockchain_txn_assert_location_v1:sign(PartialAssertLocationGW3TxnB, OwnerSigFun),
    SignedPayerAssertLocationGW3TxB = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationGW3TxB, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationGW3TxB, Chain)),

    {ok, AssertLocationGW3BlockB} = test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationGW3TxB]),
    %% add the block
    _ = blockchain:add_block(AssertLocationGW3BlockB, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 12} =:= blockchain:height(Chain) end),

    %%
    %% confirm GW2 still has 1 witness
    %% even tho GW3 has reasserted loc
    %% the purge wont happen until the next
    %% witness is added to the challengee GW
    %%
    {ok, GW2InfoC} = blockchain_ledger_v1:find_gateway_info(GW2, Ledger),
    %% witnesses/1 is not filtered of stale witnesses, it returns the unfiltered witness list
    %% as no new witness has been added yet, GW3 should still be there
    GW2WitnessesC = blockchain_ledger_gateway_v2:witnesses(GW2InfoC),
    ct:pal("GW2WitnessesC: ~p", [GW2WitnessesC]),
    ?assertEqual(1, maps:size(GW2WitnessesC)),
    ?assertNotEqual(not_found, maps:get(GW3, GW2WitnessesC, not_found)),

    %% witnesses/3 is filtered of stale witnesses, it checks for any stale entries
    %% and purges them
    %% GW3 should not be returned here
    GW2WitnessesD = blockchain_ledger_gateway_v2:witnesses(GW2, GW2InfoC, Ledger),
    ?assertEqual(0, maps:size(GW2WitnessesD)),
    ?assertEqual(not_found, maps:get(GW3, GW2WitnessesD, not_found)),


    %%
    %% Start POC 2
    %% GW1 = challenger, GW2 = Challengee, GWW = Witness
    %%

    %% add some fake blocks to get POC1 past poc interval
    Swarm =  blockchain_swarm:swarm(),
    LastFakeBlock = add_and_gossip_fake_blocks(30, ConsensusMembers, Swarm, Chain, GW1),

    POC2Keys = libp2p_crypto:generate_keys(ecc_compact),
    POC2Secret = libp2p_crypto:keys_to_bin(POC2Keys),
    #{public := POC2OnionCompactKey} = POC2Keys,
    POC2SecretHash = crypto:hash(sha256, POC2Secret),
    POC2OnionKeyHash = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(POC2OnionCompactKey)),
    POC2ReqTxn0 = blockchain_txn_poc_request_v1:new(GW1, POC2SecretHash, POC2OnionKeyHash, blockchain_block:hash_block(LastFakeBlock), 2),
    SignedPOC2ReqTxn0 = blockchain_txn_poc_request_v1:sign(POC2ReqTxn0, GW1SigFun),
    ?assertEqual(ok, blockchain_txn_poc_request_v1:is_valid(SignedPOC2ReqTxn0, Chain)),

    {ok, POC2ReqBlock} = test_utils:create_block(ConsensusMembers, [SignedPOC2ReqTxn0]),
    _ = blockchain_gossip_handler:add_block(POC2ReqBlock, Chain, self(), blockchain_swarm:tid()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 43} =:= blockchain:height(Chain) end),

    %%
    %% Make GW2 the challengee, GW4 a witness
    %%

    POC2Rx1 = blockchain_poc_receipt_v1:new(
        GW2,
        1000,
        10,
        "first_rx",
        p2p
    ),
    SignedPOC2Rx1 = blockchain_poc_receipt_v1:sign(POC2Rx1, GW2SigFun),

    POC2Witness = blockchain_poc_witness_v1:new(
        GW4,
        1001,
        10,
        crypto:strong_rand_bytes(32),
        9.8,
        915.2,
        10,
        <<"data_rate">>
    ),
    SignedPOC2Witness = blockchain_poc_witness_v1:sign(POC2Witness, GW4SigFun),

    POC2Path = blockchain_poc_path_element_v1:new(GW2, SignedPOC2Rx1, [SignedPOC2Witness]),
    ct:pal("POC2Path: ~p", [POC2Path]),

    POC2ReceiptsTxn = blockchain_txn_poc_receipts_v1:new(
        GW1,
        POC2Secret,
        POC2OnionKeyHash,
        [POC2Path]
    ),
    SignedPOC2ReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(POC2ReceiptsTxn, GW1SigFun),
    ?assertEqual(true, blockchain_poc_witness_v1:is_valid(SignedPOC2Witness, Ledger)),
    ?assertEqual(true, blockchain_poc_receipt_v1:is_valid(SignedPOC2Rx1, Ledger)),
    ?assertEqual(ok, blockchain_txn_poc_receipts_v1:is_valid(SignedPOC2ReceiptsTxn, Chain)),

    %% add the receipts txn
    {ok, AddPOC2ReceiptsBlock} = test_utils:create_block(ConsensusMembers, [SignedPOC2ReceiptsTxn]),
    %% add the block
    _ = blockchain:add_block(AddPOC2ReceiptsBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 44} =:= blockchain:height(Chain) end),

    %% confirm the challengee GW2, now contains a single witness report and from GW4
    {ok, GW2InfoE} = blockchain_ledger_v1:find_gateway_info(GW2, Ledger),
    %% witnesses/1 is not filtered of stale witnesses, it returns the unfiltered witness list
    %% which due to the new witness being added should now be purged of GW3
    GW2WitnessesPOC2 = blockchain_ledger_gateway_v2:witnesses(GW2InfoE),

    ct:pal("GW2WitnessesPOC2 ~p", [GW2WitnessesPOC2]),
    ?assertEqual(1, maps:size(GW2WitnessesPOC2)),
    ?assertNotEqual(not_found, maps:get(GW4, GW2WitnessesPOC2, not_found)),
    ?assertEqual(not_found, maps:get(GW3, GW2WitnessesPOC2, not_found)),

    %% witnesses/3 is filtered of stale witnesses, it should now
    %% return same as witnesses/1
    GW2WitnessesF = blockchain_ledger_gateway_v2:witnesses(GW2, GW2InfoE, Ledger),
    ?assertEqual(1, maps:size(GW2WitnessesPOC2)),
    ?assertNotEqual(not_found, maps:get(GW4, GW2WitnessesF, not_found)),
    ?assertEqual(not_found, maps:get(GW3, GW2WitnessesF, not_found)),


    ok.

%%--------------------------------------------------------------------
%% TEST HELPERS
%%--------------------------------------------------------------------
prices() -> [ 10000000, 20000000, 30000000]. %% 10 cents, 20 cents, 30 cents multiplied by 100 million

random_price(Prices) ->
    Pos = rand:uniform(length(Prices)),
    lists:nth(Pos, Prices).

make_oracles(N) ->
    {ok, [ libp2p_crypto:generate_keys(ecc_compact) || _ <- lists:seq(1, N) ]}.

%% N: how many sets of txns to make
%% Keys: the actual key material
%% BlockHeight: the block height to put in the transaction
make_oracle_txns(N, Keys, BlockHeight) ->
    lists:flatten([
       [
        begin
         Price = random_price(prices()),
         {Price, make_and_sign_txn(K, Price, BlockHeight)}
        end || K <- Keys ]
                || _ <- lists:seq(1, N) ]).

make_and_sign_txn(#{public := PubKey, secret := SecretKey}, Price, BlockHeight) ->
    SignFun = libp2p_crypto:mk_sig_fun(SecretKey),
    RawTxn = blockchain_txn_price_oracle_v1:new(libp2p_crypto:pubkey_to_bin(PubKey), Price, BlockHeight),
    blockchain_txn_price_oracle_v1:sign(RawTxn, SignFun).

prep_public_key(#{public := K}) ->
    BinPK = libp2p_crypto:pubkey_to_bin(K),
    <<(byte_size(BinPK)):8/unsigned-integer, BinPK/binary>>.

make_encoded_oracle_keys(Keys) ->
    {ok, << <<(prep_public_key(K))/binary>> || K <- Keys >> }.

get_prices({ok, Ps}) ->
    {ok, lists:sort([ blockchain_ledger_oracle_price_entry:price(P) || P <- Ps ])}.

median(Ps) ->
    blockchain_ledger_v1:median(Ps).

add_and_gossip_fake_blocks(NumFakeBlocks, ConsensusMembers, Swarm, Chain, From) ->
    lists:foreach(
        fun(_) ->
            {ok, B} = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(B, Chain, From, Swarm)
        end,
        lists:seq(1, NumFakeBlocks-1)
    ),
    {ok, LastFakeBlock} = test_utils:create_block(ConsensusMembers, []),
    _ = blockchain_gossip_handler:add_block(LastFakeBlock, Chain, From, Swarm),
    LastFakeBlock.
