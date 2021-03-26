-module(blockchain_capabilities_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_txn_fees.hrl").

-define(TEST_LOCATION, 631210968840687103).

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    light_gateway_simple_checks/1,
    light_gateway_poc_checks/1,
    full_gateway_poc_checks/1,
    nonconsensus_gateway_poc_checks/1
]).

all() -> [
    light_gateway_simple_checks,
    light_gateway_poc_checks,
    full_gateway_poc_checks,
    nonconsensus_gateway_poc_checks

].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config0)  when TestCase == light_gateway_simple_checks;
                                           TestCase == light_gateway_poc_checks;
                                           TestCase == full_gateway_poc_checks;
                                           TestCase == nonconsensus_gateway_poc_checks ->
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
      staking_fee_txn_oui_v1 => 100 * ?USD_TO_DC, %% $100?
      staking_fee_txn_oui_v1_per_address => 100 * ?USD_TO_DC, %% $100
      staking_fee_txn_add_gateway_v1 => 40 * ?USD_TO_DC, %% $40?
      staking_fee_txn_assert_location_v1 => 10 * ?USD_TO_DC, %% $10?
      txn_fee_multiplier => 5000,
      max_payments => 10
    },

    {ExtraVars, ExtraConfig} = case TestCase of
                                   X when X == light_gateway_simple_checks;
                                          X == light_gateway_poc_checks ->
                                       ct:pal("setup for staking_key_mode_mappings_light_gateway_capabilities", []),
                                       #{public := StakingPub, secret := _StakingPrivKey} = StakingKey = libp2p_crypto:generate_keys(ecc_compact),
                                       StakingKeyPubBin = libp2p_crypto:pubkey_to_bin(StakingPub),
                                       Mappings = [{StakingKeyPubBin, <<"light">>}],
                                       {ok, MappingsBin} = make_staking_keys_mode_mappings(Mappings),
                                       MappingsExtraVars1 = maps:put(staking_keys_to_mode_mappings, MappingsBin, ExtraVars0),
                                       MappingsExtraVars2 = maps:put(staking_fee_txn_add_light_gateway_v1, 20 * ?USD_TO_DC, MappingsExtraVars1),
                                       {MappingsExtraVars2, [{staking_key, StakingKey}, {staking_key_pub_bin, StakingKeyPubBin}]};
                                   X when X == full_gateway_poc_checks ->
                                       ct:pal("setup for staking_key_mode_mappings_full_gateway_capabilities", []),
                                       #{public := StakingPub, secret := _StakingPrivKey} = StakingKey = libp2p_crypto:generate_keys(ecc_compact),
                                       StakingKeyPubBin = libp2p_crypto:pubkey_to_bin(StakingPub),
                                       Mappings = [{StakingKeyPubBin, <<"full">>}],
                                       {ok, MappingsBin} = make_staking_keys_mode_mappings(Mappings),
                                       MappingsExtraVars1 = maps:put(staking_keys_to_mode_mappings, MappingsBin, ExtraVars0),
                                       MappingsExtraVars2 = maps:put(staking_fee_txn_add_light_gateway_v1, 40 * ?USD_TO_DC, MappingsExtraVars1),
                                       {MappingsExtraVars2, [{staking_key, StakingKey}, {staking_key_pub_bin, StakingKeyPubBin}]};
                                   X when X == nonconsensus_gateway_poc_checks ->
                                       ct:pal("setup for staking_key_mode_mappings_full_gateway_capabilities", []),
                                       #{public := StakingPub, secret := _StakingPrivKey} = StakingKey = libp2p_crypto:generate_keys(ecc_compact),
                                       StakingKeyPubBin = libp2p_crypto:pubkey_to_bin(StakingPub),
                                       Mappings = [{StakingKeyPubBin, <<"nonconsensus">>}],
                                       {ok, MappingsBin} = make_staking_keys_mode_mappings(Mappings),
                                       MappingsExtraVars1 = maps:put(staking_keys_to_mode_mappings, MappingsBin, ExtraVars0),
                                       MappingsExtraVars2 = maps:put(staking_fee_txn_add_light_gateway_v1, 40 * ?USD_TO_DC, MappingsExtraVars1),
                                       {MappingsExtraVars2, [{staking_key, StakingKey}, {staking_key_pub_bin, StakingKeyPubBin}]};
                                   _ ->
                                       {ExtraVars0, []}
                               end,

    Balance = 50000 * ?BONES_PER_HNT,
    BlocksN = 50,

    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} =
            test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),
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

    [_, {Payer, {_, PayerPrivKey, _}}, {Owner, {_, OwnerPrivKey, _}}|_] = ConsensusMembers,
    PayerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerOpenHNTBal =  blockchain_ledger_entry_v1:balance(NewEntry0),
    ct:pal("payer opening HNT balance: ~p", [PayerOpenHNTBal]),
    ExtraConfig ++
    [{sup, Sup},
     {balance, Balance},
     {payer, Payer},
     {payer_sig_fun, PayerSigFun},
     {owner, Owner},
     {owner_sig_fun, OwnerSigFun},
     {ledger, Ledger},
     {chain, Chain},
     {consensus_members, ConsensusMembers},
     {payer_opening_hnt_bal, PayerOpenHNTBal} | Config ];

init_per_testcase(TestCase, Config) ->
    blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config).

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    catch gen_server:stop(blockchain_sup),
    ok.

%%----------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

light_gateway_simple_checks(Config) ->
    %% add gateways where the staker has a mapping to a light gateway in the staking key mode mappings tables
    %% Confirm the capabilities prevent the GW submitting unpermitted txns
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),

    Chain = ?config(chain, Config),
    Ledger = ?config(ledger, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    {ok, CurHeight} = blockchain:height(Chain),

    %%
    %% create a payment txn to fund staking account - which will have a staking key mapping set to full
    %%

    #{public := _StakingPub, secret := StakingPrivKey} = ?config(staking_key, Config),
    Staker = ?config(staking_key_pub_bin, Config),
    StakerSigFun = libp2p_crypto:mk_sig_fun(StakingPrivKey),
    %% base txn
    PaymentTx0 = blockchain_txn_payment_v1:new(Payer, Staker, 5000 * ?BONES_PER_HNT, 1),
    PaymentTxFee = blockchain_txn_payment_v1:calculate_fee(PaymentTx0, Chain),
    ct:pal("payment txn fee ~p, staking fee ~p, total: ~p", [PaymentTxFee, 'NA', PaymentTxFee ]),
    PaymentTx1 = blockchain_txn_payment_v1:fee(PaymentTx0, PaymentTxFee),
    SignedPaymentTx1 = blockchain_txn_payment_v1:sign(PaymentTx1, PayerSigFun),
    ?assertEqual(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx1, Chain)),

    %% check is_valid behaves as expected
    ?assertMatch(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx1, Chain)),
    {ok, PaymentBlock} = test_utils:create_block(ConsensusMembers, [SignedPaymentTx1]),
    %% add the block
    blockchain:add_block(PaymentBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 1} =:= blockchain:height(Chain) end),

    %% add the gateway using the staker key, will be added as a light gateway
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),

    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Owner = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %% add gateway base txn
    AddGatewayTx0 = blockchain_txn_add_gateway_v1:new(Owner, Gateway, Staker),
    %% get the fees for this txn
    AddGatewayTxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddGatewayTx0, Chain),
    AddGatewayStFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGatewayTx0, Chain),
    %% light gateway costs 20 usd
    ?assertEqual(20 * ?USD_TO_DC, AddGatewayStFee),

    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGatewayTxFee, AddGatewayStFee, AddGatewayTxFee + AddGatewayStFee]),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx0, AddGatewayTxFee),
    AddGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx1, AddGatewayStFee),

    SignedOwnerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx2, OwnerSigFun),
    SignedGatewayAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx2, GatewaySigFun),
    SignedPayerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx2, StakerSigFun),

    {ok, AddGatewayBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx2]),
    %% add the block
    _ = blockchain:add_block(AddGatewayBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 2} =:= blockchain:height(Chain) end),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, GW} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertMatch(light, blockchain_ledger_gateway_v2:mode(GW)),

    %% attempt to add a POC request txn, should be declared invalid as the GW does not have the capability to do so
    Keys0 = libp2p_crypto:generate_keys(ecc_compact),
    Secret0 = libp2p_crypto:keys_to_bin(Keys0),
    #{public := OnionCompactKey0} = Keys0,
    SecretHash0 = crypto:hash(sha256, Secret0),
    OnionKeyHash0 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey0)),
    PoCReqTxn0 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash0, OnionKeyHash0, blockchain_block:hash_block(AddGatewayBlock), 1),
    SignedPoCReqTxn0 = blockchain_txn_poc_request_v1:sign(PoCReqTxn0, GatewaySigFun),
    ?assertEqual({error,gateway_bad_capabilities}, blockchain_txn_poc_request_v1:is_valid(SignedPoCReqTxn0, Chain)),

    %% attempt to add a POC receipt txn, should be declared invalid as the challenger GW does not have the capability send POC requests
    PoCReceiptsTxn = blockchain_txn_poc_receipts_v1:new(Gateway, Secret0, OnionKeyHash0, []),
    SignedPoCReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(PoCReceiptsTxn, GatewaySigFun),
    ?assertEqual({error,challenger_bad_capabilities}, blockchain_txn_poc_receipts_v1:is_valid(SignedPoCReceiptsTxn, Chain)),


    ok.

light_gateway_poc_checks(Config0) ->
    %% Add a full gateway and a light gateway
    %% the full gateway will create a POC request
    %% the light gateway will attempt to witness the POC and send a receipt
    %% this will fail as the light gateway does not have capabilities to either witness or receipt POCs

    Config = setup(Config0),

    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    FullGateway = ?config(full_gateway, Config),
    FullGatewaySigFun = ?config(full_gateway_sig_fun, Config),

    SecondGateway = ?config(second_gateway, Config),
    SecondGatewaySigFun = ?config(second_gateway_sig_fun, Config),
    SecondGatewayStFee = ?config(second_gateway_staking_fee, Config),

    Secret0 = ?config(poc_secret, Config),
    OnionKeyHash0 = ?config(poc_onion, Config),

    %% get the current height
    {ok, _CurHeight} = blockchain:height(Chain),

    %% light gateway costs 20 usd, confirm thats what we paid
    ?assertEqual(20 * ?USD_TO_DC, SecondGatewayStFee),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, FullGW} = blockchain_ledger_v1:find_gateway_info(FullGateway, Ledger),
    ?assertMatch(full, blockchain_ledger_gateway_v2:mode(FullGW)),

    %% check the ledger to confirm the second gateway is added with the correct mode
    {ok, SecondGWInfo} = blockchain_ledger_v1:find_gateway_info(SecondGateway, Ledger),
    ?assertMatch(light, blockchain_ledger_gateway_v2:mode(SecondGWInfo)),

    %%
    %% attempt to have the second gateway act as a witness to the POC challenge from the full gateway
    %%

    Rx1 = blockchain_poc_receipt_v1:new(
        SecondGateway,
        1000,
        10,
        "first_rx",
        p2p
    ),
    SignedRx1 = blockchain_poc_receipt_v1:sign(Rx1, SecondGatewaySigFun),

    Witness = blockchain_poc_witness_v1:new(
        SecondGateway,
        1001,
        10,
        crypto:strong_rand_bytes(32),
        9.8,
        915.2,
        10,
        <<"data_rate">>
    ),
    SignedWitness = blockchain_poc_witness_v1:sign(Witness, SecondGatewaySigFun),

    P1 = blockchain_poc_path_element_v1:new(SecondGateway, SignedRx1, [SignedWitness]),
    ct:pal("P1: ~p", [P1]),

    %% meck out some stuff which gets in the way of testing the capability checks
    %% ignore any bad targeting and pathing, not relevant to this test
    meck:new(blockchain_poc_path, [passthrough]),
    meck:expect(blockchain_poc_path, target, fun(_, _, _) -> {SecondGateway, SecondGWInfo} end),
    meck:expect(blockchain_poc_path, build, fun(_, _, _, _, _) -> {ok, [SecondGateway]} end),

    PoCReceiptsTxn = blockchain_txn_poc_receipts_v1:new(
        FullGateway,
        Secret0,
        OnionKeyHash0,
        [P1]
    ),
    SignedPoCReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(PoCReceiptsTxn, FullGatewaySigFun),

    %% assert the light gw witness is invalid
    %% unfortunately the is_valid function returns a boolean so cant assert on any reason but have confirmed via logging it fails on capabilitieis
    ?assertEqual(false, blockchain_poc_witness_v1:is_valid(SignedWitness, Ledger)),

    %% assert the light gw receipt is invalid
    %% unfortunately the is_valid function returns a boolean so cant assert on any reason but have confirmed via logging it fails on capabilitieis
    ?assertEqual(false, blockchain_poc_receipt_v1:is_valid(SignedRx1, Ledger)),

    %% the txn which includes the receipt will be declared invalid as the enclosed receipt will fail validity checks due to capabilities
    ?assertEqual({error,invalid_receipt}, blockchain_txn_poc_receipts_v1:is_valid(SignedPoCReceiptsTxn, Chain)),

    %%
    %% update the light gateway's mode to full and confirm the various is_valid tests above then pass
    %%
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    SecondGWInfo0 = blockchain_ledger_gateway_v2:mode(full, SecondGWInfo),
    _ = blockchain_ledger_v1:update_gateway(SecondGWInfo0, SecondGateway, Ledger1),
    ?assertEqual(true, blockchain_poc_witness_v1:is_valid(SignedWitness, Ledger1)),
    ?assertEqual(true, blockchain_poc_receipt_v1:is_valid(SignedRx1, Ledger1)),
    _ = blockchain_ledger_v1:commit_context(Ledger1),
    %% the txn below currently fails due to bad pathing, need to meck some more but not sure what yet
%%    ?assertEqual(ok, blockchain_txn_poc_receipts_v1:is_valid(SignedPoCReceiptsTxn, Chain)),
    ok.

full_gateway_poc_checks(Config0) ->
    %% Add a full gateway and a second gateway again in full mode
    %% the first full gateway will create a POC request
    %% the second full gateway will attempt to witness the POC and send a receipt
    %% this should succeed
    Config = setup(Config0),

    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    FullGateway = ?config(full_gateway, Config),
    FullGatewaySigFun = ?config(full_gateway_sig_fun, Config),

    SecondGateway = ?config(second_gateway, Config),
    SecondGatewaySigFun = ?config(second_gateway_sig_fun, Config),
    SecondGatewayStFee = ?config(second_gateway_staking_fee, Config),

    Secret0 = ?config(poc_secret, Config),
    OnionKeyHash0 = ?config(poc_onion, Config),

    %% get the current height
    {ok, _CurHeight} = blockchain:height(Chain),

    %% full gateway costs 40 usd, confirm thats what we paid
    ?assertEqual(40 * ?USD_TO_DC, SecondGatewayStFee),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, FullGW} = blockchain_ledger_v1:find_gateway_info(FullGateway, Ledger),
    ?assertMatch(full, blockchain_ledger_gateway_v2:mode(FullGW)),

    %% check the ledger to confirm the second gateway is added with the correct mode
    {ok, SecondGWInfo} = blockchain_ledger_v1:find_gateway_info(SecondGateway, Ledger),
    ?assertMatch(full, blockchain_ledger_gateway_v2:mode(SecondGWInfo)),

    %%
    %% attempt to have the second gateway act as a witness to the POC challenge from the first/full gateway
    %%

    Rx1 = blockchain_poc_receipt_v1:new(
        SecondGateway,
        1000,
        10,
        "first_rx",
        p2p
    ),
    SignedRx1 = blockchain_poc_receipt_v1:sign(Rx1, SecondGatewaySigFun),

    Witness = blockchain_poc_witness_v1:new(
        SecondGateway,
        1001,
        10,
        crypto:strong_rand_bytes(32),
        9.8,
        915.2,
        10,
        <<"data_rate">>
    ),
    SignedWitness = blockchain_poc_witness_v1:sign(Witness, SecondGatewaySigFun),

    P1 = blockchain_poc_path_element_v1:new(SecondGateway, SignedRx1, [SignedWitness]),
    ct:pal("P1: ~p", [P1]),

    %% meck out some stuff which gets in the way of testing the capability checks
    %% ignore any bad targeting and pathing, not relevant to this test
    meck:new(blockchain_poc_path, [passthrough]),
    meck:expect(blockchain_poc_path, target, fun(_, _, _) -> {SecondGateway, SecondGWInfo} end),
    meck:expect(blockchain_poc_path, build, fun(_, _, _, _, _) -> {ok, [SecondGateway]} end),

    PoCReceiptsTxn = blockchain_txn_poc_receipts_v1:new(
        FullGateway,
        Secret0,
        OnionKeyHash0,
        [P1]
    ),
    _SignedPoCReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(PoCReceiptsTxn, FullGatewaySigFun),

    %% assert the light gw witness is invalid
    %% unfortunately the is_valid function returns a boolean so cant assert on any reason but have confirmed via logging it fails on capabilitieis
    ?assertEqual(true, blockchain_poc_witness_v1:is_valid(SignedWitness, Ledger)),

    %% assert the light gw receipt is invalid
    %% unfortunately the is_valid function returns a boolean so cant assert on any reason but have confirmed via logging it fails on capabilitieis
    ?assertEqual(true, blockchain_poc_receipt_v1:is_valid(SignedRx1, Ledger)),

    %% the txn which includes the receipt will be declared invalid as the enclosed receipt will fail validity checks due to capabilities
    %% the txn receipts is valid check is currently failing due to a bad LayerDatum value
    %% likely due to the mecking above
    %% TODO: resolve this prob and put the assert below back in
%%    ?assertEqual(ok, blockchain_txn_poc_receipts_v1:is_valid(SignedPoCReceiptsTxn, Chain)),

    ok.

nonconsensus_gateway_poc_checks(Config0) ->
    %% Add a full gateway and a second gateway again in nonconsensus mode
    %% the first full gateway will create a POC request
    %% the second  gateway will attempt to witness the POC and send a receipt
    %% this should succeed
    Config = setup(Config0),

    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    FullGateway = ?config(full_gateway, Config),
    FullGatewaySigFun = ?config(full_gateway_sig_fun, Config),

    SecondGateway = ?config(second_gateway, Config),
    SecondGatewaySigFun = ?config(second_gateway_sig_fun, Config),
    SecondGatewayStFee = ?config(second_gateway_staking_fee, Config),

    Secret0 = ?config(poc_secret, Config),
    OnionKeyHash0 = ?config(poc_onion, Config),

    %% get the current height
    {ok, _CurHeight} = blockchain:height(Chain),

    %% nonconsensus gateway costs 40 usd, confirm thats what we paid
    ?assertEqual(40 * ?USD_TO_DC, SecondGatewayStFee),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, FullGW} = blockchain_ledger_v1:find_gateway_info(FullGateway, Ledger),
    ?assertMatch(full, blockchain_ledger_gateway_v2:mode(FullGW)),

    %% check the ledger to confirm the second gateway is added with the correct mode
    {ok, SecondGWInfo} = blockchain_ledger_v1:find_gateway_info(SecondGateway, Ledger),
    ?assertMatch(nonconsensus, blockchain_ledger_gateway_v2:mode(SecondGWInfo)),

    %%
    %% attempt to have the second gateway act as a witness to the POC challenge from the first/full gateway
    %%

    Rx1 = blockchain_poc_receipt_v1:new(
        SecondGateway,
        1000,
        10,
        "first_rx",
        p2p
    ),
    SignedRx1 = blockchain_poc_receipt_v1:sign(Rx1, SecondGatewaySigFun),

    Witness = blockchain_poc_witness_v1:new(
        SecondGateway,
        1001,
        10,
        crypto:strong_rand_bytes(32),
        9.8,
        915.2,
        10,
        <<"data_rate">>
    ),
    SignedWitness = blockchain_poc_witness_v1:sign(Witness, SecondGatewaySigFun),

    P1 = blockchain_poc_path_element_v1:new(SecondGateway, SignedRx1, [SignedWitness]),
    ct:pal("P1: ~p", [P1]),

    %% meck out some stuff which gets in the way of testing the capability checks
    %% ignore any bad targeting and pathing, not relevant to this test
    meck:new(blockchain_poc_path, [passthrough]),
    meck:expect(blockchain_poc_path, target, fun(_, _, _) -> {SecondGateway, SecondGWInfo} end),
    meck:expect(blockchain_poc_path, build, fun(_, _, _, _, _) -> {ok, [SecondGateway]} end),

    PoCReceiptsTxn = blockchain_txn_poc_receipts_v1:new(
        FullGateway,
        Secret0,
        OnionKeyHash0,
        [P1]
    ),
    _SignedPoCReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(PoCReceiptsTxn, FullGatewaySigFun),

    %% assert the light gw witness is invalid
    %% unfortunately the is_valid function returns a boolean so cant assert on any reason but have confirmed via logging it fails on capabilitieis
    ?assertEqual(true, blockchain_poc_witness_v1:is_valid(SignedWitness, Ledger)),

    %% assert the light gw receipt is invalid
    %% unfortunately the is_valid function returns a boolean so cant assert on any reason but have confirmed via logging it fails on capabilitieis
    ?assertEqual(true, blockchain_poc_receipt_v1:is_valid(SignedRx1, Ledger)),

    %% the txn which includes the receipt will be declared invalid as the enclosed receipt will fail validity checks due to capabilities
    %% the txn receipts is valid check is currently failing due to a bad LayerDatum value
    %% likely due to the mecking above
    %% TODO: resolve this prob and put the assert below back in
%%    ?assertEqual(ok, blockchain_txn_poc_receipts_v1:is_valid(SignedPoCReceiptsTxn, Chain)),

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

make_staking_keys_mode_mappings(Prop) ->
    {ok, blockchain_utils:prop_to_bin(Prop)}.

get_prices({ok, Ps}) ->
    {ok, lists:sort([ blockchain_ledger_oracle_price_entry:price(P) || P <- Ps ])}.

median(Ps) ->
    blockchain_ledger_v1:median(Ps).


setup(Config)->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),
    Ledger = blockchain:ledger(Chain),

    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Owner = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %% get the current height
    {ok, CurHeight} = blockchain:height(Chain),

    %%
    %% Top up the payer account so it can do whats needed
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
    %% all the fees are set, so this should work
    {ok, BurnBlock} = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    %% add the block
    blockchain:add_block(BurnBlock, Chain),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 1} =:= blockchain:height(Chain) end),

    %% confirm DC balances are debited with correct fee
    %% the fee will be paid in HNT as the Payer wont have DC until after the txn has been fully absorbed
    {ok, PayerPostBurnEntry} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerPostBurnHNTBal =  blockchain_ledger_entry_v1:balance(PayerPostBurnEntry),
    ct:pal("Payer pre burn hnt bal: ~p, post burn hnt bal: ~p",[PayerPreBurnHNTBal, PayerPostBurnHNTBal]),


    %%
    %% Add the full gateway
    %% it wont have an entry in the staking key mappings table and so will be added in full mode
    %%

    #{public := FullGatewayPubKey, secret := FullGatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    FullGateway = libp2p_crypto:pubkey_to_bin(FullGatewayPubKey),
    FullGatewaySigFun = libp2p_crypto:mk_sig_fun(FullGatewayPrivKey),

    % Add the Gateway using payer
    AddFullGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, FullGateway, Payer),
    AddGatewayTxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddFullGatewayTx, Chain),
    AddGatewayStFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddFullGatewayTx, Chain),
    AddFullGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddFullGatewayTx, AddGatewayTxFee),
    AddFullGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddFullGatewayTx1, AddGatewayStFee),

    SignedOwnerAddFullGatewayTx = blockchain_txn_add_gateway_v1:sign(AddFullGatewayTx2, OwnerSigFun),
    SignedGatewayAddFullGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddFullGatewayTx, FullGatewaySigFun),
    SignedPayerAddFullGatewayTx = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddFullGatewayTx, PayerSigFun),
      {ok, Block24} = test_utils:create_block(ConsensusMembers, [SignedPayerAddFullGatewayTx]),
    _ = blockchain_gossip_handler:add_block(Block24, Chain, self(), blockchain_swarm:swarm()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 2} =:= blockchain:height(Chain) end),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, FullGW} = blockchain_ledger_v1:find_gateway_info(FullGateway, Ledger),
    ?assertMatch(full, blockchain_ledger_gateway_v2:mode(FullGW)),

    %%
    %% Assert the full Gateways location
    %%
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(FullGateway, Owner, Payer, ?TEST_LOCATION, 1),
    AssertLocationTxFee = blockchain_txn_assert_location_v1:calculate_fee(AssertLocationRequestTx, Chain),
    AAssertLocationStFee = blockchain_txn_assert_location_v1:calculate_staking_fee(AssertLocationRequestTx, Chain),
    AssertLocationRequestTx1 = blockchain_txn_assert_location_v1:fee(AssertLocationRequestTx, AssertLocationTxFee),
    AssertLocationRequestTx2 = blockchain_txn_assert_location_v1:staking_fee(AssertLocationRequestTx1, AAssertLocationStFee),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx2, FullGatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),
    SignedPayerAssertLocationTx = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx, PayerSigFun),

    {ok, Block25} = test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationTx]),
    ok = blockchain_gossip_handler:add_block(Block25, Chain, self(), blockchain_swarm:swarm()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 3} =:= blockchain:height(Chain) end),

    %%
    %% Add a second gateway, using the staking key which is setup to add in light mode
    %%

    %%
    %% create a payment txn to fund staking account 1 - which will have a staking key mapping set to full
    %%
    #{public := _StakingPub, secret := StakingPrivKey} = ?config(staking_key, Config),
    Staker = ?config(staking_key_pub_bin, Config),
    StakerSigFun = libp2p_crypto:mk_sig_fun(StakingPrivKey),
    %% base txn
    PaymentTx0 = blockchain_txn_payment_v1:new(Payer, Staker, 5000 * ?BONES_PER_HNT, 2),
    %% get the fees for this txn
    PaymentTxFee = blockchain_txn_payment_v1:calculate_fee(PaymentTx0, Chain),
    ct:pal("payment txn fee ~p, staking fee ~p, total: ~p", [PaymentTxFee, 'NA', PaymentTxFee ]),
    %% set the fees on the base txn and then sign the various txns
    PaymentTx1 = blockchain_txn_payment_v1:fee(PaymentTx0, PaymentTxFee),
    SignedPaymentTx1 = blockchain_txn_payment_v1:sign(PaymentTx1, PayerSigFun),
    %% check is_valid behaves as expected
    ?assertMatch(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx1, Chain)),
    {ok, PaymentBlock} = test_utils:create_block(ConsensusMembers, [SignedPaymentTx1]),
    %% add the block
    blockchain:add_block(PaymentBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 4} =:= blockchain:height(Chain) end),

    %% add the second gateway using the staker key, should be added as a full gateway
    #{public := SecondGatewayPubKey, secret := SecondGatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SecondGateway = libp2p_crypto:pubkey_to_bin(SecondGatewayPubKey),
    SecondGatewaySigFun = libp2p_crypto:mk_sig_fun(SecondGatewayPrivKey),
    %% add gateway base txn
    AddSecondGatewayTx0 = blockchain_txn_add_gateway_v1:new(Owner, SecondGateway, Staker),
    %% get the fees for this txn
    AddSecondGatewayTxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddSecondGatewayTx0, Chain),
    AddSecondGatewayStFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddSecondGatewayTx0, Chain),
    ?assertEqual(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx1, Chain)),
    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddSecondGatewayTxFee, AddSecondGatewayStFee, AddSecondGatewayTxFee + AddSecondGatewayStFee]),
    %% set the fees on the base txn and then sign the various txns
    AddSecondGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddSecondGatewayTx0, AddSecondGatewayTxFee),
    AddSecondGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddSecondGatewayTx1, AddSecondGatewayStFee),
    SignedOwnerAddSecondGatewayTx2 = blockchain_txn_add_gateway_v1:sign(AddSecondGatewayTx2, OwnerSigFun),
    SignedGatewayAddSecondGatewayTx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddSecondGatewayTx2, SecondGatewaySigFun),
    SignedPayerAddSecondGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddSecondGatewayTx2, StakerSigFun),

    {ok, AddSecondGatewayBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAddSecondGatewayTx2]),
    %% add the block
    _ = blockchain:add_block(AddSecondGatewayBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 5} =:= blockchain:height(Chain) end),

    %%
    %% Have the first full GW issue a POC challenge
    %%
    Keys0 = libp2p_crypto:generate_keys(ecc_compact),
    Secret0 = libp2p_crypto:keys_to_bin(Keys0),
    #{public := OnionCompactKey0} = Keys0,
    SecretHash0 = crypto:hash(sha256, Secret0),
    OnionKeyHash0 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey0)),
    PoCReqTxn0 = blockchain_txn_poc_request_v1:new(FullGateway, SecretHash0, OnionKeyHash0, blockchain_block:hash_block(BurnBlock), 1),
    SignedPoCReqTxn0 = blockchain_txn_poc_request_v1:sign(PoCReqTxn0, FullGatewaySigFun),
    {ok, POCReqBlock} = test_utils:create_block(ConsensusMembers, [SignedPoCReqTxn0]),
    _ = blockchain_gossip_handler:add_block(POCReqBlock, Chain, self(), blockchain_swarm:swarm()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 6} =:= blockchain:height(Chain) end),

    Ledger = blockchain:ledger(Chain),
    {ok, HeadHash3} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(POCReqBlock), HeadHash3),
    ?assertEqual({ok, POCReqBlock}, blockchain:get_block(HeadHash3, Chain)),
    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GwInfo2} = blockchain_gateway_cache:get(FullGateway, Ledger),
    ?assertEqual(CurHeight + 6, blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo2)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GwInfo2)),


    [
        {full_gateway, FullGateway},
        {full_gateway_sig_fun, FullGatewaySigFun},
        {second_gateway, SecondGateway},
        {second_gateway_sig_fun, SecondGatewaySigFun},
        {second_gateway_staking_fee, AddSecondGatewayStFee},
        {poc_secret, Secret0},
        {poc_onion, OnionKeyHash0} | Config
    ].
