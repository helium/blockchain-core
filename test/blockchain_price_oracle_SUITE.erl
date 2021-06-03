-module(blockchain_price_oracle_SUITE).

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
    validate_initial_state/1,
    submit_prices/1,
    calculate_price_even/1,
    submit_bad_public_key/1,
    double_submit_prices/1,
    txn_too_high/1,
    replay_txn/1,
    txn_fees_pay_with_dc/1,
    txn_fees_pay_with_hnt/1,
    staking_key_add_gateway/1,
    staking_key_mode_mappings_add_dataonly_gateway/1,
    staking_key_mode_mappings_add_light_gateway/1,
    staking_key_mode_mappings_add_full_gateway/1
]).

all() -> [
    validate_initial_state,
    submit_prices,
    calculate_price_even,
    submit_bad_public_key,
    double_submit_prices,
    txn_too_high,
    replay_txn,
    txn_fees_pay_with_dc,
    txn_fees_pay_with_hnt,
    staking_key_add_gateway,
    staking_key_mode_mappings_add_dataonly_gateway,
    staking_key_mode_mappings_add_light_gateway,
    staking_key_mode_mappings_add_full_gateway
].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config0)  when TestCase == txn_fees_pay_with_hnt;
                                           TestCase == txn_fees_pay_with_dc;
                                           TestCase == staking_key_add_gateway;
                                           TestCase == staking_key_mode_mappings_add_dataonly_gateway;
                                           TestCase == staking_key_mode_mappings_add_light_gateway;
                                           TestCase == staking_key_mode_mappings_add_full_gateway ->
    Config = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config0),
    BaseDir = ?config(base_dir, Config),
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars0 = #{
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 25,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 0,
      price_oracle_price_scan_max => 50,
      txn_fees => true,
      staking_fee_txn_oui_v1 => 100 * ?USD_TO_DC, %% $100?
      staking_fee_txn_oui_v1_per_address => 100 * ?USD_TO_DC,
      staking_fee_txn_add_gateway_v1 => 40 * ?USD_TO_DC,
      staking_fee_txn_add_dataonly_gateway_v1 => 10 * ?USD_TO_DC,
      staking_fee_txn_add_light_gateway_v1 => 10 * ?USD_TO_DC,
      staking_fee_txn_assert_location_v1 => 10 * ?USD_TO_DC,
      staking_fee_txn_assert_location_dataonly_gateway_v1 => 5 * ?USD_TO_DC,
      staking_fee_txn_assert_location_light_gateway_v1 => 5 * ?USD_TO_DC,
      txn_fee_multiplier => 5000,
      max_payments => 10
    },

    {ExtraVars, ExtraConfig} = case TestCase of
                                   staking_key_add_gateway ->
                                       StakingKey = libp2p_crypto:generate_keys(ecc_compact),
                                       {ok, EncodedStakingKeys} = make_encoded_oracle_keys([StakingKey]),
                                       {maps:put(staking_keys, EncodedStakingKeys, ExtraVars0), [{staking_key, StakingKey}]};
                                   staking_key_mode_mappings_add_full_gateway ->
                                       #{public := StakingPub, secret := _StakingPrivKey} = StakingKey = libp2p_crypto:generate_keys(ecc_compact),
                                       StakingKeyPubBin = libp2p_crypto:pubkey_to_bin(StakingPub),
                                       Mappings = [{StakingKeyPubBin, <<"full">>}],
                                       {ok, MappingsBin} = make_staking_keys_mode_mappings(Mappings),
                                       MappingsExtraVars1 = maps:put(staking_keys_to_mode_mappings, MappingsBin, ExtraVars0),
                                       {MappingsExtraVars1, [{staking_key, StakingKey}, {staking_key_pub_bin, StakingKeyPubBin}]};
                                   staking_key_mode_mappings_add_light_gateway ->
                                       #{public := StakingPub, secret := _StakingPrivKey} = StakingKey = libp2p_crypto:generate_keys(ecc_compact),
                                       StakingKeyPubBin = libp2p_crypto:pubkey_to_bin(StakingPub),
                                       Mappings = [{StakingKeyPubBin, <<"light">>}],
                                       {ok, MappingsBin} = make_staking_keys_mode_mappings(Mappings),
                                       MappingsExtraVars1 = maps:put(staking_keys_to_mode_mappings, MappingsBin, ExtraVars0),
                                       {MappingsExtraVars1, [{staking_key, StakingKey}, {staking_key_pub_bin, StakingKeyPubBin}]};
                                   X when X == staking_key_mode_mappings_add_dataonly_gateway;
                                          X == staking_key_mode_mappings_dataonly_gateway_capabilities;
                                          X == poc_request_test ->
                                       ct:pal("setup for staking_key_mode_mappings_dataonly_gateway_capabilities", []),
                                       #{public := StakingPub, secret := _StakingPrivKey} = StakingKey = libp2p_crypto:generate_keys(ecc_compact),
                                       StakingKeyPubBin = libp2p_crypto:pubkey_to_bin(StakingPub),
                                       Mappings = [{StakingKeyPubBin, <<"dataonly">>}],
                                       {ok, MappingsBin} = make_staking_keys_mode_mappings(Mappings),
                                       MappingsExtraVars1 = maps:put(staking_keys_to_mode_mappings, MappingsBin, ExtraVars0),
                                       {MappingsExtraVars1, [{staking_key, StakingKey}, {staking_key_pub_bin, StakingKeyPubBin}]};
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

validate_initial_state(Config) ->
    %% the initial state should be a price of 0
    %% should have 3 oracle keys
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("price_oracle base dir: ~p", [BaseDir]),
    ct:pal("price_oracle base SIM dir: ~p", [SimDir]),

    Balance = 5000,
    BlocksN = 25,
    ExtraVars = #{
      price_oracle_public_keys => <<>>,
      price_oracle_refresh_interval => 50,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 10, % seconds
      price_oracle_price_scan_max => 60 % seconds
    },
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _Keys} =
                                test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),
    Chain = blockchain_worker:blockchain(),

    % Add some blocks
    _Blocks = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],

    Ledger = blockchain:ledger(Chain),
    ?assertEqual({ok, 0}, blockchain_ledger_v1:current_oracle_price(Ledger)),
    ?assertEqual({ok, []}, blockchain_ledger_v1:current_oracle_price_list(Ledger)),
    ok.

submit_prices(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars = #{
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 25,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 0,
      price_oracle_price_scan_max => 50
    },
    Balance = 5000,
    BlocksN = 50,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
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
    ?assertEqual({ok, median(ExpectedPrices)},
                    blockchain_ledger_v1:current_oracle_price(Ledger)),
    ?assertEqual({ok, lists:sort(ExpectedPrices)}, get_prices(
                    blockchain_ledger_v1:current_oracle_price_list(Ledger))),
    ok.

calculate_price_even(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),

    {ok, OracleKeys} = make_oracles(4),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars = #{
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 25,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 0, % seconds
      price_oracle_price_scan_max => 50 % seconds
    },
    Balance = 5000,
    BlocksN = 50,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
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
    ?assertEqual({ok, median(ExpectedPrices)},
                  blockchain_ledger_v1:current_oracle_price(Ledger)),
    ?assertEqual({ok, lists:sort(ExpectedPrices)},
                 get_prices(blockchain_ledger_v1:current_oracle_price_list(Ledger))),
    ok.

submit_bad_public_key(Config) ->
    %% in this test, we will generate legit oracles and then create an otherwise valid txn
    %% which does not use any of the legit oracle keys, and make sure it gets rejected
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars = #{
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 25,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 50, % seconds
      price_oracle_price_scan_max => 0 % seconds
    },
    Balance = 5000,
    BlocksN = 50,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} =
            test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),
    Chain = blockchain_worker:blockchain(),

    _Blocks0 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],


    {_ExpectedPrices, Txns} = lists:unzip(make_oracle_txns(1, OracleKeys, 50)),
    {ok, PriceBlock} = test_utils:create_block(ConsensusMembers, Txns),
    blockchain:add_block(PriceBlock, Chain),

    {ok, [BadKey]} = make_oracles(1),
    BadTxn = make_and_sign_txn(BadKey, 50, 50),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [BadTxn])),

    %% check that a bad signature is invalid
    RawTxn = blockchain_txn_price_oracle_v1:new( libp2p_crypto:pubkey_to_bin(maps:get(public, hd(OracleKeys))), 50, 50),
    SignFun = libp2p_crypto:mk_sig_fun(maps:get(secret, BadKey)),
    BadlySignedTxn = blockchain_txn_price_oracle_v1:sign(RawTxn, SignFun),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [BadlySignedTxn])),

    ok.

double_submit_prices(Config) ->
    %% in this test, we will generate 2 sets of prices for each legit oracle key and submit
    %% them all at the same time
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars = #{
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 25,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 50, % seconds
      price_oracle_price_scan_max => 0 % seconds
    },
    Balance = 5000,
    BlocksN = 50,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} =
            test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),
    Chain = blockchain_worker:blockchain(),

    _Blocks0 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],


    {_ExpectedPrices, Txns} = lists:unzip(make_oracle_txns(2, OracleKeys, 50)),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, Txns)),
    ok.

txn_too_high(Config) ->
    %% in this test, we will make some legit transactions and then delay submitting them
    %% until the blockchain height is beyond the threshold, and make sure they're rejected

    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars = #{
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 25,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 50, % seconds
      price_oracle_price_scan_max => 0 % seconds
    },
    Balance = 5000,
    BlocksN = 50,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} =
            test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),
    Chain = blockchain_worker:blockchain(),

    _Blocks0 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],


    {_ExpectedPrices, Txns} = lists:unzip(make_oracle_txns(1, OracleKeys, 10)),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, Txns)),
    ok.

replay_txn(Config) ->
    %% in this test, we will make some legit transactions, make sure they're applied
    %% successfully, and then try to replay them

    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars = #{
      price_oracle_public_keys => EncodedOracleKeys,
      price_oracle_refresh_interval => 10,
      price_oracle_height_delta => 10,
      price_oracle_price_scan_delay => 0, % seconds
      price_oracle_price_scan_max => 25 % seconds
    },
    Balance = 5000,
    BlocksN = 25,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} =
            test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),
    Chain = blockchain_worker:blockchain(),

    _Blocks0 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],


    {_ExpectedPrices, Txns} = lists:unzip(make_oracle_txns(1, OracleKeys, 25)),
    {ok, PriceBlock} = test_utils:create_block(ConsensusMembers, Txns),
    blockchain:add_block(PriceBlock, Chain),

    _Blocks1 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, 5) ],

    %% and resubmit the Txns from earlier now (they are less than the height delta)
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, Txns)),

    %% add some more blocks to push us over the height delta
    _Blocks2 = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],


    %% try to replay them one more time
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, Txns)),

    ok.


txn_fees_pay_with_dc(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),
    Balance = ?config(balance, Config),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),
    _PayerOpenHNTBal = ?config(payer_opening_hnt_bal, Config),

    Owner = ?config(owner, Config),
    OwnerSigFun = ?config(owner_sig_fun, Config),
    Chain = ?config(chain, Config),
    Ledger = ?config(ledger, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% NOTE: The same payer is used for all txns below, need to give him a DC balance
    %% start with a token burn txn, create the DC balance for the payer
    %% we will burn half our hnt
    %% and this will be used to pay fees all other txns below

    %% base txn
    BurnTx0 = blockchain_txn_token_burn_v1:new(Payer, Balance div 2, 1),
    %% get the fees for this txn
    BurnTxFee = blockchain_txn_token_burn_v1:calculate_fee(BurnTx0, Chain),
    ct:pal("Token burn txn fee ~p, staking fee ~p, total: ~p", [BurnTxFee, 'NA', BurnTxFee ]),

    %% get the payers HNT bal pre the burn
    {ok, PayerPreBurnEntry} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerPreBurnHNTBal =  blockchain_ledger_entry_v1:balance(PayerPreBurnEntry),

    %% set the fees on the base txn and then sign the various txns
    BurnTx1 = blockchain_txn_token_burn_v1:fee(BurnTx0, BurnTxFee),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, PayerSigFun),
    SignedBurnTx1 = blockchain_txn_token_burn_v1:sign(BurnTx1, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    BurnTx2 = blockchain_txn_token_burn_v1:fee(BurnTx0, BurnTxFee + 10),
    SignedBurnTx2 = blockchain_txn_token_burn_v1:sign(BurnTx2, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_token_burn_v1:is_valid(SignedBurnTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_token_burn_v1:is_valid(SignedBurnTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_token_burn_v1:is_valid(SignedBurnTx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedBurnTx0])),
    %% all the fees are set, so this should work
    {ok, BurnBlock} = test_utils:create_block(ConsensusMembers, [SignedBurnTx1]),
    %% add the block
    blockchain:add_block(BurnBlock, Chain),

    %% confirm DC balances are debited with correct fee
    %% the fee will be paid in HNT as the Payer wont have DC until after the txn has been fully absorbed
    {ok, PayerPostBurnEntry} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerPostBurnHNTBal =  blockchain_ledger_entry_v1:balance(PayerPostBurnEntry),
    ct:pal("Payer pre burn hnt bal: ~p, post burn hnt bal: ~p",[PayerPreBurnHNTBal, PayerPostBurnHNTBal]),

    %% convert the fee to HNT and confirm HNT balance is as expected
    {ok, BurnTxHNTFee} = blockchain_ledger_v1:dc_to_hnt(BurnTxFee, Ledger),
    ct:pal("token burn DC: ~p converts to ~p HNT", [BurnTxFee, BurnTxHNTFee]),
    ?assertEqual(PayerPreBurnHNTBal - (Balance div 2 + BurnTxHNTFee), PayerPostBurnHNTBal),

    %% get the payers DC balance post burn, all subsequent DC balances will be derived from this base
    {ok, PayerDCBalEntry0} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    PayerDCBal0 = blockchain_ledger_data_credits_entry_v1:balance(PayerDCBalEntry0),
    ct:pal("opening dc balance ~p", [PayerDCBal0]),


    %%
    %% OUI txn
    %%

    OUI1 = 1,
    Addresses0 = [Payer],

    {Filter, _} = xor16:to_bin(xor16:new([], fun xxhash:hash64/1)),
    %% base txn
    OUITx0 = blockchain_txn_oui_v1:new(OUI1, Payer, Addresses0, Filter, 8),
    %% get the fees for this txn
    OUITxFee = blockchain_txn_oui_v1:calculate_fee(OUITx0, Chain),
    OUIStFee = blockchain_txn_oui_v1:calculate_staking_fee(OUITx0, Chain),
    ct:pal("OUT txn fee ~p, staking fee ~p, total: ~p", [OUITxFee, OUIStFee, OUITxFee + OUIStFee]),

    %% set the fees on the base txn and then sign the various txns
    OUITx1 = blockchain_txn_oui_v1:fee(OUITx0, OUITxFee),
    OUITx2 = blockchain_txn_oui_v1:staking_fee(OUITx1, OUIStFee),
    SignedOUITx0 = blockchain_txn_oui_v1:sign(OUITx0, PayerSigFun),
    SignedOUITx1 = blockchain_txn_oui_v1:sign(OUITx1, PayerSigFun),
    SignedOUITx2 = blockchain_txn_oui_v1:sign(OUITx2, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    OUITx3 = blockchain_txn_oui_v1:fee(OUITx2, OUITxFee + 10),
    SignedOUITx3 = blockchain_txn_oui_v1:sign(OUITx3, PayerSigFun),
    %% and create version with higher staking fee, it should be declared invalid as these need to be exact
    OUITx4 = blockchain_txn_oui_v1:staking_fee(OUITx1, OUIStFee + 10),
    SignedOUITx4 = blockchain_txn_oui_v1:sign(OUITx4, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_oui_v1:is_valid(SignedOUITx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,1}}}, blockchain_txn_oui_v1:is_valid(SignedOUITx1, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,_}}}, blockchain_txn_oui_v1:is_valid(SignedOUITx4, Chain)),
    ?assertMatch(ok, blockchain_txn_oui_v1:is_valid(SignedOUITx2, Chain)),
    ?assertMatch(ok, blockchain_txn_oui_v1:is_valid(SignedOUITx3, Chain)),
    %% check create block on tx with invalid txn fee and invalid staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedOUITx0])),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedOUITx1])),
    %% all the fees are set, so this should work
    {ok, OUIBlock} = test_utils:create_block(ConsensusMembers, [SignedOUITx2]),
    %% add the block
    blockchain:add_block(OUIBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, OUITxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    OUITxDCBal = blockchain_ledger_data_credits_entry_v1:balance(OUITxDCEntry),
    ct:pal("DC balance after OUI txn ~p", [OUITxDCBal]),
    PayerDCBal1 = PayerDCBal0 - (OUITxFee + OUIStFee),
    ?assertEqual(OUITxDCBal, PayerDCBal1),



    %%
    %% add  gateway txn
    %%

    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),

    %% base txn
    AddGatewayTx0 = blockchain_txn_add_gateway_v1:new(Owner, Gateway, Payer),
    %% get the fees for this txn
    AddGatewayTxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddGatewayTx0, Chain),
    AddGatewayStFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGatewayTx0, Chain),
    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGatewayTxFee, AddGatewayStFee, AddGatewayTxFee + AddGatewayStFee]),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx0, AddGatewayTxFee),
    AddGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx1, AddGatewayStFee),

    SignedOwnerAddGatewayTx0 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx0, OwnerSigFun),
    SignedGatewayAddGatewayTx0 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx0, GatewaySigFun),
    SignedPayerAddGatewayTx0 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx0, PayerSigFun),

    SignedOwnerAddGatewayTx1 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx1, OwnerSigFun),
    SignedGatewayAddGatewayTx1 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx1, GatewaySigFun),
    SignedPayerAddGatewayTx1 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx1, PayerSigFun),

    SignedOwnerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx2, OwnerSigFun),
    SignedGatewayAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx2, GatewaySigFun),
    SignedPayerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx2, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    AddGatewayTx3 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx2, AddGatewayTxFee + 10),
    SignedOwnerAddGatewayTx3 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx3, OwnerSigFun),
    SignedGatewayAddGatewayTx3 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx3, GatewaySigFun),
    SignedPayerAddGatewayTx3 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx3, PayerSigFun),
    %% and create version with higher staking fee, it should be declared invalid as these need to be exact
    AddGatewayTx4 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx2, AddGatewayStFee + 10),
    SignedOwnerAddGatewayTx4 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx4, OwnerSigFun),
    SignedGatewayAddGatewayTx4 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx4, GatewaySigFun),
    SignedPayerAddGatewayTx4 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx4, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,1}}}, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx1, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,_}}}, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx4, Chain)),
    ?assertMatch(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx2, Chain)),
    ?assertMatch(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx3, Chain)),
    %% check create block on tx with invalid txn fee and invalid staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx0])),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx1])),
    %% all the fees are set, so this should work
    {ok, AddGatewayBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx2]),
    %% add the block
    blockchain:add_block(AddGatewayBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, AddGatewayTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    AddGatewayTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(AddGatewayTxDCEntry),
    ct:pal("DC balance after Add Gateway txn ~p", [AddGatewayTxDCBal]),
    PayerDCBal2 = PayerDCBal1 - (AddGatewayTxFee + AddGatewayStFee),
    ?assertEqual(AddGatewayTxDCBal, PayerDCBal2),


    %%
    %% assert location txn
    %%

    %% base txn
    AssertLocationRequestTx0 = blockchain_txn_assert_location_v1:new(Gateway, Owner, Payer, ?TEST_LOCATION, 1),
    %% get the fees for this txn
    AssertLocationTxFee = blockchain_txn_assert_location_v1:calculate_fee(AssertLocationRequestTx0, Chain),
    AssertLocationStFee = blockchain_txn_assert_location_v1:calculate_staking_fee(AssertLocationRequestTx0, Chain),
    ct:pal("Assert location txn fee ~p, staking fee ~p, total: ~p", [AssertLocationTxFee, AssertLocationStFee, AssertLocationTxFee + AssertLocationStFee]),

    %% set the fees on the base txn and then sign the various txns
    AssertLocationRequestTx1 = blockchain_txn_assert_location_v1:fee(AssertLocationRequestTx0, AssertLocationTxFee),
    AssertLocationRequestTx2 = blockchain_txn_assert_location_v1:staking_fee(AssertLocationRequestTx1, AssertLocationStFee),

    PartialAssertLocationTxn0 = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx0, GatewaySigFun),
    SignedAssertLocationTx0 = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn0, OwnerSigFun),
    SignedPayerAssertLocationTx0 = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx0, PayerSigFun),

    PartialAssertLocationTxn1 = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx1, GatewaySigFun),
    SignedAssertLocationTx1 = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn1, OwnerSigFun),
    SignedPayerAssertLocationTx1 = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx1, PayerSigFun),

    PartialAssertLocationTxn2 = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx2, GatewaySigFun),
    SignedAssertLocationTx2 = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn2, OwnerSigFun),
    SignedPayerAssertLocationTx2 = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx2, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    AssertLocationRequestTx3 = blockchain_txn_assert_location_v1:fee(AssertLocationRequestTx2, AssertLocationTxFee + 10),
    PartialAssertLocationTxn3 = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx3, GatewaySigFun),
    SignedAssertLocationTx3 = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn3, OwnerSigFun),
    SignedPayerAssertLocationTx3 = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx3, PayerSigFun),
    %% and create version with higher staking fee, it should be declared invalid as these need to be exact
    AssertLocationRequestTx4 = blockchain_txn_assert_location_v1:staking_fee(AssertLocationRequestTx1, AssertLocationStFee + 10),
    PartialAssertLocationTxn4 = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx4, GatewaySigFun),
    SignedAssertLocationTx4 = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn4, OwnerSigFun),
    SignedPayerAssertLocationTx4 = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx4, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,1}}}, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx1, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,_}}}, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx4, Chain)),
    ?assertMatch(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx2, Chain)),
    ?assertMatch(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx3, Chain)),
    %% check create block on tx with invalid txn fee and invalid staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationTx0])),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationTx1])),
    %% all the fees are set, so this should work
    {ok, AssertLocationBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAssertLocationTx2]),
    %% add the block
    blockchain:add_block(AssertLocationBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, AssertLocationTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    AssertLocationTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(AssertLocationTxDCEntry),
    ct:pal("DC balance after assert location txn ~p", [AssertLocationTxDCBal]),
    PayerDCBal3 = PayerDCBal2 - (AssertLocationTxFee + AssertLocationStFee),
    ?assertEqual(AssertLocationTxDCBal, PayerDCBal3),


    %%
    %% create htlc txn
    %%

    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(33),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    % Create a Payee
    #{public := PayeePubKey, secret := PayeePrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PayeePubKey),

    %% base txn
    CreateHTLCTx0 = blockchain_txn_create_htlc_v1:new(Payer, Payee, HTLCAddress, Hashlock, 3, 2500, 2),

    %% get the fees for this txn
    CreateHTLCTxFee = blockchain_txn_create_htlc_v1:calculate_fee(CreateHTLCTx0, Chain),
    ct:pal("create htlc txn fee ~p, staking fee ~p, total: ~p", [CreateHTLCTxFee, 'NA', CreateHTLCTxFee ]),

    %% set the fees on the base txn and then sign the various txns
    CreateHTLCTx1 = blockchain_txn_create_htlc_v1:fee(CreateHTLCTx0, CreateHTLCTxFee),
    SignedCreateHTLCTx0 = blockchain_txn_create_htlc_v1:sign(CreateHTLCTx0, PayerSigFun),
    SignedCreateHTLCTx1 = blockchain_txn_create_htlc_v1:sign(CreateHTLCTx1, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    CreateHTLCTx2 = blockchain_txn_create_htlc_v1:fee(CreateHTLCTx0, CreateHTLCTxFee + 10),
    SignedCreateHTLCTx2 = blockchain_txn_create_htlc_v1:sign(CreateHTLCTx2, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_create_htlc_v1:is_valid(SignedCreateHTLCTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_create_htlc_v1:is_valid(SignedCreateHTLCTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_create_htlc_v1:is_valid(SignedCreateHTLCTx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedCreateHTLCTx0])),
    %% all the fees are set, so this should work
    {ok, CreateHTLCBlock} = test_utils:create_block(ConsensusMembers, [SignedCreateHTLCTx1]),
    %% add the block
    blockchain:add_block(CreateHTLCBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, CreateHTLCTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    CreateHTLCTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(CreateHTLCTxDCEntry),
    ct:pal("DC balance after create htlc txn ~p", [CreateHTLCTxDCBal]),
    PayerDCBal4 = PayerDCBal3 - CreateHTLCTxFee,
    ?assertEqual(CreateHTLCTxDCBal, PayerDCBal4),

    {ok, _NewHTLC0} = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain:ledger(Chain)),


    %%
    %% redeem htlc txn
    %%

    PayeeSigFun = libp2p_crypto:mk_sig_fun(PayeePrivKey),
    % throw a pile of DC to the payee so he has enough to pay the redeeem fee
    PayPayeeTx0 = blockchain_txn_payment_v1:new(Payer, Payee, 500000000, 3),
    PayPayeeTx1 = blockchain_txn_payment_v1:fee(PayPayeeTx0, blockchain_txn_payment_v1:calculate_fee(PayPayeeTx0, Chain)),
    SignedPayPayeeTx = blockchain_txn_payment_v1:sign(PayPayeeTx1, PayerSigFun),
    {ok, PayPayeeBlock} = test_utils:create_block(ConsensusMembers, [SignedPayPayeeTx]),
    blockchain:add_block(PayPayeeBlock, Chain),

    %% base txn
    RedeemTx0 = blockchain_txn_redeem_htlc_v1:new(Payee, HTLCAddress, <<"sharkfed">>),

    %% get the fees for this txn
    RedeemTxFee = blockchain_txn_redeem_htlc_v1:calculate_fee(RedeemTx0, Chain),
    ct:pal("redeem htlc txn fee ~p, staking fee ~p, total: ~p", [RedeemTxFee, 'NA', RedeemTxFee ]),

    %% set the fees on the base txn and then sign the various txns
    RedeemTx1 = blockchain_txn_redeem_htlc_v1:fee(RedeemTx0, RedeemTxFee),
    SignedRedeemTx0 = blockchain_txn_redeem_htlc_v1:sign(RedeemTx0, PayeeSigFun),
    SignedRedeemTx1 = blockchain_txn_redeem_htlc_v1:sign(RedeemTx1, PayeeSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    RedeemTx2 = blockchain_txn_redeem_htlc_v1:fee(RedeemTx0, RedeemTxFee + 10),
    SignedRedeemTx2 = blockchain_txn_redeem_htlc_v1:sign(RedeemTx2, PayeeSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_redeem_htlc_v1:is_valid(SignedRedeemTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_redeem_htlc_v1:is_valid(SignedRedeemTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_redeem_htlc_v1:is_valid(SignedRedeemTx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedRedeemTx0])),
    %% all the fees are set, so this should work
    {ok, RedeemBlock} = test_utils:create_block(ConsensusMembers, [SignedRedeemTx1]),
    %% add the block
    blockchain:add_block(RedeemBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, RedeemTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    RedeemTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(RedeemTxDCEntry),
    ct:pal("DC balance after redeem htlc txn ~p", [RedeemTxDCBal]),
    PayerDCBal5 = PayerDCBal4 - RedeemTxFee,
    ?assertEqual(RedeemTxDCBal, PayerDCBal5),



    %%
    %% create a payment txn
    %%

    Recipient = blockchain_swarm:pubkey_bin(),
    %% base txn
    PaymentTx0 = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 4),

    %% get the fees for this txn
    PaymentTxFee = blockchain_txn_payment_v1:calculate_fee(PaymentTx0, Chain),
    ct:pal("payment txn fee ~p, staking fee ~p, total: ~p", [PaymentTxFee, 'NA', PaymentTxFee ]),

    %% set the fees on the base txn and then sign the various txns
    PaymentTx1 = blockchain_txn_payment_v1:fee(PaymentTx0, PaymentTxFee),
    SignedPaymentTx0 = blockchain_txn_payment_v1:sign(PaymentTx0, PayerSigFun),
    SignedPaymentTx1 = blockchain_txn_payment_v1:sign(PaymentTx1, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    PaymentTx2 = blockchain_txn_payment_v1:fee(PaymentTx0, PaymentTxFee + 10),
    SignedPaymentTx2 = blockchain_txn_payment_v1:sign(PaymentTx2, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_payment_v1:is_valid(SignedPaymentTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedPaymentTx0])),
    %% all the fees are set, so this should work
    {ok, PaymentBlock} = test_utils:create_block(ConsensusMembers, [SignedPaymentTx1]),
    %% add the block
    blockchain:add_block(PaymentBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, PaymentTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    PaymentTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(PaymentTxDCEntry),
    ct:pal("DC balance after payment txn ~p", [PaymentTxDCBal]),
    PayerDCBal6 = PayerDCBal5 - PaymentTxFee,
    ?assertEqual(PaymentTxDCBal, PayerDCBal6),


    %%
    %% Routing txn - update_router_addresses
    %%

    #{public := RouterPubKey, secret := RouterPrivKey} = libp2p_crypto:generate_keys(ed25519),
    RouterPubKeyBin = libp2p_crypto:pubkey_to_bin(RouterPubKey),
    RouterAddresses1 = [RouterPubKeyBin],
    RouterSigFun = libp2p_crypto:mk_sig_fun(RouterPrivKey),

    %% base txn
    RoutingTx0 = blockchain_txn_routing_v1:update_router_addresses(OUI1, Payer, RouterAddresses1, 1),

    %% get the fees for this txn ( NOTE: zero staking fee for update router addresses )
    RoutingTxFee = blockchain_txn_routing_v1:calculate_fee(RoutingTx0, Chain),
    ct:pal("update_router_addresses txn fee ~p, staking fee ~p, total: ~p", [RoutingTxFee, 'NA', RoutingTxFee ]),

    %% set the fees on the base txn and then sign the various txns
    RoutingTx1 = blockchain_txn_routing_v1:fee(RoutingTx0, RoutingTxFee),
    SignedRoutingTx0 = blockchain_txn_routing_v1:sign(RoutingTx0, PayerSigFun),
    SignedRoutingTx1 = blockchain_txn_routing_v1:sign(RoutingTx1, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    RoutingTx2 = blockchain_txn_routing_v1:fee(RoutingTx0, RoutingTxFee + 10),
    SignedRoutingTx2 = blockchain_txn_routing_v1:sign(RoutingTx2, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_routing_v1:is_valid(SignedRoutingTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_routing_v1:is_valid(SignedRoutingTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_routing_v1:is_valid(SignedRoutingTx2, Chain)),
    %% check create block on tx with invalid txn fee and invalid staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedRoutingTx0])),
    %% all the fees are set, so this should work
    {ok, RoutingBlock} = test_utils:create_block(ConsensusMembers, [SignedRoutingTx1]),
    %% add the block
    blockchain:add_block(RoutingBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, RoutingTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    RoutingTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(RoutingTxDCEntry),
    ct:pal("DC balance after update_router_addresses txn ~p", [RoutingTxDCBal]),
    PayerDCBal7 = PayerDCBal6 - RoutingTxFee,
    ?assertEqual(RoutingTxDCBal, PayerDCBal7),



    %%
    %% Routing txn - request_subnet
    %%

    %% base txn
    RoutingSubnetTx0 = blockchain_txn_routing_v1:request_subnet(OUI1, Payer, 8, 2),

    %% get the fees for this txn
    RoutingSubnetTxFee = blockchain_txn_routing_v1:calculate_fee(RoutingSubnetTx0, Chain),
    RoutingSubnetStFee = blockchain_txn_routing_v1:calculate_staking_fee(RoutingSubnetTx0, Chain),
    ct:pal("request_subnet txn fee ~p, staking fee ~p, total: ~p", [RoutingSubnetTxFee, RoutingSubnetStFee, RoutingSubnetTxFee + RoutingSubnetStFee]),

    %% set the fees on the base txn and then sign the various txns
    RoutingSubnetTx1 = blockchain_txn_routing_v1:fee(RoutingSubnetTx0, RoutingSubnetTxFee),
    RoutingSubnetTx2 = blockchain_txn_routing_v1:staking_fee(RoutingSubnetTx1, RoutingSubnetStFee),

    SignedRoutingSubnetTx0 = blockchain_txn_routing_v1:sign(RoutingSubnetTx0, PayerSigFun),
    SignedRoutingSubnetTx1 = blockchain_txn_routing_v1:sign(RoutingSubnetTx1, PayerSigFun),
    SignedRoutingSubnetTx2 = blockchain_txn_routing_v1:sign(RoutingSubnetTx2, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    RoutingSubnetTx3 = blockchain_txn_routing_v1:fee(RoutingSubnetTx2, RoutingSubnetStFee + 10),
    SignedRoutingSubnetTx3 = blockchain_txn_routing_v1:sign(RoutingSubnetTx3, PayerSigFun),
    %% and create version with higher staking fee, it should be declared invalid as these need to be exact
    RoutingSubnetTx4 = blockchain_txn_routing_v1:staking_fee(RoutingSubnetTx1, RoutingSubnetStFee + 10),
    SignedRoutingSubnetTx4 = blockchain_txn_routing_v1:sign(RoutingSubnetTx4, PayerSigFun),


    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,0}}}, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx1, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,_}}}, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx4, Chain)),
    ?assertMatch(ok, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx2, Chain)),
    ?assertMatch(ok, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx3, Chain)),
    %% check create block on tx with invalid txn fee and invalid staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedRoutingSubnetTx0])),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedRoutingSubnetTx1])),
    %% all the fees are set, so this should work
    {ok, RoutingSubnetBlock} = test_utils:create_block(ConsensusMembers, [SignedRoutingSubnetTx2]),
    %% add the block
    blockchain:add_block(RoutingSubnetBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, RoutingSubnetTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    RoutingSubnetTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(RoutingSubnetTxDCEntry),
    ct:pal("DC balance after routing subnet txn ~p", [RoutingSubnetTxDCBal]),
    PayerDCBal8 = PayerDCBal7 - (RoutingSubnetTxFee + RoutingSubnetStFee),
    ?assertEqual(PayerDCBal8, RoutingSubnetTxDCBal),

    %%
    %% security exchange txn
    %%

    %% base txn
    SecExchTx0 = blockchain_txn_security_exchange_v1:new(Payer, Payee, 2500, 1),

    %% get the fees for this txn
    SecExchTxFee = blockchain_txn_security_exchange_v1:calculate_fee(SecExchTx0, Chain),
    ct:pal("security exch txn fee ~p, staking fee ~p, total: ~p", [SecExchTxFee, 'NA', SecExchTxFee]),

    %% set the fees on the base txn and then sign the various txns
    SecExchTx1 = blockchain_txn_security_exchange_v1:fee(SecExchTx0, SecExchTxFee),

    SignedSecExchTx0 = blockchain_txn_security_exchange_v1:sign(SecExchTx0, PayerSigFun),
    SignedSecExchTx1 = blockchain_txn_security_exchange_v1:sign(SecExchTx1, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    SecExchTx2 = blockchain_txn_security_exchange_v1:fee(SecExchTx0, SecExchTxFee + 10),
    SignedSecExchTx2 = blockchain_txn_security_exchange_v1:sign(SecExchTx2, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_security_exchange_v1:is_valid(SignedSecExchTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_security_exchange_v1:is_valid(SignedSecExchTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_security_exchange_v1:is_valid(SignedSecExchTx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedSecExchTx0])),
    %% all the fees are set, so this should work
    {ok, SecExchBlock} = test_utils:create_block(ConsensusMembers, [SignedSecExchTx1]),
    %% add the block
    blockchain:add_block(SecExchBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, SecExchTxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    SecExchTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(SecExchTxDCEntry),
    ct:pal("DC balance after security exch txn ~p", [SecExchTxDCBal]),
    PayerDCBal9 = PayerDCBal8 - SecExchTxFee,
    ?assertEqual(PayerDCBal9, SecExchTxDCBal),



    %%
    %% create a payment v2 txn
    %%

    V2Payment = blockchain_payment_v2:new(Recipient, 2500),
    %% base txn
    PaymentV2Tx0 = blockchain_txn_payment_v2:new(Payer, [V2Payment], 5),

    %% get the fees for this txn
    PaymentV2TxFee = blockchain_txn_payment_v2:calculate_fee(PaymentV2Tx0, Chain),
    ct:pal("payment v2 txn fee ~p, staking fee ~p, total: ~p", [PaymentV2TxFee, 'NA', PaymentV2TxFee ]),

    %% set the fees on the base txn and then sign the various txns
    PaymentV2Tx1 = blockchain_txn_payment_v2:fee(PaymentV2Tx0, PaymentV2TxFee),
    SignedPaymentV2Tx0 = blockchain_txn_payment_v2:sign(PaymentV2Tx0, PayerSigFun),
    SignedPaymentV2Tx1 = blockchain_txn_payment_v2:sign(PaymentV2Tx1, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    PaymentV2Tx2 = blockchain_txn_payment_v2:fee(PaymentV2Tx0, PaymentV2TxFee + 10),
    SignedPaymentV2Tx2 = blockchain_txn_payment_v2:sign(PaymentV2Tx2, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_payment_v2:is_valid(SignedPaymentV2Tx0, Chain)),
    ?assertMatch(ok, blockchain_txn_payment_v2:is_valid(SignedPaymentV2Tx1, Chain)),
    ?assertMatch(ok, blockchain_txn_payment_v2:is_valid(SignedPaymentV2Tx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedPaymentV2Tx0])),
    %% all the fees are set, so this should work
    {ok, PaymentV2Block} = test_utils:create_block(ConsensusMembers, [SignedPaymentV2Tx1]),
    %% add the block
    blockchain:add_block(PaymentV2Block, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, PaymentV2TxDCEntry} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    PaymentV2TxDCBal = blockchain_ledger_data_credits_entry_v1:balance(PaymentV2TxDCEntry),
    ct:pal("DC balance after payment v2 txn ~p", [PaymentV2TxDCBal]),
    PayerDCBal10 = PayerDCBal9 - PaymentV2TxFee,
    ?assertEqual(PaymentV2TxDCBal, PayerDCBal10),



    %%
    %% State Channels Open txn
    %%

    ID = crypto:strong_rand_bytes(24),
    ExpireWithin = 11,
    %% base txn
    SCTx0 = blockchain_txn_state_channel_open_v1:new(ID, RouterPubKeyBin, ExpireWithin, OUI1, 1, 0),

    %% get the fees for this txn
    SCTxFee = blockchain_txn_state_channel_open_v1:calculate_fee(SCTx0, Chain),
    ct:pal("state channels open txn fee ~p, staking fee ~p, total: ~p", [SCTxFee, 'NA', SCTxFee ]),

    %% set the fees on the base txn and then sign the various txns
    SCTx1 = blockchain_txn_state_channel_open_v1:fee(SCTx0, SCTxFee),
    SignedSCTx0 = blockchain_txn_state_channel_open_v1:sign(SCTx0, RouterSigFun),
    SignedSCTx1 = blockchain_txn_state_channel_open_v1:sign(SCTx1, RouterSigFun),

    % Make a payment of HNT to the router ( owner ) so he has an account and some balance
    RouterOpenHNTBal = 500000000,
    PayRouterTx0 = blockchain_txn_payment_v1:new(Payer, RouterPubKeyBin, RouterOpenHNTBal, 6),
    PayRouterTx1 = blockchain_txn_payment_v1:fee(PayRouterTx0, blockchain_txn_payment_v1:calculate_fee(PayRouterTx0, Chain)),
    SignedPayRouterTx = blockchain_txn_payment_v1:sign(PayRouterTx1, PayerSigFun),
    {ok, PayRouterBlock} = test_utils:create_block(ConsensusMembers, [SignedPayRouterTx]),
    blockchain:add_block(PayRouterBlock, Chain),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    SCTx2 = blockchain_txn_state_channel_open_v1:fee(SCTx0, SCTxFee + 10),
    SignedSCTx2 = blockchain_txn_state_channel_open_v1:sign(SCTx2, RouterSigFun),

    %% check is_valid behaves as expected and returns blockchain_txn_state_channel_open_v1 error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_state_channel_open_v1:is_valid(SignedSCTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_state_channel_open_v1:is_valid(SignedSCTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_state_channel_open_v1:is_valid(SignedSCTx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedSCTx0])),
    %% all the fees are set, so this should work
    {ok, SCBlock} = test_utils:create_block(ConsensusMembers, [SignedSCTx1]),
    %% add the block
    blockchain:add_block(SCBlock, Chain),

    %% the Router address will be charged the txn fee
    %% this account only has a HNT balance and as such it will be debited from that
    %% a deposit of 500000000 HNT bones was made to this account above, this will be the opening balance
    %% so we need to confirm its currently sitting at OpenBal - TxnFeeInHNT
    {ok, RouterEntry0} = blockchain_ledger_v1:find_entry(RouterPubKeyBin, blockchain:ledger(Chain)),
    RouterCurHNTBal =  blockchain_ledger_entry_v1:balance(RouterEntry0),
    %% get the fee in HNT
    {ok, RouterSCHNTFee} = blockchain_ledger_v1:dc_to_hnt(SCTxFee, Ledger),
    ?assertEqual(RouterOpenHNTBal - RouterSCHNTFee, RouterCurHNTBal),

    ok.


txn_fees_pay_with_hnt(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),
    PayerOpenHNTBal = ?config(payer_opening_hnt_bal, Config),

    _Owner = ?config(owner, Config),
    _OwnerSigFun = ?config(owner_sig_fun, Config),
    Chain = ?config(chain, Config),
    Ledger = ?config(ledger, Config),
    ConsensusMembers = ?config(consensus_members, Config),



    %% NOTE:
    %% we can prob get away with only running a single txn to test the payments with HNT
    %% the same flow is used in all txns, if there is no DC available then the hnt account will be debited
    %% ???

    %%
    %% OUI txn
    OUI1 = 1,
    Addresses0 = [Payer],

    {Filter, _} = xor16:to_bin(xor16:new([], fun xxhash:hash64/1)),
    %% base txn
    OUITx0 = blockchain_txn_oui_v1:new(OUI1, Payer, Addresses0, Filter, 8),
    %% get the fees for this txn
    OUITxFee = blockchain_txn_oui_v1:calculate_fee(OUITx0, Chain),
    OUIStFee = blockchain_txn_oui_v1:calculate_staking_fee(OUITx0, Chain),
    ct:pal("OUT txn fee ~p, staking fee ~p, total: ~p", [OUITxFee, OUIStFee, OUITxFee + OUIStFee]),

    %% set the fees on the base txn and then sign the various txns
    OUITx1 = blockchain_txn_oui_v1:fee(OUITx0, OUITxFee),
    OUITx2 = blockchain_txn_oui_v1:staking_fee(OUITx1, OUIStFee),

    SignedOUITx0 = blockchain_txn_oui_v1:sign(OUITx0, PayerSigFun),
    SignedOUITx1 = blockchain_txn_oui_v1:sign(OUITx1, PayerSigFun),
    SignedOUITx2 = blockchain_txn_oui_v1:sign(OUITx2, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    %% and create version with higher staking fee, it should be declared invalid as these need to be exact
    OUITx3 = blockchain_txn_oui_v1:fee(OUITx2, OUITxFee + 10),
    SignedOUITx3 = blockchain_txn_oui_v1:sign(OUITx3, PayerSigFun),
    OUITx4 = blockchain_txn_oui_v1:staking_fee(OUITx1, OUIStFee + 10),
    SignedOUITx4 = blockchain_txn_oui_v1:sign(OUITx4, PayerSigFun),


    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_oui_v1:is_valid(SignedOUITx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,1}}}, blockchain_txn_oui_v1:is_valid(SignedOUITx1, Chain)),
    ?assertMatch({error,{wrong_staking_fee,{_,_}}}, blockchain_txn_oui_v1:is_valid(SignedOUITx4, Chain)),
    ?assertMatch(ok, blockchain_txn_oui_v1:is_valid(SignedOUITx2, Chain)),
    ?assertMatch(ok, blockchain_txn_oui_v1:is_valid(SignedOUITx3, Chain)),
    %% check create block on tx with invalid txn fee and invalid staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedOUITx0])),
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedOUITx1])),
    %% all the fees are set, so this should work
    {ok, OUIBlock} = test_utils:create_block(ConsensusMembers, [SignedOUITx2]),
    %% add the block
    blockchain:add_block(OUIBlock, Chain),

    %% confirm DC balances are debited with correct fee
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    PayerNewHNTBal =  blockchain_ledger_entry_v1:balance(NewEntry1),
    %% get the fee in HNT
    {ok, HNTFee} = blockchain_ledger_v1:dc_to_hnt((OUITxFee + OUIStFee), Ledger),
    ?assertEqual(PayerOpenHNTBal - HNTFee, PayerNewHNTBal),


    ok.


staking_key_add_gateway(Config) ->

    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),
    %PayerOpenHNTBal = ?config(payer_opening_hnt_bal, Config),

    _Owner = ?config(owner, Config),
    _OwnerSigFun = ?config(owner_sig_fun, Config),
    Chain = ?config(chain, Config),
    _Ledger = ?config(ledger, Config),
    ConsensusMembers = ?config(consensus_members, Config),


    %%
    %% create a payment txn to fund staking account
    %%

    #{public := StakingPub, secret := StakingPrivKey} = ?config(staking_key, Config),
    Staker = libp2p_crypto:pubkey_to_bin(StakingPub),
    StakerSigFun = libp2p_crypto:mk_sig_fun(StakingPrivKey),
    %% base txn
    PaymentTx0 = blockchain_txn_payment_v1:new(Payer, Staker, 2500 * ?BONES_PER_HNT, 1),

    %% get the fees for this txn
    PaymentTxFee = blockchain_txn_payment_v1:calculate_fee(PaymentTx0, Chain),
    ct:pal("payment txn fee ~p, staking fee ~p, total: ~p", [PaymentTxFee, 'NA', PaymentTxFee ]),

    %% set the fees on the base txn and then sign the various txns
    PaymentTx1 = blockchain_txn_payment_v1:fee(PaymentTx0, PaymentTxFee),
    SignedPaymentTx0 = blockchain_txn_payment_v1:sign(PaymentTx0, PayerSigFun),
    SignedPaymentTx1 = blockchain_txn_payment_v1:sign(PaymentTx1, PayerSigFun),

    %% create version of the txn with a fee higher than expected, it should be declared valid as we accept higher txn fees
    PaymentTx2 = blockchain_txn_payment_v1:fee(PaymentTx0, PaymentTxFee + 10),
    SignedPaymentTx2 = blockchain_txn_payment_v1:sign(PaymentTx2, PayerSigFun),

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,{_,0}}}, blockchain_txn_payment_v1:is_valid(SignedPaymentTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx2, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedPaymentTx0])),
    %% all the fees are set, so this should work
    {ok, PaymentBlock} = test_utils:create_block(ConsensusMembers, [SignedPaymentTx1]),
    %% add the block
    blockchain:add_block(PaymentBlock, Chain),

    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),

    #{public := OwnerPubKey, secret := OwnerPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Owner = libp2p_crypto:pubkey_to_bin(OwnerPubKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %% base txn
    AddGatewayTx0 = blockchain_txn_add_gateway_v1:new(Owner, Gateway, Staker),
    %% get the fees for this txn
    AddGatewayTxFee = blockchain_txn_add_gateway_v1:calculate_fee(AddGatewayTx0, Chain),
    AddGatewayStFee = blockchain_txn_add_gateway_v1:calculate_staking_fee(AddGatewayTx0, Chain),
    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGatewayTxFee, AddGatewayStFee, AddGatewayTxFee + AddGatewayStFee]),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx0, AddGatewayTxFee),
    AddGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx1, AddGatewayStFee),

    SignedOwnerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx2, OwnerSigFun),
    SignedGatewayAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx2, GatewaySigFun),
    SignedPayerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx2, StakerSigFun),

    {ok, AddGatewayBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx2]),
    %% add the block
    blockchain:add_block(AddGatewayBlock, Chain),

    %% base txn
    AddGatewayTx00 = blockchain_txn_add_gateway_v1:new(Owner, Gateway, Owner),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx01 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx00, AddGatewayTxFee),
    AddGatewayTx02 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx01, AddGatewayStFee),

    SignedOwnerAddGatewayTx02 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx02, OwnerSigFun),
    SignedGatewayAddGatewayTx02 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx02, GatewaySigFun),
    SignedPayerAddGatewayTx02 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx02, OwnerSigFun),


    ?assertMatch({error,payer_invalid_staking_key}, blockchain_txn:is_valid(SignedPayerAddGatewayTx02, Chain)),

    %% check no staking key fails
    AddGatewayTx000 = blockchain_txn_add_gateway_v1:new(Owner, Gateway),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx001 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx000, AddGatewayTxFee),
    AddGatewayTx002 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx001, AddGatewayStFee),

    SignedOwnerAddGatewayTx002 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx002, OwnerSigFun),
    SignedGatewayAddGatewayTx002 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx002, GatewaySigFun),
    ?assertMatch({error,payer_invalid_staking_key}, blockchain_txn:is_valid(SignedGatewayAddGatewayTx002, Chain)),

    ok.


staking_key_mode_mappings_add_full_gateway(Config) ->
    %% add gateway where the staker has a mapping to a full gateway in the staking key mode mappings tables
    %% Confirm the fees for each are correct
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),

    _Owner = ?config(owner, Config),
    _OwnerSigFun = ?config(owner_sig_fun, Config),
    Chain = ?config(chain, Config),
    Ledger = ?config(ledger, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    {ok, CurHeight} = blockchain:height(Chain),

    %%
    %% create a payment txn to fund staking account 1 - which will have a staking key mapping set to full
    %%

    #{public := _StakingPub, secret := StakingPrivKey} = ?config(staking_key, Config),
    Staker = ?config(staking_key_pub_bin, Config),
    StakerSigFun = libp2p_crypto:mk_sig_fun(StakingPrivKey),
    %% base txn
    PaymentTx0 = blockchain_txn_payment_v1:new(Payer, Staker, 5000 * ?BONES_PER_HNT, 1),

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
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 1} =:= blockchain:height(Chain) end),

    %% add the gateway using the staker key, should be added as a dataonly gateway
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
    %% full gateways costs 40 usd

    ?assertEqual(40 * ?USD_TO_DC, AddGatewayStFee),
    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGatewayTxFee, AddGatewayStFee, AddGatewayTxFee + AddGatewayStFee]),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx0, AddGatewayTxFee),
    AddGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx1, AddGatewayStFee),

    SignedOwnerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx2, OwnerSigFun),
    SignedGatewayAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx2, GatewaySigFun),
    SignedPayerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx2, StakerSigFun),
    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx2, Chain)),

    {ok, AddGatewayBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx2]),
    %% add the block
    blockchain:add_block(AddGatewayBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 2} =:= blockchain:height(Chain) end),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, GW} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertMatch(full, blockchain_ledger_gateway_v2:mode(GW)),
    ok.


staking_key_mode_mappings_add_light_gateway(Config) ->
    %% add gateways where the staker has a mapping to a light gateway in the staking key mode mappings tables
    %% Confirm the fees for each are correct
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),

    _Owner = ?config(owner, Config),
    _OwnerSigFun = ?config(owner_sig_fun, Config),
    Chain = ?config(chain, Config),
    Ledger = ?config(ledger, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    {ok, CurHeight} = blockchain:height(Chain),

    %%
    %% create a payment txn to fund staking account 1 - which will have a staking key mapping set to full
    %%

    #{public := _StakingPub, secret := StakingPrivKey} = ?config(staking_key, Config),
    Staker = ?config(staking_key_pub_bin, Config),
    StakerSigFun = libp2p_crypto:mk_sig_fun(StakingPrivKey),
    %% base txn
    PaymentTx0 = blockchain_txn_payment_v1:new(Payer, Staker, 5000 * ?BONES_PER_HNT, 1),

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
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 1} =:= blockchain:height(Chain) end),

    %% add the gateway using the staker key, should be added as a dataonly gateway
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
    %% light gateway costs same to add as a full gateway
    ?assertEqual(10 * ?USD_TO_DC, AddGatewayStFee),
    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGatewayTxFee, AddGatewayStFee, AddGatewayTxFee + AddGatewayStFee]),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx0, AddGatewayTxFee),
    AddGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx1, AddGatewayStFee),

    SignedOwnerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx2, OwnerSigFun),
    SignedGatewayAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx2, GatewaySigFun),
    SignedPayerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx2, StakerSigFun),

    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx2, Chain)),
    {ok, AddGatewayBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx2]),
    %% add the block
    blockchain:add_block(AddGatewayBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 2} =:= blockchain:height(Chain) end),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, GW} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertMatch(light, blockchain_ledger_gateway_v2:mode(GW)),
    ok.

staking_key_mode_mappings_add_dataonly_gateway(Config) ->
    %% add gateways where the staker has a mapping to a dataonly gateway in the staking key mode mappings tables
    %% Confirm the fees for each are correct
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("base dir: ~p", [BaseDir]),
    ct:pal("base SIM dir: ~p", [SimDir]),
    Payer = ?config(payer, Config),
    PayerSigFun = ?config(payer_sig_fun, Config),

    _Owner = ?config(owner, Config),
    _OwnerSigFun = ?config(owner_sig_fun, Config),
    Chain = ?config(chain, Config),
    Ledger = ?config(ledger, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    {ok, CurHeight} = blockchain:height(Chain),

    %%
    %% create a payment txn to fund staking account 1 - which will have a staking key mapping set to full
    %%

    #{public := _StakingPub, secret := StakingPrivKey} = ?config(staking_key, Config),
    Staker = ?config(staking_key_pub_bin, Config),
    StakerSigFun = libp2p_crypto:mk_sig_fun(StakingPrivKey),
    %% base txn
    PaymentTx0 = blockchain_txn_payment_v1:new(Payer, Staker, 5000 * ?BONES_PER_HNT, 1),

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
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 1} =:= blockchain:height(Chain) end),

    %% add the gateway using the staker key, should be added as a dataonly gateway
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
    %% dataonly gateway costs 20 usd
    ?assertEqual(10 * ?USD_TO_DC, AddGatewayStFee),
    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddGatewayTxFee, AddGatewayStFee, AddGatewayTxFee + AddGatewayStFee]),

    %% set the fees on the base txn and then sign the various txns
    AddGatewayTx1 = blockchain_txn_add_gateway_v1:fee(AddGatewayTx0, AddGatewayTxFee),
    AddGatewayTx2 = blockchain_txn_add_gateway_v1:staking_fee(AddGatewayTx1, AddGatewayStFee),

    SignedOwnerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx2, OwnerSigFun),
    SignedGatewayAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx2, GatewaySigFun),
    SignedPayerAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedGatewayAddGatewayTx2, StakerSigFun),

    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx2, Chain)),
    {ok, AddGatewayBlock} = test_utils:create_block(ConsensusMembers, [SignedPayerAddGatewayTx2]),
    %% add the block
    blockchain:add_block(AddGatewayBlock, Chain),
    %% confirm the block is added
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, CurHeight + 2} =:= blockchain:height(Chain) end),

    %% check the ledger to confirm the gateway is added with the correct mode
    {ok, GW} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertMatch(dataonly, blockchain_ledger_gateway_v2:mode(GW)),
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
