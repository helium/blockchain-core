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
    txn_fees_pay_with_hnt/1
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
    txn_fees_pay_with_hnt
].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config0)  when TestCase == txn_fees_pay_with_hnt;
                                           TestCase == txn_fees_pay_with_dc ->
    Config = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config0),
    BaseDir = ?config(base_dir, Config0),

    {ok, OracleKeys} = make_oracles(3),
    {ok, EncodedOracleKeys} = make_encoded_oracle_keys(OracleKeys),

    ExtraVars = #{
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
      payment_txn_fee_multiplier => 5000
    },
    Balance = 50000 * ?BONES_PER_HNT,
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
    [
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

end_per_testcase(_Test, _Config) ->
    catch gen_server:stop(blockchain_sup),
    ok.

%%--------------------------------------------------------------------
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

    %%
    %% start with a token burn txn, create a DC balance for the payer
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_token_burn_v1:is_valid(SignedBurnTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_token_burn_v1:is_valid(SignedBurnTx1, Chain)),
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_oui_v1:is_valid(SignedOUITx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,_,1}}, blockchain_txn_oui_v1:is_valid(SignedOUITx1, Chain)),
    ?assertMatch(ok, blockchain_txn_oui_v1:is_valid(SignedOUITx2, Chain)),
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,_,1}}, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_add_gateway_v1:is_valid(SignedPayerAddGatewayTx2, Chain)),
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,_,1}}, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_assert_location_v1:is_valid(SignedPayerAssertLocationTx2, Chain)),
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_create_htlc_v1:is_valid(SignedCreateHTLCTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_create_htlc_v1:is_valid(SignedCreateHTLCTx1, Chain)),
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_redeem_htlc_v1:is_valid(SignedRedeemTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_redeem_htlc_v1:is_valid(SignedRedeemTx1, Chain)),

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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_payment_v1:is_valid(SignedPaymentTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_payment_v1:is_valid(SignedPaymentTx1, Chain)),
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_routing_v1:is_valid(SignedRoutingTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_routing_v1:is_valid(SignedRoutingTx1, Chain)),
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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,_,0}}, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx1, Chain)),
    ?assertMatch(ok, blockchain_txn_routing_v1:is_valid(SignedRoutingSubnetTx2, Chain)),
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
    %% State Channels Open txn
    %%

    ID = crypto:strong_rand_bytes(24),
    ExpireWithin = 11,
    %% base txn
    SCTx0 = blockchain_txn_state_channel_open_v1:new(ID, RouterPubKeyBin, ExpireWithin, OUI1, 1),

    %% get the fees for this txn
    SCTxFee = blockchain_txn_state_channel_open_v1:calculate_fee(SCTx0, Chain),
    ct:pal("state channels open txn fee ~p, staking fee ~p, total: ~p", [SCTxFee, 'NA', SCTxFee ]),

    %% set the fees on the base txn and then sign the various txns
    SCTx1 = blockchain_txn_state_channel_open_v1:fee(SCTx0, SCTxFee),
    SignedSCTx0 = blockchain_txn_state_channel_open_v1:sign(SCTx0, RouterSigFun),
    SignedSCTx1 = blockchain_txn_state_channel_open_v1:sign(SCTx1, RouterSigFun),

    % Make a payment to the router ( owner ) so he has an account and some balance
    PayRouterTx0 = blockchain_txn_payment_v1:new(Payer, RouterPubKeyBin, 500000000, 5),
    PayRouterTx1 = blockchain_txn_payment_v1:fee(PayRouterTx0, blockchain_txn_payment_v1:calculate_fee(PayRouterTx0, Chain)),
    SignedPayRouterTx = blockchain_txn_payment_v1:sign(PayRouterTx1, PayerSigFun),
    {ok, PayRouterBlock} = test_utils:create_block(ConsensusMembers, [SignedPayRouterTx]),
    blockchain:add_block(PayRouterBlock, Chain),

    %% check is_valid behaves as expected and returns blockchain_txn_state_channel_open_v1 error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_state_channel_open_v1:is_valid(SignedSCTx0, Chain)),
    ?assertMatch(ok, blockchain_txn_state_channel_open_v1:is_valid(SignedSCTx1, Chain)),
    %% check create block on tx with invalid txn fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [SignedSCTx0])),
    %% all the fees are set, so this should work
    {ok, SCBlock} = test_utils:create_block(ConsensusMembers, [SignedSCTx1]),
    %% add the block
    blockchain:add_block(SCBlock, Chain),

    %% confirm DC balances are debited with correct fee ( NOTE: Router will be paying )
%%    {ok, SCTxDCEntry} = blockchain_ledger_v1:find_dc_entry(RouterPubKeyBin, Ledger),
%%    SCTxDCBal = blockchain_ledger_data_credits_entry_v1:balance(SCTxDCEntry),
%%    ct:pal("DC balance after state channels open update txn ~p", [SCTxDCBal]),
%%    RouterDCBal1 = RouterDCBal0 - SCTxFee,
%%    ?assertEqual(RouterDCBal1, SCTxDCBal),


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

    %% check is_valid behaves as expected and returns correct error msgs
    ?assertMatch({error,{wrong_txn_fee,_,0}}, blockchain_txn_oui_v1:is_valid(SignedOUITx0, Chain)),
    ?assertMatch({error,{wrong_staking_fee,_,1}}, blockchain_txn_oui_v1:is_valid(SignedOUITx1, Chain)),
    ?assertMatch(ok, blockchain_txn_oui_v1:is_valid(SignedOUITx2, Chain)),
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
