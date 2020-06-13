-module(blockchain_price_oracle_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_txn_fees.hrl").

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
    txn_fees/1
]).

all() -> [
    validate_initial_state,
    submit_prices,
    calculate_price_even,
    submit_bad_public_key,
    double_submit_prices,
    txn_too_high,
    replay_txn,
    txn_fees
].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

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


txn_fees(Config) ->
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
    ?assertEqual({ok, median(ExpectedPrices)},
                    blockchain_ledger_v1:current_oracle_price(Ledger)),
    ?assertEqual({ok, lists:sort(ExpectedPrices)}, get_prices(
                    blockchain_ledger_v1:current_oracle_price_list(Ledger))),


    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    OUI1 = 1,
    Addresses0 = [Payer],
    {Filter, _} = xor16:to_bin(xor16:new([], fun xxhash:hash64/1)),
    OUITxn0 = blockchain_txn_oui_v1:new(OUI1, Payer, Addresses0, Filter, 8),
    OUITxn1 = blockchain_txn_oui_v1:fee(OUITxn0, blockchain_txn_oui_v1:calculate_fee(OUITxn0, Chain)),
    OUITxn2 = blockchain_txn_oui_v1:staking_fee(OUITxn1, blockchain_txn_oui_v1:calculate_staking_fee(OUITxn1, Chain)),
    SignedOUITxn0 = blockchain_txn_oui_v1:sign(OUITxn2, SigFun),
    ct:pal("Staking fee ~p, txn fee ~p", [blockchain_txn_oui_v1:staking_fee(OUITxn2), blockchain_txn_oui_v1:fee(OUITxn2)]),


    %% insufficent txn fee and staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [blockchain_txn_oui_v1:sign(OUITxn0, SigFun)])),
    %% insufficent staking fee
    ?assertMatch({error, {invalid_txns, _}}, test_utils:create_block(ConsensusMembers, [blockchain_txn_oui_v1:sign(OUITxn1, SigFun)])),

    %% got all the fees, so this should work
    {ok, Block0} = test_utils:create_block(ConsensusMembers, [SignedOUITxn0]),

    blockchain:add_block(Block0, Chain),

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
