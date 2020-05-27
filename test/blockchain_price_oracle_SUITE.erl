-module(blockchain_price_oracle_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    validate_initial_state/1,
    submit_prices/1,
    calculate_price_even/1,
    calculate_price_odd/1,
    submit_bad_public_key/1,
    double_submit_prices/1
]).

all() -> [
    validate_initial_state,
    submit_prices,
    calculate_price_even,
    calculate_price_odd,
    submit_bad_public_key
].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config).

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_, _Config) ->
    catch gen_server:stop(blockchain_sup),
    application:unset_env(blockchain, assumed_valid_block_hash),
    application:unset_env(blockchain, assumed_valid_block_height),
    application:unset_env(blockchain, honor_quick_sync),
    persistent_term:erase(blockchain_core_assumed_valid_block_hash_and_height),
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
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} =
                                test_utils:init_chain(Balance, {PrivKey, PubKey}),
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

    Balance = 5000,
    BlocksN = 50,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain = blockchain_worker:blockchain(),

    _Blocks = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],


    {ok, OracleKeys} = make_oracles(3),
    Txns = make_price_oracle_txns(OracleKeys, 50),
    {ok, PriceBlock} = test_utils:create_block(ConsensusMembers, Txns),
    blockchain:add_block(PriceBlock, Chain),

    _Blocks = [
               begin
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                blockchain:add_block(Block, Chain),
                Block
               end || _ <- lists:seq(1, BlocksN) ],

    Ledger = blockchain:ledger(Chain),
    ?assertEqual({ok, 20}, blockchain_ledger_v1:current_oracle_price(Ledger)),
    ?assertEqual({ok, [10, 20, 30]}, blockchain_ledger_v1:current_oracle_price_list(Ledger)),
    ok.


calculate_price_even(_Config) -> ok.
calculate_price_odd(_Config) -> ok.
submit_bad_public_key(_Config) -> ok.
double_submit_prices(_Config) -> ok.


%%--------------------------------------------------------------------
%% TEST HELPERS
%%--------------------------------------------------------------------
prices() -> [ 10, 20, 30 ].

random_price(Prices) ->
    Pos = rand:uniform(length(Prices)),
    lists:nth(Pos, Prices).

make_oracles(N) ->
    ok.

make_oracle_txns(N, Keys, BlockHeight) ->
    Price = random_price(prices()),
    lists:flatten([
     [
        begin
         PK = prep_public_key(PublicKey),
         RawTxn = blockchain_txn_price_oracle_v1:new(PK, Price, BlockHeight),
         blockchain_txn_price_oracle_v1:sign(PrivKey, RawTxn)
        end || {PublicKey, PrivKey} <- Keys ]
                || _ <- lists:seq(1, N) ]).

prep_public_key(K) ->
    base64:encode(<<byte_size(K):8/unsigned-integer, K/binary>>).
