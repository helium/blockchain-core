-module(blockchain_simple_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    epoch_reward_test/1,
    election_test/1,
    chain_vars_test/1,
    token_burn_test/1,
    payer_test/1
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
        epoch_reward_test,
        election_test,
        chain_vars_test,
        token_burn_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    BaseDir = "data/test_SUITE/" ++ erlang:atom_to_list(TestCase),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers, Keys} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

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
        {basedir, BaseDir},
        {balance, Balance},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers},
        Keys
        | Config
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, Config) ->
    Sup = proplists:get_value(sup, Config),
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

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 0, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

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

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
reload_test(Config) ->
    Balance = 5000,
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Sup = proplists:get_value(sup, Config),
    Opts = proplists:get_value(opts, Config),
    Chain0 = proplists:get_value(chain, Config),
    Swarm0 = proplists:get_value(swarm, Config),
    N0 = proplists:get_value(n, Config),

    % Add some blocks
    lists:foreach(
        fun(_) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(Swarm0, Block, Chain0, N0, self())
        end,
        lists:seq(1, 10)
    ),
    ?assertEqual({ok, 11}, blockchain:height(Chain0)),

    %% Kill this blockchain sup
    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Sup) end),

    {VTxn, _Config} = blockchain_ct_utils:create_vars(#{num_consensus_members => N0}),

    InitialVars = [ VTxn ],

    % Create new genesis block
    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance + 1)
                     || {Addr, _} <- ConsensusMembers],
    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new([Addr || {Addr, _} <- ConsensusMembers], <<"proof">>, 1, 0),
    Txs = InitialVars ++ GenPaymentTxs ++ [GenConsensusGroupTx],
    NewGenBlock = blockchain_block:new_genesis_block(Txs),
    GenDir = "data/test_SUITE/reload2",
    File = filename:join(GenDir, "genesis"),
    ok = test_utils:atomic_save(File, blockchain_block:serialize(NewGenBlock)),

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
    GenDir = "data/test_SUITE/restart2",
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Sup = proplists:get_value(sup, Config),
    Opts = proplists:get_value(opts, Config),
    Chain0 = proplists:get_value(chain, Config),
    Swarm0 = proplists:get_value(swarm, Config),
    N0 = proplists:get_value(n, Config),
    {ok, GenBlock} = blockchain:head_block(Chain0),

    % Add some blocks
    [LastBlock| _Blocks] = lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(Swarm0, Block, Chain0, N0, self()),
            timer:sleep(100),
            [Block|Acc]
        end,
        [],
        lists:seq(1, 10)
    ),
    ?assertEqual({ok, 11}, blockchain:height(Chain0)),

    %% Kill this blockchain sup
    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Sup) end),

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

    true = erlang:exit(Sup1, normal),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Sup1) end),

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
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    % Create a Payee
    #{public := PayeePubKey, secret := PayeePrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PayeePubKey),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(32),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    CreateTx = blockchain_txn_create_htlc_v1:new(Payer, Payee, HTLCAddress, Hashlock, 3, 2500, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedCreateTx = blockchain_txn_create_htlc_v1:sign(CreateTx, SigFun),
    % send some money to the payee so they have enough to pay the fee for redeeming
    Tx = blockchain_txn_payment_v1:new(Payer, Payee, 100, 0, 2),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),

    %% these transactions depend on each other, but they should be able to exist in the same block
    Block = test_utils:create_block(ConsensusMembers, [SignedCreateTx, SignedTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

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
    RedeemTx = blockchain_txn_redeem_htlc_v1:new(Payee, HTLCAddress, <<"sharkfed">>, 0),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx, RedeemSigFun),
    Block2 = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check that the second block with the Redeem TX was mined properly
    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block2), HeadHash2),
    ?assertEqual({ok, Block2}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    % Check that the Payee now owns 2500
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payee, blockchain:ledger(Chain)),
    ?assertEqual(2600, blockchain_ledger_entry_v1:balance(NewEntry1)),

    ok.

htlc_payer_redeem_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(32),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    CreateTx = blockchain_txn_create_htlc_v1:new(Payer, Payer, HTLCAddress, Hashlock, 3, 2500, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedCreateTx = blockchain_txn_create_htlc_v1:sign(CreateTx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedCreateTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

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
    Block2 = test_utils:create_block(ConsensusMembers, []),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),
    Block3 = test_utils:create_block(ConsensusMembers, []),
    _ = blockchain_gossip_handler:add_block(Swarm, Block3, Chain, N, self()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check we are at height 4
    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block3), HeadHash2),
    ?assertEqual({ok, Block3}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 4}, blockchain:height(Chain)),

    ?assertEqual({ok, 4}, blockchain_ledger_v1:current_height(blockchain:ledger(Chain))),

    % Try and redeem
    RedeemTx = blockchain_txn_redeem_htlc_v1:new(Payer, HTLCAddress, <<"sharkfed">>, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx, SigFun),
    Block4 = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block4, Chain, N, self()),

    % Check that the Payer now owns 5000 again
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, blockchain:ledger(Chain)),
    ?assertEqual(5000, blockchain_ledger_entry_v1:balance(NewEntry1)),

    ok.

poc_request_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),
    Owner = libp2p_crypto:pubkey_to_bin(PubKey),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    % Create a Gateway
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),

    % Add a Gateway
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway, 0, 0),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedGatewayAddGatewayTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block), HeadHash),
    ?assertEqual({ok, Block}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    % Check that the Gateway is there
    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),
    ?assertEqual(Owner, blockchain_ledger_gateway_v1:owner_address(GwInfo)),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, ?TEST_LOCATION, 1, 0, 0),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),

    Block2 = test_utils:create_block(ConsensusMembers, [SignedAssertLocationTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),
    timer:sleep(500),

    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block2), HeadHash2),
    ?assertEqual({ok, Block2}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    % Create the PoC challenge request txn
    Keys0 = libp2p_crypto:generate_keys(ecc_compact),
    Secret0 = libp2p_crypto:keys_to_bin(Keys0),
    #{public := OnionCompactKey0} = Keys0,
    SecretHash0 = crypto:hash(sha256, Secret0),
    OnionKeyHash0 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey0)),
    PoCReqTxn0 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash0, OnionKeyHash0, blockchain_block:hash_block(Block2), 1),
    SignedPoCReqTxn0 = blockchain_txn_poc_request_v1:sign(PoCReqTxn0, GatewaySigFun),
    Block3 = test_utils:create_block(ConsensusMembers, [SignedPoCReqTxn0]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block3, Chain, N, self()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 4} =:= blockchain:height(Chain) end),

    Ledger = blockchain:ledger(Chain),
    {ok, HeadHash3} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block3), HeadHash3),
    ?assertEqual({ok, Block3}, blockchain:get_block(HeadHash3, Chain)),
    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GwInfo2} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(4, blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo2)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_gateway_v1:last_poc_onion_key_hash(GwInfo2)),

    % Check that the PoC info
    {ok, [PoC]} = blockchain_ledger_v1:find_poc(OnionKeyHash0, Ledger),
    ?assertEqual(SecretHash0, blockchain_ledger_poc_v1:secret_hash(PoC)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_poc_v1:onion_key_hash(PoC)),
    ?assertEqual(Gateway, blockchain_ledger_poc_v1:challenger(PoC)),

    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_Txn, _Chain) -> ok end),

    PoCReceiptsTxn = blockchain_txn_poc_receipts_v1:new(Gateway, Secret0, OnionKeyHash0, []),
    SignedPoCReceiptsTxn = blockchain_txn_poc_receipts_v1:sign(PoCReceiptsTxn, GatewaySigFun),
    Block4 = test_utils:create_block(ConsensusMembers, [SignedPoCReceiptsTxn]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block4, Chain, N, self()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 5} =:= blockchain:height(Chain) end),

    Block40 = lists:foldl(
        fun(_, _) ->
            B = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(Swarm, B, Chain, N, self()),
            timer:sleep(10),
            B
        end,
        <<>>,
        lists:seq(1, 35)
    ),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 40} =:= blockchain:height(Chain) end),

    % Create another PoC challenge request txn
    Keys1 = libp2p_crypto:generate_keys(ecc_compact),
    Secret1 = libp2p_crypto:keys_to_bin(Keys1),
    #{public := OnionCompactKey1} = Keys1,
    SecretHash1 = crypto:hash(sha256, Secret1),
    OnionKeyHash1 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey1)),
    PoCReqTxn1 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash1, OnionKeyHash1, blockchain_block:hash_block(Block40), 1),
    SignedPoCReqTxn1 = blockchain_txn_poc_request_v1:sign(PoCReqTxn1, GatewaySigFun),
    Block5 = test_utils:create_block(ConsensusMembers, [SignedPoCReqTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block5, Chain, N, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 41} =:= blockchain:height(Chain) end),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_poc(OnionKeyHash0, Ledger)),
    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GwInfo3} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(41, blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo3)),
    ?assertEqual(OnionKeyHash1, blockchain_ledger_gateway_v1:last_poc_onion_key_hash(GwInfo3)),

    ?assert(meck:validate(blockchain_txn_poc_receipts_v1)),
    meck:unload(blockchain_txn_poc_receipts_v1),
    ok.

bogus_coinbase_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    [{FirstMemberAddr, _} | _] = ConsensusMembers,
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    %% Lets give the first member a bunch of coinbase tokens
    BogusCoinbaseTxn = blockchain_txn_coinbase_v1:new(FirstMemberAddr, 999999),
    Block2 = test_utils:create_block(ConsensusMembers, [BogusCoinbaseTxn]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),
    timer:sleep(500),

    %% None of the balances should have changed
    lists:all(fun(Entry) ->
                      blockchain_ledger_entry_v1:balance(Entry) == Balance
              end,
              maps:values(blockchain_ledger_v1:entries(blockchain:ledger(Chain)))),

    %% Check that the chain didn't grow
    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    ok.

bogus_coinbase_with_good_payment_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    [{FirstMemberAddr, _} | _] = ConsensusMembers,
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    %% Lets give the first member a bunch of coinbase tokens
    BogusCoinbaseTxn = blockchain_txn_coinbase_v1:new(FirstMemberAddr, 999999),

    %% Create a good payment transaction as well
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 0, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedGoodPaymentTxn = blockchain_txn_payment_v1:sign(Tx, SigFun),

    Block2 = test_utils:create_block(ConsensusMembers, [BogusCoinbaseTxn, SignedGoodPaymentTxn]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),
    timer:sleep(500),

    %% Check that the chain didnt' grow
    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    ok.

export_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    [_,
     {Payer1, {PayerPubKey1, PayerPrivKey1, _}},
     {Payer2, {_, PayerPrivKey2, _}},
     {Payer3, {_, PayerPrivKey3, _}}
     | _] = ConsensusMembers,
    Amount = 2500,
    Fee = 0,
    N = length(ConsensusMembers),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),
    PaymentTxn1 = test_utils:create_payment_transaction(Payer1, PayerPrivKey1, Amount, Fee, 1, blockchain_swarm:pubkey_bin()),
    PaymentTxn2 = test_utils:create_payment_transaction(Payer2, PayerPrivKey2, Amount, Fee, 1, blockchain_swarm:pubkey_bin()),
    PaymentTxn3 = test_utils:create_payment_transaction(Payer3, PayerPrivKey3, Amount, Fee, 1, blockchain_swarm:pubkey_bin()),

    % Create a Gateway
    Owner = libp2p_crypto:pubkey_to_bin(PayerPubKey1),
    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey1),

    % Add a Gateway
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway, 0, 0),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, ?TEST_LOCATION, 1, 0, 0),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),

    %% adding the gateway and asserting a location depend on each other, but they should be able to appear in the same block
    Txns0 = [SignedAssertLocationTx, PaymentTxn2, SignedGatewayAddGatewayTx, PaymentTxn1, PaymentTxn3],
    Txns1 = lists:sort(fun blockchain_txn:sort/2, Txns0),
    Block2 = test_utils:create_block(ConsensusMembers, Txns1),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),

    % Step 2: Add exchange rate to ledger
    Ledger = blockchain:ledger(Chain),
    Rate = 1000000,
    {Priv, _} = proplists:get_value(master_key, Config),
    Vars = #{token_burn_exchange_rate => Rate},
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Vars),
    VarTxn = blockchain_txn_vars_v1:new(Vars, Proof, 2, #{}),
    Block3 = test_utils:create_block(ConsensusMembers, [VarTxn]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block3, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block3)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block3}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),
    ?assertEqual({ok, Block3}, blockchain:get_block(3, Chain)),
    lists:foreach(
        fun(_) ->
                Block = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self())
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual({ok, Rate}, blockchain_ledger_v1:config(token_burn_exchange_rate, Ledger)),

    %% Step 3
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey1),
    BurnTx0 = blockchain_txn_token_burn_v1:new(Payer1, 10, 2),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, SigFun),
    FailedBlock = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Swarm, FailedBlock, Chain, N, self()),

    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),
    ?assertEqual(Owner, blockchain_ledger_gateway_v1:owner_address(GwInfo)),

    timer:sleep(500),

    [{securities, Securities},
     {accounts, Accounts},
     {gateways, Gateways},
     {dcs, DCs}
    ] = blockchain_ledger_exporter_v1:export(blockchain:ledger(Chain)),

    %% check DC balance for Payer1
    [[{address, A}, {dc_balance, DCBalance}]] = DCs,
    ?assertEqual(A, libp2p_crypto:bin_to_b58(Payer1)),
    ?assertEqual(DCBalance, Rate*10),

    %% we added this after we add all of the existing gateways in the
    %% genesis block with nonce 0.  we filter those out to make sure
    %% we're still getting the txn that we're looking for
    Gateways1 = [G || [_, _, _, {nonce, Nonce}] = G <- Gateways, Nonce == 1],

    ?assertEqual([[{gateway_address, libp2p_crypto:pubkey_to_b58(GatewayPubKey)},
                   {owner_address,libp2p_crypto:pubkey_to_b58(PayerPubKey1)},
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

    %% check all consensus members are included in the exported securities
    ?assertEqual(length(Securities), N),

    %% check security balance for each member
    lists:all(fun(Security) ->
                      Token = proplists:get_value(token, Security),
                      Token == Balance
              end, Securities),

    ok.


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
delayed_ledger_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),
    Balance = proplists:get_value(balance, Config),

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
            Tx = blockchain_txn_payment_v1:new(Payer, Payee, 1, 0, X),
            SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
            B = test_utils:create_block(ConsensusMembers, [SignedTx]),
            _ = blockchain_gossip_handler:add_block(Swarm, B, Chain, N, self())
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

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
fees_since_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Payee = blockchain_swarm:pubkey_bin(),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, check_dc_balance, fun(_, _, _) -> ok end),
    meck:expect(blockchain_ledger_v1, debit_fee, fun(_, _, _) -> ok end),

    % Add 100 txns with 1 fee each
    lists:foreach(
        fun(X) ->
            Tx = blockchain_txn_payment_v1:new(Payer, Payee, 1, 1, X),
            SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
            B = test_utils:create_block(ConsensusMembers, [SignedTx]),
            _ = blockchain_gossip_handler:add_block(Swarm, B, Chain, N, self())
        end,
        lists:seq(1, 100)
    ),

    ?assertEqual({error, bad_height}, blockchain:fees_since(100000, Chain)),
    ?assertEqual({error, bad_height}, blockchain:fees_since(1, Chain)),
    ?assertEqual({ok, 100}, blockchain:fees_since(2, Chain)),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1).

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
security_token_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_security_exchange_v1:new(Payer, Recipient, 2500, 0, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_security_exchange_v1:sign(Tx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

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

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
routing_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),
    Ledger = blockchain:ledger(Chain),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    OUI1 = 1,
    Addresses0 = [erlang:list_to_binary(libp2p_swarm:p2p_address(Swarm))],
    OUITxn0 = blockchain_txn_oui_v1:new(Payer, Addresses0, 0),
    SignedOUITxn0 = blockchain_txn_oui_v1:sign(OUITxn0, SigFun),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    Block0 = test_utils:create_block(ConsensusMembers, [SignedOUITxn0]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block0, Chain, N, self()),

    ok = test_utils:wait_until(fun() -> {ok, 2} == blockchain:height(Chain) end),

    Routing0 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses0, 0),
    ?assertEqual({ok, Routing0}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    Addresses1 = [<<"/p2p/random">>],
    OUITxn2 = blockchain_txn_routing_v1:new(OUI1, Payer, Addresses1, 0, 1),
    SignedOUITxn2 = blockchain_txn_routing_v1:sign(OUITxn2, SigFun),
    Block1 = test_utils:create_block(ConsensusMembers, [SignedOUITxn2]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, N, self()),

    ok = test_utils:wait_until(fun() -> {ok, 3} == blockchain:height(Chain) end),

    Routing1 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses1, 1),
    ?assertEqual({ok, Routing1}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    OUI2 = 2,
    Addresses0 = [erlang:list_to_binary(libp2p_swarm:p2p_address(Swarm))],
    OUITxn3 = blockchain_txn_oui_v1:new(Payer, Addresses0, 0),
    SignedOUITxn3 = blockchain_txn_oui_v1:sign(OUITxn3, SigFun),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_routing(OUI2, Ledger)),

    Block2 = test_utils:create_block(ConsensusMembers, [SignedOUITxn3]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),

    ok = test_utils:wait_until(fun() -> {ok, 4} == blockchain:height(Chain) end),

    Routing2 = blockchain_ledger_routing_v1:new(OUI2, Payer, Addresses0, 0),
    ?assertEqual({ok, Routing2}, blockchain_ledger_v1:find_routing(OUI2, Ledger)),

    ?assertEqual({ok, [2, 1]}, blockchain_ledger_v1:find_ouis(Payer, Ledger)),
    ok.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
block_save_failed_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

     % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 0, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, save_block, fun(_, _) -> erlang:error(boom) end),
    ?assertError(boom, blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self())),
    meck:unload(blockchain),
    blockchain_lock:release(),
    ok = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

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

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorb_failed_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 0, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, save_block, fun(B, C) ->
        meck:passthrough([B, C]),
        ct:pal("BOOM"),
        blockchain_lock:release(),
        erlang:error(boom)
    end),
    ?assertError(boom, blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self())),
    meck:unload(blockchain),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    ct:pal("Try to re-add block 1 will cause the mismatch"),
    ok = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

    Ledger = blockchain:ledger(Chain),
    ok = test_utils:wait_until(fun() -> {ok, 1} =:= blockchain:height(Chain) end),
    ok = test_utils:wait_until(fun() -> {ok, 1} =:= blockchain_ledger_v1:current_height(Ledger) end),

    ct:pal("Try to re-add block 2"),
    ok = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),

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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
epoch_reward_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    [_, {PubKeyBin, {_, _PrivKey, _}}|_] = ConsensusMembers,

    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_Txn, _Chain) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_Txn, _Chain) -> ok end),

    meck:new(blockchain_txn_consensus_group_v1, [passthrough]),
    meck:expect(blockchain_txn_consensus_group_v1, is_valid, fun(_Txn, _Chain) -> ok end),

    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, find_gateway_info, fun(Address, _Ledger) ->
        {ok, blockchain_ledger_gateway_v1:new(Address, 12)}
    end),

    % Add few empty blocks to fake epoch
    _Blocks = lists:reverse(lists:foldl(
        fun(X, Acc) ->
            Txns = case X =:= 15 of
                false ->
                    [];
                true ->
                    POCReceiptTxn = blockchain_txn_poc_receipts_v1:new(PubKeyBin, <<"Secret">>, <<"OnionKeyHash">>, []),
                    [POCReceiptTxn]
            end,
            B = test_utils:create_block(ConsensusMembers, Txns),
            _ = blockchain_gossip_handler:add_block(Swarm, B, Chain, N, self()),
            [B|Acc]
        end,
        [],
        lists:seq(1, 32)
    )),
    Start = 1,
    End = 30,
    {ok, Rewards} = blockchain_txn_rewards_v1:calculate_rewards(Start, End, Chain),
    ct:pal("rewards ~p", [Rewards]),
    Tx = blockchain_txn_rewards_v1:new(Start, End, Rewards),
    B = test_utils:create_block(ConsensusMembers, [Tx]),
    _ = blockchain_gossip_handler:add_block(Swarm, B, Chain, N, self()),

    Ledger = blockchain:ledger(Chain),
    {ok, Entry} = blockchain_ledger_v1:find_entry(PubKeyBin, Ledger),

    % 2604167 (poc_challengers) + 552399 (securities) + 248016 (consensus group) + 5000 (initial balance)
    ?assertEqual(3409582, blockchain_ledger_entry_v1:balance(Entry)),

    ?assert(meck:validate(blockchain_txn_poc_receipts_v1)),
    meck:unload(blockchain_txn_poc_receipts_v1),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain_txn_consensus_group_v1)),
    meck:unload(blockchain_txn_consensus_group_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
election_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    %% Chain = proplists:get_value(chain, Config),
    Chain = blockchain_worker:blockchain(),
    _Swarm = proplists:get_value(swarm, Config),
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
         I2 = blockchain_ledger_gateway_v1:set_alpha_beta_delta(Alpha, Beta, 1, I),
         blockchain_ledger_v1:update_gateway(I2, Addr, Ledger1)
     end
     || {Addr, _} <- ConsensusMembers],
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    ct:pal("old ~p", [OldGroup]),

    %% generate new group of the same length
    New =  blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 0),
    New1 =  blockchain_election:new_group(Ledger, crypto:hash(sha256, "foo"), N, 1000),

    ct:pal("new ~p new1 ~p", [New, New1]),

    ?assertEqual(N, length(New)),
    ?assertEqual(N, length(New1)),

    %% confirm that they're sorted by score
    Scored =
        [begin
             {ok, I} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
             {_, _, Score} = blockchain_ledger_gateway_v1:score(Addr, I, 1, Ledger),
             {Score, Addr}
         end
         || Addr <- New],

    ct:pal("scored ~p", [Scored]),

    %% no dupes
    ?assertEqual(lists:usort(Scored), lists:sort(Scored)),

    %% TODO: add better tests here

    ?assertEqual(1, length(New -- OldGroup)),

    ok.

chain_vars_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    {Priv, _} = proplists:get_value(master_key, Config),
    N = proplists:get_value(n, Config),

    Ledger = blockchain:ledger(Chain),

    Vars = #{chain_var => foo},

    Proof = blockchain_txn_vars_v1:create_proof(Priv, Vars),

    ct:pal("priv_key ~p", [Priv]),

    VarTxn = blockchain_txn_vars_v1:new(Vars, Proof, 2, #{}),


    InitBlock = test_utils:create_block(ConsensusMembers, [VarTxn]),
    _ = blockchain_gossip_handler:add_block(Swarm, InitBlock, Chain, N, self()),

    {ok, Delay} = blockchain:config(vars_commit_delay, Ledger),
    ct:pal("commit delay ~p", [Delay]),
    %% Add some blocks,
    lists:foreach(
        fun(_) ->
                Block = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self()),
                {ok, Height} = blockchain:height(Chain),
                case blockchain:config(chain_var, Ledger) of
                    {error, not_found} when Height < (Delay + 1) ->
                        ok;
                    {ok, foo} when Height >= (Delay + 1) ->
                        ok;
                    Res ->
                        throw({error, {chain_var_wrong_height, Res, Height}})
                end
        end,
        lists:seq(1, 15)
    ),
    ?assertEqual({ok, 17}, blockchain:height(Chain)),
    ok.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
token_burn_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Ledger = blockchain:ledger(Chain),

    % Step 1: Simple payment txn with no fees
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    Tx0 = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 0, 1),
    SignedTx0 = blockchain_txn_payment_v1:sign(Tx0, SigFun),
    Block2 = test_utils:create_block(ConsensusMembers, [SignedTx0]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry1)),

    % Step 2: Token burn txn (without a rate) should fail and stay at same block
    BurnTx0 = blockchain_txn_token_burn_v1:new(Payer, 10, 2),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, SigFun),
    FailedBlock = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Swarm, FailedBlock, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),
    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_dc_entry(Payer, Ledger)),
    ?assertEqual({error, not_found}, blockchain_ledger_v1:token_burn_exchange_rate(Ledger)),

    % Step 3: Add exchange rate to ledger
    Rate = 1000000,
    {Priv, _} = proplists:get_value(master_key, Config),
    Vars = #{token_burn_exchange_rate => Rate},
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Vars),
    VarTxn = blockchain_txn_vars_v1:new(Vars, Proof, 2, #{}),
    Block3 = test_utils:create_block(ConsensusMembers, [VarTxn]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block3, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block3)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block3}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),
    ?assertEqual({ok, Block3}, blockchain:get_block(3, Chain)),
    lists:foreach(
        fun(_) ->
                Block = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self())
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual({ok, Rate}, blockchain_ledger_v1:config(token_burn_exchange_rate, Ledger)),

    % Step 4: Retry token burn txn should pass now
    Block24 = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block24, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block24)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block24}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 24}, blockchain:height(Chain)),
    ?assertEqual({ok, Block24}, blockchain:get_block(24, Chain)),
    {ok, NewEntry2} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500 - 10, blockchain_ledger_entry_v1:balance(NewEntry2)),
    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    ?assertEqual(10*Rate, blockchain_ledger_data_credits_entry_v1:balance(DCEntry0)),

    % Step 5: Try payment txn with fee this time
    Fee = 10,
    Tx1 = blockchain_txn_payment_v1:new(Payer, Recipient, 500, Fee, 3),
    SignedTx1 = blockchain_txn_payment_v1:sign(Tx1, SigFun),
    Block25 = test_utils:create_block(ConsensusMembers, [SignedTx1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block25, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block25)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block25}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 25}, blockchain:height(Chain)),
    ?assertEqual({ok, Block25}, blockchain:get_block(25, Chain)),
    {ok, NewEntry3} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500 + 500, blockchain_ledger_entry_v1:balance(NewEntry3)),
    {ok, NewEntry4} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2500 - 10 - 500, blockchain_ledger_entry_v1:balance(NewEntry4)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    ?assertEqual((10*Rate)-Fee, blockchain_ledger_data_credits_entry_v1:balance(DCEntry1)),

    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
payer_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    N = proplists:get_value(n, Config),

    [_, {Payer, {_, PayerPrivKey, _}}, {Owner, {_, OwnerPrivKey, _}}|_] = ConsensusMembers,
    PayerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),
    Ledger = blockchain:ledger(Chain),


    % Step 1: Add exchange rate to ledger
    Rate = 1000000,
    {Priv, _} = proplists:get_value(master_key, Config),
    Vars = #{token_burn_exchange_rate => Rate},
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Vars),
    VarTxn = blockchain_txn_vars_v1:new(Vars, Proof, 2, #{}),
    Block2 = test_utils:create_block(ConsensusMembers, [VarTxn]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),
    lists:foreach(
        fun(_) ->
                Block = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, N, self())
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual({ok, Rate}, blockchain_ledger_v1:config(token_burn_exchange_rate, Ledger)),


    % Step 2: Token burn txn should pass now
    BurnTx0 = blockchain_txn_token_burn_v1:new(Payer, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, PayerSigFun),
    Block23 = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block23, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block23)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block23}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 23}, blockchain:height(Chain)),
    ?assertEqual({ok, Block23}, blockchain:get_block(23, Chain)),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 10, blockchain_ledger_entry_v1:balance(NewEntry0)),
    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    ?assertEqual(10*Rate, blockchain_ledger_data_credits_entry_v1:balance(DCEntry0)),

    % Step 3: Add OUI, gateway, assert_location and let payer pay for it
    Addresses0 = [erlang:list_to_binary(libp2p_swarm:p2p_address(Swarm))],
    OUITxn0 = blockchain_txn_oui_v1:new(Owner, Addresses0, Payer, 10),
    SignedOUITxn0 = blockchain_txn_oui_v1:sign(OUITxn0, OwnerSigFun),
    SignedOUITxn1 = blockchain_txn_oui_v1:sign_payer(SignedOUITxn0, PayerSigFun),


    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),

    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway, Payer, 0, 10),
    SignedAddGatewayTx0 = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedAddGatewayTx1 = blockchain_txn_add_gateway_v1:sign_request(SignedAddGatewayTx0, GatewaySigFun),
    SignedAddGatewayTx2 = blockchain_txn_add_gateway_v1:sign_payer(SignedAddGatewayTx1, PayerSigFun),

    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, Payer, ?TEST_LOCATION, 1, 10, 0),
    SignedAssertLocationTx0 = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx1 = blockchain_txn_assert_location_v1:sign(SignedAssertLocationTx0, OwnerSigFun),
    SignedAssertLocationTx2 = blockchain_txn_assert_location_v1:sign_payer(SignedAssertLocationTx1, PayerSigFun),

    Block24 = test_utils:create_block(ConsensusMembers, [SignedOUITxn1, SignedAddGatewayTx2, SignedAssertLocationTx2]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block24, Chain, N, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block24)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block24}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 24}, blockchain:height(Chain)),
    ?assertEqual({ok, Block24}, blockchain:get_block(24, Chain)),
    {ok, DCEntry1} = blockchain_ledger_v1:find_dc_entry(Payer, Ledger),
    ?assertEqual(10*Rate-10*3, blockchain_ledger_data_credits_entry_v1:balance(DCEntry1)),


    Routing = blockchain_ledger_routing_v1:new(1, Owner, Addresses0, 0),
    ?assertEqual({ok, Routing}, blockchain_ledger_v1:find_routing(1, Ledger)),

    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),
    ?assertEqual(Owner, blockchain_ledger_gateway_v1:owner_address(GwInfo)),
    ?assertEqual(?TEST_LOCATION, blockchain_ledger_gateway_v1:location(GwInfo)),
    ok.
