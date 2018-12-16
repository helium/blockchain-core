-module(blockchain_simple_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1,
    reload_test/1,
    restart_test/1,
    htlc_payee_redeem_test/1,
    htlc_payer_redeem_test/1,
    poc_request_test/1,
    bogus_coinbase_test/1,
    bogus_coinbase_with_good_payment_test/1,
    export_test/1
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
    [basic_test,
     reload_test,
     restart_test,
     htlc_payee_redeem_test,
     htlc_payer_redeem_test,
     poc_request_test,
     bogus_coinbase_test,
     bogus_coinbase_with_good_payment_test,
     export_test].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    BaseDir = "data/test_SUITE/" ++ erlang:atom_to_list(TestCase),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
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
        {consensus_members, ConsensusMembers} | Config
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

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:address(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    ok = blockchain_worker:add_block(Block, self()),
    Chain = blockchain_worker:blockchain(),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain_worker:height()),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain_worker:ledger(),
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2510, blockchain_ledger_entry_v1:balance(NewEntry1)),
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

    % Add some blocks
    lists:foreach(
        fun(_) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            ok = blockchain_worker:add_block(Block, self())
        end,
        lists:seq(1, 10)
    ),
    ?assertEqual({ok, 11}, blockchain_worker:height()),
    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> not erlang:is_process_alive(Sup) end),

    % Create new genesis block
    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance + 1)
                     || {Addr, _} <- ConsensusMembers],
    GenConsensusGroupTx = blockchain_txn_gen_consensus_group_v1:new([Addr || {Addr, _} <- ConsensusMembers]),
    Txs = GenPaymentTxs ++ [GenConsensusGroupTx],
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
    ?assertEqual({ok, 1}, blockchain_worker:height()),

    true = erlang:exit(Sup1, normal),
    ok.

restart_test(Config) ->
    GenDir = "data/test_SUITE/restart2",
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Sup = proplists:get_value(sup, Config),
    Opts = proplists:get_value(opts, Config),
    Chain0 = blockchain_worker:blockchain(),
    {ok, GenBlock} = blockchain:head_block(Chain0),

    % Add some blocks
    [LastBlock| _Blocks] = lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            ok = blockchain_worker:add_block(Block, self()),
            timer:sleep(100),
            [Block|Acc]
        end,
        [],
        lists:seq(1, 10)
    ),
    ?assertEqual({ok, 11}, blockchain_worker:height()),

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
    ?assertEqual({ok, 11}, blockchain_worker:height()),

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
    ?assertEqual({ok, 11}, blockchain_worker:height()),

    true = erlang:exit(Sup2, normal),
    ok.


htlc_payee_redeem_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_address(PubKey),
    % Create a Payee
    {PayeePrivKey, PayeePubKey} = libp2p_crypto:generate_keys(),
    Payee = libp2p_crypto:pubkey_to_address(PayeePubKey),
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

    %% these transactions depend on each other, so they need to appear in different blocks
    Block0 = test_utils:create_block(ConsensusMembers, [SignedCreateTx]),
    ok = blockchain_worker:add_block(Block0, self()),

    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    ok = blockchain_worker:add_block(Block, self()),

    Chain = blockchain_worker:blockchain(),
    {ok, HeadHash} = blockchain:head_hash(Chain),

    ?assertEqual(blockchain_block:hash_block(Block), HeadHash),
    ?assertEqual({ok, Block}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 3}, blockchain_worker:height()),

    % Check that the Payer balance has been reduced by 2500
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, blockchain_worker:ledger()),
    ?assertEqual(Balance - 2600, blockchain_ledger_entry_v1:balance(NewEntry0)),

    % Check that the HLTC address exists and has the correct balance, hashlock and timelock
    % NewHTLC0 = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain_worker:ledger()),
    {ok, NewHTLC0} = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain_worker:ledger()),
    ?assertEqual(2500, blockchain_ledger_htlc_v1:balance(NewHTLC0)),
    ?assertEqual(Hashlock, blockchain_ledger_htlc_v1:hashlock(NewHTLC0)),
    ?assertEqual(3, blockchain_ledger_htlc_v1:timelock(NewHTLC0)),

    % Try and redeem
    RedeemSigFun = libp2p_crypto:mk_sig_fun(PayeePrivKey),
    RedeemTx = blockchain_txn_redeem_htlc_v1:new(Payee, HTLCAddress, <<"sharkfed">>, 0),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx, RedeemSigFun),
    Block2 = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    ok = blockchain_worker:add_block(Block2, self()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check that the second block with the Redeem TX was mined properly
    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block2), HeadHash2),
    ?assertEqual({ok, Block2}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 4}, blockchain_worker:height()),

    % Check that the Payee now owns 2500
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payee, blockchain_worker:ledger()),
    ?assertEqual(2600, blockchain_ledger_entry_v1:balance(NewEntry1)),

    ok.

htlc_payer_redeem_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    BaseDir = proplists:get_value(basedir, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),

    % Create a Payer
    Payer = libp2p_crypto:pubkey_to_address(PubKey),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(32),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),
    CreateTx = blockchain_txn_create_htlc_v1:new(Payer, Payer, HTLCAddress, Hashlock, 3, 2500, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedCreateTx = blockchain_txn_create_htlc_v1:sign(CreateTx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedCreateTx]),
    ok = blockchain_worker:add_block(Block, self()),

    Chain = blockchain_worker:blockchain(),
    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block), HeadHash),
    ?assertEqual({ok, Block}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 2}, blockchain_worker:height()),

    % Check that the Payer balance has been reduced by 2500
    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, blockchain_worker:ledger()),
    ?assertEqual(Balance - 2500, blockchain_ledger_entry_v1:balance(NewEntry0)),

    % Check that the HLTC address exists and has the correct balance, hashlock and timelock
    % NewHTLC0 = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain_worker:ledger()),
    {ok, NewHTLC0} = blockchain_ledger_v1:find_htlc(HTLCAddress, blockchain_worker:ledger()),
    ?assertEqual(2500, blockchain_ledger_htlc_v1:balance(NewHTLC0)),
    ?assertEqual(Hashlock, blockchain_ledger_htlc_v1:hashlock(NewHTLC0)),
    ?assertEqual(3, blockchain_ledger_htlc_v1:timelock(NewHTLC0)),

    % Mine another couple of blocks
    Block2 = test_utils:create_block(ConsensusMembers, []),
    ok = blockchain_worker:add_block(Block2, self()),
    Block3 = test_utils:create_block(ConsensusMembers, []),
    ok = blockchain_worker:add_block(Block3, self()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check we are at height 4
    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block3), HeadHash2),
    ?assertEqual({ok, Block3}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 4}, blockchain_worker:height()),

    % Try and redeem
    RedeemTx = blockchain_txn_redeem_htlc_v1:new(Payer, HTLCAddress, <<"sharkfed">>, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx, SigFun),
    Block4 = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    ok = blockchain_worker:add_block(Block4, self()),

    % Check that the Payer now owns 5000 again
    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, blockchain_worker:ledger()),
    ?assertEqual(5000, blockchain_ledger_entry_v1:balance(NewEntry1)),

    ok.

poc_request_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),
    Owner = libp2p_crypto:pubkey_to_address(PubKey),

    % Create a Gateway
    {GatewayPrivKey, GatewayPubKey} = libp2p_crypto:generate_keys(),
    Gateway = libp2p_crypto:pubkey_to_address(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),

    % Add a Gateway
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedGatewayAddGatewayTx]),
    ok = blockchain_worker:add_block(Block, self()),

    Chain = blockchain_worker:blockchain(),
    {ok, HeadHash} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block), HeadHash),
    ?assertEqual({ok, Block}, blockchain:get_block(HeadHash, Chain)),
    ?assertEqual({ok, 2}, blockchain_worker:height()),

    % Check that the Gateway is there
    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain_worker:ledger()),
    ?assertEqual(Owner, blockchain_ledger_gateway_v1:owner_address(GwInfo)),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, 123456, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),

    Block2 = test_utils:create_block(ConsensusMembers, [SignedAssertLocationTx]),
    ok = blockchain_worker:add_block(Block2, self()),
    timer:sleep(500),

    {ok, HeadHash2} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block2), HeadHash2),
    ?assertEqual({ok, Block2}, blockchain:get_block(HeadHash2, Chain)),
    ?assertEqual({ok, 3}, blockchain_worker:height()),

    % Create the PoC challenge request txn
    Secret = crypto:strong_rand_bytes(8),
    Tx = blockchain_txn_poc_request_v1:new(Gateway, crypto:hash(sha256, Secret)),
    SignedTx = blockchain_txn_poc_request_v1:sign(Tx, GatewaySigFun),
    Block3 = test_utils:create_block(ConsensusMembers, [SignedTx]),
    ok = blockchain_worker:add_block(Block3, self()),
    timer:sleep(500),

    {ok, HeadHash3} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block3), HeadHash3),
    ?assertEqual({ok, Block3}, blockchain:get_block(HeadHash3, Chain)),
    ?assertEqual({ok, 4}, blockchain_worker:height()),

    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GwInfo2} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain_worker:ledger()),
    ?assertEqual(4, blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo2)),

    ok.

bogus_coinbase_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    Balance = proplists:get_value(balance, Config),
    [{FirstMemberAddr, _} | _] = ConsensusMembers,

    %% Lets give the first member a bunch of coinbase tokens
    BogusCoinbaseTxn = blockchain_txn_coinbase_v1:new(FirstMemberAddr, 999999),
    Block2 = test_utils:create_block(ConsensusMembers, [BogusCoinbaseTxn]),

    ok = blockchain_worker:add_block(Block2, self()),
    timer:sleep(500),

    %% None of the balances should have changed
    lists:all(fun(Entry) ->
                      blockchain_ledger_entry_v1:balance(Entry) == Balance
              end,
              maps:values(blockchain_ledger_v1:entries(blockchain_worker:ledger()))),

    %% Check that the chain didn't grow
    ?assertEqual({ok, 1}, blockchain_worker:height()),

    ok.

bogus_coinbase_with_good_payment_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    [{FirstMemberAddr, _} | _] = ConsensusMembers,

    %% Lets give the first member a bunch of coinbase tokens
    BogusCoinbaseTxn = blockchain_txn_coinbase_v1:new(FirstMemberAddr, 999999),

    %% Create a good payment transaction as well
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:address(),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedGoodPaymentTxn = blockchain_txn_payment_v1:sign(Tx, SigFun),

    Block2 = test_utils:create_block(ConsensusMembers, [BogusCoinbaseTxn, SignedGoodPaymentTxn]),
    ok = blockchain_worker:add_block(Block2, self()),
    timer:sleep(500),

    %% Check that the chain didnt' grow
    ?assertEqual({ok, 1}, blockchain_worker:height()),

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
    Fee = 10,
    PaymentTxn1 = test_utils:create_payment_transaction(Payer1, PayerPrivKey1, Amount, Fee, 1, blockchain_swarm:address()),
    PaymentTxn2 = test_utils:create_payment_transaction(Payer2, PayerPrivKey2, Amount, Fee, 1, blockchain_swarm:address()),
    PaymentTxn3 = test_utils:create_payment_transaction(Payer3, PayerPrivKey3, Amount, Fee, 1, blockchain_swarm:address()),

    % Create a Gateway
    Owner = libp2p_crypto:pubkey_to_address(PayerPubKey1),
    {GatewayPrivKey, GatewayPubKey} = libp2p_crypto:generate_keys(),
    Gateway = libp2p_crypto:pubkey_to_address(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey1),

    % Add a Gateway
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(Owner, Gateway),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(Gateway, Owner, 123456, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),

    Block2 = test_utils:create_block(ConsensusMembers, [PaymentTxn1, PaymentTxn2, PaymentTxn3, SignedGatewayAddGatewayTx]),
    ok = blockchain_worker:add_block(Block2, self()),

    %% this has to be done in a subsequent block
    Block3 = test_utils:create_block(ConsensusMembers, [SignedAssertLocationTx]),
    ok = blockchain_worker:add_block(Block3, self()),


    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain_worker:ledger()),
    ?assertEqual(Owner, blockchain_ledger_gateway_v1:owner_address(GwInfo)),

    timer:sleep(500),

    [{accounts, Accounts}, {gateways, Gateways}] = blockchain_ledger_exporter_v1:export(blockchain_worker:ledger()),

    ?assertEqual([[{gateway_address, libp2p_crypto:pubkey_to_b58(GatewayPubKey)},
              {owner_address,libp2p_crypto:pubkey_to_b58(PayerPubKey1)},
              {location,123456},
              {last_poc_challenge,undefined},
              {nonce,1},
              {score,0.0}]], Gateways),

    FilteredExportedAccounts = lists:foldl(fun(Account, Acc) ->
                                                   AccontAddress = proplists:get_value(address, Account),
                                                   case libp2p_crypto:address_to_b58(Payer1) == AccontAddress orelse
                                                        libp2p_crypto:address_to_b58(Payer2) == AccontAddress orelse
                                                        libp2p_crypto:address_to_b58(Payer3) == AccontAddress
                                                   of
                                                       true -> [Account | Acc];
                                                       false -> Acc
                                                   end
                                           end, [], Accounts),
    lists:all(fun(Account) ->
                      (Balance - Amount - Fee) == proplists:get_value(balance, Account)
              end, FilteredExportedAccounts),

    ok.
