-module(blockchain_simple_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0
]).

-export([
    basic/1
    ,htlc_payee_redeem/1
    ,htlc_payer_redeem/1
    ,poc_request/1
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
    [basic, htlc_payee_redeem, htlc_payer_redeem, poc_request].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic(_Config) ->
    BaseDir = "data/test_SUITE/basic",
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
    Entries = blockchain_ledger:entries(Ledger),

    _ = maps:map(fun(_K, Entry) ->
                         Balance = blockchain_ledger:balance(Entry),
                         0, blockchain_ledger:payment_nonce(Entry)
                 end, Entries),

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:address(),
    Tx = blockchain_txn_payment:new(Payer, Recipient, 2500, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment:sign(Tx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    ok = blockchain_worker:add_block(Block, self()),
    Chain = blockchain_worker:blockchain(),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain:head_hash(Chain)),
    ?assertEqual(Block, blockchain:head_block(Chain)),
    ?assertEqual(2, blockchain_worker:height()),

    ?assertEqual({ok, Block}, blockchain_block:load(2, blockchain:dir(blockchain_worker:blockchain()))),

    NewEntry0 = blockchain_ledger:find_entry(Recipient, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(Balance + 2500, blockchain_ledger:balance(NewEntry0)),

    NewEntry1 = blockchain_ledger:find_entry(Payer, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(Balance - 2510, blockchain_ledger:balance(NewEntry1)),

    % Make sure blockchain saved on file =  in memory
    ok = test_utils:compare_chains(Chain, blockchain:load(BaseDir)),

    % Restart blockchain and make sure nothing has changed
    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end),

    {ok, Sup1} = blockchain_sup:start_link(Opts),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    ok = test_utils:compare_chains(Chain, blockchain_worker:blockchain()),
    true = erlang:exit(Sup1, normal),
    ok.

htlc_payee_redeem(_Config) ->
    BaseDir = "data/test_SUITE/htlc_payee_redeem",
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
    Entries = blockchain_ledger:entries(Ledger),

    _ = maps:map(fun(_K, Entry) ->
                         Balance = blockchain_ledger:balance(Entry),
                         0, blockchain_ledger:payment_nonce(Entry)
                 end, Entries),

    % Create a Payer and an HTLC transaction, add a block and check balances, hashlocks, and timelocks
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    HTLCAddress = blockchain_swarm:address(),
    CreateTx = blockchain_txn_create_htlc:new(Payer, HTLCAddress, <<"3281d585522bc6772a527f5071b149363436415ebc21cc77a8a9167abf29fb72">>, 100, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedCreateTx = blockchain_txn_create_htlc:sign(CreateTx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedCreateTx]),
    ok = blockchain_worker:add_block(Block, self()),
    ChainDir = blockchain:dir(blockchain_worker:blockchain()),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain_block:hash_block(element(2, blockchain:get_block(head, ChainDir)))),
    ?assertEqual({ok, Block}, blockchain:get_block(head, ChainDir)),
    ?assertEqual(2, blockchain_worker:height()),

    % Check that the Payer balance has been reduced by 2500
    NewEntry0 = blockchain_ledger:find_entry(Payer, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(Balance - 2500, blockchain_ledger:balance(NewEntry0)),

    % Check that the HLTC address exists and has the correct balance, hashlock and timelock
    % NewHTLC0 = blockchain_ledger:find_htlc(HTLCAddress, blockchain_worker:ledger()),
    NewHTLC0 = blockchain_ledger:find_htlc(HTLCAddress, blockchain_ledger:htlcs(blockchain_worker:ledger())),
    ?assertEqual(2500, blockchain_ledger:balance(NewHTLC0)),
    ?assertEqual(<<"3281d585522bc6772a527f5071b149363436415ebc21cc77a8a9167abf29fb72">>, blockchain_ledger:hashlock(NewHTLC0)),
    ?assertEqual(100, blockchain_ledger:timelock(NewHTLC0)),

    % Create a Payee
    {PayeePrivKey, PayeePubKey} = libp2p_crypto:generate_keys(),
    Payee = libp2p_crypto:pubkey_to_address(PayeePubKey),

    % Try and redeem
    RedeemSigFun = libp2p_crypto:mk_sig_fun(PayeePrivKey),
    RedeemTx = blockchain_txn_redeem_htlc:new(Payee, HTLCAddress, <<"sharkfed">>),
    SignedRedeemTx = blockchain_txn_redeem_htlc:sign(RedeemTx, RedeemSigFun),
    Block2 = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    ok = blockchain_worker:add_block(Block2, self()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check that the second block with the Redeem TX was mined properly
    ?assertEqual(blockchain_block:hash_block(Block2), blockchain_block:hash_block(element(2, blockchain:get_block(head, ChainDir)))),
    ?assertEqual({ok, Block2}, blockchain:get_block(head, ChainDir)),
    ?assertEqual(3, blockchain_worker:height()),

    % Check that the Payee now owns 2500
    NewEntry1 = blockchain_ledger:find_entry(Payee, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(2500, blockchain_ledger:balance(NewEntry1)),

    % Make sure blockchain saved on file =  in memory
    Chain = blockchain_worker:blockchain(),
    ok = test_utils:compare_chains(Chain, blockchain:load(BaseDir)),

    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end),

    ok.

htlc_payer_redeem(_Config) ->
    BaseDir = "data/test_SUITE/htlc_payer_redeem",
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
    Entries = blockchain_ledger:entries(Ledger),

    _ = maps:map(fun(_K, Entry) ->
                         Balance = blockchain_ledger:balance(Entry),
                         0, blockchain_ledger:payment_nonce(Entry)
                 end, Entries),

    % Create a Payer and an HTLC transaction, add a block and check balances, hashlocks, and timelocks
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    HTLCAddress = blockchain_swarm:address(),
    CreateTx = blockchain_txn_create_htlc:new(Payer, HTLCAddress, <<"3281d585522bc6772a527f5071b149363436415ebc21cc77a8a9167abf29fb72">>, 3, 2500, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedCreateTx = blockchain_txn_create_htlc:sign(CreateTx, SigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedCreateTx]),
    ok = blockchain_worker:add_block(Block, self()),
    ChainDir = blockchain:dir(blockchain_worker:blockchain()),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain_block:hash_block(element(2, blockchain:get_block(head, ChainDir)))),
    ?assertEqual({ok, Block}, blockchain:get_block(head, ChainDir)),
    ?assertEqual(2, blockchain_worker:height()),

    % Check that the Payer balance has been reduced by 2500
    NewEntry0 = blockchain_ledger:find_entry(Payer, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(Balance - 2500, blockchain_ledger:balance(NewEntry0)),

    % Check that the HLTC address exists and has the correct balance, hashlock and timelock
    % NewHTLC0 = blockchain_ledger:find_htlc(HTLCAddress, blockchain_worker:ledger()),
    NewHTLC0 = blockchain_ledger:find_htlc(HTLCAddress, blockchain_ledger:htlcs(blockchain_worker:ledger())),
    ?assertEqual(2500, blockchain_ledger:balance(NewHTLC0)),
    ?assertEqual(<<"3281d585522bc6772a527f5071b149363436415ebc21cc77a8a9167abf29fb72">>, blockchain_ledger:hashlock(NewHTLC0)),
    ?assertEqual(3, blockchain_ledger:timelock(NewHTLC0)),

    % Mine another couple of blocks
    Block2 = test_utils:create_block(ConsensusMembers, []),
    ok = blockchain_worker:add_block(Block2, self()),
    Block3 = test_utils:create_block(ConsensusMembers, []),
    ok = blockchain_worker:add_block(Block3, self()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    % Check we are at height 4
    ?assertEqual(blockchain_block:hash_block(Block3), blockchain_block:hash_block(element(2, blockchain:get_block(head, ChainDir)))),
    ?assertEqual({ok, Block3}, blockchain:get_block(head, ChainDir)),
    ?assertEqual(4, blockchain_worker:height()),

    % Try and redeem
    RedeemTx = blockchain_txn_redeem_htlc:new(Payer, HTLCAddress, <<"sharkfed">>),
    SignedRedeemTx = blockchain_txn_redeem_htlc:sign(RedeemTx, SigFun),
    Block4 = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    ok = blockchain_worker:add_block(Block4, self()),

    % Check that the Payer now owns 5000 again
    NewEntry1 = blockchain_ledger:find_entry(Payer, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(5000, blockchain_ledger:balance(NewEntry1)),

    % Make sure blockchain saved on file =  in memory
    Chain = blockchain_worker:blockchain(),
    ok = test_utils:compare_chains(Chain, blockchain:load(BaseDir)),

    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end),

    ok.

poc_request(_Config) ->
    BaseDir = "data/test_SUITE/poc_request",
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Owner = libp2p_crypto:pubkey_to_address(PubKey),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
    Entries = blockchain_ledger:entries(Ledger),

    _ = maps:map(fun(_K, Entry) ->
                         Balance = blockchain_ledger:balance(Entry),
                         0, blockchain_ledger:payment_nonce(Entry)
                 end, Entries),

    % Create a Gateway
    {GatewayPrivKey, GatewayPubKey} = libp2p_crypto:generate_keys(),
    Gateway = libp2p_crypto:pubkey_to_address(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),

    % Add a Gateway
    AddGatewayTx = blockchain_txn_add_gateway:new(Owner, Gateway),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway:sign(AddGatewayTx, OwnerSigFun),
    SignedGatewayAddGatewayTx = blockchain_txn_add_gateway:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),
    Block = test_utils:create_block(ConsensusMembers, [SignedGatewayAddGatewayTx]),
    ok = blockchain_worker:add_block(Block, self()),
    ChainDir = blockchain:dir(blockchain_worker:blockchain()),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain_block:hash_block(element(2, blockchain:get_block(head, ChainDir)))),
    ?assertEqual({ok, Block}, blockchain:get_block(head, ChainDir)),
    ?assertEqual(2, blockchain_worker:height()),

    % Check that the Gateway is there
    GwInfo = blockchain_ledger:find_gateway_info(Gateway, blockchain_worker:ledger()),
    ?assertEqual(Owner, blockchain_ledger:gateway_owner(GwInfo)),

    % Assert the Gateways location
    AssertLocationRequestTx = blockchain_txn_assert_location:new(Gateway, Owner, 123456, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTx = blockchain_txn_assert_location:sign(PartialAssertLocationTxn, OwnerSigFun),

    Block2 = test_utils:create_block(ConsensusMembers, [SignedAssertLocationTx]),
    ok = blockchain_worker:add_block(Block2, self()),
    timer:sleep(500),

    ?assertEqual(blockchain_block:hash_block(Block2), blockchain_block:hash_block(element(2, blockchain:get_block(head, ChainDir)))),
    ?assertEqual({ok, Block2}, blockchain:get_block(head, ChainDir)),
    ?assertEqual(3, blockchain_worker:height()),

    % Create the PoC challenge request txn
    Tx = blockchain_txn_poc_request:new(Gateway),
    SignedTx = blockchain_txn_poc_request:sign(Tx, GatewaySigFun),
    Block3 = test_utils:create_block(ConsensusMembers, [SignedTx]),
    ok = blockchain_worker:add_block(Block3, self()),
    timer:sleep(500),

    ?assertEqual(blockchain_block:hash_block(Block3), blockchain_block:hash_block(element(2, blockchain:get_block(head, ChainDir)))),
    ?assertEqual({ok, Block3}, blockchain:get_block(head, ChainDir)),
    ?assertEqual(4, blockchain_worker:height()),

    % Check that the last_poc_challenge block height got recorded in GwInfo
    GwInfo2 = blockchain_ledger:find_gateway_info(Gateway, blockchain_worker:ledger()),
    ?assertEqual(3, blockchain_ledger:last_poc_challenge(GwInfo2)),

    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end),

    ok.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
