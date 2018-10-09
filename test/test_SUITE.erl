-module(test_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0
]).

-export([
    basic/1
    ,htlc/1
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
    [basic, htlc].

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
    Tx = blockchain_txn_payment:new(Payer, Recipient, 2500, 1),
    SignedTx = blockchain_txn_payment:sign(Tx, PayerPrivKey),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    ok = blockchain_worker:add_block(Block, self()),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain_worker:head_hash()),
    ?assertEqual(Block, blockchain_worker:head_block()),
    ?assertEqual(2, blockchain_worker:height()),

    NewEntry0 = blockchain_ledger:find_entry(Recipient, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(Balance + 2500, blockchain_ledger:balance(NewEntry0)),

    NewEntry1 = blockchain_ledger:find_entry(Payer, blockchain_ledger:entries(blockchain_worker:ledger())),
    ?assertEqual(Balance - 2500, blockchain_ledger:balance(NewEntry1)),

    % Make sure blockchain saved on file =  in memory
    Chain = blockchain_worker:blockchain(),
    ok = test_utils:compare_chains(Chain, blockchain:load(BaseDir)),

    %% Test find_next block
    ?assertEqual({ok, Block}, blockchain_block:find_next(blockchain:genesis_hash(Chain), maps:values(blockchain:blocks(Chain)))),

    % Restart blockchain and make sure nothing has changed
    true = erlang:exit(Sup, normal),
    ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end),

    {ok, Sup1} = blockchain_sup:start_link(Opts),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    ok = test_utils:compare_chains(Chain, blockchain_worker:blockchain()),
    true = erlang:exit(Sup1, normal),
    ok.

htlc(_Config) ->
    BaseDir = "data/test_SUITE/htlc",
    Balance = 5000,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
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
    SignedCreateTx = blockchain_txn_create_htlc:sign(CreateTx, PayerPrivKey),
    Block = test_utils:create_block(ConsensusMembers, [SignedCreateTx]),
    ok = blockchain_worker:add_block(Block, self()),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain_worker:head_hash()),
    ?assertEqual(Block, blockchain_worker:head_block()),
    ?assertEqual(2, blockchain_worker:height()),

    % Check that the Payer balance has been reduced by 2500
    NewEntry0 = blockchain_ledger:find_entry(Payer, blockchain_worker:ledger()),
    ?assertEqual(Balance - 2500, blockchain_ledger:balance(NewEntry0)),

    % Check that the HLTC address exists and has the correct balance, hashlock and timelock
    NewHTLC0 = blockchain_ledger:find_htlc(HTLCAddress, blockchain_worker:ledger()),
    ?assertEqual(2500, blockchain_ledger:balance(NewHTLC0)),
    ?assertEqual(<<"3281d585522bc6772a527f5071b149363436415ebc21cc77a8a9167abf29fb72">>, blockchain_ledger:hashlock(NewHTLC0)),
    ?assertEqual(100, blockchain_ledger:timelock(NewHTLC0)),

    % Create a Payee
    {PayeePrivKey, Payee} = test_utils:generate_keys(1), %%TODO: FIX

    % Try and redeem
    RedeemTx = blockchain_txn_redeem_htlc:new(Payee, HTLCAddress, <<"sharkfed">>, 1),
    SignedRedeemTx = blockchain_txn_redeem_htlc:sign(RedeemTx, PayeePrivKey),
    Block2 = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    ok = blockchain_worker:add_block(Block2, self()),

    % Check that the second block with the Redeem TX was mined properly
    ?assertEqual(blockchain_block:hash_block(Block2), blockchain_worker:head_hash()),
    ?assertEqual(Block2, blockchain_worker:head_block()),
    ?assertEqual(3, blockchain_worker:height()),
    
    % Check that the Payee now owns 2500 HLMz
    NewEntry1 = blockchain_ledger:find_entry(Payee, blockchain_worker:ledger()),
    ?assertEqual(2500, blockchain_ledger:balance(NewEntry1)),

    % Make sure blockchain saved on file =  in memory
    Chain = blockchain_worker:blockchain(),
    ok = test_utils:compare_chains(Chain, blockchain:load(BaseDir)),

    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
