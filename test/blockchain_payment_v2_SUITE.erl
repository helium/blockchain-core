-module(blockchain_payment_v2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
         single_payee_test/1,
         same_payees_test/1,
         different_payees_test/1,
         empty_payees_test/1,
         zero_payment_test/1,
         negative_payment_test/1,
         self_payment_test/1
        ]).

all() ->
    [
     single_payee_test,
     same_payees_test,
     different_payees_test,
     empty_payees_test,
     zero_payment_test,
     negative_payment_test,
     self_payment_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars = #{?max_payments => 20},

    {ok, GenesisMembers, ConsensusMembers, Keys} = test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),

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
     {balance, Balance},
     {sup, Sup},
     {pubkey, PubKey},
     {privkey, PrivKey},
     {opts, Opts},
     {chain, Chain},
     {swarm, Swarm},
     {n, N},
     {consensus_members, ConsensusMembers},
     {genesis_members, GenesisMembers},
     Keys
     | Config0
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_, Config) ->
    Sup = ?config(sup, Config),
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

single_payee_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),
    Swarm = ?config(swarm, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 2500,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),


    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + Amount, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - Amount, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

same_payees_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),
    Swarm = ?config(swarm, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee TWICE
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 1000,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),
    Payment2 = blockchain_payment_v2:new(Recipient, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + 2*Amount, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2*Amount, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

different_payees_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),
    Swarm = ?config(swarm, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}, {Recipient2, _} |_] = ConsensusMembers,

    %% Create a payment to a payee1
    Recipient1 = blockchain_swarm:pubkey_bin(),
    Amount1 = 1000,
    Payment1 = blockchain_payment_v2:new(Recipient1, Amount1),

    %% Create a payment to the other payee2
    Amount2 = 2000,
    Payment2 = blockchain_payment_v2:new(Recipient2, Amount2),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, self()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Recipient1, Ledger),
    ?assertEqual(Balance + Amount1, blockchain_ledger_entry_v1:balance(NewEntry1)),

    {ok, NewEntry2} = blockchain_ledger_v1:find_entry(Recipient2, Ledger),
    ?assertEqual(Balance + Amount2, blockchain_ledger_entry_v1:balance(NewEntry2)),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - Amount1 - Amount2, blockchain_ledger_entry_v1:balance(NewEntry0)),
    ok.

empty_payees_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    Tx = blockchain_txn_payment_v2:new(Payer, [], 1, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),
    ?assertEqual({error, zero_payees}, blockchain_txn_payment_v2:is_valid(SignedTx, Chain)),

    ok.

zero_payment_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    BaseDir = ?config(base_dir, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {_Payer, {_, _PayerPrivKey, _}}, {Recipient2, _} |_] = ConsensusMembers,

    %% Create a payment to a payee1
    Recipient1 = blockchain_swarm:pubkey_bin(),
    Amount1 = 1000,
    _Payment1 = blockchain_payment_v2:new(Recipient1, Amount1),

    %% Create a payment to the other payee2. This should blow up.
    Amount2 = 0,

    ?assertException(error, function_clause, blockchain_payment_v2:new(Recipient2, Amount2)),
    ok.

negative_payment_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    BaseDir = ?config(base_dir, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {_Payer, {_, _PayerPrivKey, _}}, {Recipient2, _} |_] = ConsensusMembers,

    %% Create a payment to a payee1
    Recipient1 = blockchain_swarm:pubkey_bin(),
    Amount1 = 1000,
    _Payment1 = blockchain_payment_v2:new(Recipient1, Amount1),

    %% Create a payment to the other payee2. This should also blow up.
    Amount2 = -100,

    ?assertException(error, function_clause, blockchain_payment_v2:new(Recipient2, Amount2)),
    ok.

self_payment_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee TWICE
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 1000,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),
    Payment2 = blockchain_payment_v2:new(Payer, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),
    ?assertEqual({error, self_payment}, blockchain_txn_payment_v2:is_valid(SignedTx, Chain)),

    ok.
