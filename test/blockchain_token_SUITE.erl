-module(blockchain_token_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    multi_token_coinbase_test/1,
    multi_token_payment_test/1,
    multi_token_payment_failure_test/1,
    entry_migration_test/1,
    entry_migration_with_payment_test/1
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
        multi_token_coinbase_test,
        multi_token_payment_test,
        multi_token_payment_failure_test,
        entry_migration_test,
        entry_migration_with_payment_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),

    HNTBal = 50000,
    HSTBal = 10000,
    MobileBal = 1000,
    IOTBal = 100,

    Config1 = [
        {hnt_bal, HNTBal},
        {hst_bal, HSTBal},
        {mobile_bal, MobileBal},
        {iot_bal, IOTBal}
        | Config0
    ],

    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config1)),

    ExtraVars = extra_vars(TestCase),
    TokenAllocations = token_allocations(TestCase, Config1),

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain_with_opts(
            #{
                balance =>
                    HNTBal,
                sec_balance =>
                    HSTBal,
                keys =>
                    {PrivKey, PubKey},
                in_consensus =>
                    false,
                have_init_dc =>
                    true,
                extra_vars =>
                    ExtraVars,
                token_allocations =>
                    TokenAllocations
            }
        ),

    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    Swarm = blockchain_swarm:tid(),
    N = length(ConsensusMembers),

    {EntryMod, _} = blockchain_ledger_v1:versioned_entry_mod_and_entries_cf(Ledger),

    [
        {hnt_bal, HNTBal},
        {hst_bal, HSTBal},
        {mobile_bal, MobileBal},
        {iot_bal, IOTBal},
        {entry_mod, EntryMod},
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
        | Config1
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_, Config) ->
    Sup = ?config(sup, Config),
    meck:unload(),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    test_utils:cleanup_tmp_dir(?config(base_dir, Config)),
    {comment, done}.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

multi_token_coinbase_test(Config) ->
    Chain = ?config(chain, Config),
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    MobileBal = ?config(mobile_bal, Config),
    IOTBal = ?config(iot_bal, Config),
    EntryMod = ?config(entry_mod, Config),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),

    Entries = blockchain_ledger_v1:entries(Ledger),

    _ = lists:foreach(
        fun(Entry) ->
            HNTBal = EntryMod:balance(Entry),
            0 = EntryMod:nonce(Entry),
            HSTBal = EntryMod:balance(Entry, hst),
            0 = EntryMod:nonce(Entry),
            MobileBal = EntryMod:balance(Entry, mobile),
            0 = EntryMod:nonce(Entry),
            IOTBal = EntryMod:balance(Entry, iot),
            0 = EntryMod:nonce(Entry)
        end,
        maps:values(Entries)
    ),
    ok.

multi_token_payment_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    MobileBal = ?config(mobile_bal, Config),
    IOTBal = ?config(iot_bal, Config),
    EntryMod = ?config(entry_mod, Config),
    Ledger = blockchain:ledger(Chain),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}} | _] = ConsensusMembers,

    %% Generate two random recipients
    [{Recipient1, _}, {Recipient2, _}] = test_utils:generate_keys(2),

    HNTAmt1 = 20000,
    HSTAmt1 = 2000,
    MobileAmt1 = 200,
    IOTAmt1 = 20,
    HNTAmt2 = 10000,
    HSTAmt2 = 1000,
    MobileAmt2 = 100,
    IOTAmt2 = 10,

    P1 = blockchain_payment_v2:new(Recipient1, HNTAmt1),
    P2 = blockchain_payment_v2:new(Recipient1, HSTAmt1, hst),
    P3 = blockchain_payment_v2:new(Recipient1, MobileAmt1, mobile),
    P4 = blockchain_payment_v2:new(Recipient1, IOTAmt1, iot),

    P5 = blockchain_payment_v2:new(Recipient2, HNTAmt2),
    P6 = blockchain_payment_v2:new(Recipient2, HSTAmt2, hst),
    P7 = blockchain_payment_v2:new(Recipient2, MobileAmt2, mobile),
    P8 = blockchain_payment_v2:new(Recipient2, IOTAmt2, iot),

    Payments = [P1, P2, P3, P4, P5, P6, P7, P8],

    Tx = blockchain_txn_payment_v2:new(Payer, Payments, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    {ok, RecipientEntry1} = blockchain_ledger_v1:find_entry(Recipient1, Ledger),
    {ok, RecipientEntry2} = blockchain_ledger_v1:find_entry(Recipient2, Ledger),

    ?assertEqual(HNTAmt1, EntryMod:balance(RecipientEntry1)),
    ?assertEqual(HSTAmt1, EntryMod:balance(RecipientEntry1, hst)),
    ?assertEqual(MobileAmt1, EntryMod:balance(RecipientEntry1, mobile)),
    ?assertEqual(IOTAmt1, EntryMod:balance(RecipientEntry1, iot)),

    ?assertEqual(HNTAmt2, EntryMod:balance(RecipientEntry2)),
    ?assertEqual(HSTAmt2, EntryMod:balance(RecipientEntry2, hst)),
    ?assertEqual(MobileAmt2, EntryMod:balance(RecipientEntry2, mobile)),
    ?assertEqual(IOTAmt2, EntryMod:balance(RecipientEntry2, iot)),

    {ok, PayerEntry} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(1, EntryMod:nonce(PayerEntry)),
    ?assertEqual(HNTBal - (HNTAmt1 + HNTAmt2), EntryMod:balance(PayerEntry)),
    ?assertEqual(HSTBal - (HSTAmt1 + HSTAmt2), EntryMod:balance(PayerEntry, hst)),
    ?assertEqual(MobileBal - (MobileAmt1 + MobileAmt2), EntryMod:balance(PayerEntry, mobile)),
    ?assertEqual(IOTBal - (IOTAmt1 + IOTAmt2), EntryMod:balance(PayerEntry, iot)),

    %% Do another payment

    [{Recipient3, _}] = test_utils:generate_keys(1),

    HNTAmt3 = 2000,
    HSTAmt3 = 200,
    MobileAmt3 = 20,
    IOTAmt3 = 2,

    P9 = blockchain_payment_v2:new(Recipient3, HNTAmt3, hnt),
    P10 = blockchain_payment_v2:new(Recipient3, HSTAmt3, hst),
    P11 = blockchain_payment_v2:new(Recipient3, MobileAmt3, mobile),
    P12 = blockchain_payment_v2:new(Recipient3, IOTAmt3, iot),

    Payments3 = [P9, P10, P11, P12],

    Tx3 = blockchain_txn_payment_v2:new(Payer, Payments3, 2),
    SignedTx3 = blockchain_txn_payment_v2:sign(Tx3, SigFun),

    {ok, Block3} = test_utils:create_block(ConsensusMembers, [SignedTx3]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block3)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block3}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    ?assertEqual({ok, Block3}, blockchain:get_block(3, Chain)),

    {ok, RecipientEntry3} = blockchain_ledger_v1:find_entry(Recipient3, Ledger),

    ?assertEqual(HNTAmt3, EntryMod:balance(RecipientEntry3)),
    ?assertEqual(HSTAmt3, EntryMod:balance(RecipientEntry3, hst)),
    ?assertEqual(MobileAmt3, EntryMod:balance(RecipientEntry3, mobile)),
    ?assertEqual(IOTAmt3, EntryMod:balance(RecipientEntry3, iot)),

    {ok, PayerEntry3} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(2, EntryMod:nonce(PayerEntry3)),
    ?assertEqual(
        HNTBal - (HNTAmt1 + HNTAmt2 + HNTAmt3),
        EntryMod:balance(PayerEntry3)
    ),
    ?assertEqual(
        HSTBal - (HSTAmt1 + HSTAmt2 + HSTAmt3),
        EntryMod:balance(PayerEntry3, hst)
    ),
    ?assertEqual(
        MobileBal - (MobileAmt1 + MobileAmt2 + MobileAmt3),
        EntryMod:balance(PayerEntry3, mobile)
    ),
    ?assertEqual(
        IOTBal - (IOTAmt1 + IOTAmt2 + IOTAmt3),
        EntryMod:balance(PayerEntry3, iot)
    ),

    ok.

multi_token_payment_failure_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    MobileBal = ?config(mobile_bal, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}} | _] = ConsensusMembers,

    %% Generate two random recipients
    [{Recipient1, _}] = test_utils:generate_keys(1),

    MobileAmt = 200 * 10000,

    P1 = blockchain_payment_v2:new(Recipient1, MobileAmt, mobile),

    Payments = [P1],

    Tx = blockchain_txn_payment_v2:new(Payer, Payments, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {error, InvalidReason} = blockchain_txn:is_valid(SignedTx, Chain),
    ct:pal("InvalidReason: ~p", [InvalidReason]),

    ?assertEqual(
        {invalid_transaction,
            {amount_check,
                {error, {amount_check_v2_failed, [{mobile, false, MobileBal, MobileAmt}]}}},
            {memo_check, ok}, {token_check, ok}},
        InvalidReason
    ),

    ok.

entry_migration_test(Config) ->
    Chain = ?config(chain, Config),
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    EntryMod = ?config(entry_mod, Config),
    {Priv, _} = ?config(master_key, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    ct:pal("BEFORE Entries: ~p", [Entries]),

    Securities = blockchain_ledger_v1:securities(Ledger),
    ct:pal("BEFORE Securities: ~p", [Securities]),

    %% Ensure we are on v1 style entries
    ?assertEqual(blockchain_ledger_entry_v1, EntryMod),

    ok = lists:foreach(
        fun(Entry) ->
            HNTBal = EntryMod:balance(Entry),
            0 = EntryMod:nonce(Entry)
        end,
        maps:values(Entries)
    ),

    ok = lists:foreach(
        fun(Entry) ->
            HSTBal = blockchain_ledger_security_entry_v1:balance(Entry),
            0 = blockchain_ledger_security_entry_v1:nonce(Entry)
        end,
        maps:values(Securities)
    ),

    %% Send var txn with ledger_entry_version = 2 and token_version = 2
    %% to trigger ledger entry migration

    Vars = #{ledger_entry_version => 2, token_version => 2},
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),

    {ok, Block2} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block2}, blockchain:get_block(2, Chain)),

    %% At this point we should switch to v2 style entries

    AfterEntries = blockchain_ledger_v1:entries(Ledger),
    ct:pal("AFTER Entries: ~p", [AfterEntries]),

    %% Ensure we are on v2 style entries after the var
    {NewEntryMod, _} = blockchain_ledger_v1:versioned_entry_mod_and_entries_cf(Ledger),
    ?assertEqual(blockchain_ledger_entry_v2, NewEntryMod),

    ok = lists:foreach(
        fun(Entry) ->
            HNTBal = NewEntryMod:balance(Entry),
            0 = NewEntryMod:nonce(Entry),
            HSTBal = NewEntryMod:balance(Entry, hst)
        end,
        maps:values(AfterEntries)
    ),

    ok.

entry_migration_with_payment_test(Config) ->
    Chain = ?config(chain, Config),
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    {Priv, _} = ?config(master_key, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    Ledger = blockchain:ledger(Chain),

    BeforeEntries = blockchain_ledger_v1:entries(Ledger),
    ct:pal("BEFORE Entries: ~p", [BeforeEntries]),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}} | _] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 2500,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    %% Create a security exchange transaction

    SecAmount = 100,
    STx = blockchain_txn_security_exchange_v1:new(
        Payer,
        Recipient,
        SecAmount,
        1
    ),
    SignedSTx = blockchain_txn_security_exchange_v1:sign(STx, SigFun),
    ct:pal("~s", [blockchain_txn:print(SignedSTx)]),

    {ok, Block3} = test_utils:create_block(ConsensusMembers, [SignedSTx]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block3)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block3}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),
    ?assertEqual({ok, Block3}, blockchain:get_block(3, Chain)),

    %% Send var txn with ledger_entry_version = 2 and token_version = 2
    %% to trigger ledger entry migration

    Vars = #{ledger_entry_version => 2, token_version => 2},
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),

    {ok, Block4} = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    _ = blockchain_gossip_handler:add_block(Block4, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block4)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block4}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 4}, blockchain:height(Chain)),
    ?assertEqual({ok, Block4}, blockchain:get_block(4, Chain)),

    %% At this point we should switch to v2 style entries
    AfterEntries = blockchain_ledger_v1:entries(Ledger),
    ct:pal("AFTER Entries: ~p", [AfterEntries]),

    %% Ensure we are on v2 style entries after the var
    {NewEntryMod, _} = blockchain_ledger_v1:versioned_entry_mod_and_entries_cf(Ledger),
    ?assertEqual(blockchain_ledger_entry_v2, NewEntryMod),

    {ok, RecipientEntry} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ct:pal("RecipientEntry: ~p", [RecipientEntry]),
    ?assertEqual(0, NewEntryMod:nonce(RecipientEntry)),
    ?assertEqual(Amount, NewEntryMod:balance(RecipientEntry)),
    ?assertEqual(SecAmount, NewEntryMod:balance(RecipientEntry, hst)),

    {ok, PayerEntry} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ct:pal("RecipientEntry: ~p", [RecipientEntry]),

    %% +1 from hnt payment +1 from security payment
    ?assertEqual(2, NewEntryMod:nonce(PayerEntry)),
    ?assertEqual(HNTBal - Amount, NewEntryMod:balance(PayerEntry)),
    ?assertEqual(HSTBal - SecAmount, NewEntryMod:balance(PayerEntry, hst)),

    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

extra_vars(entry_migration_with_payment_test) ->
    #{?max_payments => 20, ?allow_zero_amount => false};
extra_vars(entry_migration_test) ->
    #{?max_payments => 20, ?allow_zero_amount => false};
extra_vars(_) ->
    #{?token_version => 2, ?max_payments => 20, ?allow_zero_amount => false}.

token_allocations(entry_migration_with_payment_test, _Config) ->
    undefined;
token_allocations(entry_migration_test, _Config) ->
    undefined;
token_allocations(_, Config) ->
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    MobileBal = ?config(mobile_bal, Config),
    IOTBal = ?config(iot_bal, Config),
    #{hnt => HNTBal, hst => HSTBal, mobile => MobileBal, iot => IOTBal}.
