-module(blockchain_token_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    multi_token_coinbase_test/1,
    multi_token_payment_test/1
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
        multi_token_payment_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),

    HNTBal = 50000,
    HSTBal = 10000,
    HGTBal = 1000,
    HLTBal = 100,

    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars = extra_vars(TestCase),

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain_with_opts(
            #{
                balance =>
                    HNTBal,
                keys =>
                    {PrivKey, PubKey},
                in_consensus =>
                    false,
                have_init_dc =>
                    true,
                extra_vars =>
                    ExtraVars,
                token_allocations =>
                    #{hnt => HNTBal, hst => HSTBal, hgt => HGTBal, hlt => HLTBal}
            }
        ),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:tid(),
    N = length(ConsensusMembers),

    [
        {hnt_bal, HNTBal},
        {hst_bal, HSTBal},
        {hgt_bal, HGTBal},
        {hlt_bal, HLTBal},
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
    HGTBal = ?config(hgt_bal, Config),
    HLTBal = ?config(hlt_bal, Config),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries_v2(Ledger),
    _ = lists:foreach(
        fun(Entry) ->
            HNTBal = blockchain_ledger_entry_v2:balance(Entry, hnt),
            0 = blockchain_ledger_entry_v2:nonce(Entry),
            HSTBal = blockchain_ledger_entry_v2:balance(Entry, hst),
            0 = blockchain_ledger_entry_v2:nonce(Entry),
            HGTBal = blockchain_ledger_entry_v2:balance(Entry, hgt),
            0 = blockchain_ledger_entry_v2:nonce(Entry),
            HLTBal = blockchain_ledger_entry_v2:balance(Entry, hlt),
            0 = blockchain_ledger_entry_v2:nonce(Entry)
        end,
        maps:values(Entries)
    ),
    ok.

multi_token_payment_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    HGTBal = ?config(hgt_bal, Config),
    HLTBal = ?config(hlt_bal, Config),
    Ledger = blockchain:ledger(Chain),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}} | _] = ConsensusMembers,

    %% Generate two random recipients
    [{Recipient1, _}, {Recipient2, _}] = test_utils:generate_keys(2),

    HNTAmt1 = 20000,
    HSTAmt1 = 2000,
    HGTAmt1 = 200,
    HLTAmt1 = 20,
    HNTAmt2 = 10000,
    HSTAmt2 = 1000,
    HGTAmt2 = 100,
    HLTAmt2 = 10,

    P1 = blockchain_payment_v2:new(Recipient1, HNTAmt1, hnt),
    P2 = blockchain_payment_v2:new(Recipient1, HSTAmt1, hst),
    P3 = blockchain_payment_v2:new(Recipient1, HGTAmt1, hgt),
    P4 = blockchain_payment_v2:new(Recipient1, HLTAmt1, hlt),

    P5 = blockchain_payment_v2:new(Recipient2, HNTAmt2, hnt),
    P6 = blockchain_payment_v2:new(Recipient2, HSTAmt2, hst),
    P7 = blockchain_payment_v2:new(Recipient2, HGTAmt2, hgt),
    P8 = blockchain_payment_v2:new(Recipient2, HLTAmt2, hlt),

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

    ?assertEqual(HNTAmt1, blockchain_ledger_entry_v2:balance(RecipientEntry1, hnt)),
    ?assertEqual(HSTAmt1, blockchain_ledger_entry_v2:balance(RecipientEntry1, hst)),
    ?assertEqual(HGTAmt1, blockchain_ledger_entry_v2:balance(RecipientEntry1, hgt)),
    ?assertEqual(HLTAmt1, blockchain_ledger_entry_v2:balance(RecipientEntry1, hlt)),

    ?assertEqual(HNTAmt2, blockchain_ledger_entry_v2:balance(RecipientEntry2, hnt)),
    ?assertEqual(HSTAmt2, blockchain_ledger_entry_v2:balance(RecipientEntry2, hst)),
    ?assertEqual(HGTAmt2, blockchain_ledger_entry_v2:balance(RecipientEntry2, hgt)),
    ?assertEqual(HLTAmt2, blockchain_ledger_entry_v2:balance(RecipientEntry2, hlt)),

    {ok, PayerEntry} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(1, blockchain_ledger_entry_v2:nonce(PayerEntry)),
    ?assertEqual(HNTBal - (HNTAmt1 + HNTAmt2), blockchain_ledger_entry_v2:balance(PayerEntry, hnt)),
    ?assertEqual(HSTBal - (HSTAmt1 + HSTAmt2), blockchain_ledger_entry_v2:balance(PayerEntry, hst)),
    ?assertEqual(HGTBal - (HGTAmt1 + HGTAmt2), blockchain_ledger_entry_v2:balance(PayerEntry, hgt)),
    ?assertEqual(HLTBal - (HLTAmt1 + HLTAmt2), blockchain_ledger_entry_v2:balance(PayerEntry, hlt)),

    %% Do another payment

    [{Recipient3, _}] = test_utils:generate_keys(1),

    HNTAmt3 = 2000,
    HSTAmt3 = 200,
    HGTAmt3 = 20,
    HLTAmt3 = 2,

    P9 = blockchain_payment_v2:new(Recipient3, HNTAmt3, hnt),
    P10 = blockchain_payment_v2:new(Recipient3, HSTAmt3, hst),
    P11 = blockchain_payment_v2:new(Recipient3, HGTAmt3, hgt),
    P12 = blockchain_payment_v2:new(Recipient3, HLTAmt3, hlt),

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

    ?assertEqual(HNTAmt3, blockchain_ledger_entry_v2:balance(RecipientEntry3, hnt)),
    ?assertEqual(HSTAmt3, blockchain_ledger_entry_v2:balance(RecipientEntry3, hst)),
    ?assertEqual(HGTAmt3, blockchain_ledger_entry_v2:balance(RecipientEntry3, hgt)),
    ?assertEqual(HLTAmt3, blockchain_ledger_entry_v2:balance(RecipientEntry3, hlt)),

    {ok, PayerEntry3} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(2, blockchain_ledger_entry_v2:nonce(PayerEntry3)),
    ?assertEqual(
        HNTBal - (HNTAmt1 + HNTAmt2 + HNTAmt3),
        blockchain_ledger_entry_v2:balance(PayerEntry3, hnt)
    ),
    ?assertEqual(
        HSTBal - (HSTAmt1 + HSTAmt2 + HSTAmt3),
        blockchain_ledger_entry_v2:balance(PayerEntry3, hst)
    ),
    ?assertEqual(
        HGTBal - (HGTAmt1 + HGTAmt2 + HGTAmt3),
        blockchain_ledger_entry_v2:balance(PayerEntry3, hgt)
    ),
    ?assertEqual(
        HLTBal - (HLTAmt1 + HLTAmt2 + HLTAmt3),
        blockchain_ledger_entry_v2:balance(PayerEntry3, hlt)
    ),

    ok.

extra_vars(_) ->
    #{?protocol_version => 2, ?max_payments => 20, ?allow_zero_amount => false}.
