-module(blockchain_implicit_burn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
         basic_test/1
        ]).

all() ->
    [
     basic_test
    ].

-define(MAX_PAYMENTS, 20).

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars = extra_vars(TestCase),

    ok = application:set_env(blockchain, store_implicit_burns, true),

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),

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

basic_test(Config) ->
    BaseDir = ?config(base_dir, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    BaseDir = ?config(base_dir, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 2500,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),

    Tx0 = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    Fee = blockchain_txn_payment_v2:calculate_fee(Tx0, Chain),
    Tx1 = blockchain_txn_payment_v2:fee(Tx0, Fee),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx1, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),

    TxHash = blockchain_txn:hash(SignedTx),

    %% Check that there is an implicit burn
    {ok, {implicit_burn, _, _}} = blockchain:get_implicit_burn(TxHash, Chain),
    ct:pal("implicit burn: ~p", [blockchain:get_implicit_burn(TxHash, Chain)]),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + Amount, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - Amount, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

%%--------------------------------------------------------------------
%% HELPERS
%%--------------------------------------------------------------------
extra_vars(_) ->
    #{?txn_fees => true, ?max_payments => ?MAX_PAYMENTS, ?allow_zero_amount => false}.