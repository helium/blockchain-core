-module(blockchain_htlc_receipt_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_txn_fees.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
         enable_htlc_receipt_test/1,
         disabled_htlc_receipt_test/1
        ]).

all() ->
    [
     enable_htlc_receipt_test,
     disabled_htlc_receipt_test
    ].

-define(MAX_PAYMENTS, 20).

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000 * ?BONES_PER_HNT,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars = extra_vars(TestCase),

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

    meck:new(blockchain_ledger_v1, [passthrough]),

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
    meck:unload(blockchain_ledger_v1),
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

enable_htlc_receipt_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    meck:expect(
        blockchain_ledger_v1,
        current_oracle_price,
        fun(_) ->
                {ok, 15 * ?BONES_PER_HNT}
        end
    ),

    %% Set env to store htlc receipts %%
    ok = application:set_env(blockchain, store_htlc_receipts, true),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    % Create a Payee
    #{public := PayeePubKey, secret := PayeePrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PayeePubKey),
    Amount = 250 * ?BONES_PER_HNT,
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(33),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"wenmoon">>),
    HTLC0 = blockchain_txn_create_htlc_v1:new(Payer, Payee, HTLCAddress, Hashlock, 3, Amount, 1),
    Fee0 = blockchain_txn_create_htlc_v1:calculate_fee(HTLC0, Chain),
    HTLC1 = blockchain_txn_create_htlc_v1:fee(HTLC0, Fee0),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedHTLC1 = blockchain_txn_create_htlc_v1:sign(HTLC1, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedHTLC1)]),

    % send some money to the payee so they have enough to pay the fee for redeeming
    Tx0 = blockchain_txn_payment_v1:new(Payer, Payee, 100 * ?BONES_PER_HNT, 2),
    Fee = blockchain_txn_payment_v1:calculate_fee(Tx0, Chain),
    Tx1 = blockchain_txn_payment_v1:fee(Tx0, Fee),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx1, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    %% check both txns are valid, in context
    ?assertMatch({_, []}, blockchain_txn:validate([SignedHTLC1, SignedTx], Chain)),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedHTLC1, SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

    % Try and redeem
    RedeemSigFun = libp2p_crypto:mk_sig_fun(PayeePrivKey),
    RedeemTx0 = blockchain_txn_redeem_htlc_v1:new(Payee, HTLCAddress, <<"wenmoon">>),
    RedeemFee = blockchain_txn_redeem_htlc_v1:calculate_fee(RedeemTx0, Chain),
    RedeemTx1 = blockchain_txn_redeem_htlc_v1:fee(RedeemTx0, RedeemFee),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx1, RedeemSigFun),
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:tid()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    %% Check that there is a htlc receipt
    {ok, HTLCReceipt} = blockchain:get_htlc_receipt(HTLCAddress, Chain),

    %% Check that the receipt was stored correctly
    ?assertEqual(Payer, blockchain_htlc_receipt:payer(HTLCReceipt)),
    ?assertEqual(Payee, blockchain_htlc_receipt:payee(HTLCReceipt)),
    ?assertEqual(HTLCAddress, blockchain_htlc_receipt:address(HTLCReceipt)),
    ?assertEqual(Amount, blockchain_htlc_receipt:balance(HTLCReceipt)),
    ?assertEqual(Hashlock, blockchain_htlc_receipt:hashlock(HTLCReceipt)),
    ?assertEqual(3, blockchain_htlc_receipt:timelock(HTLCReceipt)),
    ?assertEqual(2, blockchain_htlc_receipt:redeemed_at(HTLCReceipt)),

    ct:pal("HTLC Receipt: ~p", [HTLCReceipt]),

    ok.

disabled_htlc_receipt_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    meck:expect(
        blockchain_ledger_v1,
        current_oracle_price,
        fun(_) ->
                {ok, 15 * ?BONES_PER_HNT}
        end
    ),

    %% Set env to NOT store htlc receipts %%
    ok = application:set_env(blockchain, store_htlc_receipts, false),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    % Create a Payee
    #{public := PayeePubKey, secret := PayeePrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PayeePubKey),
    Amount = 250 * ?BONES_PER_HNT,
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(33),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"wenmoon">>),
    HTLC0 = blockchain_txn_create_htlc_v1:new(Payer, Payee, HTLCAddress, Hashlock, 3, Amount, 1),
    Fee0 = blockchain_txn_create_htlc_v1:calculate_fee(HTLC0, Chain),
    HTLC1 = blockchain_txn_create_htlc_v1:fee(HTLC0, Fee0),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedHTLC1 = blockchain_txn_create_htlc_v1:sign(HTLC1, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedHTLC1)]),

    % send some money to the payee so they have enough to pay the fee for redeeming
    Tx0 = blockchain_txn_payment_v1:new(Payer, Payee, 100 * ?BONES_PER_HNT, 2),
    Fee = blockchain_txn_payment_v1:calculate_fee(Tx0, Chain),
    Tx1 = blockchain_txn_payment_v1:fee(Tx0, Fee),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx1, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    %% check both txns are valid, in context
    ?assertMatch({_, []}, blockchain_txn:validate([SignedHTLC1, SignedTx], Chain)),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedHTLC1, SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

    % Try and redeem
    RedeemSigFun = libp2p_crypto:mk_sig_fun(PayeePrivKey),
    RedeemTx0 = blockchain_txn_redeem_htlc_v1:new(Payee, HTLCAddress, <<"wenmoon">>),
    RedeemFee = blockchain_txn_redeem_htlc_v1:calculate_fee(RedeemTx0, Chain),
    RedeemTx1 = blockchain_txn_redeem_htlc_v1:fee(RedeemTx0, RedeemFee),
    SignedRedeemTx = blockchain_txn_redeem_htlc_v1:sign(RedeemTx1, RedeemSigFun),
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [SignedRedeemTx]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:tid()),
    timer:sleep(500), %% add block is a cast, need some time for this to happen

    %% Check that there is NO htlc receipt
    {error, not_found} = blockchain:get_htlc_receipt(HTLCAddress, Chain),

    ct:pal("Confirmed that htlc receipt has not been stored for ~p", [HTLCAddress]),
    ok.

%%--------------------------------------------------------------------
%% HELPERS
%%--------------------------------------------------------------------
extra_vars(_) ->
    #{?txn_fees => true, ?max_payments => ?MAX_PAYMENTS, ?allow_zero_amount => false, ?txn_fee_multiplier => 5000}.
