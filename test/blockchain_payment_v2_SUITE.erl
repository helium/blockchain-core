-module(blockchain_payment_v2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
         multisig_test/1,
         single_payee_test/1,
         same_payees_test/1,
         different_payees_test/1,
         empty_payees_test/1,
         self_payment_test/1,
         max_payments_test/1,
         signature_test/1,
         zero_amount_test/1,
         negative_amount_test/1,
         valid_memo_test/1,
         negative_memo_test/1,
         valid_memo_not_set_test/1,
         invalid_memo_not_set_test/1,
         big_memo_valid_test/1,
         big_memo_invalid_test/1
        ]).



all() ->
    [
     multisig_test,
     single_payee_test,
     same_payees_test,
     different_payees_test,
     empty_payees_test,
     self_payment_test,
     max_payments_test,
     signature_test,
     zero_amount_test,
     negative_amount_test,
     valid_memo_test,
     negative_memo_test,
     valid_memo_not_set_test,
     invalid_memo_not_set_test,
     big_memo_valid_test,
     big_memo_invalid_test
    ].

-define(MAX_PAYMENTS, 20).
-define(VALID_GIANT_MEMO, 18446744073709551615).        %% max 64 bit number
-define(INVALID_GIANT_MEMO, 18446744073709551616).      %% max 64 bit number + 1, takes 72 bits

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
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

multisig_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    Amount = 10,
    InitialHeight = 2,

    %% Transfer plan:
    %% A -> B[i] --> C[i]
    %%      |
    %%      +------> D[i]
    %% All funds will come from A, so only it needs a starting balance,
    %% the rest can be freshly created.
    [_, {A, {_, _, A_SigFun}} | _ ] = ConsensusMembers,
    lists:foldl(
        fun ({M, N}, H) ->
            {B, B_SigFun} = make_multisig_addr(M, N),
            {C, _} = make_multisig_addr(M, N),
            {D, D_SigFun} = make_multisig_addr(M, N),
            %% * 2 because B will later pay twice (to C and D)
            ?assertEqual(ok, transfer(Amount * 2, {A, A_SigFun}, B, H, Chain, ConsensusMembers)),
            ?assertEqual(ok, transfer(Amount, {B, B_SigFun}, C, H + 1, Chain, ConsensusMembers)),
            %% B->D wrong SigFun
            ?assertError(
                {badmatch, {error, {invalid_txns, [{_, bad_signature}]}}},
                transfer(1, {B, D_SigFun}, D, H + 2, Chain, ConsensusMembers)
            ),
            %% B->D correct SigFun
            ?assertEqual(ok, transfer(Amount, {B, B_SigFun}, D, H + 2, Chain, ConsensusMembers)),
            H + 3
        end,
        InitialHeight,
        [
            {M, N}
        ||
            N <- lists:seq(1, 10),
            M <- lists:seq(1, N)
        ]
    ).

single_payee_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

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

    Ledger = blockchain:ledger(Chain),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + Amount, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - Amount, blockchain_ledger_entry_v1:balance(NewEntry1)),
    ok.

same_payees_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    _Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),
    _Swarm = ?config(swarm, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee TWICE
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 1000,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),
    Payment2 = blockchain_payment_v2:new(Recipient, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),
    ?assertEqual({error, duplicate_payees}, blockchain_txn_payment_v2:is_valid(SignedTx, Chain)),
    ok.

different_payees_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}, {Recipient2, _} |_] = ConsensusMembers,

    %% Create a payment to a payee1
    Recipient1 = blockchain_swarm:pubkey_bin(),
    Amount1 = 1000,
    Payment1 = blockchain_payment_v2:new(Recipient1, Amount1),

    %% Create a payment to the other payee2
    Amount2 = 2000,
    Payment2 = blockchain_payment_v2:new(Recipient2, Amount2),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

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
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    Tx = blockchain_txn_payment_v2:new(Payer, [], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),
    ?assertEqual({error, zero_payees}, blockchain_txn_payment_v2:is_valid(SignedTx, Chain)),

    ok.

self_payment_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee TWICE
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 1000,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),
    Payment2 = blockchain_payment_v2:new(Payer, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),
    ?assertEqual({error, self_payment}, blockchain_txn_payment_v2:is_valid(SignedTx, Chain)),

    ok.

max_payments_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create 1+max_payments
    Payees = [P || {P, _} <- test_utils:generate_keys(?MAX_PAYMENTS + 1, ed25519)],

    Payments = lists:foldl(fun(PayeePubkeyBin, Acc) ->
                                   Amount = rand:uniform(100),
                                   [blockchain_payment_v2:new(PayeePubkeyBin, Amount) | Acc]
                           end,
                           [],
                           Payees),

    Tx = blockchain_txn_payment_v2:new(Payer, Payments, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ?assertEqual({error, {exceeded_max_payments, {length(Payments), ?MAX_PAYMENTS}}},
                 blockchain_txn_payment_v2:is_valid(SignedTx, Chain)),

    ok.

signature_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    _Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),
    _Swarm = ?config(swarm, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, _PayerPrivKey, _}}, {_Other, {_, OtherPrivKey, _}} |_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 2500,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),

    %% Use someone elses' signature
    SigFun = libp2p_crypto:mk_sig_fun(OtherPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("~s", [blockchain_txn:print(SignedTx)]),

    ?assertEqual({error, bad_signature}, blockchain_txn_payment_v2:is_valid(SignedTx, Chain)),
    ok.

zero_amount_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 0,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {[], [{SignedTx, _InvalidReason}]} = blockchain_txn:validate([SignedTx], Chain),
    ok.

negative_amount_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = -100,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {[], [{SignedTx, _InvalidReason}]} = blockchain_txn:validate([SignedTx], Chain),
    ok.

valid_memo_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}, {OtherRecipient, _} |_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 1000,
    Memo = 1,

    Payment1 = blockchain_payment_v2:new(Recipient, Amount, Memo),
    Payment2 = blockchain_payment_v2:memo(blockchain_payment_v2:new(OtherRecipient, Amount), Memo),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    ct:pal("SignedTx: ~p", [SignedTx]),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient, Ledger),
    ?assertEqual(Balance + Amount, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(OtherRecipient, Ledger),
    ?assertEqual(Balance + Amount, blockchain_ledger_entry_v1:balance(NewEntry1)),

    {ok, NewEntry2} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - 2*Amount, blockchain_ledger_entry_v1:balance(NewEntry2)),
    ok.

negative_memo_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 100,
    Memo = -1,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount, Memo),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {[], [{SignedTx, InvalidReason}]} = blockchain_txn:validate([SignedTx], Chain),
    ?assertEqual(invalid_memo, InvalidReason),
    ok.

valid_memo_not_set_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}, {Recipient2, _} |_] = ConsensusMembers,

    Amount = 100,

    %% Create a payment to a single payee, valid
    Recipient1 = blockchain_swarm:pubkey_bin(),
    Payment1 = blockchain_payment_v2:new(Recipient1, Amount),

    %% Create a payment to another payee, also valid
    Memo = 0,
    Payment2 = blockchain_payment_v2:new(Recipient2, Amount, Memo),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1, Payment2], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    Ledger = blockchain:ledger(Chain),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Recipient1, Ledger),
    ?assertEqual(Balance + Amount, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, NewEntry1} = blockchain_ledger_v1:find_entry(Recipient2, Ledger),
    ?assertEqual(Balance + Amount, blockchain_ledger_entry_v1:balance(NewEntry1)),

    {ok, NewEntry2} = blockchain_ledger_v1:find_entry(Payer, Ledger),
    ?assertEqual(Balance - Amount * 2, blockchain_ledger_entry_v1:balance(NewEntry2)),

    ok.

invalid_memo_not_set_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 100,
    Memo = 1,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount, Memo),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {[], [{SignedTx, InvalidReason}]} = blockchain_txn:validate([SignedTx], Chain),
    ?assertEqual(invalid_memo_before_var, InvalidReason),
    ok.

big_memo_valid_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Balance = ?config(balance, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}} | _] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 100,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount, ?VALID_GIANT_MEMO),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {ok, Block} = test_utils:create_block(ConsensusMembers, [SignedTx]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),

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

big_memo_invalid_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),

    %% Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,

    %% Create a payment to a single payee
    Recipient = blockchain_swarm:pubkey_bin(),
    Amount = 100,
    Payment1 = blockchain_payment_v2:new(Recipient, Amount, ?INVALID_GIANT_MEMO),

    Tx = blockchain_txn_payment_v2:new(Payer, [Payment1], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v2:sign(Tx, SigFun),

    {[], [{SignedTx, InvalidReason}]} = blockchain_txn:validate([SignedTx], Chain),
    ct:pal("InvalidReason: ~p", [InvalidReason]),
    ?assertEqual(invalid_memo, InvalidReason),
    ok.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

extra_vars(big_memo_valid_test) ->
    #{?max_payments => ?MAX_PAYMENTS, ?allow_zero_amount => false, ?allow_payment_v2_memos => true};
extra_vars(big_memo_invalid_test) ->
    #{?max_payments => ?MAX_PAYMENTS, ?allow_zero_amount => false, ?allow_payment_v2_memos => true};
extra_vars(valid_memo_test) ->
    #{?max_payments => ?MAX_PAYMENTS, ?allow_zero_amount => false, ?allow_payment_v2_memos => true};
extra_vars(negative_memo_test) ->
    #{?max_payments => ?MAX_PAYMENTS, ?allow_zero_amount => false, ?allow_payment_v2_memos => true};
extra_vars(_) ->
    #{?max_payments => ?MAX_PAYMENTS, ?allow_zero_amount => false}.

%% Helpers --------------------------------------------------------------------

make_multisig_addr(M, N) ->
    Network = mainnet,
    KeyType = ecc_compact,
    KeyMaps = [libp2p_crypto:generate_keys(KeyType) || _ <- lists:duplicate(N, {})],
    MemberKeys = [P || #{public := P} <- KeyMaps],
    MakeFun = fun libp2p_crypto:mk_sig_fun/1,
    IFuns =
        [
            {libp2p_crypto:multisig_member_key_index(P, MemberKeys), MakeFun(S)}
        ||
            #{secret := S, public := P} <- KeyMaps
        ],
    {ok, MultisigPK} = libp2p_crypto:make_multisig_pubkey(Network, M, N, MemberKeys),
    MultisigSign =
        fun (Msg) ->
            ISigs = [{I, F(Msg)}|| {I, F} <- IFuns],
            {ok, Sig} =
                libp2p_crypto:make_multisig_signature(Network, Msg, MultisigPK, MemberKeys, ISigs),
            Sig
        end,
    {libp2p_crypto:pubkey_to_bin(Network, MultisigPK), MultisigSign}.

% TODO Use in the rest of cases?
transfer(Amount, {Src, SrcSigFun}, Dst, ExpectHeight, Chain, ConsensusMembers) ->
    SrcBalance0 = balance(Src, Chain),
    DstBalance0 = balance(Dst, Chain),
    Ledger = blockchain:ledger(Chain),
    Nonce = case blockchain_ledger_v1:find_entry(Src, Ledger) of
                {error, address_entry_not_found} -> 1;
                {ok, Entry} -> blockchain_ledger_entry_v1:nonce(Entry) + 1
            end,
    Tx = blockchain_txn_payment_v2:new(Src, [blockchain_payment_v2:new(Dst, Amount)], Nonce),
    TxSigned = blockchain_txn_payment_v2:sign(Tx, SrcSigFun),
    {ok, Block} = test_utils:create_block(ConsensusMembers, [TxSigned]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, ExpectHeight}, blockchain:height(Chain)),
    ?assertEqual({ok, Block}, blockchain:get_block(ExpectHeight, Chain)),
    SrcBalance1 = balance(Src, Chain),
    DstBalance1 = balance(Dst, Chain),
    ?assertEqual(SrcBalance1, SrcBalance0 - Amount),
    ?assertEqual(DstBalance1, DstBalance0 + Amount),
    ok.

% TODO Use in the rest of cases?
balance(<<Addr/binary>>, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(Addr, Ledger) of
        {error, address_entry_not_found} ->
            0;
        {ok, Entry} ->
            blockchain_ledger_entry_v1:balance(Entry)
    end.
