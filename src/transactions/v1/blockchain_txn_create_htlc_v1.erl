%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Create Hashed Timelock ==
%% == Creates a transaction that can only be redeemed
%% == by providing the correct pre-image to the hashlock
%% == within the specified timelock
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_create_htlc_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_create_htlc_v1_pb.hrl").

-export([
    new/7,
    hash/1,
    payer/1,
    payee/1,
    address/1,
    hashlock/1,
    timelock/1,
    amount/1,
    fee/1, fee/2,
    fee_payer/2,
    calculate_fee/2, calculate_fee/5,
    nonce/1,
    signature/1,
    sign/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_create_htlc_v1_pb).

-type t() :: txn_create_htlc().

-type txn_create_htlc() :: ?T{}.

-export_type([t/0, txn_create_htlc/0]).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), binary(),
          non_neg_integer(), non_neg_integer(), non_neg_integer()) -> txn_create_htlc().
new(Payer, Payee, Address, Hashlock, Timelock, Amount, Nonce) ->
    #blockchain_txn_create_htlc_v1_pb{
        payer=Payer,
        payee=Payee,
        address=Address,
        hashlock=Hashlock,
        timelock=Timelock,
        amount=Amount,
        fee=?LEGACY_TXN_FEE,
        nonce=Nonce,
        signature = <<>>
    }.


-spec hash(txn_create_htlc()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_create_htlc_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_create_htlc_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec payer(txn_create_htlc()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.payer.

-spec payee(txn_create_htlc()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.payee.

-spec address(txn_create_htlc()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.address.

-spec hashlock(txn_create_htlc()) -> binary().
hashlock(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.hashlock.

-spec timelock(txn_create_htlc()) -> non_neg_integer().
timelock(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.timelock.

-spec amount(txn_create_htlc()) -> non_neg_integer().
amount(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.amount.

-spec fee(txn_create_htlc()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.fee.

-spec fee_payer(txn_create_htlc(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    payer(Txn).

-spec fee(txn_create_htlc(), non_neg_integer()) -> txn_create_htlc().
fee(Txn, Fee) ->
    Txn#blockchain_txn_create_htlc_v1_pb{fee=Fee}.

-spec nonce(txn_create_htlc()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_create_htlc()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_create_htlc(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_create_htlc(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_create_htlc_v1_pb{fee=0, signature = <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_create_htlc(), libp2p_crypto:sig_fun()) -> txn_create_htlc().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_create_htlc_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_create_htlc_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_create_htlc(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_create_htlc_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_create_htlc_v1_pb:encode_msg(BaseTxn),
    FieldValidation = case blockchain:config(?txn_field_validation_version, Ledger) of
                          {ok, 1} ->
                              [{{payee, ?MODULE:payee(Txn)}, {address, libp2p}},
                               {{hashlock, ?MODULE:hashlock(Txn)}, {binary, 32}},
                               {{address, ?MODULE:address(Txn)}, {address, libp2p}}];
                          _ ->
                              [{{payee, ?MODULE:payee(Txn)}, {address, libp2p}},
                               {{hashlock, ?MODULE:hashlock(Txn)}, {binary, 32, 64}},
                               {{address, ?MODULE:address(Txn)}, {binary, 32, 33}}]
                      end,

    case blockchain_txn:validate_fields(FieldValidation) of
        ok ->
            case blockchain_ledger_v1:find_htlc(?MODULE:address(Txn), Ledger) of
                {ok, _HTLC} ->
                    {error, htlc_address_already_in_use};
                {error, _} ->
                    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                        false ->
                            {error, bad_signature};
                        true ->
                            case blockchain_ledger_v1:find_entry(Payer, Ledger) of
                                {error, _}=Error0 ->
                                    Error0;
                                {ok, Entry} ->
                                    TxnNonce = ?MODULE:nonce(Txn),
                                    LedgerNonce = blockchain_ledger_entry_v1:nonce(Entry),
                                    case TxnNonce =:= LedgerNonce + 1 of
                                        false ->
                                            {error, {bad_nonce, {create_htlc, TxnNonce, LedgerNonce}}};
                                        true ->
                                            Amount = ?MODULE:amount(Txn),
                                            TxnFee = ?MODULE:fee(Txn),
                                            AmountCheck = case blockchain:config(?allow_zero_amount, Ledger) of
                                                              {ok, false} ->
                                                                  %% check that amount is greater than 0
                                                                  Amount > 0;
                                                              _ ->
                                                                  %% if undefined or true, use the old check
                                                                  Amount >= 0
                                                          end,
                                            case AmountCheck of
                                                false ->
                                                    lager:error("amount < 0 for CreateHTLCTxn: ~p", [Txn]),
                                                    {error, invalid_transaction};
                                                true ->
                                                    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                                    TxnFee = ?MODULE:fee(Txn),
                                                    ExpectedTxnFee = calculate_fee(Txn, Chain),
                                                    case (ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled) of
                                                        false ->
                                                            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                                        true ->
                                                            blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
                                                    end
                                            end
                                    end
                            end
                    end
            end;
        Error ->
            Error
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_create_htlc(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Amount = ?MODULE:amount(Txn),
    TxnFee = ?MODULE:fee(Txn),
    TxnHash = ?MODULE:hash(Txn),
    Payer = ?MODULE:payer(Txn),
    Payee = ?MODULE:payee(Txn),
    Nonce = ?MODULE:nonce(Txn),
    case blockchain_ledger_v1:find_entry(Payer, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, _Entry} ->
            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
            case blockchain_ledger_v1:debit_fee(Payer,TxnFee, Ledger, AreFeesEnabled, TxnHash, Chain) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    case blockchain_ledger_v1:debit_account(Payer, Amount, Nonce, Ledger) of
                        {error, _Reason}=Error ->
                            Error;
                        ok ->
                            Address = ?MODULE:address(Txn),
                            blockchain_ledger_v1:add_htlc(Address,
                                                          Payer,
                                                          Payee,
                                                          Amount,
                                                          Nonce,
                                                          ?MODULE:hashlock(Txn),
                                                          ?MODULE:timelock(Txn),
                                                          Ledger)
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_create_htlc()) -> iodata().
print(#blockchain_txn_create_htlc_v1_pb{
        payer=Payer, payee=Payee, address=Address,
        hashlock=Hashlock, timelock=Timelock, amount=Amount,
        fee=Fee, signature = Sig}) ->
    io_lib:format("type=create_htlc payer=~p payee=~p, address=~p, hashlock=~p, timelock=~p, amount=~p, fee=~p, signature=~p",
                  [?TO_B58(Payer), ?TO_B58(Payee), Address, Hashlock, Timelock, Amount, Fee, Sig]).

json_type() ->
    <<"create_htlc_v1">>.

-spec to_json(txn_create_htlc(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      payer => ?BIN_TO_B58(payer(Txn)),
      payee => ?BIN_TO_B58(payee(Txn)),
      address => ?BIN_TO_B58(address(Txn)),
      hashlock => ?BIN_TO_B64(hashlock(Txn)),
      timelock => timelock(Txn),
      amount => amount(Txn),
      fee => fee(Txn),
      nonce => nonce(Txn)
     }.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_create_htlc_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        address= <<"address">>,
        hashlock= <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>,
        timelock=0,
        amount=666,
        fee=?LEGACY_TXN_FEE,
        signature= <<>>,
        nonce=1
    },
    ?assertEqual(Tx, new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1)).

payer_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).

address_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"address">>, address(Tx)).

amount_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(666, amount(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

hashlock_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, hashlock(Tx)).

timelock_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(0, timelock(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_create_htlc_v1_pb:encode_msg(Tx1#blockchain_txn_create_htlc_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, payer, payee, address, hashlock, timelock, amount, fee, nonce])).

is_valid_with_extended_validation_test() ->
    {timeout, 40,
     fun() ->
             BaseDir = test_utils:tmp_dir("is_valid_with_extended_validation_test"),
             Block = blockchain_block:new_genesis_block([]),
             {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
             meck:new(blockchain_ledger_v1, [passthrough]),

             %% These are all required
             meck:expect(blockchain_ledger_v1, config,
                         fun(?deprecate_payment_v1, _) ->
                                 {ok, false};
                            (?txn_field_validation_version, _) ->
                                 %% This is new
                                 {ok, 1};
                            (?allow_zero_amount, _) ->
                                 {ok, false};
                            (?dc_payload_size, _) ->
                                 {error, not_found};
                            (?txn_fee_multiplier, _) ->
                                 {error, not_found}
                         end),
             meck:expect(blockchain_ledger_v1, txn_fees_active, fun(_) -> true end),

             #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
             SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
             Payer = libp2p_crypto:pubkey_to_bin(PubKey),
             Tx = sign(new(Payer, <<"payee">>, <<"address">>, crypto:strong_rand_bytes(32), 0, 666, 1), SigFun),
             ?assertEqual({error, {invalid_address, payee}}, is_valid(Tx, Chain)),

             Tx1 = sign(new(Payer, libp2p_crypto:b58_to_bin("1BR9RgYoP5psbcw9aKh1cDskLaGMBmkb8"), <<"address">>, crypto:strong_rand_bytes(32), 0, 666, 1), SigFun),
             ?assertEqual({error, {invalid_address, payee}}, is_valid(Tx1, Chain)),

             #{public := PayeePubkey, secret := _PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
             ValidPayee = libp2p_crypto:pubkey_to_bin(PayeePubkey),
             Tx2 = sign(new(Payer, ValidPayee, ValidPayee, crypto:strong_rand_bytes(32), 0, 666, 1), SigFun),
             %% This check can be improved but whatever (it fails on fee)
             ?assertNotEqual({error, {invalid_address, payee}}, is_valid(Tx2, Chain)),

             Tx3 = sign(new(Payer, ValidPayee, <<"address">>, crypto:strong_rand_bytes(32), 0, 666, 1), SigFun),
             ?assertEqual({error, {invalid_address, address}}, is_valid(Tx3, Chain)),

             meck:unload(blockchain_ledger_v1),
             test_utils:cleanup_tmp_dir(BaseDir)
     end}.

-endif.
