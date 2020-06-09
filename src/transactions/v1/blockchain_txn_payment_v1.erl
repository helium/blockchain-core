%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_payment_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    payer/1,
    payee/1,
    amount/1,
    fee/1, fee/2,
    calculate_fee/2, calculate_fee/3,
    nonce/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_payment() :: #blockchain_txn_payment_v1_pb{}.
-export_type([txn_payment/0]).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), pos_integer(),
          non_neg_integer()) -> txn_payment().
new(Payer, Recipient, Amount, Nonce) ->
    #blockchain_txn_payment_v1_pb{
        payer=Payer,
        payee=Recipient,
        amount=Amount,
        fee=?LEGACY_TXN_FEE,
        nonce=Nonce,
        signature = <<>>
    }.

-spec hash(txn_payment()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_payment_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec payer(txn_payment()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.payer.

-spec payee(txn_payment()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.payee.

-spec amount(txn_payment()) -> non_neg_integer().
amount(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.amount.

-spec fee(txn_payment()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.fee.

-spec fee(txn_payment(), non_neg_integer()) -> txn_payment().
fee(Txn, Fee) ->
    Txn#blockchain_txn_payment_v1_pb{fee=Fee}.

-spec nonce(txn_payment()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.nonce.

-spec signature(txn_payment()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_payment(), libp2p_crypto:sig_fun()) -> txn_payment().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_payment_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_payment_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_payment(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    calculate_fee(Txn, Ledger, blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_fee(txn_payment(), blockchain_ledger_v1:ledger(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, true) ->
    ?fee(Txn#blockchain_txn_payment_v1_pb{fee=0, signature = <<0:512>>}, Ledger).

-spec is_valid(txn_payment(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Payee = ?MODULE:payee(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_payment_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v1_pb:encode_msg(BaseTxn),
    case blockchain:config(?deprecate_payment_v1, Ledger) of
        {ok, true} ->
            {error, payment_v1_deprecated};
        _ ->
            case blockchain_txn:validate_fields([{{payee, Payee}, {binary, 20, 33}}]) of
                ok ->
                    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                        false ->
                            {error, bad_signature};
                        true ->
                            case Payer == Payee of
                                false ->
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
                                                {error, invalid_transaction};
                                            true ->
                                                AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                                ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                                case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                                    false ->
                                                        {error, {wrong_txn_fee, ExpectedTxnFee, TxnFee}};
                                                    true ->
                                                        blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
                                                end
                                        end;
                                true ->
                                    {error, invalid_transaction_self_payment}
                            end
                    end;
                Error ->
                    Error
            end
    end.

-spec absorb(txn_payment(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Amount = ?MODULE:amount(Txn),
    TxnFee = ?MODULE:fee(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, TxnFee, Ledger, AreFeesEnabled) of
        {error, _Reason}=Error -> Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, Amount, Nonce, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    Payee = ?MODULE:payee(Txn),
                    blockchain_ledger_v1:credit_account(Payee, Amount, Ledger)
            end
    end.

-spec print(txn_payment()) -> iodata().
print(undefined) -> <<"type=payment, undefined">>;
print(#blockchain_txn_payment_v1_pb{payer=Payer, payee=Recipient, amount=Amount,
                                    fee=Fee, nonce=Nonce, signature = S }) ->
    io_lib:format("type=payment, payer=~p, payee=~p, amount=~p, fee=~p, nonce=~p, signature=~p",
                  [?TO_B58(Payer), ?TO_B58(Recipient), Amount, Fee, Nonce, S]).


-spec to_json(txn_payment(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"payment_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      payer => ?BIN_TO_B58(payer(Txn)),
      payee => ?BIN_TO_B58(payee(Txn)),
      amount => amount(Txn),
      fee => fee(Txn),
      nonce => nonce(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_payment_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        amount=666,
        fee=?LEGACY_TXN_FEE,
        nonce=1,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"payer">>, <<"payee">>, 666, 1)).

payer_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).


amount_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(666, amount(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_payment_v1_pb:encode_msg(Tx1#blockchain_txn_payment_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, payer, payee, amount, fee, nonce])).

-endif.
