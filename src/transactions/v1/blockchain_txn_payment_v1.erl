%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_payment_v1).

-behavior(blockchain_txn).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v1_pb.hrl").

-export([
    new/5,
    hash/1,
    payer/1,
    payee/1,
    amount/1,
    fee/1,
    nonce/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_payment() :: #blockchain_txn_payment_v1_pb{}.
-export_type([txn_payment/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), pos_integer(),
          non_neg_integer(), non_neg_integer()) -> txn_payment().
new(Payer, Recipient, Amount, Fee, Nonce) ->
    #blockchain_txn_payment_v1_pb{
        payer=Payer,
        payee=Recipient,
        amount=Amount,
        fee=Fee,
        nonce=Nonce,
        signature = <<>>
    }.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_payment()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_payment_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_payment()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.payer.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_payment()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_payment()) -> pos_integer().
amount(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_payment()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_payment()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_payment_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
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
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_payment(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Payee = ?MODULE:payee(Txn),
    Nonce = ?MODULE:nonce(Txn),
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
                                    Fee = ?MODULE:fee(Txn),
                                    case blockchain_ledger_v1:transaction_fee(Ledger) of
                                        {error, _}=Error0 ->
                                            Error0;
                                        {ok, MinerFee} ->
                                            case (Amount >= 0) andalso (Fee >= MinerFee) of
                                                false ->
                                                    {error, invalid_transaction};
                                                true ->
                                                    case blockchain_ledger_v1:check_dc_balance(Payer, Fee, Ledger) of
                                                        {error, _}=Error1 ->
                                                            Error1;
                                                        ok ->
                                                            case blockchain_ledger_v1:check_balance(Payer, Amount, Ledger) of
                                                                ok ->
                                                                    {ok, Entry} = blockchain_ledger_v1:find_entry(Payer, Ledger),
                                                                    case Nonce =:= blockchain_ledger_entry_v1:nonce(Entry) + 1 of
                                                                        true -> ok;
                                                                        false -> {error, {bad_nonce, {payment, Nonce, blockchain_ledger_entry_v1:nonce(Entry)}}}
                                                                    end;
                                                                Error2 ->
                                                                    Error2
                                                            end
                                                    end
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_payment(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Amount = ?MODULE:amount(Txn),
    Fee = ?MODULE:fee(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, Amount, Nonce, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    Payee = ?MODULE:payee(Txn),
                    blockchain_ledger_v1:credit_account(Payee, Amount, Ledger)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_payment()) -> iodata().
print(undefined) -> <<"type=payment, undefined">>;
print(#blockchain_txn_payment_v1_pb{payer=Payer, payee=Recipient, amount=Amount,
                                    fee=Fee, nonce=Nonce, signature = S }) ->
    io_lib:format("type=payment, payer=~p, payee=~p, amount=~p, fee=~p, nonce=~p, signature=~p",
                  [?TO_B58(Payer), ?TO_B58(Recipient), Amount, Fee, Nonce, S]).


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_payment_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        amount=666,
        fee=10,
        nonce=1,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"payer">>, <<"payee">>, 666, 10, 1)).

payer_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).


amount_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    ?assertEqual(666, amount(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    ?assertEqual(10, fee(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_payment_v1_pb:encode_msg(Tx1#blockchain_txn_payment_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.
