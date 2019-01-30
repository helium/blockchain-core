%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_payment_v1).

-behavior(blockchain_txn).

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
    is_valid/1,
    is/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_payment_v1, {
    payer :: libp2p_crypto:address(),
    payee :: libp2p_crypto:address(),
    amount :: integer(),
    fee :: integer(),
    nonce :: non_neg_integer(),
    signature :: binary()
}).

-type txn_payment() :: #txn_payment_v1{}.
-export_type([txn_payment/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), libp2p_crypto:address(), pos_integer(),
          non_neg_integer(), non_neg_integer()) -> txn_payment().
new(Payer, Recipient, Amount, Fee, Nonce) ->
    #txn_payment_v1{
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
    BaseTxn = Txn#txn_payment_v1{signature = <<>>},
    crypto:hash(sha256, erlang:term_to_binary(BaseTxn)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_payment()) -> libp2p_crypto:address().
payer(Txn) ->
    Txn#txn_payment_v1.payer.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_payment()) -> libp2p_crypto:address().
payee(Txn) ->
    Txn#txn_payment_v1.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_payment()) -> pos_integer().
amount(Txn) ->
    Txn#txn_payment_v1.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_payment()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_payment_v1.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_payment()) -> non_neg_integer().
nonce(Txn) ->
    Txn#txn_payment_v1.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_payment()) -> binary().
signature(Txn) ->
    Txn#txn_payment_v1.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_payment(), libp2p_crypto:sig_fun()) -> txn_payment().
sign(Txn, SigFun) ->
    Txn#txn_payment_v1{signature=SigFun(erlang:term_to_binary(Txn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_payment()) -> boolean().
is_valid(Txn=#txn_payment_v1{payer=Payer, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_payment_v1{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_payment_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_payment(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    Amount = ?MODULE:amount(Txn),
    Fee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:transaction_fee(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, MinerFee} ->
            case (Amount >= 0) andalso (Fee >= MinerFee) of
                false ->
                    lager:error("Error, incorrect_amount or incorrect_fee for PaymentTxn: ~p, Amount: ~p, Fee: ~p, MinerFee: ~p", [Txn, Amount, Fee, MinerFee]),
                    {error, invalid_transaction};
                true ->
                    case ?MODULE:is_valid(Txn) of
                        true ->
                            Payer = ?MODULE:payer(Txn),
                            Nonce = ?MODULE:nonce(Txn),
                            case blockchain_ledger_v1:debit_account(Payer, Amount + Fee, Nonce, Ledger) of
                                {error, _Reason}=Error ->
                                    Error;
                                ok ->
                                    Payee = ?MODULE:payee(Txn),
                                    blockchain_ledger_v1:credit_account(Payee, Amount, Ledger)
                            end;
                        false ->
                            {error, bad_signature}
                    end
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_payment_v1{
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
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ed25519),
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_payment_v1{signature = <<>>}), Sig1, PubKey)).

is_valid_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ed25519),
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    Tx0 = new(Payer, <<"payee">>, 666, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    #{public := PubKey2} = libp2p_crypto:generate_keys(ed25519),
    Payer2 = libp2p_crypto:pubkey_to_bin(PubKey2),
    Tx2 = new(Payer2, <<"payee">>, 666, 10, 1),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

is_test() ->
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 10, 1),
    ?assert(is(Tx0)).

-endif.
