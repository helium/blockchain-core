%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Security Exchange ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_security_exchange_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_security_exchange_v1_pb.hrl").

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
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_security_exchange() :: #blockchain_txn_security_exchange_v1_pb{}.
-export_type([txn_security_exchange/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), pos_integer(),
          non_neg_integer(), non_neg_integer()) -> txn_security_exchange().
new(Payer, Recipient, Amount, Fee, Nonce) ->
    #blockchain_txn_security_exchange_v1_pb{
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
-spec hash(txn_security_exchange()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_security_exchange_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_security_exchange_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_security_exchange()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.payer.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_security_exchange()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_security_exchange()) -> pos_integer().
amount(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_security_exchange()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_security_exchange()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_security_exchange()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_security_exchange(), libp2p_crypto:sig_fun()) -> txn_security_exchange().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_security_exchange_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_security_exchange_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_security_exchange(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Payee = ?MODULE:payee(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_security_exchange_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_security_exchange_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            case Payer == Payee of
                false ->
                    Amount = ?MODULE:amount(Txn),
                    blockchain_ledger_v1:check_security_balance(Payer, Amount, Ledger);
                true ->
                    {error, invalid_transaction_self_payment}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_security_exchange(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Amount = ?MODULE:amount(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    case blockchain_ledger_v1:debit_security(Payer, Amount, Nonce, Ledger) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            Payee = ?MODULE:payee(Txn),
            blockchain_ledger_v1:credit_security(Payee, Amount, Ledger)
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_security_exchange_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        amount=666,
        fee=1,
        nonce=1,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"payer">>, <<"payee">>, 666, 1, 1)).

payer_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1, 1),
    ?assertEqual(666, amount(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1, 1),
    ?assertEqual(1, fee(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1, 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 1, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_security_exchange_v1_pb:encode_msg(Tx1#blockchain_txn_security_exchange_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.
