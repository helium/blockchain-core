%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_payment).

-export([
    new/4
    ,payer/1
    ,payee/1
    ,amount/1
    ,nonce/1
    ,signature/1
    ,sign/2
    ,is_valid/1
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_payment, {
    payer :: libp2p_crypto:address()
    ,payee :: libp2p_crypto:address()
    ,amount :: integer()
    ,nonce :: non_neg_integer()
    ,signature :: binary()
}).

-type txn_payment() :: #txn_payment{}.
-export_type([txn_payment/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), libp2p_crypto:address() ,integer()
          ,non_neg_integer()) -> txn_payment().
new(Payer, Recipient, Amount, Nonce) ->
    #txn_payment{
        payer=Payer
        ,payee=Recipient
        ,amount=Amount
        ,nonce=Nonce
        ,signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_payment()) -> libp2p_crypto:address().
payer(Txn) ->
    Txn#txn_payment.payer.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_payment()) -> libp2p_crypto:address().
payee(Txn) ->
    Txn#txn_payment.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_payment()) -> integer().
amount(Txn) ->
    Txn#txn_payment.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_payment()) -> non_neg_integer().
nonce(Txn) ->
    Txn#txn_payment.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_payment()) -> binary().
signature(Txn) ->
    Txn#txn_payment.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_payment(), libp2p_crypto:sig_fun()) -> txn_payment().
sign(Txn, SigFun) ->
    Txn#txn_payment{signature=SigFun(erlang:term_to_binary(Txn))}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_payment()) -> boolean().
is_valid(Txn=#txn_payment{payer=Payer, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Payer),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_payment{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_payment).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_payment{
        payer= <<"payer">>
        ,payee= <<"payee">>
        ,amount=666
        ,nonce=1
        ,signature = <<>>
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

nonce_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_payment{signature = <<>>}), Sig1, PubKey)).

is_valid_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Payer = libp2p_crypto:pubkey_to_address(PubKey),
    Tx0 = new(Payer, <<"payee">>, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    {_, PubKey2} = libp2p_crypto:generate_keys(),
    Payer2 = libp2p_crypto:pubkey_to_address(PubKey2),
    Tx2 = new(Payer2, <<"payee">>, 666, 1),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

is_test() ->
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assert(is(Tx0)).

-endif.
