%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Create Hashed Timelock ==
%% == Creates a transaction that can only be redeemed
%% == by providing the correct pre-image to the hashlock
%% == within the specified timelock
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_create_htlc).

-export([
    new/6
    ,payer/1
    ,address/1
    ,hashlock/1
    ,timelock/1
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

-record(txn_create_htlc, {
    payer :: libp2p_crypto:address()
    ,address :: libp2p_crypto:address()
    ,hashlock :: binary()
    ,timelock :: integer()
    ,amount :: integer()
    ,nonce :: non_neg_integer()
    ,signature :: binary()
}).

-type txn_create_htlc() :: #txn_create_htlc{}.
-export_type([txn_create_htlc/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), libp2p_crypto:address(), binary(), integer(), integer()
          ,non_neg_integer()) -> txn_create_htlc().
new(Payer, Address, Hashlock, Timelock, Amount, Nonce) ->
    #txn_create_htlc{
        payer=Payer
        ,address=Address
        ,hashlock=Hashlock
        ,timelock=Timelock
        ,amount=Amount
        ,nonce=Nonce
        ,signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_create_htlc()) -> libp2p_crypto:address().
payer(Txn) ->
    Txn#txn_create_htlc.payer.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(txn_create_htlc()) -> libp2p_crypto:address().
address(Txn) ->
    Txn#txn_create_htlc.address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hashlock(txn_create_htlc()) -> binary().
hashlock(Txn) ->
    Txn#txn_create_htlc.hashlock.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec timelock(txn_create_htlc()) -> integer().
timelock(Txn) ->
    Txn#txn_create_htlc.timelock.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_create_htlc()) -> integer().
amount(Txn) ->
    Txn#txn_create_htlc.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_create_htlc()) -> non_neg_integer().
nonce(Txn) ->
    Txn#txn_create_htlc.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_create_htlc()) -> binary().
signature(Txn) ->
    Txn#txn_create_htlc.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_create_htlc(), libp2p_crypto:sig_fun()) -> txn_create_htlc().
sign(Txn, SigFun) ->
    Txn#txn_create_htlc{signature=SigFun(erlang:term_to_binary(Txn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_create_htlc()) -> boolean().
is_valid(Txn=#txn_create_htlc{payer=Payer, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Payer),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_create_htlc{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_create_htlc).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_create_htlc{
        payer= <<"payer">>
        ,address= <<"address">>
        ,hashlock= <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>
        ,timelock=0
        ,amount=666
        ,nonce=1
        ,signature= <<>>
    },
    ?assertEqual(Tx, new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1)).

payer_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

address_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"address">>, address(Tx)).

amount_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(666, amount(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(1, nonce(Tx)).

hashlock_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, hashlock(Tx)).

timelock_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(0, timelock(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_create_htlc{signature = <<>>}), Sig1, PubKey)).

 is_valid_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Payer = libp2p_crypto:pubkey_to_address(PubKey),
    Tx0 = new(Payer, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    {_, PubKey2} = libp2p_crypto:generate_keys(),
    Payer2 = libp2p_crypto:pubkey_to_address(PubKey2),
    Tx2 = new(Payer2, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

is_test() ->
    Tx = new(<<"payer">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assert(is(Tx)).

-endif.
