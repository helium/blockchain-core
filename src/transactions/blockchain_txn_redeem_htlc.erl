%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Redeem Hashed Timelock ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_redeem_htlc).

-behavior(blockchain_txn).

-export([
    new/4
    ,hash/1
    ,payee/1
    ,address/1
    ,preimage/1
    ,fee/1
    ,signature/1
    ,sign/2
    ,is_valid/1
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_redeem_htlc, {
    payee :: libp2p_crypto:address()
    ,address :: libp2p_crypto:address()
    ,preimage :: undefined | binary()
    ,fee :: non_neg_integer()
    ,signature :: binary()
}).

-type txn_redeem_htlc() :: #txn_redeem_htlc{}.
-export_type([txn_redeem_htlc/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), libp2p_crypto:address(), binary(), non_neg_integer()) -> txn_redeem_htlc().
new(Payee, Address, PreImage, Fee) ->
    #txn_redeem_htlc{
        payee=Payee
        ,address=Address
        ,preimage=PreImage
        ,fee=Fee
        ,signature= <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_redeem_htlc()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#txn_redeem_htlc{signature = <<>>},
    crypto:hash(sha256, erlang:term_to_binary(BaseTxn)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_redeem_htlc()) -> libp2p_crypto:address().
payee(Txn) ->
    Txn#txn_redeem_htlc.payee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(txn_redeem_htlc()) -> libp2p_crypto:address().
address(Txn) ->
    Txn#txn_redeem_htlc.address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec preimage(txn_redeem_htlc()) -> binary().
preimage(Txn) ->
    Txn#txn_redeem_htlc.preimage.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_redeem_htlc()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_redeem_htlc.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_redeem_htlc()) -> binary().
signature(Txn) ->
    Txn#txn_redeem_htlc.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_redeem_htlc(), libp2p_crypto:sig_fun()) -> txn_redeem_htlc().
sign(Txn, SigFun) ->
    Txn#txn_redeem_htlc{signature=SigFun(erlang:term_to_binary(Txn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_redeem_htlc()) -> boolean().
is_valid(Txn=#txn_redeem_htlc{payee=Payee, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Payee),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_redeem_htlc{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_redeem_htlc).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_redeem_htlc{
        payee= <<"payee">>
        ,address= <<"address">>
        ,preimage= <<"yolo">>
        ,fee= 1
        ,signature= <<>>
    },
    ?assertEqual(Tx, new(<<"payee">>, <<"address">>, <<"yolo">>, 1)).

payee_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).

address_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(<<"address">>, address(Tx)).

preimage_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(<<"yolo">>, preimage(Tx)).

fee_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(1, fee(Tx)).

is_valid_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Payee = libp2p_crypto:pubkey_to_address(PubKey),
    Tx0 = new(Payee, <<"address">>, <<"yolo">>, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    {_, PubKey2} = libp2p_crypto:generate_keys(),
    Payee2 = libp2p_crypto:pubkey_to_address(PubKey2),
    Tx2 = new(Payee2, <<"address">>, <<"yolo">>, 1),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

is_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assert(is(Tx)).

-endif.
