%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Create Proof of Coverage Request ==
%% Submitted by a gateway who wishes to initiate a PoC Challenge
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_request).

-export([
    new/1
    ,gateway_address/1
    ,signature/1
    ,fee/1
    ,sign/2
    ,is_valid/1
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_poc_request, {
    gateway_address :: libp2p_crypto:address()
    ,signature :: binary()
    ,fee = 1 :: non_neg_integer()
}).

-type txn_poc_request() :: #txn_poc_request{}.
-export_type([txn_poc_request/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address()) -> txn_poc_request().
new(Address) ->
    #txn_poc_request{
        gateway_address=Address
        ,signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(txn_poc_request()) -> libp2p_crypto:address().
gateway_address(Txn) ->
    Txn#txn_poc_request.gateway_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_poc_request()) -> binary().
signature(Txn) ->
    Txn#txn_poc_request.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_poc_request()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_poc_request.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_poc_request(), libp2p_crypto:sig_fun()) -> txn_poc_request().
sign(Txn, SigFun) ->
    Txn#txn_poc_request{signature=SigFun(erlang:term_to_binary(Txn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_request()) -> boolean().
is_valid(Txn=#txn_poc_request{gateway_address=GatewayAddress, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(GatewayAddress),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_poc_request{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_poc_request).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_poc_request{
        gateway_address= <<"gateway_address">>
        ,signature= <<>>
    },
    ?assertEqual(Tx, new(<<"gateway_address">>)).

gateway_address_test() ->
    Tx = new(<<"gateway_address">>),
    ?assertEqual(<<"gateway_address">>, gateway_address(Tx)).

signature_test() ->
    Tx = new(<<"gateway_address">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(<<"gateway_address">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_poc_request{signature = <<>>}), Sig1, PubKey)).

is_test() ->
    Tx = new(<<"gateway_address">>),
    ?assert(is(Tx)).

-endif.
