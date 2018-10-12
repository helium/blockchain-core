%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location).

-export([
    new/3
    ,gateway_address/1
    ,signature/1
    ,location/1
    ,nonce/1
    ,sign/2
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_assert_location, {
    gateway_address :: libp2p_crypto:address()
    ,signature :: binary()
    ,location :: location()
    ,nonce = 0 :: non_neg_integer()
}).

-type location() :: non_neg_integer(). %% h3 index
-type txn_assert_location() :: #txn_assert_location{}.
-export_type([txn_assert_location/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), location(), non_neg_integer()) -> txn_assert_location().
new(Address, Location, Nonce) ->
    #txn_assert_location{
        gateway_address=Address
        ,location=Location
        ,signature = <<>>
        ,nonce=Nonce
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(txn_assert_location()) -> libp2p_crypto:address().
gateway_address(Txn) ->
    Txn#txn_assert_location.gateway_address.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(txn_assert_location()) -> location().
location(Txn) ->
    Txn#txn_assert_location.location.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_assert_location()) -> binary().
signature(Txn) ->
    Txn#txn_assert_location.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_assert_location()) -> non_neg_integer().
nonce(Txn) ->
    Txn#txn_assert_location.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_assert_location(), libp2p_crypto:sig_fun()) -> txn_assert_location().
sign(Txn, SigFun) ->
    Txn#txn_assert_location{signature=SigFun(erlang:term_to_binary(Txn))}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_assert_location).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_assert_location{
        gateway_address= <<"gateway_address">>
        ,signature= <<>>
        ,location= 1
        ,nonce = 1
    },
    ?assertEqual(Tx, new(<<"gateway_address">>, 1, 1)).

gateway_address_test() ->
    Tx = new(<<"gateway_address">>, 1, 1),
    ?assertEqual(<<"gateway_address">>, gateway_address(Tx)).

signature_test() ->
    Tx = new(<<"gateway_address">>, 1, 1),
    ?assertEqual(<<>>, signature(Tx)).

location_test() ->
    Tx = new(<<"gateway_address">>, 1, 1),
    ?assertEqual(1, location(Tx)).

nonce_test() ->
    Tx = new(<<"gateway_address">>, 1, 1),
    ?assertEqual(1, nonce(Tx)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(<<"gateway_address">>, 1, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_assert_location{signature = <<>>}), Sig1, PubKey)).

is_test() ->
    Tx0 = new(<<"gateway_address">>, 1, 1),
    ?assert(is(Tx0)).

-endif.
