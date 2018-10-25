%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location).

-export([
    new/4
    ,gateway_address/1
    ,owner_address/1
    ,location/1
    ,gateway_signature/1
    ,owner_signature/1
    ,nonce/1
    ,fee/1
    ,sign_request/2
    ,sign/2
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_assert_location, {
    gateway_address :: libp2p_crypto:address()
    ,owner_address :: libp2p_crypto:address()
    ,gateway_signature :: binary()
    ,owner_signature :: binary()
    ,location :: location()
    ,nonce = 0 :: non_neg_integer()
    %% TODO: fee needs to be calculated on the fly
    %% using the gateways already present in the nearby geography
    ,fee = 1 :: non_neg_integer()
}).

-type location() :: non_neg_integer(). %% h3 index
-type txn_assert_location() :: #txn_assert_location{}.
-export_type([txn_assert_location/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(GatewayAddress :: libp2p_crypto:address(),
          OwnerAddress :: libp2p_crypto:address(),
          Location :: location(),
          Nonce :: non_neg_integer()) -> txn_assert_location().
new(GatewayAddress, OwnerAddress, Location, Nonce) ->
    #txn_assert_location{
        gateway_address=GatewayAddress
        ,owner_address=OwnerAddress
        ,location=Location
        ,gateway_signature = <<>>
        ,owner_signature = <<>>
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
-spec owner_address(txn_assert_location()) -> libp2p_crypto:address().
owner_address(Txn) ->
    Txn#txn_assert_location.owner_address.

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
-spec gateway_signature(txn_assert_location()) -> binary().
gateway_signature(Txn) ->
    Txn#txn_assert_location.gateway_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_signature(txn_assert_location()) -> binary().
owner_signature(Txn) ->
    Txn#txn_assert_location.owner_signature.

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
-spec fee(txn_assert_location()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_assert_location.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_request(Txn :: txn_assert_location(),
                   SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign_request(Txn, SigFun) ->
    BinTxn = erlang:term_to_binary(Txn#txn_assert_location{owner_signature= <<>>
                                                           ,gateway_signature= <<>>}),
    Txn#txn_assert_location{gateway_signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(Txn :: txn_assert_location(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign(Txn, SigFun) ->
    BinTxn = erlang:term_to_binary(Txn#txn_assert_location{owner_signature= <<>>
                                                           ,gateway_signature= <<>>}),
    Txn#txn_assert_location{owner_signature=SigFun(BinTxn)}.

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

new() ->
    #txn_assert_location{
       gateway_address= <<"gateway_address">>
       ,owner_address= <<"owner_address">>
       ,gateway_signature= <<>>
       ,owner_signature= << >>
       ,location= 1
       ,nonce = 1
      }.

new_test() ->
    Tx = new(),
    ?assertEqual(Tx, new(<<"gateway_address">>, <<"owner_address">>, 1, 1)).

location_test() ->
    Tx = new(),
    ?assertEqual(1, location(Tx)).

nonce_test() ->
    Tx = new(),
    ?assertEqual(1, nonce(Tx)).

fee_test() ->
    Tx = new(),
    ?assertEqual(0, fee(Tx)).

owner_address_test() ->
    Tx = new(),
    ?assertEqual(<<"owner_address">>, owner_address(Tx)).

gateway_address_test() ->
    Tx = new(),
    ?assertEqual(<<"gateway_address">>, gateway_address(Tx)).

owner_signature_test() ->
    Tx = new(),
    ?assertEqual(<<>>, owner_signature(Tx)).

gateway_signature_test() ->
    Tx = new(),
    ?assertEqual(<<>>, gateway_signature(Tx)).

sign_request_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Sig1 = gateway_signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_assert_location{gateway_signature = <<>>, owner_signature = << >>}), Sig1, PubKey)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = owner_signature(Tx2),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_assert_location{gateway_signature = <<>>, owner_signature = << >>}), Sig2, PubKey)).

is_test() ->
    Tx0 = new(),
    ?assert(is(Tx0)).

-endif.
