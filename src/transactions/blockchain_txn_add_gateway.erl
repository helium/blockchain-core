%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Add Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_add_gateway).

-export([
    new/2
    ,owner_address/1
    ,gateway_address/1
    ,owner_signature/1
    ,gateway_signature/1
    ,sign/2
    ,sign_request/2
    ,is_valid_gateway/1
    ,is_valid_owner/1
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_add_gateway, {
    owner_address :: libp2p_crypto:address()
    ,gateway_address :: libp2p_crypto:address()
    ,owner_signature = <<>> :: binary()
    ,gateway_signature = <<>> :: binary()
}).

-type txn_add_gateway() :: #txn_add_gateway{}.
-export_type([txn_add_gateway/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), libp2p_crypto:address()) -> txn_add_gateway().
new(OwnerAddress, GatewayAddress) ->
    #txn_add_gateway{
        owner_address=OwnerAddress
        ,gateway_address=GatewayAddress
        ,owner_signature = <<>>
        ,gateway_signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(txn_add_gateway()) -> libp2p_crypto:address().
owner_address(Txn) ->
    Txn#txn_add_gateway.owner_address.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(txn_add_gateway()) -> libp2p_crypto:address().
gateway_address(Txn) ->
    Txn#txn_add_gateway.gateway_address.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_signature(txn_add_gateway()) -> binary().
owner_signature(Txn) ->
    Txn#txn_add_gateway.owner_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_signature(txn_add_gateway()) -> binary().
gateway_signature(Txn) ->
    Txn#txn_add_gateway.gateway_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_add_gateway(), libp2p_crypto:sig_fun()) -> txn_add_gateway().
sign(Txn, SigFun) ->
    BinTxn = erlang:term_to_binary(Txn#txn_add_gateway{owner_signature= <<>>
                                                       ,gateway_signature= <<>>}),
    Txn#txn_add_gateway{owner_signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_request(txn_add_gateway(), fun()) -> txn_add_gateway().
sign_request(Txn, SigFun) ->
    BinTxn = erlang:term_to_binary(Txn#txn_add_gateway{owner_signature= <<>>
                                                       ,gateway_signature= <<>>}),
    Txn#txn_add_gateway{gateway_signature=SigFun(BinTxn)}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_gateway(txn_add_gateway()) -> boolean().
is_valid_gateway(#txn_add_gateway{gateway_address=Address
                                  ,gateway_signature=Signature}=Txn) ->
    BinTxn = erlang:term_to_binary(Txn#txn_add_gateway{owner_signature= <<>>
                                                       ,gateway_signature= <<>>}),
    PubKey = libp2p_crypto:address_to_pubkey(Address),
    libp2p_crypto:verify(BinTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_owner(txn_add_gateway()) -> boolean().
is_valid_owner(#txn_add_gateway{owner_address=Address
                                ,owner_signature=Signature}=Txn) ->
    BinTxn = erlang:term_to_binary(Txn#txn_add_gateway{owner_signature= <<>>
                                                       ,gateway_signature= <<>>}),
    PubKey = libp2p_crypto:address_to_pubkey(Address),
    libp2p_crypto:verify(BinTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_add_gateway).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_add_gateway{
        owner_address= <<"owner_address">>
        ,gateway_address= <<"gateway_address">>
        ,owner_signature= <<>>
        ,gateway_signature = <<>>
    },
    ?assertEqual(Tx, new(<<"owner_address">>, <<"gateway_address">>)).

owner_address_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<"owner_address">>, owner_address(Tx)).

gateway_address_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<"gateway_address">>, gateway_address(Tx)).

owner_signature_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<>>, owner_signature(Tx)).

gateway_signature_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<>>, gateway_signature(Tx)).

sign_request_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Sig1 = gateway_signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_add_gateway{gateway_signature = <<>>, owner_signature = << >>}), Sig1, PubKey)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = owner_signature(Tx2),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_add_gateway{gateway_signature = <<>>, owner_signature = << >>}), Sig2, PubKey)).

is_test() ->
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>),
    ?assert(is(Tx0)).

-endif.
