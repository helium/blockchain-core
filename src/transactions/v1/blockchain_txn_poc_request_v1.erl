%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Create Proof of Coverage Request ==
%% Submitted by a gateway who wishes to initiate a PoC Challenge
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_request_v1).

-export([
    new/3,
    gateway_address/1,
    hash/1,
    onion/1,
    signature/1,
    fee/1,
    sign/2,
    is_valid/1,
    is/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_poc_request_v1, {
    gateway_address :: libp2p_crypto:pubkey_bin(),
    hash :: binary(),
    onion :: binary(),
    signature :: binary(),
    fee = 0 :: non_neg_integer()
}).

-type txn_poc_request() :: #txn_poc_request_v1{}.
-export_type([txn_poc_request/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), binary(),  binary()) -> txn_poc_request().
new(Address, Hash, Onion) ->
    #txn_poc_request_v1{
        gateway_address=Address,
        hash=Hash,
        onion=Onion,
        signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(txn_poc_request()) -> libp2p_crypto:pubkey_bin().
gateway_address(Txn) ->
    Txn#txn_poc_request_v1.gateway_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_poc_request()) -> binary().
hash(Txn) ->
    Txn#txn_poc_request_v1.hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec onion(txn_poc_request()) -> binary().
onion(Txn) ->
    Txn#txn_poc_request_v1.onion.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_poc_request()) -> binary().
signature(Txn) ->
    Txn#txn_poc_request_v1.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_poc_request()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_poc_request_v1.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_poc_request(), libp2p_crypto:sig_fun()) -> txn_poc_request().
sign(Txn, SigFun) ->
    Txn#txn_poc_request_v1{signature=SigFun(erlang:term_to_binary(Txn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_request()) -> boolean().
is_valid(Txn=#txn_poc_request_v1{gateway_address=GatewayAddress, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(GatewayAddress),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_poc_request_v1{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_poc_request_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_poc_request(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    case ?MODULE:is_valid(Txn) of
        true ->
            GatewayAddress = ?MODULE:gateway_address(Txn),
            Hash = ?MODULE:hash(Txn),
            Onion = ?MODULE:onion(Txn),
            blockchain_ledger_v1:request_poc(GatewayAddress, {Hash, Onion}, Ledger);
        false ->
            {error, bad_signature}
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_poc_request_v1{
        gateway_address= <<"gateway_address">>,
        hash= <<"hash">>,
        onion = <<"onion">>,
        signature= <<>>
    },
    ?assertEqual(Tx, new(<<"gateway_address">>, <<"hash">>, <<"onion">>)).

hash_test() ->
    Tx = new(<<"gateway_address">>, <<"hash">>, <<"onion">>),
    ?assertEqual(<<"hash">>, hash(Tx)).

onion_test() ->
    Tx = new(<<"gateway_address">>, <<"hash">>, <<"onion">>),
    ?assertEqual(<<"onion">>, onion(Tx)).

gateway_address_test() ->
    Tx = new(<<"gateway_address">>, <<"hash">>, <<"onion">>),
    ?assertEqual(<<"gateway_address">>, gateway_address(Tx)).

signature_test() ->
    Tx = new(<<"gateway_address">>, <<"hash">>, <<"onion">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ed25519),
    Tx0 = new(<<"gateway_address">>, <<"hash">>, <<"onion">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_poc_request_v1{signature = <<>>}), Sig1, PubKey)).

is_test() ->
    Tx = new(<<"gateway_address">>, <<"hash">>, <<"onion">>),
    ?assert(is(Tx)).

-endif.
