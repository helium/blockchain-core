%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location_v1).

-behavior(blockchain_txn).

-export([
    new/4,
    hash/1,
    gateway_address/1,
    owner_address/1,
    location/1,
    gateway_signature/1,
    owner_signature/1,
    nonce/1,
    fee/1,
    sign_request/2,
    sign/2,
    is/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_assert_location_v1, {
    gateway_address :: libp2p_crypto:address(),
    owner_address :: libp2p_crypto:address(),
    gateway_signature :: binary(),
    owner_signature :: binary(),
    location :: location(),
    nonce = 0 :: non_neg_integer(),
    %% TODO: fee needs to be calculated on the fly
    %% using the gateways already present in the nearby geography
    fee = 1 :: non_neg_integer()
}).

-type location() :: non_neg_integer(). %% h3 index
-type txn_assert_location() :: #txn_assert_location_v1{}.
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
    #txn_assert_location_v1{
        gateway_address=GatewayAddress,
        owner_address=OwnerAddress,
        location=Location,
        gateway_signature = <<>>,
        owner_signature = <<>>,
        nonce=Nonce
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_assert_location()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#txn_assert_location_v1{owner_signature = <<>>, gateway_signature = <<>>},
    crypto:hash(sha256, erlang:term_to_binary(BaseTxn)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(txn_assert_location()) -> libp2p_crypto:address().
gateway_address(Txn) ->
    Txn#txn_assert_location_v1.gateway_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(txn_assert_location()) -> libp2p_crypto:address().
owner_address(Txn) ->
    Txn#txn_assert_location_v1.owner_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(txn_assert_location()) -> location().
location(Txn) ->
    Txn#txn_assert_location_v1.location.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_signature(txn_assert_location()) -> binary().
gateway_signature(Txn) ->
    Txn#txn_assert_location_v1.gateway_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_signature(txn_assert_location()) -> binary().
owner_signature(Txn) ->
    Txn#txn_assert_location_v1.owner_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_assert_location()) -> non_neg_integer().
nonce(Txn) ->
    Txn#txn_assert_location_v1.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_assert_location()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_assert_location_v1.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_request(Txn :: txn_assert_location(),
                   SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign_request(Txn, SigFun) ->
    BinTxn = erlang:term_to_binary(Txn#txn_assert_location_v1{owner_signature= <<>>,
                                                              gateway_signature= <<>>}),
    Txn#txn_assert_location_v1{gateway_signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(Txn :: txn_assert_location(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign(Txn, SigFun) ->
    BinTxn = erlang:term_to_binary(Txn#txn_assert_location_v1{owner_signature= <<>>,
                                                              gateway_signature= <<>>}),
    Txn#txn_assert_location_v1{owner_signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_assert_location_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_assert_location(), blockchain_ledger_v1:ledger()) -> {ok, blockchain_ledger_v1:ledger()}
                                                                   | {error, any()}.
absorb(Txn, Ledger0) ->
    GatewayAddress = ?MODULE:gateway_address(Txn),
    OwnerAddress = ?MODULE:owner_address(Txn),
    Location = ?MODULE:location(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Fee = ?MODULE:fee(Txn),
    Entries = blockchain_ledger_v1:entries(Ledger0),
    LastEntry = blockchain_ledger_v1:find_entry(OwnerAddress, Entries),
    PaymentNonce = blockchain_ledger_v1:payment_nonce(LastEntry) + 1,
    case blockchain_ledger_v1:debit_account(OwnerAddress, Fee, PaymentNonce, Ledger0) of
        {error, _Reason}=Error -> Error;
        Ledger1 ->
            case assert_gateway_location(GatewayAddress, Location, Nonce, Ledger1) of
                {error, _}=Error2 -> Error2;
                Ledger2 ->
                    {ok, Ledger2}
            end
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
assert_gateway_location(GatewayAddress, Location, Nonce, Ledger0) ->
    case blockchain_ledger_v1:find_gateway_info(GatewayAddress, Ledger0) of
        undefined ->
            {error, {unknown_gateway, GatewayAddress, Ledger0}};
        GwInfo ->
            lager:info("gw_info from ledger: ~p", [GwInfo]),
            LedgerNonce = blockchain_ledger_gateway_v1:nonce(GwInfo),
            lager:info("assert_gateway_location, gw_address: ~p, Nonce: ~p, LedgerNonce: ~p", [GatewayAddress, Nonce, LedgerNonce]),
            case Nonce == LedgerNonce + 1 of
                true ->
                    %% update the ledger with new gw_info
                    case blockchain_ledger_v1:add_gateway_location(GatewayAddress, Location, Nonce, Ledger0) of
                        {error, _Reason} ->
                            lager:error("fail to add gateway location: ~p", _Reason),
                            Ledger0;
                        Ledger1 ->
                            Ledger1
                    end;
                false ->
                    {error, {bad_nonce, {assert_location, Nonce, LedgerNonce}}}
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new() ->
    #txn_assert_location_v1{
       gateway_address= <<"gateway_address">>,
       owner_address= <<"owner_address">>,
       gateway_signature= <<>>,
       owner_signature= << >>,
       location= 1,
       nonce = 1
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
    ?assertEqual(1, fee(Tx)).

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
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_assert_location_v1{gateway_signature = <<>>, owner_signature = << >>}), Sig1, PubKey)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = owner_signature(Tx2),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_assert_location_v1{gateway_signature = <<>>, owner_signature = << >>}), Sig2, PubKey)).

is_test() ->
    Tx0 = new(),
    ?assert(is(Tx0)).

-endif.
