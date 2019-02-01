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
    gateway_address :: libp2p_crypto:pubkey_bin(),
    owner_address :: libp2p_crypto:pubkey_bin(),
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
-spec new(GatewayAddress :: libp2p_crypto:pubkey_bin(),
          OwnerAddress :: libp2p_crypto:pubkey_bin(),
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
-spec gateway_address(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
gateway_address(Txn) ->
    Txn#txn_assert_location_v1.gateway_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
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
-spec absorb(txn_assert_location(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    GatewayAddress = ?MODULE:gateway_address(Txn),
    OwnerAddress = ?MODULE:owner_address(Txn),
    Location = ?MODULE:location(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Fee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:find_entry(OwnerAddress, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, LastEntry} ->
            PaymentNonce = blockchain_ledger_entry_v1:nonce(LastEntry) + 1,
            case blockchain_ledger_v1:debit_account(OwnerAddress, Fee, PaymentNonce, Ledger) of
                {error, _Reason}=Error -> Error;
                ok -> assert_gateway_location(GatewayAddress, Location, Nonce, Ledger)
            end
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec assert_gateway_location(libp2p_crypto:pubkey_bin(), non_neg_integer(), non_neg_integer(),
                              blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
assert_gateway_location(GatewayAddress, Location, Nonce, Ledger) ->
    case blockchain_ledger_v1:find_gateway_info(GatewayAddress, Ledger) of
        {error, _} ->
            {error, {unknown_gateway, GatewayAddress, Ledger}};
        {ok, GwInfo} ->
            lager:info("gw_info from ledger: ~p", [GwInfo]),
            LedgerNonce = blockchain_ledger_gateway_v1:nonce(GwInfo),
            lager:info("assert_gateway_location, gw_address: ~p, Nonce: ~p, LedgerNonce: ~p", [GatewayAddress, Nonce, LedgerNonce]),
            case Nonce == LedgerNonce + 1 of
                true ->
                    blockchain_ledger_v1:add_gateway_location(GatewayAddress, Location, Nonce, Ledger);
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
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Sig1 = gateway_signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_assert_location_v1{gateway_signature = <<>>, owner_signature = << >>}), Sig1, PubKey)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
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
