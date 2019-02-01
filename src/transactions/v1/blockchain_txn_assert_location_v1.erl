%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_assert_location_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    gateway/1,
    owner/1,
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

-type location() :: non_neg_integer(). %% h3 index
-type txn_assert_location() :: #blockchain_txn_assert_location_v1_pb{}.
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
    #blockchain_txn_assert_location_v1_pb{
       gateway=GatewayAddress,
       owner=OwnerAddress,
       location=Location,
       gateway_signature = <<>>,
       owner_signature = <<>>,
       nonce=Nonce,
       fee=1
      }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_assert_location()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature = <<>>, gateway_signature = <<>>},
    EncodedTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.gateway.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(txn_assert_location()) -> location().
location(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.location.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_signature(txn_assert_location()) -> binary().
gateway_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.gateway_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_signature(txn_assert_location()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.owner_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_assert_location()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_assert_location()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_request(Txn :: txn_assert_location(),
                   SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign_request(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature= <<>>,
                                                       gateway_signature= <<>>},
    BinTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_assert_location_v1_pb{gateway_signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(Txn :: txn_assert_location(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature= <<>>,
                                                       gateway_signature= <<>>},
    BinTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_assert_location_v1_pb{owner_signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, blockchain_txn_assert_location_v1_pb).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_assert_location(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    Gateway = ?MODULE:gateway(Txn),
    Owner = ?MODULE:owner(Txn),
    Location = ?MODULE:location(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Fee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:find_entry(Owner, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, LastEntry} ->
            PaymentNonce = blockchain_ledger_entry_v1:nonce(LastEntry) + 1,
            case blockchain_ledger_v1:debit_account(Owner, Fee, PaymentNonce, Ledger) of
                {error, _Reason}=Error -> Error;
                ok -> assert_gateway_location(Gateway, Location, Nonce, Ledger)
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
assert_gateway_location(Gateway, Location, Nonce, Ledger) ->
    case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
        {error, _} ->
            {error, {unknown_gateway, Gateway, Ledger}};
        {ok, GwInfo} ->
            lager:info("gw_info from ledger: ~p", [GwInfo]),
            LedgerNonce = blockchain_ledger_gateway_v1:nonce(GwInfo),
            lager:info("assert_gateway_location, gw_address: ~p, Nonce: ~p, LedgerNonce: ~p",
                       [Gateway, Nonce, LedgerNonce]),
            case Nonce == LedgerNonce + 1 of
                true ->
                    blockchain_ledger_v1:add_gateway_location(Gateway, Location, Nonce, Ledger);
                false ->
                    {error, {bad_nonce, {assert_location, Nonce, LedgerNonce}}}
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new() ->
    #blockchain_txn_assert_location_v1_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       gateway_signature= <<>>,
       owner_signature= << >>,
       location= 1,
       nonce = 1,
       fee = 1
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

owner_test() ->
    Tx = new(),
    ?assertEqual(<<"owner_address">>, owner(Tx)).

gateway_test() ->
    Tx = new(),
    ?assertEqual(<<"gateway_address">>, gateway(Tx)).

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
    BaseTxn1 = Tx1#blockchain_txn_assert_location_v1_pb{gateway_signature = <<>>, owner_signature = << >>},
    ?assert(libp2p_crypto:verify(blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn1), Sig1, PubKey)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = owner_signature(Tx2),
    BaseTxn1 = Tx1#blockchain_txn_assert_location_v1_pb{gateway_signature = <<>>, owner_signature = << >>},
    ?assert(libp2p_crypto:verify(blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn1), Sig2, PubKey)).

is_test() ->
    Tx0 = new(),
    ?assert(is(Tx0)).

-endif.
