%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_assert_location_v1_pb.hrl").

-export([
    new/5,
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
    is_valid_owner/1,
    is_valid_gateway/1,
    is_valid_location/2,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type location() :: h3:h3index().
-type txn_assert_location() :: #blockchain_txn_assert_location_v1_pb{}.
-export_type([txn_assert_location/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(GatewayAddress :: libp2p_crypto:pubkey_bin(),
          OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: location(),
          Nonce :: non_neg_integer(),
          Fee :: pos_integer()) -> txn_assert_location().
new(GatewayAddress, OwnerAddress, Location, Nonce, Fee) ->
    #blockchain_txn_assert_location_v1_pb{
       gateway=GatewayAddress,
       owner=OwnerAddress,
       location=h3:to_string(Location),
       gateway_signature = <<>>,
       owner_signature = <<>>,
       nonce=Nonce,
       fee=Fee
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
    h3:from_string(Txn#blockchain_txn_assert_location_v1_pb.location).

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
-spec is_valid_gateway(txn_assert_location()) -> boolean().
is_valid_gateway(#blockchain_txn_assert_location_v1_pb{gateway=PubKeyBin,
                                                       gateway_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature= <<>>,
                                                       gateway_signature= <<>>},
    EncodedTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_owner(txn_assert_location()) -> boolean().
is_valid_owner(#blockchain_txn_assert_location_v1_pb{owner=PubKeyBin,
                                                     owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature= <<>>,
                                                       gateway_signature= <<>>},
    EncodedTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid_location(txn_assert_location(), pos_integer()) -> boolean().
is_valid_location(#blockchain_txn_assert_location_v1_pb{location=Location}, MinH3Res) ->
    h3:get_resolution(h3:from_string(Location)) >= MinH3Res.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_assert_location(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, MinAssertH3Res} = blockchain:config(min_assert_h3_res, Ledger),
    case ?MODULE:is_valid_gateway(Txn) andalso ?MODULE:is_valid_owner(Txn) of
        false ->
            {error, bad_signature};
        true ->
            Owner = ?MODULE:owner(Txn),
            Nonce = ?MODULE:nonce(Txn),
            Fee = ?MODULE:fee(Txn),
            Location = ?MODULE:location(Txn),
            case blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger) of
                {error, _}=Error ->
                    Error;
                ok ->
                    Gateway = ?MODULE:gateway(Txn),
                    case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
                        {error, _} ->
                            {error, {unknown_gateway, Gateway, Ledger}};
                        {ok, GwInfo} ->
                            GwOwner = blockchain_ledger_gateway_v1:owner_address(GwInfo),
                            case Owner == GwOwner of
                                false ->
                                    {error, {bad_owner, {assert_location, Owner, GwOwner}}};
                                true ->
                                    case ?MODULE:is_valid_location(Txn, MinAssertH3Res) of
                                        false ->
                                            {error, {insufficient_assert_res, {assert_location, Location, Gateway}}};
                                        true ->
                                            LedgerNonce = blockchain_ledger_gateway_v1:nonce(GwInfo),
                                            case Nonce =:= LedgerNonce + 1 of
                                                false ->
                                                    {error, {bad_nonce, {assert_location, Nonce, LedgerNonce}}};
                                                true ->
                                                    ok
                                            end
                                    end
                            end
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_assert_location(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Gateway = ?MODULE:gateway(Txn),
    Owner = ?MODULE:owner(Txn),
    Location = ?MODULE:location(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Fee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            blockchain_ledger_v1:add_gateway_location(Gateway, Location, Nonce, Ledger)
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-define(TEST_LOCATION, 631210968840687103).

new() ->
    #blockchain_txn_assert_location_v1_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       gateway_signature= <<>>,
       owner_signature= << >>,
       location= h3:to_string(?TEST_LOCATION),
       nonce = 1,
       fee = 1
      }.

invalid_new() ->
    #blockchain_txn_assert_location_v1_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       gateway_signature= <<>>,
       owner_signature= << >>,
       location= h3:to_string(599685771850416127),
       nonce = 1,
       fee = 1
      }.

new_test() ->
    Tx = new(),
    ?assertEqual(Tx, new(<<"gateway_address">>, <<"owner_address">>, ?TEST_LOCATION, 1, 1)).

location_test() ->
    Tx = new(),
    ?assertEqual(?TEST_LOCATION, location(Tx)).

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

valid_location_test() ->
    Tx = new(),
    ?assert(is_valid_location(Tx, 12)).

invalid_location_test() ->
    Tx = invalid_new(),
    ?assertNot(is_valid_location(Tx, 12)).

-endif.
