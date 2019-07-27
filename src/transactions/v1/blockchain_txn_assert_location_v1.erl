%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_assert_location_v1_pb.hrl").
-include("blockchain_vars.hrl").

-export([
    new/6, new/7,
    hash/1,
    gateway/1,
    owner/1,
    payer/1,
    location/1,
    gateway_signature/1,
    owner_signature/1,
    payer_signature/1,
    nonce/1,
    staking_fee/1,
    fee/1,
    sign_request/2,
    sign_payer/2,
    sign/2,
    is_valid_owner/1,
    is_valid_gateway/1,
    is_valid_location/2,
    is_valid_payer/1,
    is_valid/2,
    absorb/2,
    calculate_staking_fee/1
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
-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Location :: location(),
          Nonce :: non_neg_integer(),
          StakingFee :: pos_integer(),
          Fee :: pos_integer()) -> txn_assert_location().
new(Gateway, Owner, Location, Nonce, StakingFee, Fee) ->
    #blockchain_txn_assert_location_v1_pb{
        gateway=Gateway,
        owner=Owner,
        payer = <<>>,
        location=h3:to_string(Location),
        gateway_signature = <<>>,
        owner_signature = <<>>,
        payer_signature = <<>>,
        nonce=Nonce,
        staking_fee=StakingFee,
        fee=Fee
    }.

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Payer :: libp2p_crypto:pubkey_bin(),
          Location :: location(),
          Nonce :: non_neg_integer(),
          StakingFee :: pos_integer(),
          Fee :: pos_integer()) -> txn_assert_location().
new(Gateway, Owner, Payer, Location, Nonce, StakingFee, Fee) ->
    #blockchain_txn_assert_location_v1_pb{
        gateway=Gateway,
        owner=Owner,
        payer = Payer,
        location=h3:to_string(Location),
        gateway_signature = <<>>,
        owner_signature = <<>>,
        payer_signature = <<>>,
        nonce=Nonce,
        staking_fee=StakingFee,
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
-spec payer(txn_assert_location()) -> libp2p_crypto:pubkey_bin() | <<>> | undefined.
payer(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.payer.

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
-spec payer_signature(txn_assert_location()) -> binary().
payer_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.payer_signature.

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
-spec staking_fee(txn_assert_location()) -> non_neg_integer().
staking_fee(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.staking_fee.

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
                                                       gateway_signature= <<>>,
                                                       payer_signature= <<>>},
    BinTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_assert_location_v1_pb{gateway_signature=SigFun(BinTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_payer(txn_assert_location(), fun()) -> txn_assert_location().
sign_payer(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature= <<>>,
                                                       gateway_signature= <<>>,
                                                       payer_signature= <<>>},
    EncodedTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_assert_location_v1_pb{payer_signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(Txn :: txn_assert_location(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature= <<>>,
                                                       gateway_signature= <<>>,
                                                       payer_signature= <<>>},
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
                                                       gateway_signature= <<>>,
                                                       payer_signature= <<>>},
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
                                                       gateway_signature= <<>>,
                                                       payer_signature= <<>>},
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
-spec is_valid_payer(txn_assert_location()) -> boolean().
is_valid_payer(#blockchain_txn_assert_location_v1_pb{payer=undefined}) ->
    %% no payer
    true;
is_valid_payer(#blockchain_txn_assert_location_v1_pb{payer= <<>>,
                                                     payer_signature= <<>>}) ->
    %% empty payer, empty signature
    true;
is_valid_payer(#blockchain_txn_assert_location_v1_pb{payer=PubKeyBin,
                                                     payer_signature=Signature}=Txn) ->

    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature= <<>>,
                                                       gateway_signature= <<>>,
                                                       payer_signature= <<>>},
    EncodedTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_assert_location(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case {?MODULE:is_valid_owner(Txn),
          ?MODULE:is_valid_gateway(Txn),
          ?MODULE:is_valid_payer(Txn)} of
        {false, _, _} ->
            {error, bad_owner_signature};
        {_, false, _} ->
            {error, bad_gateway_signature};
        {_, _, false} ->
            {error, bad_payer_signature};
        {true, true, true} ->
            Owner = ?MODULE:owner(Txn),
            Nonce = ?MODULE:nonce(Txn),
            StakingFee = ?MODULE:staking_fee(Txn),
            Fee = ?MODULE:fee(Txn),
            Payer = ?MODULE:payer(Txn),
            ActualPayer = case Payer == undefined orelse Payer == <<>> of
                true -> Owner;
                false -> Payer
            end,
            StakingFee = ?MODULE:staking_fee(Txn),
            ExpectedStakingFee = ?MODULE:calculate_staking_fee(Chain),
            case ExpectedStakingFee == StakingFee of
                false ->
                    {error, {wrong_stacking_fee, ExpectedStakingFee, StakingFee}};
                true ->
                    case blockchain_ledger_v1:check_dc_balance(ActualPayer, Fee + StakingFee, Ledger) of
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
                                            {ok, MinAssertH3Res} = blockchain:config(?min_assert_h3_res, Ledger),
                                            Location = ?MODULE:location(Txn),
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
    StakingFee = ?MODULE:staking_fee(Txn),
    Fee = ?MODULE:fee(Txn),
    Payer = ?MODULE:payer(Txn),
    ActualPayer = case Payer == undefined orelse Payer == <<>> of
        true -> Owner;
        false -> Payer
    end,
    case blockchain_ledger_v1:debit_fee(ActualPayer, Fee + StakingFee, Ledger) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            blockchain_ledger_v1:add_gateway_location(Gateway, Location, Nonce, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% TODO: We should calulate this (one we have a token burn rate)
%%       maybe using location and/or demand
%% @end
%%--------------------------------------------------------------------
-spec calculate_staking_fee(blockchain:blockchain()) -> non_neg_integer().
calculate_staking_fee(_Chain) ->
    1.

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
       payer= <<>>,
       gateway_signature= <<>>,
       owner_signature= <<>>,
       payer_signature= <<>>,
       location= h3:to_string(?TEST_LOCATION),
       nonce = 1,
       staking_fee = 1,
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
       staking_fee = 1,
       fee = 1
      }.

missing_payer_signature_new() ->
    #{public := PubKey, secret := _PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    #blockchain_txn_assert_location_v1_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       payer= libp2p_crypto:pubkey_to_bin(PubKey),
       payer_signature = <<>>,
       gateway_signature= <<>>,
       owner_signature= << >>,
       location= h3:to_string(599685771850416127),
       nonce = 1,
       staking_fee = 1,
       fee = 1
      }.

new_test() ->
    Tx = new(),
    ?assertEqual(Tx, new(<<"gateway_address">>, <<"owner_address">>, ?TEST_LOCATION, 1, 1, 1)).

location_test() ->
    Tx = new(),
    ?assertEqual(?TEST_LOCATION, location(Tx)).

nonce_test() ->
    Tx = new(),
    ?assertEqual(1, nonce(Tx)).

staking_fee_test() ->
    Tx = new(),
    ?assertEqual(1, staking_fee(Tx)).

fee_test() ->
    Tx = new(),
    ?assertEqual(1, fee(Tx)).

owner_test() ->
    Tx = new(),
    ?assertEqual(<<"owner_address">>, owner(Tx)).

gateway_test() ->
    Tx = new(),
    ?assertEqual(<<"gateway_address">>, gateway(Tx)).

payer_test() ->
    Tx = new(),
    ?assertEqual(<<>>, payer(Tx)).

owner_signature_test() ->
    Tx = new(),
    ?assertEqual(<<>>, owner_signature(Tx)).

gateway_signature_test() ->
    Tx = new(),
    ?assertEqual(<<>>, gateway_signature(Tx)).

payer_signature_missing_test() ->
    Tx = missing_payer_signature_new(),
    ?assertNot(is_valid_payer(Tx)).

payer_signature_test() ->
    Tx = new(),
    ?assertEqual(<<>>, payer_signature(Tx)).

sign_request_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Sig1 = gateway_signature(Tx1),
    BaseTxn1 = Tx1#blockchain_txn_assert_location_v1_pb{gateway_signature = <<>>, owner_signature = <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn1), Sig1, PubKey)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = owner_signature(Tx2),
    BaseTxn1 = Tx1#blockchain_txn_assert_location_v1_pb{gateway_signature = <<>>, owner_signature = <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn1), Sig2, PubKey)).

sign_payer_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign_payer(Tx1, SigFun),
    Tx3 = sign(Tx2, SigFun),
    Sig2 = payer_signature(Tx2),
    BaseTx1 = Tx3#blockchain_txn_assert_location_v1_pb{gateway_signature = <<>>, owner_signature = <<>>, payer_signature= <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_assert_location_v1_pb:encode_msg(BaseTx1), Sig2, PubKey)).

valid_location_test() ->
    Tx = new(),
    ?assert(is_valid_location(Tx, 12)).

invalid_location_test() ->
    Tx = invalid_new(),
    ?assertNot(is_valid_location(Tx, 12)).

-endif.
