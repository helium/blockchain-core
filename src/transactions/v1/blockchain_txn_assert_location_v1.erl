%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include_lib("helium_proto/include/blockchain_txn_assert_location_v1_pb.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").

-export([
    new/4, new/5,
    hash/1,
    gateway/1,
    owner/1,
    payer/1,
    location/1,
    gateway_signature/1,
    owner_signature/1,
    payer_signature/1,
    nonce/1,
    staking_fee/1, staking_fee/2,
    fee/1, fee/2,
    fee_payer/2,
    sign_request/2,
    sign_payer/2,
    sign/2,
    is_valid_owner/1,
    is_valid_gateway/1,
    is_valid_location/2,
    is_valid_payer/1,
    is_valid/2,
    absorb/2,
    calculate_fee/2, calculate_fee/5, calculate_staking_fee/2, calculate_staking_fee/5,
    print/1,
    json_type/0,
    to_json/2
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
          Nonce :: non_neg_integer()) -> txn_assert_location().
new(Gateway, Owner, Location, Nonce) ->
    #blockchain_txn_assert_location_v1_pb{
        gateway=Gateway,
        owner=Owner,
        payer = <<>>,
        location=h3:to_string(Location),
        gateway_signature = <<>>,
        owner_signature = <<>>,
        payer_signature = <<>>,
        nonce=Nonce,
        staking_fee=?LEGACY_STAKING_FEE,
        fee=?LEGACY_TXN_FEE
    }.

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Payer :: libp2p_crypto:pubkey_bin(),
          Location :: location(),
          Nonce :: non_neg_integer()) -> txn_assert_location().
new(Gateway, Owner, Payer, Location, Nonce) ->
    #blockchain_txn_assert_location_v1_pb{
        gateway=Gateway,
        owner=Owner,
        payer = Payer,
        location=h3:to_string(Location),
        gateway_signature = <<>>,
        owner_signature = <<>>,
        payer_signature = <<>>,
        nonce=Nonce,
        staking_fee=?LEGACY_STAKING_FEE,
        fee=?LEGACY_TXN_FEE
    }.

-spec hash(txn_assert_location()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v1_pb{owner_signature = <<>>, gateway_signature = <<>>},
    EncodedTxn = blockchain_txn_assert_location_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec gateway(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.gateway.

-spec owner(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.owner.

-spec payer(txn_assert_location()) -> libp2p_crypto:pubkey_bin() | <<>> | undefined.
payer(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.payer.

-spec location(txn_assert_location()) -> location().
location(Txn) ->
    h3:from_string(Txn#blockchain_txn_assert_location_v1_pb.location).

-spec gateway_signature(txn_assert_location()) -> binary().
gateway_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.gateway_signature.

-spec owner_signature(txn_assert_location()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.owner_signature.

-spec payer_signature(txn_assert_location()) -> binary().
payer_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.payer_signature.

-spec nonce(txn_assert_location()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.nonce.

-spec staking_fee(txn_assert_location()) -> non_neg_integer().
staking_fee(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.staking_fee.

-spec staking_fee(txn_assert_location(), non_neg_integer()) -> txn_assert_location().
staking_fee(Txn, Fee) ->
    Txn#blockchain_txn_assert_location_v1_pb{staking_fee=Fee}.

-spec fee(txn_assert_location()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_assert_location_v1_pb.fee.

-spec fee_payer(txn_assert_location(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    Payer = ?MODULE:payer(Txn),
    case Payer == undefined orelse Payer == <<>> of
        true -> ?MODULE:owner(Txn);
        false -> Payer
    end.

-spec fee(txn_assert_location(), non_neg_integer()) -> txn_assert_location().
fee(Txn, Fee) ->
    Txn#blockchain_txn_assert_location_v1_pb{fee=Fee}.


%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_assert_location(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_assert_location(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    case Txn#blockchain_txn_assert_location_v1_pb.payer of
        Payer when Payer == undefined; Payer == <<>> ->
          %% no payer signature if there's no payer
          ?calculate_fee(Txn#blockchain_txn_assert_location_v1_pb{fee=0, staking_fee=0,
                                                       owner_signature= <<0:512>>,
                                                       gateway_signature = <<0:512>>,
                                                       payer_signature= <<>>}, Ledger, DCPayloadSize, TxnFeeMultiplier);
        _ ->
          ?calculate_fee(Txn#blockchain_txn_assert_location_v1_pb{fee=0, staking_fee=0,
                                                       owner_signature= <<0:512>>,
                                                       gateway_signature = <<0:512>>,
                                                       payer_signature= <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the staking fee using the price oracles
%% returns the fee in DC
%% @end
%%--------------------------------------------------------------------
-spec calculate_staking_fee(txn_assert_location(), blockchain:blockchain()) -> non_neg_integer().
calculate_staking_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Gateway = ?MODULE:gateway(Txn),
    Fee =
        case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
            {error, _} ->
                %% err we cant find gateway what to do??
                %% defaulting to regular fee
                 blockchain_ledger_v1:staking_fee_txn_assert_location_v1(Ledger);
            {ok, GwInfo} ->
                GWMode = blockchain_ledger_gateway_v2:mode(GwInfo),
                staking_fee_for_gw_mode(GWMode, Ledger)
        end,
    calculate_staking_fee(Txn, Ledger, Fee, [],blockchain_ledger_v1:txn_fees_active(Ledger)).
-spec calculate_staking_fee(txn_assert_location(), blockchain_ledger_v1:ledger(), non_neg_integer(), [{atom(), non_neg_integer()}], boolean()) -> non_neg_integer().
calculate_staking_fee(_Txn, _Ledger, _Fee, _ExtraData, false) ->
    ?LEGACY_STAKING_FEE;
calculate_staking_fee(_Txn, _Ledger, Fee, _ExtraData, true) ->
    Fee.

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
-spec is_valid(txn_assert_location(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
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
            Payer = ?MODULE:payer(Txn),
            ActualPayer = case Payer == undefined orelse Payer == <<>> of
                true -> Owner;
                false -> Payer
            end,
            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
            StakingFee = ?MODULE:staking_fee(Txn),
            ExpectedStakingFee = ?MODULE:calculate_staking_fee(Txn, Chain),
            TxnFee = ?MODULE:fee(Txn),
            ExpectedTxnFee = calculate_fee(Txn, Chain),
            case {(ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled), ExpectedStakingFee == StakingFee} of
                {false,_} ->
                    {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                {_,false} ->
                    {error, {wrong_staking_fee, {ExpectedStakingFee, StakingFee}}};
                {true, true} ->
                    case blockchain_ledger_v1:check_dc_or_hnt_balance(ActualPayer, TxnFee + StakingFee, Ledger, AreFeesEnabled) of
                        {error, _}=Error ->
                            Error;
                        ok ->
                            Gateway = ?MODULE:gateway(Txn),
                            case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
                                {error, _} ->
                                    {error, {unknown_gateway, {Gateway, Ledger}}};
                                {ok, GwInfo} ->
                                    GwOwner = blockchain_ledger_gateway_v2:owner_address(GwInfo),
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
                                                    LedgerNonce = blockchain_ledger_gateway_v2:nonce(GwInfo),
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
-spec absorb(txn_assert_location(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Gateway = ?MODULE:gateway(Txn),
    Location = ?MODULE:location(Txn),
    Nonce = ?MODULE:nonce(Txn),
    StakingFee = ?MODULE:staking_fee(Txn),
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    ActualPayer = fee_payer(Txn, Ledger),

    {ok, OldGw} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    case blockchain_ledger_v1:debit_fee(ActualPayer, Fee + StakingFee, Ledger, AreFeesEnabled, Hash, Chain) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            blockchain_ledger_v1:add_gateway_location(Gateway, Location, Nonce, Ledger),
            Res =
              case blockchain:config(?poc_target_hex_parent_res, Ledger) of
                {ok, ResV} -> ResV;
                _ -> 5
              end,
            OldLoc = blockchain_ledger_gateway_v2:location(OldGw),
            OldHex =
                case OldLoc of
                    undefined ->
                        undefined;
                    _ ->
                        h3:parent(OldLoc, Res)
                end,
            Hex = h3:parent(Location, Res),

            case {OldLoc, Location, Hex} of
                {undefined, New, _H} ->
                    %% no previous location
                    %% add new hex
                    blockchain_ledger_v1:add_to_hex(New, Gateway, Res, Ledger);
                {Old, Old, _H} ->
                    %% why even check this, same loc as old loc
                    ok;
                {Old, New, H} when H == OldHex ->
                    %% moved within the same Hex
                    %% remove old location of this gateway from h3dex
                  blockchain_ledger_v1:remove_from_hex(Old, Gateway, Res, Ledger),
                  %% add new location of this gateway to h3dex
                  blockchain_ledger_v1:add_to_hex(New, Gateway, Res, Ledger);
                {Old, New, _H} ->
                    %% moved to a different hex
                    %% remove this hex
                  blockchain_ledger_v1:remove_from_hex(Old, Gateway, Res, Ledger),
                  %% add new hex
                  blockchain_ledger_v1:add_to_hex(New, Gateway, Res, Ledger)
            end,

            case blockchain:config(?poc_version, Ledger) of
                {ok, V} when V > 3 ->
                    %% don't update neighbours anymore
                    ok;
                _ ->
                    %% TODO gc this nonsense in some deterministic way
                    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
                    Neighbors = blockchain_poc_path:neighbors(Gateway, Gateways, Ledger),
                    {ok, Gw} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
                    ok = blockchain_ledger_v1:fixup_neighbors(Gateway, Gateways, Neighbors, Ledger),
                    Gw1 = blockchain_ledger_gateway_v2:neighbors(Neighbors, Gw),
                    ok = blockchain_ledger_v1:update_gateway(Gw1, Gateway, Ledger)

            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_assert_location()) -> iodata().
print(undefined) -> <<"type=assert_location, undefined">>;
print(#blockchain_txn_assert_location_v1_pb{
        gateway = Gateway, owner = Owner, payer = Payer,
        location = Loc, gateway_signature = GS,
        owner_signature = OS, payer_signature = PS, nonce = Nonce,
        staking_fee = StakingFee, fee = Fee}) ->
    io_lib:format("type=assert_location, gateway=~p, owner=~p, payer=~p, location=~p, gateway_signature=~p, owner_signature=~p, payer_signature=~p, nonce=~p, staking_fee=~p, fee=~p",
                  [?TO_ANIMAL_NAME(Gateway), ?TO_B58(Owner), ?TO_B58(Payer),
		   Loc, GS, OS, PS, Nonce, StakingFee, Fee]).

json_type() ->
    <<"assert_location_v1">>.

-spec to_json(txn_assert_location(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      gateway => ?BIN_TO_B58(gateway(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      payer => ?MAYBE_B58(payer(Txn)),
      location => ?MAYBE_H3(location(Txn)),
      nonce => nonce(Txn),
      staking_fee => staking_fee(Txn),
      fee => fee(Txn)
     }.

-spec staking_fee_for_gw_mode(blockchain_ledger_gateway_v2:mode(), blockchain_ledger_v1:ledger()) -> non_neg_integer().
staking_fee_for_gw_mode(dataonly, Ledger)->
    blockchain_ledger_v1:staking_fee_txn_assert_location_dataonly_gateway_v1(Ledger);
staking_fee_for_gw_mode(light, Ledger)->
    blockchain_ledger_v1:staking_fee_txn_assert_location_light_gateway_v1(Ledger);
staking_fee_for_gw_mode(_, Ledger)->
    blockchain_ledger_v1:staking_fee_txn_assert_location_v1(Ledger).

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
       staking_fee = ?LEGACY_STAKING_FEE,
       fee = ?LEGACY_TXN_FEE
      }.

invalid_new() ->
    #blockchain_txn_assert_location_v1_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       gateway_signature= <<>>,
       owner_signature= << >>,
       location= h3:to_string(599685771850416127),
       nonce = 1,
       staking_fee = ?LEGACY_STAKING_FEE,
       fee = ?LEGACY_TXN_FEE
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
       staking_fee = ?LEGACY_STAKING_FEE,
       fee = ?LEGACY_TXN_FEE
      }.

new_test() ->
    Tx = new(),
    ?assertEqual(Tx, new(<<"gateway_address">>, <<"owner_address">>, ?TEST_LOCATION, 1)).

location_test() ->
    Tx = new(),
    ?assertEqual(?TEST_LOCATION, location(Tx)).

nonce_test() ->
    Tx = new(),
    ?assertEqual(1, nonce(Tx)).

staking_fee_test() ->
    Tx = new(),
    ?assertEqual(?LEGACY_STAKING_FEE, staking_fee(Tx)).

fee_test() ->
    Tx = new(),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

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

to_json_test() ->
    Tx = new(),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, gateway, owner, payer, location, nonce, staking_fee, fee])).

-endif.
