%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Assert Location V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_assert_location_v2).

-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include_lib("helium_proto/include/blockchain_txn_assert_location_v2_pb.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").

-export([
    new/4, new/5,
    hash/1,
    gateway/1,
    owner/1,
    payer/1,
    location/1, location/2,
    gain/1, gain/2,
    elevation/1, elevation/2,
    owner_signature/1,
    payer_signature/1,
    nonce/1,
    staking_fee/1, staking_fee/2,
    fee/1, fee/2,
    fee_payer/2,
    sign_payer/2,
    sign/2,
    is_valid_owner/1,
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
-type txn_assert_location() :: #blockchain_txn_assert_location_v2_pb{}.
-export_type([txn_assert_location/0]).

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Location :: location(),
          Nonce :: non_neg_integer()) -> txn_assert_location().
new(Gateway, Owner, Location, Nonce) ->
    #blockchain_txn_assert_location_v2_pb{
        gateway=Gateway,
        owner=Owner,
        payer = <<>>,
        location=h3:to_string(Location),
        owner_signature = <<>>,
        payer_signature = <<>>,
        nonce=Nonce,
        gain=?DEFAULT_GAIN,
        elevation=?DEFAULT_ELEVATION,
        staking_fee=?LEGACY_STAKING_FEE,
        fee=?LEGACY_TXN_FEE
    }.

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Payer :: libp2p_crypto:pubkey_bin(),
          Location :: location(),
          Nonce :: non_neg_integer()) -> txn_assert_location().
new(Gateway, Owner, Payer, Location, Nonce) ->
    #blockchain_txn_assert_location_v2_pb{
        gateway=Gateway,
        owner=Owner,
        payer = Payer,
        location=h3:to_string(Location),
        owner_signature = <<>>,
        payer_signature = <<>>,
        nonce=Nonce,
        gain=?DEFAULT_GAIN,
        elevation=?DEFAULT_ELEVATION,
        staking_fee=?LEGACY_STAKING_FEE,
        fee=?LEGACY_TXN_FEE
    }.

-spec hash(txn_assert_location()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v2_pb{owner_signature = <<>>},
    EncodedTxn = blockchain_txn_assert_location_v2_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec gateway(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.gateway.

-spec owner(txn_assert_location()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.owner.

-spec payer(txn_assert_location()) -> libp2p_crypto:pubkey_bin() | <<>> | undefined.
payer(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.payer.

-spec location(txn_assert_location()) -> location().
location(Txn) ->
    h3:from_string(Txn#blockchain_txn_assert_location_v2_pb.location).

-spec location(Location :: h3:index(), Txn :: txn_assert_location()) -> location().
location(Location, Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb{location = h3:to_string(Location)}.

-spec gain(Txn :: txn_assert_location()) -> integer().
gain(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.gain.

-spec gain(Txn :: txn_assert_location(), Gain :: integer()) -> txn_assert_location().
gain(Txn, Gain) ->
    Txn#blockchain_txn_assert_location_v2_pb{gain = Gain}.

-spec elevation(txn_assert_location()) -> integer().
elevation(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.elevation.

-spec elevation(Txn :: txn_assert_location(), Elevation :: integer()) -> txn_assert_location().
elevation(Txn, Elevation) ->
    Txn#blockchain_txn_assert_location_v2_pb{elevation = Elevation}.

-spec owner_signature(txn_assert_location()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.owner_signature.

-spec payer_signature(txn_assert_location()) -> binary().
payer_signature(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.payer_signature.

-spec nonce(txn_assert_location()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.nonce.

-spec staking_fee(txn_assert_location()) -> non_neg_integer().
staking_fee(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.staking_fee.

-spec staking_fee(txn_assert_location(), non_neg_integer()) -> txn_assert_location().
staking_fee(Txn, Fee) ->
    Txn#blockchain_txn_assert_location_v2_pb{staking_fee=Fee}.

-spec fee(txn_assert_location()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_assert_location_v2_pb.fee.

-spec fee(txn_assert_location(), non_neg_integer()) -> txn_assert_location().
fee(Txn, Fee) ->
    Txn#blockchain_txn_assert_location_v2_pb{fee=Fee}.

-spec fee_payer(txn_assert_location(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    Payer = payer(Txn),
    case Payer == undefined orelse Payer == <<>> of
        true -> owner(Txn);
        false -> Payer
    end.

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
    case Txn#blockchain_txn_assert_location_v2_pb.payer of
        Payer when Payer == undefined; Payer == <<>> ->
          %% no payer signature if there's no payer
          ?calculate_fee(Txn#blockchain_txn_assert_location_v2_pb{fee=0, staking_fee=0,
                                                       owner_signature= <<0:512>>,
                                                       payer_signature= <<>>}, Ledger, DCPayloadSize, TxnFeeMultiplier);
        _ ->
          ?calculate_fee(Txn#blockchain_txn_assert_location_v2_pb{fee=0, staking_fee=0,
                                                       owner_signature= <<0:512>>,
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
    calculate_staking_fee(Txn, Ledger, Fee, [], blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_staking_fee(txn_assert_location(), blockchain_ledger_v1:ledger(), non_neg_integer(), [{atom(), non_neg_integer()}], boolean()) -> non_neg_integer().
calculate_staking_fee(_Txn, _Ledger, _Fee, _ExtraData, false) ->
    ?LEGACY_STAKING_FEE;
calculate_staking_fee(_Txn, ignore_ledger, Fee, _ExtraData, true) ->
    %% For testing
    Fee;
calculate_staking_fee(Txn, Ledger, Fee, _ExtraData, true) ->
    %% fee is active
    %% 0 staking_fee if the location hasn't actually changed
    %% it's possible that the only change are gain/elevation
    case is_new_location(Txn, Ledger) of
        true -> Fee;
        false -> 0
    end.

-spec sign_payer(txn_assert_location(), fun()) -> txn_assert_location().
sign_payer(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v2_pb{owner_signature= <<>>,
                                                       payer_signature= <<>>},
    EncodedTxn = blockchain_txn_assert_location_v2_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_assert_location_v2_pb{payer_signature=SigFun(EncodedTxn)}.

-spec sign(Txn :: txn_assert_location(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_assert_location().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v2_pb{owner_signature= <<>>,
                                                       payer_signature= <<>>},
    BinTxn = blockchain_txn_assert_location_v2_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_assert_location_v2_pb{owner_signature=SigFun(BinTxn)}.

-spec is_valid_owner(txn_assert_location()) -> boolean().
is_valid_owner(#blockchain_txn_assert_location_v2_pb{owner=PubKeyBin,
                                                     owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_assert_location_v2_pb{owner_signature= <<>>,
                                                       payer_signature= <<>>},
    EncodedTxn = blockchain_txn_assert_location_v2_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid_location(txn_assert_location(), pos_integer()) -> boolean().
is_valid_location(#blockchain_txn_assert_location_v2_pb{location=Location}, MinH3Res) ->
    h3:get_resolution(h3:from_string(Location)) >= MinH3Res.

-spec is_valid_gain(txn_assert_location(), pos_integer(), pos_integer()) -> boolean().
is_valid_gain(#blockchain_txn_assert_location_v2_pb{gain=Gain}, MinGain, MaxGain) ->
    not (Gain < MinGain orelse Gain > MaxGain).

-spec is_valid_payer(txn_assert_location()) -> boolean().
is_valid_payer(#blockchain_txn_assert_location_v2_pb{payer=undefined}) ->
    %% no payer
    true;
is_valid_payer(#blockchain_txn_assert_location_v2_pb{payer= <<>>,
                                                     payer_signature= <<>>}) ->
    %% empty payer, empty signature
    true;
is_valid_payer(#blockchain_txn_assert_location_v2_pb{payer=PubKeyBin,
                                                     payer_signature=Signature}=Txn) ->

    BaseTxn = Txn#blockchain_txn_assert_location_v2_pb{owner_signature= <<>>,
                                                       payer_signature= <<>>},
    EncodedTxn = blockchain_txn_assert_location_v2_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_assert_location(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Gateway = ?MODULE:gateway(Txn),
    Owner = ?MODULE:owner(Txn),
    Payer = ?MODULE:payer(Txn),
    Location = ?MODULE:location(Txn),
    Gain = ?MODULE:gain(Txn),
    Elevation = ?MODULE:elevation(Txn),
    Ledger = blockchain:ledger(Chain),

    case blockchain:config(?assert_loc_txn_version, Ledger) of
        {ok, V} when V >= 2 ->
            case blockchain:config(?min_antenna_gain, Ledger) of
                {ok, MinGain} ->
                    case blockchain:config(?max_antenna_gain, Ledger) of
                        {ok, MaxGain} ->
                            case is_valid_gain(Txn, MinGain, MaxGain) of
                                false ->
                                    {error, {invalid_assert_loc_txn_v2, {invalid_antenna_gain, Gain, MinGain, MaxGain}}};
                                true ->
                                    Res = blockchain_txn:validate_fields([{{gateway, Gateway}, {address, libp2p}},
                                                                          {{owner, Owner}, {address, libp2p}},
                                                                          {{payer, Payer}, {address, libp2p}},
                                                                          {{location, Location}, {is_integer, 0}},
                                                                          {{elevation, Elevation}, {is_integer, -2147483648}}
                                                                         ]),

                                    case Res of
                                        {error, _}=E -> E;
                                        ok ->
                                            do_is_valid_checks(Txn, Chain)
                                    end
                            end;
                        _ ->
                            {error, {invalid_assert_loc_txn_v2, max_antenna_gain_not_set}}
                    end;
                _ ->
                    {error, {invalid_assert_loc_txn_v2, min_antenna_gain_not_set}}
            end;
        _ ->
            {error, {invalid_assert_loc_txn_v2, insufficient_assert_loc_txn_version}}
    end.


-spec do_is_valid_checks(Txn :: txn_assert_location(),
                         Chain :: blockchain:blockchain()) -> ok | {error, any()}.
do_is_valid_checks(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case {?MODULE:is_valid_owner(Txn),
          ?MODULE:is_valid_payer(Txn)} of
        {false, _} ->
            {error, {bad_owner_signature, {assert_location_v2, owner(Txn)}}};
        {_, false} ->
            {error, {bad_payer_signature, {assert_location_v2, payer(Txn)}}};
        {true, true} ->
            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
            StakingFee = ?MODULE:staking_fee(Txn),
            ExpectedStakingFee = ?MODULE:calculate_staking_fee(Txn, Chain),
            TxnFee = ?MODULE:fee(Txn),
            ExpectedTxnFee = calculate_fee(Txn, Chain),
            case {(ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled), ExpectedStakingFee == StakingFee} of
                {false,_} ->
                    {error, {wrong_txn_fee, {assert_location_v2, ExpectedTxnFee, TxnFee}}};
                {_,false} ->
                    {error, {wrong_staking_fee, {assert_location_v2, ExpectedStakingFee, StakingFee}}};
                {true, true} ->
                    do_remaining_checks(Txn, TxnFee + StakingFee, Ledger)
            end
    end.

-spec is_new_location(Txn :: txn_assert_location(), Ledger :: blockchain_ledger_v1:ledger()) -> boolean().
is_new_location(Txn, Ledger) ->
    GwPubkeyBin = ?MODULE:gateway(Txn),
    NewLoc = ?MODULE:location(Txn),
    case blockchain_ledger_v1:find_gateway_info(GwPubkeyBin, Ledger) of
        {ok, Gw} ->
            ExistingLoc = blockchain_ledger_gateway_v2:location(Gw),
            NewLoc /= ExistingLoc;
        {error, _Reason} ->
            %% if GW doesnt exist, default to true
            %% we could throw an error here but as this
            %% is called from calculate_staking_fee
            %% which itself is used in txn prep it is thought
            %% best to allow calculate_staking_fee to
            %% return a fee value, if that fee is incorrect
            %% the txn will fail as part of validations anyway
            true
    end.

-spec do_remaining_checks(Txn :: txn_assert_location(),
                          TotalFee :: non_neg_integer(),
                          Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
do_remaining_checks(Txn, TotalFee, Ledger) ->
    Owner = ?MODULE:owner(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Payer = ?MODULE:payer(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    ActualPayer = case Payer == undefined orelse Payer == <<>> of
                      true -> Owner;
                      false -> Payer
                  end,
    case blockchain_ledger_v1:check_dc_or_hnt_balance(ActualPayer, TotalFee, Ledger, AreFeesEnabled) of
        {error, _}=Error ->
            Error;
        ok ->
            Gateway = ?MODULE:gateway(Txn),
            case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
                {error, _} ->
                    {error, {unknown_gateway, {assert_location_v2, Gateway, Ledger}}};
                {ok, GwInfo} ->
                    GwOwner = blockchain_ledger_gateway_v2:owner_address(GwInfo),
                    case Owner == GwOwner of
                        false ->
                            {error, {bad_owner, {assert_location_v2, Owner, GwOwner}}};
                        true ->
                            {ok, MinAssertH3Res} = blockchain:config(?min_assert_h3_res, Ledger),
                            Location = ?MODULE:location(Txn),
                            case ?MODULE:is_valid_location(Txn, MinAssertH3Res) of
                                false ->
                                    {error, {insufficient_assert_res, {assert_location_v2, Location, Gateway}}};
                                true ->
                                    LedgerNonce = blockchain_ledger_gateway_v2:nonce(GwInfo),
                                    case Nonce =:= LedgerNonce + 1 of
                                        false ->
                                            {error, {bad_nonce, {assert_location_v2, Nonce, LedgerNonce}}};
                                        true ->
                                            ok
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
    Gain = ?MODULE:gain(Txn),
    Elevation = ?MODULE:elevation(Txn),
    TxnHash = ?MODULE:hash(Txn),
    ActualPayer = fee_payer(Txn, Ledger),

    {ok, OldGw} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    %% NOTE: It is assumed that the staking_fee is set to 0 at user level for assert_location_v2 transactions
    %% which only update gain/elevation
    case blockchain_ledger_v1:debit_fee(ActualPayer, Fee + StakingFee, Ledger, AreFeesEnabled, TxnHash, Chain) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            blockchain_ledger_v1:add_gateway_location(Gateway, Location, Nonce, Ledger),
            blockchain_ledger_v1:add_gateway_gain(Gateway, Gain, Nonce, Ledger),
            blockchain_ledger_v1:add_gateway_elevation(Gateway, Elevation, Nonce, Ledger),
            maybe_alter_hex(OldGw, Gateway, Location, Ledger),
            maybe_update_neighbors(Gateway, Ledger)
    end.

-spec maybe_update_neighbors(Gateway :: libp2p_crypto:pubkey_bin(),
                             Ledger :: blockchain_ledger_v1:ledger()) -> ok.
maybe_update_neighbors(Gateway, Ledger) ->
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
            ok = blockchain_ledger_v1:update_gateway(Gw, Gw1, Gateway, Ledger)
    end.

-spec maybe_alter_hex(OldGw :: blockchain_ledger_gateway_v2:gateway(),
                      Gateway :: libp2p_crypto:pubkey_bin(),
                      Location :: h3:index(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> ok.
maybe_alter_hex(OldGw, Gateway, Location, Ledger) ->
    Res =
      case blockchain:config(?poc_target_hex_parent_res, Ledger) of
        {ok, V} -> V;
        _ -> 5
      end,
    OldLoc = blockchain_ledger_gateway_v2:location(OldGw),

    case {OldLoc, Location} of
        {undefined, New} ->
            %% no previous location
            %% add new hex
            blockchain_ledger_v1:add_to_hex(New, Gateway, Res, Ledger);
        {Old, Old} ->
            %% why even check this, same loc as old loc
            ok;
        {Old, New} ->
            %% moving this to the h3dex targeting code means that we can't optimize the same way,
            %% but in return we get to control what code is actually run via chain var.  if the
            %% performance of this is terrible wrt syncing old blocks, we can add ledger:move_hex 

            %% remove old location
            blockchain_ledger_v1:remove_from_hex(Old, Gateway, Res, Ledger),
            %% add new location
            blockchain_ledger_v1:add_to_hex(New, Gateway, Res, Ledger)
    end.

-spec print(txn_assert_location()) -> iodata().
print(undefined) -> <<"type=assert_location, undefined">>;
print(#blockchain_txn_assert_location_v2_pb{
        gateway = Gateway, owner = Owner, payer = Payer,
        location = Loc, gain = Gain, elevation = Elevation,
        owner_signature = OS, payer_signature = PS, nonce = Nonce,
        staking_fee = StakingFee, fee = Fee}) ->
    io_lib:format("type=assert_location, gateway=~p, owner=~p, payer=~p, location=~p, owner_signature=~p, payer_signature=~p, nonce=~p, gain=~p, elevation=~p, staking_fee=~p, fee=~p", [?TO_ANIMAL_NAME(Gateway), ?TO_B58(Owner), ?TO_B58(Payer), Loc, OS, PS, Nonce, Gain, Elevation, StakingFee, Fee]).

json_type() ->
    <<"assert_location_v2">>.

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
      fee => fee(Txn),
      gain => gain(Txn),
      elevation => elevation(Txn)
     }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec staking_fee_for_gw_mode(blockchain_ledger_gateway_v2:mode(), blockchain_ledger_v1:ledger()) -> non_neg_integer().
staking_fee_for_gw_mode(dataonly, Ledger)->
    blockchain_ledger_v1:staking_fee_txn_assert_location_dataonly_gateway_v1(Ledger);
staking_fee_for_gw_mode(light, Ledger)->
    blockchain_ledger_v1:staking_fee_txn_assert_location_light_gateway_v1(Ledger);
staking_fee_for_gw_mode(_, Ledger)->
    blockchain_ledger_v1:staking_fee_txn_assert_location_v1(Ledger).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-define(TEST_LOCATION, 631210968840687103).

new() ->
    #blockchain_txn_assert_location_v2_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       payer= <<>>,
       owner_signature= <<>>,
       payer_signature= <<>>,
       location= h3:to_string(?TEST_LOCATION),
       nonce = 1,
       staking_fee = ?LEGACY_STAKING_FEE,
       fee = ?LEGACY_TXN_FEE,
       gain = ?DEFAULT_GAIN,
       elevation = ?DEFAULT_ELEVATION
      }.

invalid_new() ->
    #blockchain_txn_assert_location_v2_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       owner_signature= << >>,
       location= h3:to_string(599685771850416127),
       nonce = 1,
       staking_fee = ?LEGACY_STAKING_FEE,
       fee = ?LEGACY_TXN_FEE
      }.

missing_payer_signature_new() ->
    #{public := PubKey, secret := _PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    #blockchain_txn_assert_location_v2_pb{
       gateway= <<"gateway_address">>,
       owner= <<"owner_address">>,
       payer= libp2p_crypto:pubkey_to_bin(PubKey),
       payer_signature = <<>>,
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

payer_signature_missing_test() ->
    Tx = missing_payer_signature_new(),
    ?assertNot(is_valid_payer(Tx)).

payer_signature_test() ->
    Tx = new(),
    ?assertEqual(<<>>, payer_signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = owner_signature(Tx1),
    BaseTxn1 = Tx1#blockchain_txn_assert_location_v2_pb{owner_signature = <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_assert_location_v2_pb:encode_msg(BaseTxn1), Sig1, PubKey)).

sign_payer_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_payer(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = payer_signature(Tx2),
    BaseTx1 = Tx2#blockchain_txn_assert_location_v2_pb{owner_signature = <<>>, payer_signature= <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_assert_location_v2_pb:encode_msg(BaseTx1), Sig2, PubKey)).

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

is_valid_gain_test() ->
    MinGain = 10,
    MaxGain = 150,
    Tx = new(),
    InvT1 = gain(Tx, 9),
    InvT2 = gain(Tx, 8),
    InvT3 = gain(Tx, 151),
    InvT4 = gain(Tx, 152),
    ValidT1 = gain(Tx, 10),
    ValidT2 = gain(Tx, 11),
    ValidT3 = gain(Tx, 150),
    ValidT4 = gain(Tx, 149),
    ?assertNot(is_valid_gain(InvT1, MinGain, MaxGain)),
    ?assertNot(is_valid_gain(InvT2, MinGain, MaxGain)),
    ?assertNot(is_valid_gain(InvT3, MinGain, MaxGain)),
    ?assertNot(is_valid_gain(InvT4, MinGain, MaxGain)),
    ?assert(is_valid_gain(ValidT1, MinGain, MaxGain)),
    ?assert(is_valid_gain(ValidT2, MinGain, MaxGain)),
    ?assert(is_valid_gain(ValidT3, MinGain, MaxGain)),
    ?assert(is_valid_gain(ValidT4, MinGain, MaxGain)).

-endif.
