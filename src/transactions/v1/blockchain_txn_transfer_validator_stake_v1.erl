%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Transfer Validator Stake ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_transfer_validator_stake_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_transfer_validator_stake_v1_pb.hrl").

-export([
         new/5, new/7,
         hash/1,
         old_validator/1,
         new_validator/1,
         old_owner/1,
         new_owner/1,
         old_owner_signature/1, old_owner_signature/2,
         new_owner_signature/1,
         stake_amount/1,
         payment_amount/1,
         fee/1, calculate_fee/2, calculate_fee/5,
         fee_payer/2,
         sign/2,
         new_owner_sign/2,
         is_valid/2,
         is_well_formed/1,
         is_prompt/2,
         absorb/2,
         print/1,
         json_type/0,
         to_json/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([
         is_valid_new_owner/1,
         is_valid_old_owner/1
        ]).
-endif.

-define(T, #blockchain_txn_transfer_validator_stake_v1_pb).

-type t() :: txn_transfer_validator_stake().

-type txn_transfer_validator_stake() :: ?T{}.

-export_type([t/0, txn_transfer_validator_stake/0]).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          libp2p_crypto:pubkey_bin(),
          pos_integer(), pos_integer()) ->
          txn_transfer_validator_stake().
new(OldValidatorAddress, NewValidatorAddress,
    OwnerAddress, StakeAmount, Fee) ->
    new(OldValidatorAddress, NewValidatorAddress,
        OwnerAddress, <<>>, StakeAmount, 0, Fee).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          non_neg_integer(), non_neg_integer(), pos_integer()) ->
          txn_transfer_validator_stake().
new(OldValidatorAddress, NewValidatorAddress,
    OldOwnerAddress, NewOwnerAddress,
    StakeAmount, PaymentAmount, Fee) ->
    #blockchain_txn_transfer_validator_stake_v1_pb{
       old_address = OldValidatorAddress,
       new_address = NewValidatorAddress,
       new_owner = NewOwnerAddress,
       old_owner = OldOwnerAddress,
       stake_amount = StakeAmount,
       payment_amount = PaymentAmount,
       fee = Fee
    }.

-spec hash(txn_transfer_validator_stake()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = base(Txn),
    EncodedTxn = blockchain_txn_transfer_validator_stake_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec old_owner(txn_transfer_validator_stake()) -> libp2p_crypto:pubkey_bin().
old_owner(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.old_owner.

-spec new_owner(txn_transfer_validator_stake()) -> libp2p_crypto:pubkey_bin().
new_owner(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.new_owner.

-spec new_validator(txn_transfer_validator_stake()) -> libp2p_crypto:pubkey_bin().
new_validator(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.new_address.

-spec old_validator(txn_transfer_validator_stake()) -> libp2p_crypto:pubkey_bin().
old_validator(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.old_address.

-spec stake_amount(txn_transfer_validator_stake()) -> non_neg_integer().
stake_amount(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.stake_amount.

-spec payment_amount(txn_transfer_validator_stake()) -> non_neg_integer().
payment_amount(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.payment_amount.

-spec fee(txn_transfer_validator_stake()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.fee.

-spec fee_payer(txn_transfer_validator_stake(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    old_owner(Txn).

-spec calculate_fee(txn_transfer_validator_stake(), blockchain:blockchain()) ->
          non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_transfer_validator_stake(), blockchain_ledger_v1:ledger(),
                    pos_integer(), pos_integer(), boolean()) ->
          non_neg_integer().
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, _) ->
    ?calculate_fee(Txn#blockchain_txn_transfer_validator_stake_v1_pb{fee=0,
                                                                     old_owner_signature = <<0:512>>,
                                                                     new_owner_signature = <<0:512>>},
    Ledger, DCPayloadSize, TxnFeeMultiplier).


-spec new_owner_signature(txn_transfer_validator_stake()) -> binary().
new_owner_signature(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.new_owner_signature.

-spec old_owner_signature(txn_transfer_validator_stake()) -> binary().
old_owner_signature(Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb.old_owner_signature.

-spec old_owner_signature(any(), txn_transfer_validator_stake()) -> txn_transfer_validator_stake().
old_owner_signature(Sig, Txn) ->
    Txn#blockchain_txn_transfer_validator_stake_v1_pb{old_owner_signature = Sig}.

-spec sign(txn_transfer_validator_stake(), libp2p_crypto:sig_fun()) -> txn_transfer_validator_stake().
sign(Txn, SigFun) ->
    BaseTxn = base(Txn),
    EncodedTxn = blockchain_txn_transfer_validator_stake_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_transfer_validator_stake_v1_pb{old_owner_signature=SigFun(EncodedTxn)}.

-spec new_owner_sign(txn_transfer_validator_stake(), libp2p_crypto:sig_fun()) -> txn_transfer_validator_stake().
new_owner_sign(Txn, SigFun) ->
    BaseTxn = base(Txn),
    EncodedTxn = blockchain_txn_transfer_validator_stake_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_transfer_validator_stake_v1_pb{new_owner_signature=SigFun(EncodedTxn)}.

base(Txn) ->
  Txn#blockchain_txn_transfer_validator_stake_v1_pb{old_owner_signature = <<>>,
                                                    new_owner_signature = <<>>}.

-spec is_valid_new_owner(txn_transfer_validator_stake()) -> boolean().
is_valid_new_owner(#blockchain_txn_transfer_validator_stake_v1_pb{
                      new_owner=PubKeyBin,
                      new_owner_signature=Signature}=Txn) ->
    BaseTxn = base(Txn),
    EncodedTxn = blockchain_txn_transfer_validator_stake_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid_old_owner(txn_transfer_validator_stake()) -> boolean().
is_valid_old_owner(#blockchain_txn_transfer_validator_stake_v1_pb{
                      old_owner=PubKeyBin,
                      old_owner_signature=Signature}=Txn) ->
    BaseTxn = base(Txn),
    EncodedTxn = blockchain_txn_transfer_validator_stake_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_transfer_validator_stake(), blockchain:blockchain()) ->
          ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    NewValidator = new_validator(Txn),
    OldValidator = old_validator(Txn),
    Fee = fee(Txn),
    case is_valid_old_owner(Txn) of
        false ->
            {error, bad_old_owner_signature};
        true ->
            try
                case blockchain:config(?validator_version, Ledger) of
                    {ok, Vers} when Vers >= 3 ->
                        ok;
                    _ -> throw(unsupported_txn)
                end,
                case new_owner(Txn) /= <<>> of
                    true ->
                        case is_valid_new_owner(Txn) of
                            true ->
                                %% make sure that no one is re-using miner keys
                                case blockchain_ledger_v1:find_gateway_info(NewValidator, Ledger) of
                                    {ok, _} -> throw(reused_miner_key);
                                    {error, not_found} -> ok
                                end;
                            false ->
                                throw(bad_new_owner_signature)
                        end;
                    _ ->
                        %% no new owner just means this is an in-account transfer
                        ok
                end,
                %% check that the network is correct for the new validator pubkey_bin
                case blockchain:config(?validator_key_check, Ledger) of
                    %% assert that validator is on the right network by decoding its key
                    {ok, true} ->
                        try
                            libp2p_crypto:bin_to_pubkey(NewValidator),
                            ok
                        catch throw:Why ->
                                  throw({unusable_miner_key, Why})
                        end;
                    _ -> ok
                end,
                %% check if the old validator currently in the group
                {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
                case lists:member(OldValidator, ConsensusAddrs) of
                    true -> throw(cannot_transfer_while_in_consensus);
                    false -> ok
                end,
                %% make sure the amount is encoded correctly
                %% and is only specified in the correct case
                case payment_amount(Txn) of
                    %% 0 is always ok
                    0 -> ok;
                    N when is_integer(N) andalso N >= 0 ->
                        case new_owner(Txn) /= <<>> of
                            true -> ok;
                            false -> throw(amount_set_for_intra_account_transfer)
                        end;
                    N -> throw({bad_amount, N})
                end,
                %% check fee
                AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                ExpectedTxnFee = calculate_fee(Txn, Chain),
                case ExpectedTxnFee =< Fee orelse not AreFeesEnabled of
                    false -> throw({wrong_txn_fee, {ExpectedTxnFee, Fee}});
                    true -> ok
                end,
                %% make sure that this validator doesn't already exist
                case blockchain_ledger_v1:get_validator(NewValidator, Ledger) of
                    {ok, _} -> throw(new_validator_already_exists);
                    {error, not_found} -> ok;
                    {error, Reason} -> throw({new_validator_fetch_error, Reason})
                end,
                %% make sure that existing validator exists and is staked
                case blockchain_ledger_v1:get_validator(OldValidator, Ledger) of
                    {ok, OV} ->
                        OldOwner = old_owner(Txn),
                        case blockchain_ledger_validator_v1:owner_address(OV) of
                            OldOwner ->
                                %% check staked status
                                case blockchain_ledger_validator_v1:status(OV) of
                                    staked -> ok;
                                    %% can be either unstaked or cooldown
                                    _ -> throw(cant_transfer_unstaked_validator)
                                end,
                                %% check stake is not 0
                                case blockchain_ledger_validator_v1:stake(OV) of
                                    0 -> throw(cant_transfer_zero_stake);
                                    ChainStake ->
                                        case stake_amount(Txn) of
                                            ChainStake -> ok;
                                            Else -> throw({bad_stake, exp, ChainStake, got, Else})
                                        end
                                end;
                            _ -> throw(bad_owner)
                        end;
                    {error, not_found} -> throw(old_validator_non_existant);
                    {error, Reason1} -> throw({validator_fetch_error, Reason1})
                end,
                ok
            catch throw:Cause ->
                    {error, Cause}
            end
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

-spec absorb(txn_transfer_validator_stake(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    NewOwner = new_owner(Txn),
    OldOwner = old_owner(Txn),
    NewValidator = new_validator(Txn),
    OldValidator = old_validator(Txn),
    Amount = payment_amount(Txn),
    Fee = fee(Txn),
    Hash = ?MODULE:hash(Txn),

    case blockchain_ledger_v1:debit_fee(OldOwner, Fee, Ledger, true, Hash, Chain) of
        {error, _Reason} = Err -> Err;
        ok ->
            case blockchain_ledger_v1:get_validator(OldValidator, Ledger) of
                {ok, OV} ->
                    %% for old set stake to 0 and mark as unstaked
                    OV1 = blockchain_ledger_validator_v1:status(unstaked, OV),
                    OV2 = blockchain_ledger_validator_v1:stake(0, OV1),
                    ok = blockchain_ledger_v1:update_validator(OldValidator, OV2, Ledger),
                    %% change address on old record
                    NV =  blockchain_ledger_validator_v1:address(NewValidator, OV),
                    NV1 = blockchain_ledger_validator_v1:nonce(1, NV),
                    NV2 = blockchain_ledger_validator_v1:last_heartbeat(1, NV1),
                    NV3 =
                        case NewOwner == <<>> of
                            true -> NV2;
                            false -> blockchain_ledger_validator_v1:owner_address(NewOwner, NV2)
                        end,
                    %% do the swap if an amount is specified
                    ok =
                        case Amount > 0 of
                            true ->
                                {ok, NewOwnerEntry} = blockchain_ledger_v1:find_entry(NewOwner, Ledger),
                                NewOwnerNonce = blockchain_ledger_entry_v1:nonce(NewOwnerEntry),
                                case blockchain_ledger_v1:debit_account(NewOwner, Amount, NewOwnerNonce + 1, Ledger) of
                                    {error, _Reason} = Err ->
                                        Err;
                                    ok ->
                                        blockchain_ledger_v1:credit_account(OldOwner, Amount, Ledger)
                                end;
                            false ->
                                ok
                        end,
                    ok = blockchain_ledger_v1:update_validator(NewValidator, NV3, Ledger);
                Err -> Err
            end
    end.

-spec print(txn_transfer_validator_stake()) -> iodata().
print(undefined) -> <<"type=transfer_validator_stake, undefined">>;
print(#blockchain_txn_transfer_validator_stake_v1_pb{
         new_owner = NO,
         old_owner = OO,
         new_address = NewVal,
         old_address = OldVal,
         stake_amount = StakeAmount,
         payment_amount = PaymentAmount}) ->
    io_lib:format("type=transfer_validator_stake, old_owner=~p, new_owner(optional)=~p "
                  "new_validator=~p, old_validator=~p, stake_amount=~p, payment_amount=~p",
                  [?TO_B58(OO), ?TO_B58(NO), ?TO_ANIMAL_NAME(NewVal), ?TO_ANIMAL_NAME(OldVal),
                   StakeAmount, PaymentAmount]).

json_type() ->
    <<"transfer_validator_stake_v1">>.

-spec to_json(txn_transfer_validator_stake(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      new_address => ?BIN_TO_B58(new_validator(Txn)),
      old_address => ?BIN_TO_B58(old_validator(Txn)),
      new_owner => ?BIN_TO_B58(new_owner(Txn)),
      old_owner => ?BIN_TO_B58(old_owner(Txn)),
      old_owner_signature => ?BIN_TO_B64(old_owner_signature(Txn)),
      new_owner_signature => ?BIN_TO_B64(new_owner_signature(Txn)),
      stake_amount => stake_amount(Txn),
      payment_amount => payment_amount(Txn),
      fee => fee(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    Tx = new(<<"old_validator_address">>, <<"new_validator_address">>,
             <<"owner_address">>, 10, 10),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_transfer_validator_stake_v1_pb))).


-endif.
