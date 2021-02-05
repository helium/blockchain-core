%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Stake Validator ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_change_validator_description_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_change_validator_description_v1_pb.hrl").

-export([
         new/6,
         hash/1,
         addr/1,
         owner/1,
         description/1,
         owner_signature/1,
         nonce/1,
         fee/1, calculate_fee/2, calculate_fee/5,
         sign/2,
         is_valid/2,
         absorb/2,
         print/1,
         to_json/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_change_validator_description() :: #blockchain_txn_change_validator_description_v1_pb{}.
-export_type([txn_change_validator_description/0]).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          binary(), binary(), pos_integer(), pos_integer()) ->
          txn_change_validator_description().
new(ValidatorAddress, OwnerAddress,
    Description, OwnerSignature, Fee, Nonce) ->
    #blockchain_txn_change_validator_description_v1_pb{
       addr = ValidatorAddress,
       owner = OwnerAddress,
       description = Description,
       owner_signature = OwnerSignature,
       fee = Fee,
       nonce = Nonce
    }.

-spec hash(txn_change_validator_description()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_change_validator_description_v1_pb{owner_signature = <<>>},
    EncodedTxn = blockchain_txn_change_validator_description_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec owner(txn_change_validator_description()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_change_validator_description_v1_pb.owner.

-spec addr(txn_change_validator_description()) -> libp2p_crypto:pubkey_bin().
addr(Txn) ->
    Txn#blockchain_txn_change_validator_description_v1_pb.addr.

-spec description(txn_change_validator_description()) -> libp2p_crypto:pubkey_bin().
description(Txn) ->
    Txn#blockchain_txn_change_validator_description_v1_pb.description.

-spec fee(txn_change_validator_description()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_change_validator_description_v1_pb.fee.

-spec calculate_fee(txn_change_validator_description(), blockchain:blockchain()) ->
          non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_change_validator_description(), blockchain_ledger_v1:ledger(),
                    pos_integer(), pos_integer(), boolean()) ->
          non_neg_integer().
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_change_validator_description_v1_pb{fee=0,
                                                              owner_signature = <<0:512>>},
    Ledger, DCPayloadSize, TxnFeeMultiplier).

-spec owner_signature(txn_change_validator_description()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_change_validator_description_v1_pb.owner_signature.

-spec nonce(txn_change_validator_description()) -> pos_integer().
nonce(Txn) ->
    Txn#blockchain_txn_change_validator_description_v1_pb.nonce.

-spec sign(txn_change_validator_description(), libp2p_crypto:sig_fun()) -> txn_change_validator_description().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_change_validator_description_v1_pb{owner_signature= <<>>},
    EncodedTxn = blockchain_txn_change_validator_description_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_change_validator_description_v1_pb{owner_signature=SigFun(EncodedTxn)}.

-spec is_valid_owner(txn_change_validator_description()) -> boolean().
is_valid_owner(#blockchain_txn_change_validator_description_v1_pb{owner=PubKeyBin,
                                                                  owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_change_validator_description_v1_pb{owner_signature= <<>>},
    EncodedTxn = blockchain_txn_change_validator_description_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_change_validator_description(), blockchain:blockchain()) ->
          ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = addr(Txn),
    Description = description(Txn),
    Nonce = nonce(Txn),
    Fee = fee(Txn),
    case is_valid_owner(Txn) of
        false ->
            {error, bad_owner_signature};
        _ ->
            try
                %% check fee
                AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                ExpectedTxnFee = calculate_fee(Txn, Chain),
                case ExpectedTxnFee =< Fee orelse not AreFeesEnabled of
                    false -> throw({wrong_txn_fee, {ExpectedTxnFee, Fee}});
                    true -> ok
                end,
                {ok, MaxLen} = blockchain_ledger_v1:config(?validator_description_max_len, Ledger),
                DLen = byte_size(Description),
                case DLen of
                    N when N =< MaxLen ->
                        ok;
                    _ ->
                        throw({description_too_long, exp, MaxLen, got, DLen})
                end,
                %% make sure that this validator exists and is staked
                case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                    {ok, V} ->
                        %% make sure that the nonce is correct
                        VNonce = blockchain_ledger_validator_v1:nonce(V),
                        case Nonce == (VNonce + 1) of
                            true -> ok;
                            false -> throw({bad_nonce, exp, VNonce + 1, got, Nonce})
                        end;
                    {error, not_found} -> throw(nonexistent_validator);
                    {error, Reason} -> throw({validator_fetch_error, Reason})
                end,
                ok
            catch throw:Cause ->
                    {error, Cause}
            end
    end.

-spec absorb(txn_change_validator_description(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = owner(Txn),
    Validator = addr(Txn),
    Description = description(Txn),
    Fee = fee(Txn),

    case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger, true) of
        {error, _Reason} = Err -> Err;
        ok ->
            case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                {ok, V} ->
                    %% make sure that the nonce is correct
                    V1 = blockchain_ledger_validator_v1:description(Description, V),
                    blockchain_ledger_v1:update_validator(Validator, V1, Ledger);
                Err1 -> Err1
            end
    end.

-spec print(txn_change_validator_description()) -> iodata().
print(undefined) -> <<"type=change_validator_description, undefined">>;
print(#blockchain_txn_change_validator_description_v1_pb{
         owner = O,
         addr = Val,
         description = D,
         nonce = N}) ->
    io_lib:format("type=change_validator_description, owner=~p, validator=~p, description=\"~s\", nonce=~p",
                  [?TO_B58(O), ?TO_ANIMAL_NAME(Val), D, N]).


-spec to_json(txn_change_validator_description(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"change_validator_description_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      addr => ?BIN_TO_B58(addr(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      description => description(Txn),
      owner_signature => ?BIN_TO_B64(owner_signature(Txn)),
      fee => fee(Txn),
      nonce => nonce(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    Tx = new(<<"validator_address">>, <<"owner_address">>,
             <<"some description">>, <<"sdasdasdasd">>, 10, 10),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_change_validator_description_v1_pb))).

-endif.
