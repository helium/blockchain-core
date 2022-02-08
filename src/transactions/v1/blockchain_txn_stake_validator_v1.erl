%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Stake Validator ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_stake_validator_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_stake_validator_v1_pb.hrl").

-export([
         new/4,
         hash/1,
         validator/1,
         owner/1,
         stake/1,
         owner_signature/1,
         fee/1, calculate_fee/2, calculate_fee/5,
         fee_payer/2,
         sign/2,
         is_valid/2,
         is_well_formed/1,
         is_prompt/2,
         absorb/2,
         print/1,
         json_type/0,
         to_json/2
        ]).

-define(T, #blockchain_txn_stake_validator_v1_pb).

-type t() :: txn_stake_validator().

-type txn_stake_validator() :: ?T{}.

-export_type([t/0, txn_stake_validator/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([
         is_valid_owner/1,
         owner_signature/2
        ]).

-spec owner_signature(any(), txn_stake_validator()) -> txn_stake_validator().
owner_signature(Sig, Txn) ->
    Txn#blockchain_txn_stake_validator_v1_pb{owner_signature = Sig}.

-endif.

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          pos_integer(), non_neg_integer()) ->
          txn_stake_validator().
new(ValidatorAddress, OwnerAddress,
    Stake, Fee) ->
    #blockchain_txn_stake_validator_v1_pb{
       address = ValidatorAddress,
       owner = OwnerAddress,
       stake = Stake,
       fee = Fee
    }.

-spec hash(txn_stake_validator()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_stake_validator_v1_pb{owner_signature = <<>>},
    EncodedTxn = blockchain_txn_stake_validator_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec owner(txn_stake_validator()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_stake_validator_v1_pb.owner.

-spec validator(txn_stake_validator()) -> libp2p_crypto:pubkey_bin().
validator(Txn) ->
    Txn#blockchain_txn_stake_validator_v1_pb.address.

-spec stake(txn_stake_validator()) -> pos_integer().
stake(Txn) ->
    Txn#blockchain_txn_stake_validator_v1_pb.stake.

-spec fee(txn_stake_validator()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_stake_validator_v1_pb.fee.

-spec fee_payer(txn_stake_validator(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    owner(Txn).

-spec calculate_fee(txn_stake_validator(), blockchain:blockchain()) ->
          non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_stake_validator(), blockchain_ledger_v1:ledger(),
                    pos_integer(), pos_integer(), boolean()) ->
          non_neg_integer().
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, _) ->
    ?calculate_fee(Txn#blockchain_txn_stake_validator_v1_pb{fee=0,
                                                            owner_signature = <<0:512>>},
    Ledger, DCPayloadSize, TxnFeeMultiplier).


-spec owner_signature(txn_stake_validator()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_stake_validator_v1_pb.owner_signature.

-spec sign(txn_stake_validator(), libp2p_crypto:sig_fun()) -> txn_stake_validator().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_stake_validator_v1_pb{owner_signature= <<>>},
    EncodedTxn = blockchain_txn_stake_validator_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_stake_validator_v1_pb{owner_signature=SigFun(EncodedTxn)}.

-spec is_valid_owner(txn_stake_validator()) -> boolean().
is_valid_owner(#blockchain_txn_stake_validator_v1_pb{owner=PubKeyBin,
                                                     owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_stake_validator_v1_pb{owner_signature= <<>>},
    EncodedTxn = blockchain_txn_stake_validator_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_stake_validator(), blockchain:blockchain()) ->
          ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = ?MODULE:owner(Txn),
    Validator = ?MODULE:validator(Txn),
    Stake = stake(Txn),
    Fee = fee(Txn),
    case is_valid_owner(Txn) of
        false ->
            {error, bad_owner_signature};
        true ->
            try
                %% explicit activation gate on the stake threshold
                case blockchain:config(?validator_version, Ledger) of
                    {ok, Vers} when Vers >= 1 ->
                        ok;
                    _ -> throw(unsupported_txn)
                end,
                {ok, MinStake} = blockchain:config(?validator_minimum_stake, Ledger),
                %% check that the network is correct
                case blockchain:config(?validator_key_check, Ledger) of
                    %% assert that validator is on the right network by decoding its key
                    {ok, true} ->
                        try
                            libp2p_crypto:bin_to_pubkey(Validator),
                            ok
                        catch throw:Why ->
                                  throw({unusable_miner_key, Why})
                        end;
                    _ -> ok
                end,
                %% check fee
                AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                ExpectedTxnFee = calculate_fee(Txn, Chain),
                case ExpectedTxnFee =< Fee orelse not AreFeesEnabled of
                    false -> throw({wrong_txn_fee, {ExpectedTxnFee, Fee}});
                    true -> ok
                end,
                %% make sure that no one is re-using miner keys
                case blockchain_ledger_v1:find_gateway_info(Validator, Ledger) of
                    {ok, _} -> throw(reused_miner_key);
                    {error, not_found} -> ok
                end,
                %% make sure that this validator doesn't already exist
                case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                    {ok, _} ->
                        throw(validator_already_exists);
                    {error, not_found} ->
                        %% make sure the staking amount is high enough
                        case Stake == MinStake of
                            true -> ok;
                            false -> throw({incorrect_stake, {exp, MinStake, got, Stake}})
                        end,
                        %% make sure that the owner has enough HNT to stake
                        case blockchain_ledger_v1:find_entry(Owner, Ledger) of
                            {ok, Entry} ->
                                Balance = blockchain_ledger_entry_v1:balance(Entry),
                                case Balance >= Stake of
                                    true -> ok;
                                    false -> throw({balance_too_low, {bal, Balance, stk, Stake}})
                                end;
                            {error, address_entry_not_found} ->
                                throw(unknown_owner);
                            {error, Error} ->
                                throw(Error)
                        end,
                        ok;
                    {error, Reason} -> throw({validator_fetch_error, Reason})
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

-spec absorb(txn_stake_validator(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = owner(Txn),
    Validator = validator(Txn),
    Stake = stake(Txn),
    Fee = fee(Txn),
    Hash = ?MODULE:hash(Txn),
    {ok, Entry} = blockchain_ledger_v1:find_entry(Owner, Ledger),
    Nonce = blockchain_ledger_entry_v1:nonce(Entry),

    case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger, true, Hash, Chain) of
        {error, _Reason} = Err -> Err;
        ok ->
            case blockchain_ledger_v1:debit_account(Owner, Stake, Nonce + 1, Ledger) of
                {error, _Reason} = Err1 -> Err1;
                ok -> blockchain_ledger_v1:add_validator(Validator, Owner, Stake, Ledger)
            end
    end.

-spec print(txn_stake_validator()) -> iodata().
print(undefined) -> <<"type=stake_validator, undefined">>;
print(#blockchain_txn_stake_validator_v1_pb{
         owner = O,
         address = Val,
         stake = S}) ->
    io_lib:format("type=stake_validator, owner=~p, validator=~p, stake=~p",
                  [?TO_B58(O), ?TO_ANIMAL_NAME(Val), S]).

json_type() ->
    <<"stake_validator_v1">>.

-spec to_json(txn_stake_validator(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      address => ?BIN_TO_B58(validator(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      owner_signature => ?BIN_TO_B64(owner_signature(Txn)),
      fee => fee(Txn),
      stake => stake(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    Tx = new(<<"validator_address">>, <<"owner_address">>, 1000, 20),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_stake_validator_v1_pb))).


-endif.
