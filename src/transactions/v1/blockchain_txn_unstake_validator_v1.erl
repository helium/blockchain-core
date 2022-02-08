%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Stake Validator ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_unstake_validator_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_unstake_validator_v1_pb.hrl").

-export([
         new/5,
         hash/1,
         address/1,
         owner/1,
         stake_amount/1,
         stake_release_height/1,
         owner_signature/1, owner_signature/2,
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([is_valid_owner/1]).
-endif.

-define(T, #blockchain_txn_unstake_validator_v1_pb).

-type t() :: txn_unstake_validator().

-type txn_unstake_validator() :: ?T{}.

-export_type([t/0, txn_unstake_validator/0]).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          pos_integer(), pos_integer(), pos_integer()) ->
          txn_unstake_validator().
new(ValidatorAddress, OwnerAddress, StakeAmount, StakeReleaseHeight, Fee) ->
    #blockchain_txn_unstake_validator_v1_pb{
       address = ValidatorAddress,
       owner = OwnerAddress,
       stake_amount = StakeAmount,
       stake_release_height = StakeReleaseHeight,
       fee = Fee
    }.

-spec hash(txn_unstake_validator()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_unstake_validator_v1_pb{owner_signature = <<>>},
    EncodedTxn = blockchain_txn_unstake_validator_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec owner(txn_unstake_validator()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_unstake_validator_v1_pb.owner.

-spec stake_amount(txn_unstake_validator()) -> pos_integer().
stake_amount(Txn) ->
    Txn#blockchain_txn_unstake_validator_v1_pb.stake_amount.

-spec address(txn_unstake_validator()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_unstake_validator_v1_pb.address.

-spec stake_release_height(txn_unstake_validator()) -> pos_integer().
stake_release_height(Txn) ->
    Txn#blockchain_txn_unstake_validator_v1_pb.stake_release_height.

-spec fee(txn_unstake_validator()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_unstake_validator_v1_pb.fee.

-spec fee_payer(txn_unstake_validator(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    owner(Txn).

-spec calculate_fee(txn_unstake_validator(), blockchain:blockchain()) ->
          non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_unstake_validator(), blockchain_ledger_v1:ledger(),
                    pos_integer(), pos_integer(), boolean()) ->
          non_neg_integer().
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, _) ->
    ?calculate_fee(Txn#blockchain_txn_unstake_validator_v1_pb{fee=0,
                                                              owner_signature = <<0:512>>},
    Ledger, DCPayloadSize, TxnFeeMultiplier).

-spec owner_signature(txn_unstake_validator()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_unstake_validator_v1_pb.owner_signature.

-spec owner_signature(any(), txn_unstake_validator()) -> txn_unstake_validator().
owner_signature(Sig, Txn) ->
    Txn#blockchain_txn_unstake_validator_v1_pb{owner_signature = Sig}.

-spec sign(txn_unstake_validator(), libp2p_crypto:sig_fun()) -> txn_unstake_validator().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_unstake_validator_v1_pb{owner_signature= <<>>},
    EncodedTxn = blockchain_txn_unstake_validator_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_unstake_validator_v1_pb{owner_signature=SigFun(EncodedTxn)}.

-spec is_valid_owner(txn_unstake_validator()) -> boolean().
is_valid_owner(#blockchain_txn_unstake_validator_v1_pb{owner=PubKeyBin,
                                                       owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_unstake_validator_v1_pb{owner_signature= <<>>},
    EncodedTxn = blockchain_txn_unstake_validator_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_unstake_validator(), blockchain:blockchain()) ->
          ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = address(Txn),
    Owner = owner(Txn),
    Fee = fee(Txn),
    StakeReleaseHeight = stake_release_height(Txn),
    case is_valid_owner(Txn) of
        false ->
            {error, bad_owner_signature};
        _ ->
            try
                case blockchain:config(?validator_version, Ledger) of
                    {ok, Vers} when Vers >= 2 ->
                        ok;
                    _ -> throw(unsupported_txn)
                end,
                %% check fee
                AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                ExpectedTxnFee = calculate_fee(Txn, Chain),
                case ExpectedTxnFee =< Fee orelse not AreFeesEnabled of
                    false -> throw({wrong_txn_fee, {ExpectedTxnFee, Fee}});
                    true -> ok
                end,
                %% check if we're currently in the group
                {ok, ConsensusAddrs} = blockchain_ledger_v1:consensus_members(Ledger),
                case lists:member(Validator, ConsensusAddrs) of
                    true -> throw(cannot_unstake_while_in_consensus);
                    false -> ok
                end,
                %% make sure that this validator exists and is staked
                case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                    {ok, V} ->
                        case blockchain_ledger_validator_v1:status(V) of
                            staked -> ok;
                            cooldown -> throw(already_cooldown);
                            unstaked -> throw(already_unstaked)
                        end,
                        ChainStake = blockchain_ledger_validator_v1:stake(V),
                        case stake_amount(Txn) of
                            ChainStake -> ok;
                            Else -> throw({bad_stake, exp, ChainStake, got, Else})
                        end,
                        case blockchain_ledger_validator_v1:owner_address(V) of
                            Owner -> ok;
                            Other -> throw({not_owner, Other})
                        end,
                        {ok, Cooldown} = blockchain:config(?stake_withdrawal_cooldown, Ledger),
                        {ok, CooldownMax} = blockchain:config(?stake_withdrawal_max, Ledger),
                        {ok, CurrentHeight} = blockchain_ledger_v1:current_height(Ledger),
                        %% for more understandable semantics, we need to validate not against the
                        %% height of the given ledger, but the height of the block that will include
                        %% this transaction, hence we add one here.
                        ThisBlockHeight = CurrentHeight + 1,
                        case StakeReleaseHeight >= (ThisBlockHeight + Cooldown) andalso
                             StakeReleaseHeight < (ThisBlockHeight + Cooldown + CooldownMax) of
                            true -> ok;
                            false -> throw({invalid_stake_release_height, StakeReleaseHeight})
                        end;
                    {error, not_found} -> throw(nonexistent_validator);
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

-spec absorb(txn_unstake_validator(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = owner(Txn),
    Validator = address(Txn),
    StakeReleaseHeight = stake_release_height(Txn),
    Fee = fee(Txn),
    Hash = ?MODULE:hash(Txn),

    case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger, true, Hash, Chain) of
        {error, _Reason} = Err -> Err;
        ok ->
            blockchain_ledger_v1:deactivate_validator(Validator, StakeReleaseHeight, Ledger)
    end.

-spec print(txn_unstake_validator()) -> iodata().
print(undefined) -> <<"type=unstake_validator, undefined">>;
print(#blockchain_txn_unstake_validator_v1_pb{
         owner = O,
         address = Val,
         stake_amount = A,
         stake_release_height = SRH
        }) ->
    io_lib:format("type=unstake_validator, owner=~p, validator=~p, stake_amount=~p, stake_release_height=~p",
                  [?TO_B58(O), ?TO_ANIMAL_NAME(Val), A, SRH]).

json_type() ->
    <<"unstake_validator_v1">>.

-spec to_json(txn_unstake_validator(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      address => ?BIN_TO_B58(address(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      owner_signature => ?BIN_TO_B64(owner_signature(Txn)),
      fee => fee(Txn),
      stake_amount => stake_amount(Txn),
      stake_release_height => stake_release_height(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    Tx = new(<<"validator_address">>, <<"owner_address">>, 10, 200, 3000),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_unstake_validator_v1_pb))).


-endif.
