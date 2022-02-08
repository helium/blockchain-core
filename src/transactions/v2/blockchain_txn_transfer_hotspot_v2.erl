-module(blockchain_txn_transfer_hotspot_v2).
-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_transfer_hotspot_v2_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/4,
    gateway/1,
    owner/1,
    nonce/1, nonce/2,
    owner_signature/1,
    new_owner/1,
    fee/2, fee/1,
    fee_payer/2,
    calculate_fee/2, calculate_fee/5,
    hash/1,
    sign/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    is_valid_owner/1,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-define(T, #blockchain_txn_transfer_hotspot_v2_pb).

-type t() :: txn_transfer_hotspot_v2().

-type txn_transfer_hotspot_v2() :: ?T{}.
-export_type([t/0, txn_transfer_hotspot_v2/0]).

-spec new(
    Gateway :: libp2p_crypto:pubkey_bin(),
    Owner :: libp2p_crypto:pubkey_bin(),
    NewOwner :: libp2p_crypto:pubkey_bin(),
    GwNonce :: non_neg_integer()
) -> txn_transfer_hotspot_v2().
new(Gateway, Owner, NewOwner, GwNonce) ->
    #blockchain_txn_transfer_hotspot_v2_pb{
        gateway = Gateway,
        owner = Owner,
        new_owner = NewOwner,
        owner_signature = <<>>,
        fee = 0,
        nonce=GwNonce
    }.

-spec hash(txn_transfer_hotspot_v2()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v2_pb{owner_signature = <<>>},
    EncodedTxn = blockchain_txn_transfer_hotspot_v2_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec gateway(txn_transfer_hotspot_v2()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb.gateway.

-spec owner(txn_transfer_hotspot_v2()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb.owner.

-spec nonce(txn_transfer_hotspot_v2()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb.nonce.

-spec nonce(txn_transfer_hotspot_v2(), non_neg_integer()) -> txn_transfer_hotspot_v2().
nonce(Txn, Nonce) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb{nonce = Nonce}.

-spec owner_signature(txn_transfer_hotspot_v2()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb.owner_signature.

-spec new_owner(txn_transfer_hotspot_v2()) -> libp2p_crypto:pubkey_bin().
new_owner(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb.new_owner.

-spec fee(txn_transfer_hotspot_v2()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb.fee.

-spec fee(txn_transfer_hotspot_v2(), non_neg_integer()) -> txn_transfer_hotspot_v2().
fee(Txn, Fee) ->
    Txn#blockchain_txn_transfer_hotspot_v2_pb{fee = Fee}.

-spec calculate_fee(txn_transfer_hotspot_v2(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec fee_payer(
    Txn :: txn_transfer_hotspot_v2(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, Ledger) ->
    case blockchain:config(?transaction_validity_version, Ledger) of
        {ok, 3} -> owner(Txn);
        {ok, 2} -> new_owner(Txn);
        _ -> undefined
    end.

-spec calculate_fee(
    txn_transfer_hotspot_v2(),
    blockchain_ledger_v1:ledger(),
    pos_integer(),
    pos_integer(),
    boolean()
) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(
        Txn#blockchain_txn_transfer_hotspot_v2_pb{
            fee = 0,
            owner_signature = <<0:512>>
        },
        Ledger,
        DCPayloadSize,
        TxnFeeMultiplier
    ).

-spec sign(
    Txn :: txn_transfer_hotspot_v2(),
    SigFun :: libp2p_crypto:sig_fun()
) -> txn_transfer_hotspot_v2().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v2_pb{owner_signature = <<>>},
    BinTxn = blockchain_txn_transfer_hotspot_v2_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_transfer_hotspot_v2_pb{owner_signature = SigFun(BinTxn)}.

-spec is_valid_owner(txn_transfer_hotspot_v2()) -> boolean().
is_valid_owner(
    #blockchain_txn_transfer_hotspot_v2_pb{
        owner = Owner,
        owner_signature = OwnerSig
    } = Txn
) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v2_pb{owner_signature = <<>>},
    EncodedTxn = blockchain_txn_transfer_hotspot_v2_pb:encode_msg(BaseTxn),
    Pubkey = libp2p_crypto:bin_to_pubkey(Owner),
    libp2p_crypto:verify(EncodedTxn, OwnerSig, Pubkey).

-spec is_valid(txn_transfer_hotspot_v2(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    BaseChecks = base_validity_checks(Txn, Ledger, Chain),
    case blockchain:config(?transaction_validity_version, Ledger) of
        {ok, 3} ->
            OwnerFeeCheck = {fun() -> owner_can_pay_fee(Txn, Ledger) end, {error, gateway_owner_cannot_pay_fee}},
            blockchain_utils:fold_condition_checks(BaseChecks ++ [OwnerFeeCheck]);
        {ok, 2} ->
            blockchain_utils:fold_condition_checks(BaseChecks);
        _ ->
            {error, transaction_validity_version_not_set}
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

-spec absorb(txn_transfer_hotspot_v2(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Gateway = ?MODULE:gateway(Txn),
    NewOwner = ?MODULE:new_owner(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    FeePayer = fee_payer(Txn, Ledger),

    {ok, GWInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),

    %% fees here are in DC
    case blockchain_ledger_v1:debit_fee(FeePayer, Fee, Ledger, AreFeesEnabled, Hash, Chain) of
        {error, _Reason} = Error ->
            Error;
        ok ->
            NewGWInfo0 = blockchain_ledger_gateway_v2:owner_address(NewOwner, GWInfo),
            NewGWInfo1 = blockchain_ledger_gateway_v2:nonce(Nonce, NewGWInfo0),
            ok = blockchain_ledger_v1:update_gateway(GWInfo, NewGWInfo1, Gateway, Ledger)
    end.

-spec print(txn_transfer_hotspot_v2()) -> iodata().
print(undefined) ->
    <<"type=transfer_hotspot_v2, undefined">>;
print(#blockchain_txn_transfer_hotspot_v2_pb{
    gateway = GW,
    owner = Owner,
    new_owner = NewOwner,
    owner_signature = OS,
    fee = Fee,
    nonce = Nonce
}) ->
    io_lib:format(
        "type=transfer_hotspot_v2, gateway=~p, owner=~p, new_owner=~p, owner_signature=~p, fee=~p (dc), nonce=~p",
        [?TO_ANIMAL_NAME(GW), ?TO_B58(Owner), ?TO_B58(NewOwner), OS, Fee, Nonce]
    ).

json_type() ->
    <<"transfer_hotspot_v2">>.

-spec to_json(txn_transfer_hotspot_v2(), blockchain_json:options()) ->
    blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
        type => ?MODULE:json_type(),
        hash => ?BIN_TO_B64(hash(Txn)),
        gateway => ?BIN_TO_B58(gateway(Txn)),
        owner => ?BIN_TO_B58(owner(Txn)),
        new_owner => ?BIN_TO_B58(new_owner(Txn)),
        fee => fee(Txn),
        nonce => nonce(Txn)
    }.

%% Helper Functions

-spec owner_owns_gateway(txn_transfer_hotspot_v2(), blockchain_ledger_v1:ledger()) -> boolean().
owner_owns_gateway(
    #blockchain_txn_transfer_hotspot_v2_pb{gateway = GW, owner = Owner},
    Ledger
) ->
    case blockchain_ledger_v1:find_gateway_info(GW, Ledger) of
        {error, _} ->
            false;
        {ok, GwInfo} ->
            GwOwner = blockchain_ledger_gateway_v2:owner_address(GwInfo),
            Owner == GwOwner
    end.

-spec owner_can_pay_fee(txn_transfer_hotspot_v2(), blockchain_ledger_v1:ledger()) -> boolean().
owner_can_pay_fee(
    #blockchain_txn_transfer_hotspot_v2_pb{owner = Owner, fee = Fee},
    Ledger
) ->
    BalanceCheckFun =
    fun(Owner0, Ledger0) ->
            %% check if the owner has enough HNT to cover the fee
            case blockchain_ledger_v1:find_entry(Owner0, Ledger0) of
                {error, _} ->
                    false;
                {ok, BalanceEntry} ->
                    {ok, FeeInHNT} = blockchain_ledger_v1:dc_to_hnt(Fee, Ledger0),
                    EntryBalance = blockchain_ledger_entry_v1:balance(BalanceEntry),
                    (EntryBalance - FeeInHNT) >= 0
            end
    end,

    case blockchain_ledger_v1:find_dc_entry(Owner, Ledger) of
        {error, _} ->
            BalanceCheckFun(Owner, Ledger);
        {ok, DCEntry} ->
            DCBalance = blockchain_ledger_data_credits_entry_v1:balance(DCEntry),
            case (DCBalance - Fee) >= 0 of
                false ->
                    BalanceCheckFun(Owner, Ledger);
                true ->
                    %% Owner has enough DC balance to pay the required fee
                    true
            end
    end.

-spec txn_fee_valid(txn_transfer_hotspot_v2(), blockchain:blockchain(), boolean()) -> boolean().
txn_fee_valid(#blockchain_txn_transfer_hotspot_v2_pb{fee = Fee} = Txn, Chain, AreFeesEnabled) ->
    ExpectedTxnFee = calculate_fee(Txn, Chain),
    ExpectedTxnFee =< Fee orelse not AreFeesEnabled.

-spec is_valid_nonce(Txn :: txn_transfer_hotspot_v2(),
                     Ledger :: blockchain_ledger_v1:ledger()) -> boolean().
is_valid_nonce(#blockchain_txn_transfer_hotspot_v2_pb{gateway=GWPubkeyBin, nonce=Nonce}, Ledger) ->
    case blockchain_ledger_v1:find_gateway_info(GWPubkeyBin, Ledger) of
        {ok, Gw} ->
            GwNonce = blockchain_ledger_gateway_v2:nonce(Gw),
            Nonce == GwNonce + 1;
        _ ->
            false
    end.

-spec is_gateway_on_chain(Txn :: txn_transfer_hotspot_v2(),
                          Ledger :: blockchain_ledger_v1:ledger()) -> boolean().
is_gateway_on_chain(#blockchain_txn_transfer_hotspot_v2_pb{gateway=GWPubkeyBin}, Ledger) ->
    case blockchain_ledger_v1:find_gateway_info(GWPubkeyBin, Ledger) of
        {ok, _Gw} ->
            true;
        _ ->
            false
    end.

-spec base_validity_checks(Txn :: txn_transfer_hotspot_v2(),
                           Ledger :: blockchain_ledger_v1:ledger(),
                           Chain :: blockchain:blockchain()) -> [{fun(), {error, any()}}, ...].
base_validity_checks(Txn=#blockchain_txn_transfer_hotspot_v2_pb{owner=Owner, new_owner=NewOwner, nonce=Nonce},
                     Ledger,
                     Chain) ->
    %% NOTE: Conditional checks are processed sequentially
    [
        {fun() -> is_gateway_on_chain(Txn, Ledger) end, {error, unknown_gateway}},
        {fun() -> is_valid_nonce(Txn, Ledger) end, {error, {invalid_nonce, Nonce}}},
        {fun() -> ?MODULE:is_valid_owner(Txn) end, {error, bad_owner_signature}},
        {fun() -> Owner /= NewOwner end, {error, owner_is_buyer}},
        {fun() -> owner_owns_gateway(Txn, Ledger) end, {error, gateway_not_owned_by_owner}},
        {fun() -> txn_fee_valid(Txn, Chain, blockchain_ledger_v1:txn_fees_active(Ledger)) end, {error, wrong_txn_fee}}
    ].

-ifdef(TEST).
new_test() ->
    Tx = #blockchain_txn_transfer_hotspot_v2_pb{
        gateway = <<"gateway">>,
        owner = <<"owner">>,
        owner_signature = <<>>,
        new_owner = <<"new_owner">>,
        fee = 0,
        nonce = 1
    },
    ?assertEqual(Tx, new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1)).

gateway_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1),
    ?assertEqual(<<"gateway">>, gateway(Tx)).

owner_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1),
    ?assertEqual(<<"owner">>, owner(Tx)).

owner_signature_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1),
    ?assertEqual(<<>>, owner_signature(Tx)).

fee_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1),
    ?assertEqual(20, fee(fee(Tx, 20))).

nonce_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1),
    ?assertEqual(1, nonce(Tx)).

nonce_set_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1),
    ?assertEqual(2, nonce(nonce(Tx, 2))).

sign_owner_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx = new(<<"gateway">>, libp2p_crypto:pubkey_to_bin(PubKey), <<"new_owner">>, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = sign(Tx, SigFun),
    ?assert(is_valid_owner(Tx0)).

to_json_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>, 1),
    Json = to_json(Tx, []),
    ?assert(
        lists:all(
            fun(K) -> maps:is_key(K, Json) end,
            [type, hash, gateway, owner, new_owner, fee, nonce]
        )
    ).

-endif.
