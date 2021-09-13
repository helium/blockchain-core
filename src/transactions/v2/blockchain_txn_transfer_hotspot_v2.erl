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
    new/3,
    gateway/1,
    owner/1,
    owner_signature/1,
    new_owner/1,
    fee/2, fee/1,
    fee_payer/2,
    calculate_fee/2, calculate_fee/5,
    hash/1,
    sign/2,
    is_valid/2,
    is_valid_owner/1,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-type txn_transfer_hotspot_v2() :: #blockchain_txn_transfer_hotspot_v2_pb{}.
-export_type([txn_transfer_hotspot_v2/0]).

-spec new(
    Gateway :: libp2p_crypto:pubkey_bin(),
    Owner :: libp2p_crypto:pubkey_bin(),
    NewOwner :: libp2p_crypto:pubkey_bin()
) -> txn_transfer_hotspot_v2().
new(Gateway, Owner, NewOwner) ->
    #blockchain_txn_transfer_hotspot_v2_pb{
        gateway = Gateway,
        owner = Owner,
        new_owner = NewOwner,
        owner_signature = <<>>,
        fee = 0
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
fee_payer(Txn, _Ledger) ->
    new_owner(Txn).

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
is_valid(
    #blockchain_txn_transfer_hotspot_v2_pb{
        owner = Owner,
        new_owner = NewOwner
    } = Txn,
    Chain
) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Conditions = [
        {fun() -> Owner /= NewOwner end, {error, owner_is_buyer}},
        {fun() -> ?MODULE:is_valid_owner(Txn) end, {error, bad_owner_signature}},
        {fun() -> owner_owns_gateway(Txn, Ledger) end, {error, gateway_not_owned_by_owner}},
        {fun() -> txn_fee_valid(Txn, Chain, AreFeesEnabled) end, {error, wrong_txn_fee}}
    ],
    blockchain_utils:fold_condition_checks(Conditions).

-spec absorb(txn_transfer_hotspot_v2(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Gateway = ?MODULE:gateway(Txn),
    NewOwner = ?MODULE:new_owner(Txn),
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),

    {ok, GWInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    %% fees here are in DC
    case blockchain_ledger_v1:debit_fee(NewOwner, Fee, Ledger, AreFeesEnabled, Hash, Chain) of
        {error, _Reason} = Error ->
            Error;
        ok ->
            NewGWInfo = blockchain_ledger_gateway_v2:owner_address(NewOwner, GWInfo),
            ok = blockchain_ledger_v1:update_gateway(NewGWInfo, Gateway, Ledger)
    end.

-spec print(txn_transfer_hotspot_v2()) -> iodata().
print(undefined) ->
    <<"type=transfer_hotspot_v2, undefined">>;
print(#blockchain_txn_transfer_hotspot_v2_pb{
    gateway = GW,
    owner = Owner,
    new_owner = NewOwner,
    owner_signature = OS,
    fee = Fee
}) ->
    io_lib:format(
        "type=transfer_hotspot_v2, gateway=~p, owner=~p, new_owner=~p, owner_signature=~p, fee=~p (dc)",
        [?TO_ANIMAL_NAME(GW), ?TO_B58(Owner), ?TO_B58(NewOwner), OS, Fee]
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
        fee => fee(Txn)
    }.

%% private functions
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

-spec txn_fee_valid(txn_transfer_hotspot_v2(), blockchain:blockchain(), boolean()) -> boolean().
txn_fee_valid(#blockchain_txn_transfer_hotspot_v2_pb{fee = Fee} = Txn, Chain, AreFeesEnabled) ->
    ExpectedTxnFee = calculate_fee(Txn, Chain),
    ExpectedTxnFee =< Fee orelse not AreFeesEnabled.

-ifdef(TEST).
new_test() ->
    Tx = #blockchain_txn_transfer_hotspot_v2_pb{
        gateway = <<"gateway">>,
        owner = <<"owner">>,
        owner_signature = <<>>,
        new_owner = <<"new_owner">>,
        fee = 0
    },
    ?assertEqual(Tx, new(<<"gateway">>, <<"owner">>, <<"new_owner">>)).

gateway_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>),
    ?assertEqual(<<"gateway">>, gateway(Tx)).

owner_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>),
    ?assertEqual(<<"owner">>, owner(Tx)).

owner_signature_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>),
    ?assertEqual(<<>>, owner_signature(Tx)).

fee_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>),
    ?assertEqual(20, fee(fee(Tx, 20))).

sign_owner_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx = new(<<"gateway">>, libp2p_crypto:pubkey_to_bin(PubKey), <<"new_owner">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = sign(Tx, SigFun),
    ?assert(is_valid_owner(Tx0)).

to_json_test() ->
    Tx = new(<<"gateway">>, <<"owner">>, <<"new_owner">>),
    Json = to_json(Tx, []),
    ?assert(
        lists:all(
            fun(K) -> maps:is_key(K, Json) end,
            [type, hash, gateway, owner, new_owner, fee]
        )
    ).

-endif.
