%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Update Gateway OUI ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_update_gateway_oui_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_update_gateway_oui_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    gateway/1,
    oui/1,
    nonce/1,
    fee/1, fee/2,
    fee_payer/2,
    calculate_fee/2, calculate_fee/5,
    gateway_owner_signature/1,
    oui_owner_signature/1,
    sign/2,
    gateway_owner_sign/2,
    oui_owner_sign/2,
    is_valid_gateway_owner/2,
    is_valid_oui_owner/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-include("blockchain_utils.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_update_gateway_oui_v1_pb).

-type t() :: txn_update_gateway_oui().

-type txn_update_gateway_oui() :: ?T{}.

-export_type([t/0, txn_update_gateway_oui/0]).

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          OUI :: pos_integer(),
          Nonce :: non_neg_integer()) -> txn_update_gateway_oui().
new(Gateway, OUI, Nonce) ->
    #blockchain_txn_update_gateway_oui_v1_pb{
        gateway=Gateway,
        oui=OUI,
        nonce=Nonce,
        fee=?LEGACY_TXN_FEE,
        gateway_owner_signature= <<>>,
        oui_owner_signature= <<>>
    }.

-spec hash(txn_update_gateway_oui()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_update_gateway_oui_v1_pb{gateway_owner_signature = <<>>, oui_owner_signature = <<>>},
    EncodedTxn = blockchain_txn_update_gateway_oui_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec gateway(txn_update_gateway_oui()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_update_gateway_oui_v1_pb.gateway.

-spec oui(txn_update_gateway_oui()) -> pos_integer().
oui(Txn) ->
    Txn#blockchain_txn_update_gateway_oui_v1_pb.oui.

-spec nonce(txn_update_gateway_oui()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_update_gateway_oui_v1_pb.nonce.

-spec fee(txn_update_gateway_oui()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_update_gateway_oui_v1_pb.fee.

-spec fee(txn_update_gateway_oui(), non_neg_integer()) -> txn_update_gateway_oui().
fee(Txn, Fee) ->
    Txn#blockchain_txn_update_gateway_oui_v1_pb{fee=Fee}.

-spec fee_payer(txn_update_gateway_oui(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, Ledger) ->
    {ok, GWInfo} = blockchain_ledger_v1:find_gateway_info(gateway(Txn), Ledger),
    blockchain_ledger_gateway_v2:owner_address(GWInfo).
 
-spec gateway_owner_signature(txn_update_gateway_oui()) -> binary().
gateway_owner_signature(Txn) ->
    Txn#blockchain_txn_update_gateway_oui_v1_pb.gateway_owner_signature.

-spec oui_owner_signature(txn_update_gateway_oui()) -> binary().
oui_owner_signature(Txn) ->
    Txn#blockchain_txn_update_gateway_oui_v1_pb.oui_owner_signature.

%%--------------------------------------------------------------------
%% @doc
%% Only kept for callback
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_update_gateway_oui(), libp2p_crypto:sig_fun()) -> txn_update_gateway_oui().
sign(Txn, _SigFun) ->
    Txn.

-spec gateway_owner_sign(txn_update_gateway_oui(), libp2p_crypto:sig_fun()) -> txn_update_gateway_oui().
gateway_owner_sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_update_gateway_oui_v1_pb{gateway_owner_signature= <<>>, oui_owner_signature= <<>>},
    EncodedTxn = blockchain_txn_update_gateway_oui_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_update_gateway_oui_v1_pb{gateway_owner_signature=SigFun(EncodedTxn)}.

-spec oui_owner_sign(txn_update_gateway_oui(), libp2p_crypto:sig_fun()) -> txn_update_gateway_oui().
oui_owner_sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_update_gateway_oui_v1_pb{gateway_owner_signature= <<>>, oui_owner_signature= <<>>},
    EncodedTxn = blockchain_txn_update_gateway_oui_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_update_gateway_oui_v1_pb{oui_owner_signature=SigFun(EncodedTxn)}.

-spec is_valid_gateway_owner(libp2p_crypto:pubkey_bin(), txn_update_gateway_oui()) -> boolean().
is_valid_gateway_owner(GatewayOwner, #blockchain_txn_update_gateway_oui_v1_pb{gateway_owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_update_gateway_oui_v1_pb{gateway_owner_signature= <<>>, oui_owner_signature= <<>>},
    EncodedTxn = blockchain_txn_update_gateway_oui_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(GatewayOwner),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid_oui_owner(libp2p_crypto:pubkey_bin(), txn_update_gateway_oui()) -> boolean().
is_valid_oui_owner(OUIOwner, #blockchain_txn_update_gateway_oui_v1_pb{oui_owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_update_gateway_oui_v1_pb{gateway_owner_signature= <<>>, oui_owner_signature= <<>>},
    EncodedTxn = blockchain_txn_update_gateway_oui_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(OUIOwner),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_update_gateway_oui(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_update_gateway_oui(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_update_gateway_oui_v1_pb{fee=0, gateway_owner_signature = <<0:512>>, oui_owner_signature = <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).


-spec is_valid(txn_update_gateway_oui(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case {validate_oui(Txn, Ledger),
          validate_gateway(Txn, Ledger)} of
        {{error, _}=Err, _} ->
            Err;
        {_, {error, _}=Err} ->
            Err;
        {ok, {ok, GWInfo}} ->
            LedgerNonce = blockchain_ledger_gateway_v2:nonce(GWInfo),
            Nonce = ?MODULE:nonce(Txn),
            case Nonce =:= LedgerNonce + 1 of
                false ->
                    {error, {bad_nonce, {update_gateway_oui, Nonce, LedgerNonce}}};
                true ->
                    TxnFee = ?MODULE:fee(Txn),
                    GatewayOwner = blockchain_ledger_gateway_v2:owner_address(GWInfo),
                    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                    case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                        false ->
                            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                        true ->
                            blockchain_ledger_v1:check_dc_or_hnt_balance(GatewayOwner, TxnFee, Ledger, AreFeesEnabled)
                    end
            end
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

-spec absorb(txn_update_gateway_oui(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Gateway = ?MODULE:gateway(Txn),
    Nonce = ?MODULE:nonce(Txn),
    case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
        {error, _Reason}=Error ->
            Error;
        {ok, GWInfo} ->
            Fee = ?MODULE:fee(Txn),
            Hash = ?MODULE:hash(Txn),
            GatewayOwner = blockchain_ledger_gateway_v2:owner_address(GWInfo),
            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
            case blockchain_ledger_v1:debit_fee(GatewayOwner, Fee, Ledger, AreFeesEnabled, Hash, Chain) of
                {error, _}=Error ->
                    Error;
                ok ->
                    OUI = ?MODULE:oui(Txn),
                    blockchain_ledger_v1:update_gateway_oui(Gateway, OUI, Nonce, Ledger)
            end
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec validate_oui(txn_update_gateway_oui(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
validate_oui(Txn, Ledger) ->
    OUI = ?MODULE:oui(Txn),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, not_found} ->
            {error, oui_not_found};
        {error, _Reason}=Error ->
            Error;
        {ok, Routing} ->
            OUIOwner = blockchain_ledger_routing_v1:owner(Routing),
            case ?MODULE:is_valid_oui_owner(OUIOwner, Txn) of
                false -> {error, invalid_oui_owner_signature};
                true -> ok
            end
    end.

-spec validate_gateway(txn_update_gateway_oui(), blockchain_ledger_v1:ledger()) -> {ok, libp2p_crypto:pubkey_bin()} | {error, any()}.
validate_gateway(Txn, Ledger) ->
    Gateway = ?MODULE:gateway(Txn),
    case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
        {error, not_found} ->
            {error, gateway_not_found};
        {error, _Reason}=Error ->
            Error;
        {ok, GWInfo} ->
            GatewayOwner = blockchain_ledger_gateway_v2:owner_address(GWInfo),
            case ?MODULE:is_valid_gateway_owner(GatewayOwner, Txn) of
                false -> {error, invalid_gateway_owner_signature};
                true -> {ok, GWInfo}
            end
    end.

-spec print(txn_update_gateway_oui()) -> iodata().
print(undefined) -> <<"type=update_gateway_oui, undefined">>;
print(#blockchain_txn_update_gateway_oui_v1_pb{gateway=GW, oui=OUI, nonce=Nonce, fee=Fee}) ->
    io_lib:format("type=update_gateway_oui, gateway=~p, oui=~p, nonce=~p, fee=~p", [?TO_ANIMAL_NAME(GW), OUI, Nonce, Fee]).

json_type() ->
    <<"update_gateway_oui_v1">>.

-spec to_json(txn_update_gateway_oui(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      gateway => ?BIN_TO_B58(gateway(Txn)),
      oui => oui(Txn),
      fee => fee(Txn),
      nonce => nonce(Txn)
     }.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Update = #blockchain_txn_update_gateway_oui_v1_pb{
        gateway= <<"gateway">>,
        oui=1,
        nonce=0,
        fee=?LEGACY_TXN_FEE,
        gateway_owner_signature= <<>>,
        oui_owner_signature= <<>>
    },
    ?assertEqual(Update, new(<<"gateway">>, 1, 0)).

gateway_test() ->
    Update = new(<<"gateway">>, 1, 0),
    ?assertEqual(<<"gateway">>, gateway(Update)).

oui_test() ->
    Update = new(<<"gateway">>, 1, 0),
    ?assertEqual(1, oui(Update)).

nonce_test() ->
    Update = new(<<"gateway">>, 1, 0),
    ?assertEqual(0, nonce(Update)).

fee_test() ->
    Update = new(<<"gateway">>, 1, 0),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Update)).

gateway_owner_signature_test() ->
    Update = new(<<"gateway">>, 1, 0),
    ?assertEqual(<<>>, gateway_owner_signature(Update)).

oui_owner_signature_test() ->
    Update = new(<<"gateway">>, 1, 0),
    ?assertEqual(<<>>, oui_owner_signature(Update)).

is_valid_gateway_owner_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Update0 = new(<<"gateway">>, 1, 0),
    Update1 = gateway_owner_sign(Update0, SigFun),
    ?assert(is_valid_gateway_owner(PubKeyBin, Update1)).

is_valid_oui_owner_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Update0 = new(<<"gateway">>, 1, 0),
    Update1 = oui_owner_sign(Update0, SigFun),
    ?assert(is_valid_oui_owner(PubKeyBin, Update1)).

to_json_test() ->
    Tx = new(<<"gateway">>, 1, 0),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, gateway, oui, fee, nonce])).

-endif.
