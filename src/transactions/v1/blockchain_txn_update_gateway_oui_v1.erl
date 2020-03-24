%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Update Gateway OUI ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_update_gateway_oui_v1).

-behavior(blockchain_txn).

-include_lib("helium_proto/include/blockchain_txn_update_gateway_oui_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    gateway/1,
    oui/1,
    nonce/1,
    fee/1,
    gateway_owner_signature/1,
    oui_owner_signature/1,
    sign/2,
    gateway_owner_sign/2,
    oui_owner_sign/2,
    is_valid_gateway_owner/2,
    is_valid_oui_owner/2,
    is_valid/2,
    absorb/2,
    print/1
]).

-include("blockchain_utils.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_update_gateway_oui() :: #blockchain_txn_update_gateway_oui_v1_pb{}.
-export_type([txn_update_gateway_oui/0]).

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          OUI :: pos_integer(),
          Nonce :: non_neg_integer(),
          Fee :: non_neg_integer()) -> txn_update_gateway_oui().
new(Gateway, OUI, Nonce, Fee) ->
    #blockchain_txn_update_gateway_oui_v1_pb{
        gateway=Gateway,
        oui=OUI,
        nonce=Nonce,
        fee=Fee,
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

-spec is_valid(txn_update_gateway_oui(), blockchain:blockchain()) -> ok | {error, any()}.
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
                    Fee = ?MODULE:fee(Txn),
                    GatewayOwner = blockchain_ledger_gateway_v2:owner_address(GWInfo),
                    blockchain_ledger_v1:check_dc_balance(GatewayOwner, Fee, Ledger)
            end
    end.

-spec absorb(txn_update_gateway_oui(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Gateway = ?MODULE:gateway(Txn),
    case blockchain_ledger_v1:find_gateway_info(Gateway, Ledger) of
        {error, _Reason}=Error ->
            Error;
        {ok, GWInfo} ->
            Fee = ?MODULE:fee(Txn),
            GatewayOwner = blockchain_ledger_gateway_v2:owner_address(GWInfo),
            case blockchain_ledger_v1:debit_fee(GatewayOwner, Fee, Ledger) of
                {error, _}=Error ->
                    Error;
                ok ->
                    OUI = ?MODULE:oui(Txn),
                    blockchain_ledger_v1:update_gateway_oui(Gateway, OUI, Ledger)
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

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Update = #blockchain_txn_update_gateway_oui_v1_pb{
        gateway= <<"gateway">>,
        oui=1,
        nonce=0,
        fee=12,
        gateway_owner_signature= <<>>,
        oui_owner_signature= <<>>
    },
    ?assertEqual(Update, new(<<"gateway">>, 1, 0, 12)).

gateway_test() ->
    Update = new(<<"gateway">>, 1, 0, 12),
    ?assertEqual(<<"gateway">>, gateway(Update)).

oui_test() ->
    Update = new(<<"gateway">>, 1, 0, 12),
    ?assertEqual(1, oui(Update)).

nonce_test() ->
    Update = new(<<"gateway">>, 1, 0, 12),
    ?assertEqual(0, nonce(Update)).

fee_test() ->
    Update = new(<<"gateway">>, 1, 0, 12),
    ?assertEqual(12, fee(Update)).

gateway_owner_signature_test() ->
    Update = new(<<"gateway">>, 1, 0, 12),
    ?assertEqual(<<>>, gateway_owner_signature(Update)).

oui_owner_signature_test() ->
    Update = new(<<"gateway">>, 1, 0, 12),
    ?assertEqual(<<>>, oui_owner_signature(Update)).

is_valid_gateway_owner_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Update0 = new(<<"gateway">>, 1, 0, 12),
    Update1 = gateway_owner_sign(Update0, SigFun),
    ?assert(is_valid_gateway_owner(PubKeyBin, Update1)).

is_valid_oui_owner_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Update0 = new(<<"gateway">>, 1, 0, 12),
    Update1 = oui_owner_sign(Update0, SigFun),
    ?assert(is_valid_oui_owner(PubKeyBin, Update1)).

-endif.
