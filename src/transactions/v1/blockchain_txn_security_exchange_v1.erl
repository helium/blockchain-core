%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Security Exchange ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_security_exchange_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_security_exchange_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    payer/1,
    payee/1,
    amount/1,
    fee/1, fee/2,
    fee_payer/2,
    calculate_fee/2, calculate_fee/5,
    nonce/1,
    signature/1,
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
-endif.

-define(T, #blockchain_txn_security_exchange_v1_pb).

-type t() :: txn_security_exchange().

-type txn_security_exchange() :: ?T{}.

-export_type([t/0, txn_security_exchange/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), pos_integer(),
          non_neg_integer()) -> txn_security_exchange().
new(Payer, Recipient, Amount, Nonce) ->
    #blockchain_txn_security_exchange_v1_pb{
        payer=Payer,
        payee=Recipient,
        amount=Amount,
        fee=?LEGACY_TXN_FEE,
        nonce=Nonce,
        signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_security_exchange()) -> iodata().
print(undefined) -> <<"type=security_exchange undefined">>;
print(#blockchain_txn_security_exchange_v1_pb{payer=Payer, payee=Recipient,
                                              amount=Amount, fee=Fee,
                                              nonce=Nonce, signature = Sig}) ->
    io_lib:format("type=security_exchange payer=~p payee=~p amount=~p fee=~p nonce=~p signature=~p",
                  [?TO_B58(Payer), ?TO_B58(Recipient), Amount, Fee, Nonce, Sig]).

json_type() ->
    <<"security_exchange_v1">>.

-spec to_json(txn_security_exchange(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      payer => ?BIN_TO_B58(payer(Txn)),
      payee => ?BIN_TO_B58(payee(Txn)),
      amount => amount(Txn),
      fee => fee(Txn),
      nonce => nonce(Txn)
     }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_security_exchange()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_security_exchange_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_security_exchange_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_security_exchange()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.payer.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_security_exchange()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_security_exchange()) -> pos_integer().
amount(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_security_exchange()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_security_exchange()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.fee.

-spec fee(txn_security_exchange(), non_neg_integer()) -> txn_security_exchange().
fee(Txn, Fee) ->
    Txn#blockchain_txn_security_exchange_v1_pb{fee=Fee}.

-spec fee_payer(txn_security_exchange(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    payer(Txn).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_security_exchange()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_security_exchange_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_security_exchange(), libp2p_crypto:sig_fun()) -> txn_security_exchange().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_security_exchange_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_security_exchange_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_security_exchange(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_security_exchange(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_security_exchange_v1_pb{fee=0, signature= <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_security_exchange(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Payee = ?MODULE:payee(Txn),
    TxnFee = ?MODULE:fee(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_security_exchange_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_security_exchange_v1_pb:encode_msg(BaseTxn),
    case blockchain_txn:validate_fields([{{payee, Payee}, {address, libp2p}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                false ->
                    {error, bad_signature};
                true ->
                    case Payer == Payee of
                        false ->
                            Amount = ?MODULE:amount(Txn),
                            case blockchain_ledger_v1:check_security_balance(Payer, Amount, Ledger) of
                                ok ->
                                    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                    case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                        false ->
                                            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                        true ->
                                            blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
                                    end;
                                Error ->
                                    Error
                            end;
                        true ->
                            {error, invalid_transaction_self_payment}
                    end
            end;
        Error -> Error
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_security_exchange(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Amount = ?MODULE:amount(Txn),
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger, AreFeesEnabled, Hash, Chain) of
        ok ->
            case blockchain_ledger_v1:debit_security(Payer, Amount, Nonce, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    Payee = ?MODULE:payee(Txn),
                    blockchain_ledger_v1:credit_security(Payee, Amount, Ledger)
            end;
        FeeError ->
            FeeError
    end.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_security_exchange_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        amount=666,
        fee=?LEGACY_TXN_FEE,
        nonce=1,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"payer">>, <<"payee">>, 666, 1)).

payer_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(666, amount(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, <<"payee">>, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_security_exchange_v1_pb:encode_msg(Tx1#blockchain_txn_security_exchange_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Tx = new(<<"payer">>, <<"payee">>, 666, 10),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, payer, payee, amount, fee, nonce])).

-endif.
