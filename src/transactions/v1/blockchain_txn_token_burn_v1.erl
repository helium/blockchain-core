%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_token_burn_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include_lib("helium_proto/include/blockchain_txn_token_burn_v1_pb.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").

  -export([
    new/3, new/4,
    hash/1,
    payer/1,
    payee/1,
    amount/1,
    nonce/1,
    fee/1, fee/2,
    calculate_fee/2, calculate_fee/3,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_token_burn() :: #blockchain_txn_token_burn_v1_pb{}.
-export_type([txn_token_burn/0]).

-spec new(libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer()) -> txn_token_burn().
new(Payer, Amount, Nonce) ->
    #blockchain_txn_token_burn_v1_pb{
        payer=Payer,
        payee=Payer,
        amount=Amount,
        nonce=Nonce,
        fee=?LEGACY_TXN_FEE,
        signature = <<>>
    }.

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer()) -> txn_token_burn().
new(Payer, Payee, Amount, Nonce) ->
    #blockchain_txn_token_burn_v1_pb{
        payer=Payer,
        payee=Payee,
        amount=Amount,
        nonce=Nonce,
        fee=?LEGACY_TXN_FEE,
        signature = <<>>
    }.

-spec hash(txn_token_burn()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_token_burn_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec payer(txn_token_burn()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.payer.

-spec payee(txn_token_burn()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.payee.

-spec amount(txn_token_burn()) -> pos_integer().
amount(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.amount.

-spec nonce(txn_token_burn()) -> pos_integer().
nonce(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.nonce.

-spec fee(txn_token_burn()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.fee.

-spec fee(txn_token_burn(), non_neg_integer()) -> txn_token_burn().
fee(Txn, Fee) ->
    Txn#blockchain_txn_token_burn_v1_pb{fee=Fee}.

-spec signature(txn_token_burn()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.signature.

 %%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_token_burn(), libp2p_crypto:sig_fun()) -> txn_token_burn().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_token_burn_v1_pb{signature=SigFun(EncodedTxn)}.


%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_token_burn(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    calculate_fee(Txn, Ledger, blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_fee(txn_token_burn(), blockchain_ledger_v1:ledger(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, true) ->
    ?fee(Txn#blockchain_txn_token_burn_v1_pb{fee=0, signature= <<0:512>>}) * blockchain_ledger_v1:payment_txn_fee_multiplier(Ledger).


-spec is_valid(txn_token_burn(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_token_burn_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(BaseTxn),
    case blockchain_txn:validate_fields([{{payee, ?MODULE:payee(Txn)}, {address, libp2p}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                false ->
                    {error, bad_signature};
                true ->
                    case blockchain_ledger_v1:current_oracle_price_list(Ledger) of
                        {ok, []} ->
                            %% no oracle price exists
                            {error, no_oracle_prices};
                        _ ->
                            HNTAmount = ?MODULE:amount(Txn),
                            case blockchain_ledger_v1:check_balance(Payer, HNTAmount, Ledger) of
                                {error, _}=Error ->
                                    Error;
                                ok ->
                                    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                    TxnFee = ?MODULE:fee(Txn),
                                    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                    case ExpectedTxnFee == TxnFee orelse not AreFeesEnabled of
                                        false ->
                                            {error, {wrong_txn_fee, ExpectedTxnFee, TxnFee}};
                                        true ->
                                            blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
                                    end
                            end
                    end
            end;
        Error ->
            Error
    end.

-spec absorb(txn_token_burn(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    HNTAmount = ?MODULE:amount(Txn),
    {ok, DCAmount} = blockchain_ledger_v1:hnt_to_dc(HNTAmount, Ledger),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    TxnFee = ?MODULE:fee(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, TxnFee, Ledger, AreFeesEnabled) of
        {error, _Reason}=Error -> Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, HNTAmount, Nonce, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    Payee = ?MODULE:payee(Txn),
                    blockchain_ledger_v1:credit_dc(Payee, DCAmount, Ledger)
            end
    end.

-spec print(txn_token_burn()) -> iodata().
print(undefined) -> <<"type=token_burn, undefined">>;
print(#blockchain_txn_token_burn_v1_pb{payer=Payer, payee=Payee, amount=Amount, nonce=Nonce}) ->
    io_lib:format("type=token_burn, payer=~p, payee=~p, amount=~p, nonce=~p",
                  [?TO_B58(Payer), ?TO_B58(Payee), Amount, Nonce]).


-spec to_json(txn_token_burn(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"token_burn_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      payer => ?BIN_TO_B58(payer(Txn)),
      amount => amount(Txn),
      nonce => nonce(Txn)
     }.

 %% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

  new_test() ->
    Tx0 = #blockchain_txn_token_burn_v1_pb{
        payer= <<"payer">>,
        payee= <<"payer">>,
        amount=666,
        nonce=1,
        fee=0,
        signature = <<>>
    },
    ?assertEqual(Tx0, new(<<"payer">>, 666, 1)),
    Tx1 = #blockchain_txn_token_burn_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        amount=666,
        nonce=1,
        fee=?LEGACY_TXN_FEE,
        signature = <<>>
    },
    ?assertEqual(Tx1, new(<<"payer">>, <<"payee">>, 666, 1)).

payer_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(<<"payer">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(666, amount(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(1, nonce(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_token_burn_v1_pb:encode_msg(Tx1#blockchain_txn_token_burn_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Tx = new(<<"payer">>, 666, 1),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, payer, amount, nonce])).

-endif.
