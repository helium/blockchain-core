%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Payment V2 ==
%%
%% Support payer_A -> [payee_X, payee_Y, payee_Z...] transactions
%%
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_payment_v2).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v2_pb.hrl").

-export([
         new/3,
         hash/1,
         payer/1,
         payments/1,
         payees/1,
         amounts/1,
         total_amount/1,
         fee/1, fee/2,
         calculate_fee/2, calculate_fee/5,
         nonce/1,
         signature/1,
         sign/2,
         is_well_formed/1,
         is_absorbable/2,
         is_valid/2,
         absorb/2,
         print/1,
         to_json/2
        ]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen/1]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_payment_v2() :: #blockchain_txn_payment_v2_pb{}.

-export_type([txn_payment_v2/0]).

-spec new(Payer :: libp2p_crypto:pubkey_bin(),
          Payments :: blockchain_payment_v2:payments(),
          Nonce :: non_neg_integer()) -> txn_payment_v2().
new(Payer, Payments, Nonce) ->
    #blockchain_txn_payment_v2_pb{
       payer=Payer,
       payments=Payments,
       nonce=Nonce,
       fee=?LEGACY_TXN_FEE,
       signature = <<>>
      }.

-spec hash(txn_payment_v2()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_payment_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v2_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec payer(txn_payment_v2()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.payer.

-spec payments(txn_payment_v2()) -> blockchain_payment_v2:payments().
payments(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.payments.

-spec payees(txn_payment_v2()) -> [libp2p_crypto:pubkey_bin()].
payees(Txn) ->
    [blockchain_payment_v2:payee(Payment) || Payment <- ?MODULE:payments(Txn)].

-spec amounts(txn_payment_v2()) -> [pos_integer()].
amounts(Txn) ->
    [blockchain_payment_v2:amount(Payment) || Payment <- ?MODULE:payments(Txn)].

-spec total_amount(txn_payment_v2()) -> pos_integer().
total_amount(Txn) ->
    lists:sum(?MODULE:amounts(Txn)).

-spec fee(txn_payment_v2()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.fee.

-spec fee(txn_payment_v2(), non_neg_integer()) -> txn_payment_v2().
fee(Txn, Fee) ->
    Txn#blockchain_txn_payment_v2_pb{fee=Fee}.

-spec nonce(txn_payment_v2()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.nonce.

-spec signature(txn_payment_v2()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.signature.

-spec sign(txn_payment_v2(), libp2p_crypto:sig_fun()) -> txn_payment_v2().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_payment_v2_pb:encode_msg(Txn),
    Txn#blockchain_txn_payment_v2_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_payment_v2(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_payment_v2(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_payment_v2_pb{fee=0, signature = <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).

is_well_formed(Txn) ->
    Payments = ?MODULE:payments(Txn),
    case blockchain_txn:validate_fields([{{payee, P}, {address, libp2p}} || P <- ?MODULE:payees(Txn)] ++
                                       [{{payer, payer(Txn)}, {address, libp2p}},
                                       {{nonce, nonce(Txn)}, {is_integer, 1}},
                                       {{payment_count, length(Payments)}, {is_integer, 1}}]) of
        ok ->
            case lists:member(payer(Txn), payees(Txn)) of
                false ->
                    %% check that every payee is unique
                    case has_unique_payees(Payments) of
                        false ->
                            {error, duplicate_payees};
                        true ->
                            ok
                    end;
                true ->
                    {error, self_payment}
            end;
        Error -> Error
    end.

-spec is_absorbable(txn_payment_v2(), blockchain:blockchain()) -> boolean().
is_absorbable(Txn, Chain) ->
    Payer = ?MODULE:payer(Txn),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(Payer, Ledger) of
        {ok, Entry} ->
            %% nonce must be 1 past the current one
            nonce(Txn) == blockchain_ledger_entry_v1:nonce(Entry) + 1;
        _ ->
            false
    end.

-spec is_valid(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain:config(?max_payments, Ledger) of
        {ok, M} when is_integer(M) ->
            do_is_valid_checks(Txn, Chain, M);
        _ ->
            {error, {invalid, max_payments_not_set}}
    end.


-spec absorb(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    TotAmount = ?MODULE:total_amount(Txn),
    Fee = ?MODULE:fee(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger, AreFeesEnabled) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, TotAmount, Nonce, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    Payments = ?MODULE:payments(Txn),
                    ok = lists:foreach(fun(Payment) ->
                                               PayeePubkeyBin = blockchain_payment_v2:payee(Payment),
                                               PayeeAmount = blockchain_payment_v2:amount(Payment),
                                               blockchain_ledger_v1:credit_account(PayeePubkeyBin, PayeeAmount, Ledger)
                                       end, Payments)
            end
    end.

-spec print(txn_payment_v2()) -> iodata().
print(undefined) -> <<"type=payment_v2, undefined">>;
print(#blockchain_txn_payment_v2_pb{payer=Payer,
                                    fee=Fee,
                                    payments=Payments,
                                    nonce=Nonce,
                                    signature = S}=Txn) ->
    io_lib:format("type=payment_v2, payer=~p, total_amount: ~p, fee=~p, nonce=~p, signature=~s~n payments: ~s",
                  [?TO_B58(Payer), ?MODULE:total_amount(Txn), Fee, Nonce, ?TO_B58(S), print_payments(Payments)]).

print_payments(Payments) ->
    string:join(lists:map(fun(Payment) ->
                                  blockchain_payment_v2:print(Payment)
                          end,
                          Payments), "\n\t").

-spec to_json(txn_payment_v2(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"payment_v2">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      payer => ?BIN_TO_B58(payer(Txn)),
      payments => [blockchain_payment_v2:to_json(Payment, []) || Payment <- payments(Txn)],
      fee => fee(Txn),
      nonce => nonce(Txn)
     }.

%% ------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------
-spec do_is_valid_checks(Txn :: txn_payment_v2(),
                         Chain :: blockchain:blockchain(),
                         MaxPayments :: pos_integer()) -> ok | {error, any()}.
do_is_valid_checks(Txn, Chain, MaxPayments) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Signature = ?MODULE:signature(Txn),
    Payments = ?MODULE:payments(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_payment_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v2_pb:encode_msg(BaseTxn),

    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            LengthPayments = length(Payments),
            case LengthPayments > MaxPayments of
                %% Check that we don't exceed max payments
                true ->
                    {error, {exceeded_max_payments, {LengthPayments, MaxPayments}}};
                false ->
                    TotAmount = ?MODULE:total_amount(Txn),
                    TxnFee = ?MODULE:fee(Txn),
                    AmountCheck = case blockchain:config(?allow_zero_amount, Ledger) of
                                      {ok, false} ->
                                          %% check that none of the payments have a zero amount
                                          has_non_zero_amounts(Payments);
                                      _ ->
                                          %% if undefined or true, use the old check
                                          (TotAmount >= 0)
                                  end,
                    case AmountCheck of
                        false ->
                            {error, invalid_transaction};
                        true ->
                            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                            ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                            case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                false ->
                                    {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                true ->
                                    blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
                            end
                    end
            end
    end.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

-spec has_unique_payees(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_unique_payees(Payments) ->
    Payees = [blockchain_payment_v2:payee(P) || P <- Payments],
    length(lists:usort(Payees)) == length(Payees).

-spec has_non_zero_amounts(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_non_zero_amounts(Payments) ->
    Amounts = [blockchain_payment_v2:amount(P) || P <- Payments],
    lists:all(fun(A) -> A > 0 end, Amounts).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],

    Tx = #blockchain_txn_payment_v2_pb{
            payer= <<"payer">>,
            payments=Payments,
            fee=0,
            nonce=1,
            signature = <<>>
           },
    New = new(<<"payer">>, Payments, 1),
    ?assertEqual(Tx, New).

payer_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payments_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(Payments, ?MODULE:payments(Tx)).

payees_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual([<<"x">>, <<"y">>, <<"z">>], ?MODULE:payees(Tx)).

amounts_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual([10, 20, 30], ?MODULE:amounts(Tx)).

total_amount_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(60, total_amount(Tx)).

fee_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(0, fee(Tx)).

nonce_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, Payments, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_payment_v2_pb:encode_msg(Tx1#blockchain_txn_payment_v2_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = #blockchain_txn_payment_v2_pb{
            payer= <<"payer">>,
            payments=Payments,
            fee=?LEGACY_TXN_FEE,
            nonce=1,
            signature = <<>>
           },
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, payer, payments, fee, nonce])).


-endif.

-ifdef(EQC).
gen(Keys) ->
    ?SUCHTHAT({_, [P1, P2|_]}, {fun(Payer, Payees, Amount, Nonce) ->
            #{secret := PayerSK, public := PayerPK} = libp2p_crypto:keys_from_bin(Payer),
            Payments = [blockchain_payment_v2:new(libp2p_crypto:pubkey_to_bin(maps:get(public, libp2p_crypto:keys_from_bin(K))), abs(Amount)+1) || K <- lists:usort(Payees)],
            sign(new(libp2p_crypto:pubkey_to_bin(PayerPK), Payments, abs(Nonce)+1), libp2p_crypto:mk_sig_fun(PayerSK))
    end, [eqc_gen:oneof(Keys), eqc_gen:list(5, eqc_gen:oneof(Keys)), eqc_gen:int(), eqc_gen:int()]}, not lists:member(P1, P2) andalso length(P2) > 0).
-endif.
