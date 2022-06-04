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
    amounts/2,
    total_amount/2,
    fee/1, fee/2,
    fee_payer/2,
    calculate_fee/2, calculate_fee/5,
    nonce/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_payment_v2() :: #blockchain_txn_payment_v2_pb{}.

-export_type([txn_payment_v2/0]).

-spec new(
    Payer :: libp2p_crypto:pubkey_bin(),
    Payments :: blockchain_payment_v2:payments(),
    Nonce :: non_neg_integer()
) -> txn_payment_v2().
new(Payer, Payments, Nonce) ->
    #blockchain_txn_payment_v2_pb{
        payer = Payer,
        payments = Payments,
        nonce = Nonce,
        fee = ?LEGACY_TXN_FEE,
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

-spec amounts(txn_payment_v2(), blockchain_ledger_v1:ledger()) -> [pos_integer()].
amounts(Txn, Ledger) ->
    case split_payment_amounts(Txn, Ledger) of
        {undefined, Payments} -> Payments;
        {MaxPayment, Payments} -> [MaxPayment | Payments]
    end.

-spec total_amount(txn_payment_v2(), blockchain_ledger_v1:ledger()) -> pos_integer().
total_amount(Txn, Ledger) ->
    lists:sum(?MODULE:amounts(Txn, Ledger)).

-spec fee(txn_payment_v2()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.fee.

-spec fee(txn_payment_v2(), non_neg_integer()) -> txn_payment_v2().
fee(Txn, Fee) ->
    Txn#blockchain_txn_payment_v2_pb{fee = Fee}.

-spec fee_payer(txn_payment_v2(), blockchain_ledger_v1:ledger()) ->
    libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    payer(Txn).

-spec nonce(txn_payment_v2()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.nonce.

-spec signature(txn_payment_v2()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_payment_v2_pb.signature.

-spec sign(txn_payment_v2(), libp2p_crypto:sig_fun()) -> txn_payment_v2().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_payment_v2_pb:encode_msg(Txn),
    Txn#blockchain_txn_payment_v2_pb{signature = SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_payment_v2(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(
    txn_payment_v2(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()
) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(
        Txn#blockchain_txn_payment_v2_pb{fee = 0, signature = <<0:512>>},
        Ledger,
        DCPayloadSize,
        TxnFeeMultiplier
    ).

-spec is_valid(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain:config(?max_payments, Ledger) of
        {ok, M} when is_integer(M) ->
            case
                blockchain_txn:validate_fields([
                    {{payee, P}, {address, libp2p}}
                 || P <- ?MODULE:payees(Txn)
                ])
            of
                ok ->
                    do_is_valid_checks(Txn, Chain, M);
                Error ->
                    Error
            end;
        _ ->
            {error, {invalid, max_payments_not_set}}
    end.

-spec absorb(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    SpecifiedAmount = lists:sum([blockchain_payment_v2:amount(Payment) || Payment <- ?MODULE:payments(Txn)]),
    TotAmount = ?MODULE:total_amount(Txn, Ledger),
    MaxPayment = TotAmount - SpecifiedAmount,
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger, AreFeesEnabled, Hash, Chain) of
        {error, _Reason} = Error ->
            Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, TotAmount, Nonce, Ledger) of
                {error, _Reason} = Error ->
                    Error;
                ok ->
                    Payments = ?MODULE:payments(Txn),
                    ok = lists:foreach(
                        fun(Payment) ->
                            PayeePubkeyBin = blockchain_payment_v2:payee(Payment),
                            PayeeAmount = case blockchain_payment_v2:amount(Payment) of
                                              0 -> MaxPayment;
                                              Amount when Amount > 0 -> Amount
                                          end,
                            blockchain_ledger_v1:credit_account(PayeePubkeyBin, PayeeAmount, Ledger)
                        end,
                        Payments
                    )
            end
    end.

-spec print(txn_payment_v2()) -> iodata().
print(undefined) ->
    <<"type=payment_v2, undefined">>;
print(
    #blockchain_txn_payment_v2_pb{
        payer = Payer,
        fee = Fee,
        payments = Payments,
        nonce = Nonce,
        signature = S
    }
) ->
    {MaxPayment, SpecifiedPayments} = split_max_payment(Payments),
    SpecifiedTotal = lists:sum([blockchain_payment_v2:amount(Payment) || Payment <- SpecifiedPayments]),
    TotalAmount = case length(MaxPayment) > 1 of
                      true -> erlang:integer_to_list(SpecifiedTotal) ++ " + a balance clearing payment";
                      false -> SpecifiedTotal
                  end,
    io_lib:format(
        "type=payment_v2, payer=~p, total_amount: ~p, fee=~p, nonce=~p, signature=~s~n payments: ~s",
        [
            ?TO_B58(Payer),
            TotalAmount,
            Fee,
            Nonce,
            ?TO_B58(S),
            print_payments(Payments)
        ]
    ).

print_payments(Payments) ->
    string:join(
        lists:map(
            fun(Payment) ->
                blockchain_payment_v2:print(Payment)
            end,
            Payments
        ),
        "\n\t"
    ).

json_type() ->
    <<"payment_v2">>.

-spec to_json(txn_payment_v2(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
        type => ?MODULE:json_type(),
        hash => ?BIN_TO_B64(hash(Txn)),
        payer => ?BIN_TO_B58(payer(Txn)),
        payments => [blockchain_payment_v2:to_json(Payment, []) || Payment <- payments(Txn)],
        fee => fee(Txn),
        nonce => nonce(Txn)
    }.

%% ------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------
-spec do_is_valid_checks(
    Txn :: txn_payment_v2(),
    Chain :: blockchain:blockchain(),
    MaxPayments :: pos_integer()
) -> ok | {error, any()}.
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
            case LengthPayments == 0 of
                true ->
                    %% Check that there are payments
                    {error, zero_payees};
                false ->
                    case blockchain_ledger_v1:find_entry(Payer, Ledger) of
                        {error, _} = Error0 ->
                            Error0;
                        {ok, Entry} ->
                            TxnNonce = ?MODULE:nonce(Txn),
                            LedgerNonce = blockchain_ledger_entry_v1:nonce(Entry),
                            case TxnNonce =:= LedgerNonce + 1 of
                                false ->
                                    {error, {bad_nonce, {payment_v2, TxnNonce, LedgerNonce}}};
                                true ->
                                    case LengthPayments > MaxPayments of
                                        %% Check that we don't exceed max payments
                                        true ->
                                            {error, {exceeded_max_payments, {LengthPayments, MaxPayments}}};
                                        false ->
                                            case lists:member(Payer, ?MODULE:payees(Txn)) of
                                                false ->
                                                    %% check that every payee is unique
                                                    case has_unique_payees(Payments) of
                                                        false ->
                                                            {error, duplicate_payees};
                                                        true ->
                                                            AmountCheck = amount_check(Txn, Ledger),
                                                            MemoCheck = memo_check(Txn, Ledger),

                                                            case {AmountCheck, MemoCheck} of
                                                                {false, _} ->
                                                                    {error, invalid_transaction};
                                                                {_, {error, _} = E} ->
                                                                    E;
                                                                {true, ok} ->
                                                                    fee_check(Txn, Chain, Ledger)
                                                            end
                                                    end;
                                                true ->
                                                    {error, self_payment}
                                            end
                                    end
                            end
                    end
            end
    end.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

-spec fee_check(
    Txn :: txn_payment_v2(),
    Chain :: blockchain:blockchain(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> ok | {error, any()}.
fee_check(Txn, Chain, Ledger) ->
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
    Payer = ?MODULE:payer(Txn),
    TxnFee = ?MODULE:fee(Txn),
    case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
        false ->
            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
        true ->
            blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
    end.

-spec memo_check(Txn :: txn_payment_v2(), Ledger :: blockchain_ledger_v1:ledger()) ->
    ok | {error, any()}.
memo_check(Txn, Ledger) ->
    Payments = ?MODULE:payments(Txn),
    case blockchain:config(?allow_payment_v2_memos, Ledger) of
        {ok, true} ->
            %% check that the memos are valid
            case has_valid_memos(Payments) of
                true -> ok;
                false -> {error, invalid_memo}
            end;
        _ ->
            %% old behavior before var, allow only if memo=0 (default)
            case has_default_memos(Payments) of
                true -> ok;
                false -> {error, invalid_memo_before_var}
            end
    end.

-spec amount_check(Txn :: txn_payment_v2(), Ledger :: blockchain_ledger_v1:ledger()) -> boolean().
amount_check(Txn, Ledger) ->
    Payments = ?MODULE:payments(Txn),
    case blockchain:config(?enable_balance_clearing, Ledger) of
        {ok, true} ->
            %% balance clearing txns should be mutually exclusive with allowing zero amount txns
            %% or else we can't tell whether or not a payment with a 0 amount is one or the other;
            %% no more than one payment has a 0 amount and only if `max' is `true'
            case has_single_max_payment(Payments) of
                true ->
                    case split_payment_amounts(Txn, Ledger) of
                        {MaxPmt, _OtherPmts} when MaxPmt > 0 -> true;
                        _ -> false
                    end;
                false -> false
            end;
        _ ->
            case blockchain:config(?allow_zero_amount, Ledger) of
                {ok, false} ->
                    %% check that none of the payments have a zero amount
                    has_non_zero_amounts(Payments);
                _ ->
                    %% if undefined or true, use the old check
                    ?MODULE:total_amount(Txn, Ledger) >= 0
            end
    end.

-spec split_payment_amounts(Txn :: txn_payment_v2(), Ledger :: blockchain_ledger_v1:ledger()) -> {pos_integer() | undefined, [pos_integer()]}.
split_payment_amounts(#blockchain_txn_payment_v2_pb{payer=Payer, fee=Fee}=Txn, Ledger) ->
    {MaxPayment, SpecifiedPayments} = split_max_payment(?MODULE:payments(Txn)),
    SpecifiedAmounts = [blockchain_payment_v2:amount(Payment) || Payment <- SpecifiedPayments],
    case length(MaxPayment) == 1 of
        true ->
            {ok, PayerEntry} = blockchain_ledger_v1:find_entry(Payer, Ledger),
            Balance = blockchain_ledger_entry_v1:balance(PayerEntry),
            MaxPaymentAmount = Balance - lists:sum(SpecifiedAmounts) - Fee,
            {MaxPaymentAmount, SpecifiedAmounts};
        false ->
            {undefined, SpecifiedAmounts}
    end.

-spec has_unique_payees(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_unique_payees(Payments) ->
    Payees = [blockchain_payment_v2:payee(P) || P <- Payments],
    length(lists:usort(Payees)) == length(Payees).

-spec has_non_zero_amounts(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_non_zero_amounts(Payments) ->
    Amounts = [blockchain_payment_v2:amount(P) || P <- Payments],
    lists:all(fun(A) -> A > 0 end, Amounts).

-spec has_single_max_payment(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_single_max_payment(Payments) ->
    {MaxTrue, MaxFalse} = split_max_payment(Payments),
    length(MaxTrue) =< 1 andalso
        lists:all(fun(Payment) -> blockchain_payment_v2:amount(Payment) > 0 end, MaxFalse).

-spec has_valid_memos(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_valid_memos(Payments) ->
    lists:all(
        fun(Payment) ->
            %% check that the memo field is valid
            FieldCheck = blockchain_txn:validate_fields([
                {{memo, blockchain_payment_v2:memo(Payment)}, {is_integer, 0}}
            ]),
            case FieldCheck of
                ok ->
                    %% check that the memo field is within limits
                    blockchain_payment_v2:is_valid_memo(Payment);
                _ ->
                    false
            end
        end,
        Payments
    ).

-spec split_max_payment(Payments) -> {Payments, Payments} when
    Payments :: blockchain_payment_v2:payments().
split_max_payment(Payments) ->
    {MaxPayment, _OtherPayments} = SplitPayments = lists:partition(
            fun(Payment) ->
                blockchain_payment_v2:max(Payment) andalso
                    blockchain_payment_v2:is_valid_max(Payment)
            end,
            Payments
        ),
    case length(MaxPayment) > 1 of
        true ->
            throw({error, invalid_payment_txn});
        false ->
            SplitPayments
    end.

-spec has_default_memos(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_default_memos(Payments) ->
    lists:all(
        fun(Payment) ->
            0 == blockchain_payment_v2:memo(Payment) orelse
                undefined == blockchain_payment_v2:memo(Payment)
        end,
        Payments
    ).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],

    Tx = #blockchain_txn_payment_v2_pb{
        payer = <<"payer">>,
        payments = Payments,
        fee = 0,
        nonce = 1,
        signature = <<>>
    },
    New = new(<<"payer">>, Payments, 1),
    ?assertEqual(Tx, New).

payer_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payments_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(Payments, ?MODULE:payments(Tx)).

payees_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual([<<"x">>, <<"y">>, <<"z">>], ?MODULE:payees(Tx)).

amounts_test() ->
    BaseDir = test_utils:tmp_dir("amounts_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual([10, 20, 30], ?MODULE:amounts(Tx, Ledger)).

total_amount_test() ->
    BaseDir = test_utils:tmp_dir("total_amount_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(60, total_amount(Tx, Ledger)).

total_amount_with_max_test() ->
    BaseDir = test_utils:tmp_dir("total_amount_with_max_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    ok = blockchain_ledger_v1:credit_account(<<"payer">>, 100, Ledger1),
    ok = blockchain_ledger_v1:commit_context(Ledger1),
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 20),
        blockchain_payment_v2:new(<<"y">>, max),
        blockchain_payment_v2:new(<<"z">>, 40)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(100, total_amount(Tx, Ledger)).

reject_multi_max_test() ->
    BaseDir = test_utils:tmp_dir("reject_multi_max_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    ok = blockchain_ledger_v1:credit_account(<<"payer">>, 100, Ledger1),
    ok = blockchain_ledger_v1:commit_context(Ledger1),
    Payments = [
        blockchain_payment_v2:new(<<"x">>, max),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, max)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertThrow({error, invalid_payment_txn}, total_amount(Tx, Ledger)).

fee_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(0, fee(Tx)).

nonce_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, Payments, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_payment_v2_pb:encode_msg(Tx1#blockchain_txn_payment_v2_pb{
        signature = <<>>
    }),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30)
    ],
    Tx = #blockchain_txn_payment_v2_pb{
        payer = <<"payer">>,
        payments = Payments,
        fee = ?LEGACY_TXN_FEE,
        nonce = 1,
        signature = <<>>
    },
    Json = to_json(Tx, []),
    ?assert(
        lists:all(
            fun(K) -> maps:is_key(K, Json) end,
            [type, payer, payments, fee, nonce]
        )
    ).

-endif.
