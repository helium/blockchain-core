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
         total_amounts/2,
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

-type total_amounts() :: #{blockchain_token_v1:type() => non_neg_integer()}.
-type txn_payment_v2() :: #blockchain_txn_payment_v2_pb{}.

-export_type([txn_payment_v2/0, total_amounts/0]).

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

-spec amounts(txn_payment_v2(), blockchain_ledger_v1:ledger()) -> [{blockchain_token_v1:type(), pos_integer()}].
amounts(Txn, Ledger) ->
    {MaxPayments, Payments} = split_payment_amounts(Txn, Ledger),
    Payments ++ MaxPayments.

-spec total_amounts(txn_payment_v2(), blockchain_ledger_v1:ledger()) -> total_amounts().
total_amounts(Txn, Ledger) ->
    lists:foldl(
      fun({TT, Amt}, Acc) ->
          maps:update_with(TT, fun(V) -> V + Amt end, Amt, Acc)
      end, #{}, ?MODULE:amounts(Txn, Ledger)).

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

-spec is_valid(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case ?get_var(?max_payments, Ledger) of
        {ok, M} when is_integer(M) ->
            case
                blockchain_txn:validate_fields([
                    {{payee, P}, {address, libp2p}}
                 || P <- ?MODULE:payees(Txn)
                ])
            of
                ok ->
                    case ?get_var(?token_version, Ledger) of
                        {ok, 2} ->
                            do_is_valid_checks_v2(Txn, Chain, M);
                        _ ->
                            do_is_valid_checks(Txn, Chain, M)
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, {invalid, max_payments_not_set}}
    end.

-spec absorb(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case ?get_var(?token_version, Ledger) of
        {ok, 2} ->
            absorb_v2_(Txn, Ledger, Chain);
        _ ->
            absorb_(Txn, Ledger, Chain)
    end.

-spec absorb_v2_(txn_payment_v2(), blockchain_ledger_v1:ledger(), blockchain:blockchain()) -> ok | {error, any()}.
absorb_v2_(Txn, Ledger, Chain) ->
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    TotalAmounts = ?MODULE:total_amounts(Txn, Ledger),
    ShouldImplicitBurn = blockchain_ledger_v1:txn_fees_active(Ledger),
    {MaxAmounts, _} = split_payment_amounts(Txn, Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger, ShouldImplicitBurn, Hash, Chain) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, TotalAmounts, Nonce, Ledger) of
                {error, _Reason} = Error ->
                    Error;
                ok ->
                    Payments = ?MODULE:payments(Txn),
                    MaxPaymentsMap = maps:from_list(MaxAmounts),
                    ok = lists:foreach(
                        fun(Payment) ->
                            PayeePubkeyBin = blockchain_payment_v2:payee(Payment),
                            TT = blockchain_payment_v2:token_type(Payment),
                            PayeeAmount = case blockchain_payment_v2:amount(Payment) of
                                              0 -> maps:get(TT, MaxPaymentsMap, 0);
                                              Amount when Amount > 0 -> Amount
                                          end,
                            blockchain_ledger_v1:credit_account(PayeePubkeyBin, PayeeAmount, TT, Ledger)
                        end,
                        Payments
                    )
            end
    end.

-spec absorb_(txn_payment_v2(), blockchain_ledger_v1:ledger(), blockchain:blockchain()) -> ok | {error, any()}.
absorb_(Txn, Ledger, Chain) ->
    {_, SpecifiedAmounts} = split_payment_amounts(Txn, Ledger),
    TotalAmount = lists:foldl(fun({_, TAmt}, Acc) -> TAmt + Acc end, 0, ?MODULE:amounts(Txn, Ledger)),
    SpecifiedSubTotal = lists:foldl(fun({_, SAmt}, Acc) -> SAmt + Acc end, 0, SpecifiedAmounts),
    MaxPayment = TotalAmount - SpecifiedSubTotal,
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    ShouldImplicitBurn = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger, ShouldImplicitBurn, Hash, Chain) of
        {error, _Reason} = Error ->
            Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, TotalAmount, Nonce, Ledger) of
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
    io_lib:format(
        "type=payment_v2, payer=~p, fee=~p, nonce=~p, signature=~s~n payments: ~s",
        [
            ?TO_B58(Payer),
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
                                                                {ok, ok} -> fee_check(Txn, Chain, Ledger);
                                                                _ ->
                                                                    {error, {invalid_transaction,
                                                                             {amount_check, AmountCheck},
                                                                             {memo_check, MemoCheck}}}
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

-spec do_is_valid_checks_v2(
        Txn :: txn_payment_v2(),
        Chain :: blockchain:blockchain(),
        MaxPayments :: pos_integer()) -> ok | {error, any()}.
do_is_valid_checks_v2(Txn, Chain, MaxPayments) ->
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
                        {error, _}=Error0 ->
                            Error0;
                        {ok, Entry} ->
                            TxnNonce = ?MODULE:nonce(Txn),
                            LedgerEntryNonce = blockchain_ledger_entry_v2:nonce(Entry),
                            case TxnNonce =:= LedgerEntryNonce + 1 of
                                false ->
                                    {error, {bad_nonce, {payment_v2, TxnNonce, LedgerEntryNonce}}};
                                true ->
                                    case LengthPayments > MaxPayments of
                                        %% Check that we don't exceed max payments
                                        true ->
                                            {error, {exceeded_max_payments, {LengthPayments, MaxPayments}}};
                                        false ->
                                            case lists:member(Payer, ?MODULE:payees(Txn)) of
                                                false ->
                                                    %% check that every payee is unique
                                                    case has_unique_payees_v2(Payments) of
                                                        false ->
                                                            {error, duplicate_payees};
                                                        true ->
                                                            TokenCheck = token_check(Txn, Ledger),
                                                            AmountCheck = amount_check_v2(Txn, Ledger),
                                                            MemoCheck = memo_check(Txn, Ledger),

                                                            case {AmountCheck, MemoCheck, TokenCheck} of
                                                                {ok, ok, ok} ->
                                                                    %% Everthing looks good so far, do the fee check last
                                                                    fee_check(Txn, Chain, Ledger);
                                                                _ ->
                                                                    {error, {invalid_transaction,
                                                                             {amount_check, AmountCheck},
                                                                             {memo_check, MemoCheck},
                                                                             {token_check, TokenCheck}}}
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
    case ?get_var(?allow_payment_v2_memos, Ledger) of
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

-spec amount_check_v2(Txn :: txn_payment_v2(), Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
amount_check_v2(Txn, Ledger) ->
    TotAmounts = ?MODULE:total_amounts(Txn, Ledger),
    Payer = ?MODULE:payer(Txn),

    {ok, PayerEntry} = blockchain_ledger_v1:find_entry(Payer, Ledger),

    BalanceList = lists:foldl(
      fun(TT, Acc) ->
              PayerTTBalance = blockchain_ledger_entry_v2:balance(PayerEntry, TT),
              Amount = maps:get(TT, TotAmounts, 0),
              IsSufficient = PayerTTBalance >= Amount,
              [{TT, IsSufficient, PayerTTBalance, Amount} | Acc]
      end,
      [],
      blockchain_token_v1:supported_tokens()),

    case lists:filter(
           fun({_TT, IsSufficient, _PayerTTBalance, _Amount}) ->
                   IsSufficient == false
           end,
           BalanceList) of
        [ ] ->
            %% If the txn amounts have successfully validated to this point
            %% perform the same checks previously done prior to token_version 2
            amount_check(Txn, Ledger);
        Failure ->
            {error, {amount_check_v2_failed, Failure}}
    end.

-spec amount_check(Txn :: txn_payment_v2(), Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, amount_check_failed}.
amount_check(Txn, Ledger) ->
    Payments = ?MODULE:payments(Txn),
    AmtCheck = case ?get_var(?enable_balance_clearing, Ledger) of
                   {ok, true} ->
                       %% See above note in amount_check_v2
                       {MaxPayments, _OtherPayments} = split_payment_amounts(Txn, Ledger),
                       lists:all(fun({_, Amt}) -> Amt > 0 end, MaxPayments);
                   _ ->
                       case ?get_var(?allow_zero_amount, Ledger) of
                           {ok, false} ->
                               %% check that none of the payments have a zero amount
                               has_non_zero_amounts(Payments);
                           _ ->
                               %% if undefined or true, use the old check
                               lists:sum(maps:values(?MODULE:total_amounts(Txn, Ledger))) >= 0
                       end
               end,
    case AmtCheck of
        true -> ok;
        false -> {error, amount_check_failed}
    end.

-spec split_payment_amounts(Txn :: txn_payment_v2(), Ledger :: blockchain_ledger_v1:ledger()) -> {[{TT, Amt}], [{TT, Amt}]} when TT :: blockchain_token_v1:type(), Amt :: pos_integer().
split_payment_amounts(#blockchain_txn_payment_v2_pb{payer=Payer, fee=Fee}=Txn, Ledger) ->
    {MaxPayments, SpecifiedPayments} = split_max_payments(?MODULE:payments(Txn)),
    SpecifiedPayments1 = lists:map(fun(Pmt) -> {blockchain_payment_v2:token_type(Pmt), blockchain_payment_v2:amount(Pmt)} end, SpecifiedPayments),
    case length(MaxPayments) of
        0 ->
            {MaxPayments, SpecifiedPayments1};
        _ ->
            case ?get_var(?token_version, Ledger) of
                {ok, 2} ->
                    {ok, PayerEntry2} = blockchain_ledger_v1:find_entry(Payer, Ledger),
                    MaxPayments1 = lists:map(fun(MaxPayment) ->
                                                 TokenType = blockchain_payment_v2:token_type(MaxPayment),
                                                 TypeBalance = blockchain_ledger_entry_v2:balance(PayerEntry2, TokenType),
                                                 TypeSpecifiedAmt = lists:foldl(fun({Type, Val}, Acc) ->
                                                                                     case Type =:= TokenType of
                                                                                         true -> Val + Acc;
                                                                                         false -> Acc
                                                                                     end
                                                                                 end, 0, SpecifiedPayments1),
                                                 MaxPaymentAmt = case TokenType of
                                                                     hnt -> TypeBalance - TypeSpecifiedAmt - calculate_hnt_fee(Payer, Fee, Ledger);
                                                                     _ -> TypeBalance - TypeSpecifiedAmt
                                                                 end,
                                                 {TokenType, MaxPaymentAmt}
                                             end, MaxPayments),
                    {MaxPayments1, SpecifiedPayments1};
                _ ->
                    %% Prior to token_version 2 there can only be a single balance-clearing
                    %% `max` payment per txn
                    {ok, PayerEntry} = blockchain_ledger_v1:find_entry(Payer, Ledger),
                    Balance = blockchain_ledger_entry_v1:balance(PayerEntry),
                    SpecifiedAmts = lists:foldl(fun({_, Val}, Acc) -> Val + Acc end, 0, SpecifiedPayments1),
                    MaxPaymentAmt = Balance - SpecifiedAmts - calculate_hnt_fee(Payer, Fee, Ledger),
                    {[{hnt, MaxPaymentAmt}], SpecifiedPayments1}
            end
    end.

-spec calculate_hnt_fee(Payer :: libp2p_crypto:pubkey_bin(), Fee :: non_neg_integer(), Ledger :: blockchain_ledger_v1:ledger()) -> non_neg_integer().
calculate_hnt_fee(_, 0, _) -> 0;
calculate_hnt_fee(Payer, Fee, Ledger) when Fee > 0 ->
    case blockchain_ledger_v1:find_dc_entry(Payer, Ledger) of
        {error, dc_entry_not_found} ->
            {ok, FeeInHNT} = blockchain_ledger_v1:dc_to_hnt(Fee, Ledger),
            FeeInHNT;
        {ok, Entry} ->
            DCBalance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            case (DCBalance - Fee) >= 0 of
                true -> 0;
                false ->
                    {ok, FeeInHNT} = blockchain_ledger_v1:dc_to_hnt(Fee, Ledger),
                    FeeInHNT
            end
    end.

-spec has_unique_payees(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_unique_payees(Payments) ->
    Payees = [blockchain_payment_v2:payee(P) || P <- Payments],
    length(lists:usort(Payees)) == length(Payees).

-spec has_unique_payees_v2(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_unique_payees_v2(Payments) ->
    %% Check that {payee, token} is unique across payments
    PayeesAndTokens =
    lists:foldl(
      fun(Payment, Acc) ->
              TT = blockchain_payment_v2:token_type(Payment),
              Payee = blockchain_payment_v2:payee(Payment),
              [{Payee, TT} | Acc]
      end, [], Payments),
    length(lists:usort(PayeesAndTokens)) == length(PayeesAndTokens).


-spec has_non_zero_amounts(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_non_zero_amounts(Payments) ->
    Amounts = [blockchain_payment_v2:amount(P) || P <- Payments],
    lists:all(fun(A) -> A > 0 end, Amounts).

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

-spec split_max_payments(Payments) -> {Payments, Payments} when
    Payments :: [blockchain_payment_v2:payments()].
split_max_payments(Payments) ->
    case lists:all(fun blockchain_payment_v2:is_valid_max/1, Payments) of
        true ->
            {MaxPayments, _OtherPayments} = SplitPayments = lists:partition(fun blockchain_payment_v2:max/1, Payments),
            %% This usort should be converted to `lists:uniq/2` once OTP 25 is fully supported as it's clearer and preserves
            %% the list order so a single `=:=` comparison of input and output lists should suffice
            UniqMaxPayments = lists:usort(fun(P1, P2) ->
                                 blockchain_payment_v2:token_type(P1) =< blockchain_payment_v2:token_type(P2)
                             end, MaxPayments),
            case length(UniqMaxPayments) =:= length(MaxPayments) of
                true -> SplitPayments;
                false -> throw({error, invalid_payment_txn})
            end;
        false -> throw({error, invalid_payment_txn})
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

-spec token_check(Txn :: txn_payment_v2(), Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
token_check(Txn, Ledger) ->
    Payments = ?MODULE:payments(Txn),
    case ?get_var(?token_version, Ledger) of
        {ok, 2} ->
            %% check that the tokens are valid
            case has_valid_tokens(Payments) of
                true -> ok;
                false -> {error, invalid_tokens}
            end;
        _ ->
            %% old behavior before var, allow only if token_type=hnt (default)
            case has_default_tokens(Payments) of
                true -> ok;
                false -> {error, invalid_token_before_var}
            end
    end.

-spec has_valid_tokens(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_valid_tokens(Payments) ->
    lists:all(
        fun(Payment) ->
                blockchain_payment_v2:is_valid_token_type(Payment)
        end,
        Payments
    ).

-spec has_default_tokens(Payments :: blockchain_payment_v2:payments()) -> boolean().
has_default_tokens(Payments) ->
    lists:all(
        fun(Payment) ->
            hnt == blockchain_payment_v2:token_type(Payment)
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
        blockchain_payment_v2:new(<<"z">>, 30, mobile)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual([{hnt, 10}, {hnt, 20}, {mobile, 30}], ?MODULE:amounts(Tx, Ledger)).

total_amount_test() ->
    BaseDir = test_utils:tmp_dir("total_amount_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Payments = [
        blockchain_payment_v2:new(<<"x">>, 10),
        blockchain_payment_v2:new(<<"y">>, 20),
        blockchain_payment_v2:new(<<"z">>, 30, iot)
    ],
    Tx = new(<<"payer">>, Payments, 1),
    ?assertEqual(#{hnt => 30, iot => 30}, total_amounts(Tx, Ledger)).

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
    ?assertEqual(#{hnt => 100}, total_amounts(Tx, Ledger)).

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
    ?assertThrow({error, invalid_payment_txn}, total_amounts(Tx, Ledger)).

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
