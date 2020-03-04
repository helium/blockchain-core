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

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v2_pb.hrl").

-export([
         new/4,
         hash/1,
         payer/1,
         payments/1,
         payees/1,
         amounts/1,
         total_amount/1,
         fee/1,
         nonce/1,
         signature/1,
         sign/2,
         is_valid/2,
         absorb/2,
         print/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_payment_v2() :: #blockchain_txn_payment_v2_pb{}.

-export_type([txn_payment_v2/0]).

-spec new(Payer :: libp2p_crypto:pubkey_bin(),
          Payments :: blockchain_payment_v2:payments(),
          Nonce :: non_neg_integer(),
          Fee :: non_neg_integer()) -> txn_payment_v2().
new(Payer, Payments, Nonce, Fee) ->
    #blockchain_txn_payment_v2_pb{
       payer=Payer,
       payments=Payments,
       nonce=Nonce,
       fee=Fee,
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

-spec is_valid(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    is_valid_(Txn, Ledger).

-spec absorb(txn_payment_v2(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    TotAmount = ?MODULE:total_amount(Txn),
    Fee = ?MODULE:fee(Txn),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),

    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger) of
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


%% ------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------

-spec is_valid_(Txn :: txn_payment_v2(),
                Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
is_valid_(Txn, Ledger) ->
    Payer = ?MODULE:payer(Txn),
    Signature = ?MODULE:signature(Txn),
    Payments = ?MODULE:payments(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_payment_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v2_pb:encode_msg(BaseTxn),
    Payees = ?MODULE:payees(Txn),
    Fee = ?MODULE:fee(Txn),
    TotAmount = ?MODULE:total_amount(Txn),

    %% Initial Data to be supplied to railway validation
    Data = #{ledger => Ledger,
             payer => Payer,
             payees => Payees,
             signature => Signature,
             payments => Payments,
             len_payments => length(Payments),
             pubkey => PubKey,
             base_txn => BaseTxn,
             encoded_txn => EncodedTxn,
             fee => Fee,
             tot_amt => TotAmount
            },

    %% NOTE: The order of the validity steps matters!
    %% Because we modify the initial data supplied at some of the intermediate steps.
    ValiditySteps = [
                     {check_chain_var, fun check_chain_var/1},
                     {check_signature, fun check_signature/1},
                     {check_self_payment, fun check_self_payment/1},
                     {check_empty_payments, fun check_empty_payments/1},
                     {check_max_payments, fun check_max_payments/1},
                     {check_unique_payees, fun check_unique_payees/1},
                     {check_txn_fee, fun check_txn_fee/1},
                     {check_amount, fun check_amount/1},
                     {check_dc_balance, fun check_dc_balance/1},
                     {check_balance, fun check_balance/1}
                    ],

    case blockchain_utils:railway(ValiditySteps, Data) of
        {error, Step, Reason, _Data} ->
            lager:error("Step: ~p, Reason: ~p", [Step, Reason]),
            {error, Reason};
        {ok, _Data} ->
            %% We don't really care about the eventual data
            ok
    end.

check_chain_var(#{ledger := Ledger}=Data) ->
    case blockchain:config(?max_payments, Ledger) of
        {ok, M} when is_integer(M) ->
            %% We insert the chain var in Data
            {ok, maps:put(max_payments, M, Data)};
        _ ->
            {error, {invalid, max_payments_not_set}}
    end.

check_signature(#{encoded_txn := EncodedTxn, signature := Signature, pubkey := PubKey}) ->
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            ok
    end.

check_self_payment(#{payer := Payer, payees := Payees}) ->
    case lists:member(Payer, Payees) of
        true ->
            {error, self_payment};
        false ->
            ok
    end.

check_empty_payments(#{len_payments := LenPayments}) ->
    case LenPayments == 0 of
        true ->
            %% Check that there are payments
            {error, zero_payees};
        false ->
            ok
    end.

check_max_payments(#{len_payments := LenPayments, max_payments := MaxPayments}) ->
    case LenPayments > MaxPayments of
        %% Check that we don't exceed max payments
        true ->
            {error, {exceeded_max_payments, LenPayments, MaxPayments}};
        false ->
            ok
    end.

check_unique_payees(#{payments := Payments}) ->
    Payees = [blockchain_payment_v2:payee(P) || P <- Payments],
    case length(lists:usort(Payees)) == length(Payees) of
        false ->
            {error, duplicate_payees};
        true ->
            ok
    end.

check_txn_fee(#{ledger := Ledger}=Data) ->
    case blockchain_ledger_v1:transaction_fee(Ledger) of
        {error, _}=Error0 ->
            Error0;
        {ok, MinerFee} ->
            %% We put the miner_fee in the data
            {ok, maps:put(miner_fee, MinerFee, Data)}
    end.

check_amount(#{fee := Fee, miner_fee := MinerFee, tot_amt := TotAmount}) ->
    case (TotAmount >= 0) andalso (Fee >= MinerFee) of
        false ->
            {error, invalid_transaction};
        true ->
            ok
    end.

check_dc_balance(#{payer := Payer, fee := Fee, ledger := Ledger}) ->
    case blockchain_ledger_v1:check_dc_balance(Payer, Fee, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            ok
    end.

check_balance(#{payer := Payer, ledger := Ledger, tot_amt := TotAmount}) ->
    case blockchain_ledger_v1:check_balance(Payer, TotAmount, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            ok
    end.

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
            fee=10,
            nonce=1,
            signature = <<>>
           },
    New = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual(Tx, New).

payer_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual(<<"payer">>, payer(Tx)).

payments_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual(Payments, ?MODULE:payments(Tx)).

payees_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual([<<"x">>, <<"y">>, <<"z">>], ?MODULE:payees(Tx)).

amounts_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual([10, 20, 30], ?MODULE:amounts(Tx)).

total_amount_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual(60, total_amount(Tx)).

fee_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual(10, fee(Tx)).

nonce_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual(1, nonce(Tx)).

signature_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    Tx = new(<<"payer">>, Payments, 1, 10),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    Payments = [blockchain_payment_v2:new(<<"x">>, 10),
                blockchain_payment_v2:new(<<"y">>, 20),
                blockchain_payment_v2:new(<<"z">>, 30)],
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, Payments, 1, 10),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_payment_v2_pb:encode_msg(Tx1#blockchain_txn_payment_v2_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.
