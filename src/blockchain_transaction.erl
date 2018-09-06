%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_transaction).

-export([
    new_payment_txn/4
    ,new_add_gateway_txn/1
    ,new_coinbase_txn/2
    ,new_assert_location_txn/2
    ,new_genesis_consensus_group/1
    ,sign_payment_txn/2
    ,sign_add_gateway_txn/2
    ,sign_assert_location_txn/2
    ,is_valid_payment_txn/1
    ,gateway_address/1
    ,is_add_gateway_txn/1
    ,is_coinbase_txn/1
    ,is_payment_txn/1
    ,is_genesis_consensus_group_txn/1
    ,payer/1
    ,payee/1
    ,amount/1
    ,nonce/1
    ,signature/1
    ,coinbase_payee/1
    ,coinbase_amount/1
    ,genesis_consensus_group_members/1
    ,validate_transactions/2
    ,absorb_transactions/2
]).

-record(add_gateway_txn, {
    address :: libp2p_crypto:address()
    ,signature :: binary()
}).

-record(payment_txn, {
    payer :: libp2p_crypto:address()
    ,payee :: libp2p_crypto:address()
    ,amount :: pos_integer()
    ,nonce :: non_neg_integer()
    ,signature :: binary()
}).

-record(coinbase_txn, {
    payee :: libp2p_crypto:address()
    ,amount :: pos_integer()
}).

%% XXX ONLY valid in the genesis block
-record(genesis_consensus_group_txn, {
    members = [] :: [libp2p_crypto:address()]
}).

-record(assert_location_txn, {
    address :: libp2p_crypto:adddress()
    ,signature :: binary()
    ,location :: location()
}).

-type location() :: non_neg_integer(). %% h3 index
-type assert_location_txn() :: #assert_location_txn{}.
-type add_gateway_txn() :: #add_gateway_txn{}.
-type payment_txn() :: #payment_txn{}.
-type coinbase_txn() :: #coinbase_txn{}.
-type genesis_consensus_group_txn() :: #genesis_consensus_group_txn{}.
-type transactions() :: [add_gateway_txn() | payment_txn() | coinbase_txn()
                         | assert_location_txn() | genesis_consensus_group_txn()].

-export_type([transactions/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_payment_txn(libp2p_crypto:address(), libp2p_crypto:address()
                      ,non_neg_integer(), non_neg_integer()) -> payment_txn().
new_payment_txn(Payer, Recipient, Amount, Nonce) ->
    #payment_txn{
        payer=Payer
        ,payee=Recipient
        ,amount=Amount
        ,nonce=Nonce
        ,signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_add_gateway_txn(string()) -> add_gateway_txn().
new_add_gateway_txn(Addr) ->
    #add_gateway_txn{address=libp2p_crypto:b58_to_address(Addr), signature = <<>>}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_coinbase_txn(string(), non_neg_integer()) -> coinbase_txn().
new_coinbase_txn(Payee, Amount) ->
    #coinbase_txn{payee=libp2p_crypto:b58_to_address(Payee), amount=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_assert_location_txn(libp2p_crypto:address(), location()) -> assert_location_txn().
new_assert_location_txn(Address, Location) ->
    #assert_location_txn{address=libp2p_crypto:b58_to_address(Address), location=Location, signature = <<>>}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_genesis_consensus_group([libp2p_crypto:address()]) -> genesis_consensus_group_txn().
new_genesis_consensus_group(Members) ->
    #genesis_consensus_group_txn{members=Members}.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign_payment_txn(payment_txn(), pid() | libp2p_crypto:private_key()) -> payment_txn().
sign_payment_txn(PaymentTxn, Swarm) when is_pid(Swarm) ->
    {ok, _PubKey, Sigfun} = libp2p_swarm:keys(Swarm),
    Signature = Sigfun(erlang:term_to_binary(PaymentTxn)),
    PaymentTxn#payment_txn{signature=Signature};
sign_payment_txn(PaymentTxn, PrivKey) ->
    Sign = libp2p_crypto:mk_sig_fun(PrivKey),
    PaymentTxn#payment_txn{signature=Sign(erlang:term_to_binary(PaymentTxn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_add_gateway_txn(add_gateway_txn(), pid() | libp2p_crypto:private_key()) -> add_gateway_txn().
sign_add_gateway_txn(AddGwTxn, Swarm) when is_pid(Swarm) ->
    {ok, _PubKey, Sigfun} = libp2p_swarm:keys(Swarm),
    Signature = Sigfun(erlang:term_to_binary(AddGwTxn)),
    AddGwTxn#add_gateway_txn{signature=Signature};
sign_add_gateway_txn(AddGwTxn, PrivKey) ->
    Sign = libp2p_crypto:mk_sig_fun(PrivKey),
    AddGwTxn#add_gateway_txn{signature=Sign(erlang:term_to_binary(AddGwTxn))}.

-spec sign_assert_location_txn(assert_location_txn(), pid() | libp2p_crypto:private_key()) -> assert_location_txn().
sign_assert_location_txn(AssertLocationTxn, Swarm) when is_pid(Swarm) ->
    {ok, _PubKey, Sigfun} = libp2p_swarm:keys(Swarm),
    Signature = Sigfun(erlang:term_to_binary(AssertLocationTxn)),
    AssertLocationTxn#assert_location_txn{signature=Signature};
sign_assert_location_txn(AssertLocationTxn, PrivKey) ->
    Sign = libp2p_crypto:mk_sig_fun(PrivKey),
    AssertLocationTxn#assert_location_txn{signature=Sign(erlang:term_to_binary(AssertLocationTxn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_payment_txn(payment_txn()) -> boolean().
is_valid_payment_txn(PaymentTxn=#payment_txn{payer=Payer,
                                             signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Payer),
    libp2p_crypto:verify(erlang:term_to_binary(PaymentTxn#payment_txn{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(add_gateway_txn()) -> libp2p_crypto:address().
gateway_address(Transaction) when is_record(Transaction, add_gateway_txn) ->
    Transaction#add_gateway_txn.address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_add_gateway_txn(add_gateway_txn()) -> boolean().
is_add_gateway_txn(Transaction) ->
    erlang:is_record(Transaction, add_gateway_txn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_coinbase_txn(coinbase_txn()) -> boolean().
is_coinbase_txn(Transaction) ->
    erlang:is_record(Transaction, coinbase_txn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_payment_txn(payment_txn()) -> boolean().
is_payment_txn(Transaction) ->
    erlang:is_record(Transaction, payment_txn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_genesis_consensus_group_txn(genesis_consensus_group_txn()) -> boolean().
is_genesis_consensus_group_txn(Transaction) ->
    erlang:is_record(Transaction, genesis_consensus_group_txn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(payment_txn()) -> libp2p_crypto:address().
payer(PaymentTxn) ->
    PaymentTxn#payment_txn.payer.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(payment_txn()) -> libp2p_crypto:address().
payee(PaymentTxn) ->
    PaymentTxn#payment_txn.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(payment_txn()) -> non_neg_integer().
amount(PaymentTxn) ->
    PaymentTxn#payment_txn.amount.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(payment_txn()) -> binary().
signature(PaymentTxn) ->
    PaymentTxn#payment_txn.signature.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(payment_txn()) -> non_neg_integer().
nonce(PaymentTxn) ->
    PaymentTxn#payment_txn.nonce.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec coinbase_payee(coinbase_txn()) -> libp2p_crypto:address().
coinbase_payee(CoinbaseTxn) ->
    CoinbaseTxn#coinbase_txn.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec coinbase_amount(coinbase_txn()) -> non_neg_integer().
coinbase_amount(CoinbaseTxn) ->
    CoinbaseTxn#coinbase_txn.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
genesis_consensus_group_members(#genesis_consensus_group_txn{members=Members}) ->
    Members.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec validate_transactions(blockchain_transaction:transactions()
                            ,blockchain_ledger:ledger()) -> {blockchain_transaction:transactions()
                                                             ,blockchain_transaction:transactions()}.
validate_transactions(Transactions, Ledger) ->
    validate_transactions(Transactions, [], [], Ledger).

validate_transactions([], Valid, Remaining, _Ledger) ->
    {Valid, Remaining};
validate_transactions([Txn | Tail], Valid, Remaining, Ledger) ->
    case is_payment_txn(Txn) of
        true ->
            %% sort the new transaction in with the accumulated list
            SortedPaymentTxns = Valid ++ [Txn],
            %% check that these transactions are valid to apply in this order
            case absorb_transactions(SortedPaymentTxns, Ledger) of
                {ok, _NewLedger} ->
                    validate_transactions(Tail, SortedPaymentTxns, Remaining, Ledger);
                _ ->
                    validate_transactions(Tail, Valid, [Txn | Remaining], Ledger)
            end;
        false ->
            validate_transactions(Tail, [Txn | Valid], Remaining, Ledger)
    end.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
% TODO: Simplify this
-spec absorb_transactions(blockchain_transaction:transactions() | []
                          ,blockchain_ledger:ledger()) -> {ok, blockchain_ledger:ledger()}
                                                          | {error, any()}.
absorb_transactions([], Ledger) ->
    {ok, Ledger};
absorb_transactions([#add_gateway_txn{address=Address} | Tail], Ledger) ->
    absorb_transactions(Tail, blockchain_ledger:add_gateway(Address, Ledger));
absorb_transactions([#assert_location_txn{address=Address, location=Location} | Tail], Ledger0) when Address /= undefined andalso Location /= undefined ->
    case blockchain_ledger:add_gateway_location(Address, Location, Ledger0) of
        false ->
            {error, non_existent_gateway};
        Ledger1 ->
            absorb_transactions(Tail, Ledger1)
    end;
absorb_transactions([#coinbase_txn{payee=Address, amount=Amount} | Tail], Ledger) when Amount > 0 ->
    absorb_transactions(Tail, blockchain_ledger:credit_account(Address, Amount, Ledger));
absorb_transactions([#payment_txn{amount=Amount} | _Tail], _Ledger) when Amount =< 0 ->
    {error, invalid_transaction};
absorb_transactions([PaymentTxn=#payment_txn{payer=Payer
                                             ,payee=Payee
                                             ,amount=Amount
                                             ,nonce=Nonce} | Tail], Ledger0) ->
    case blockchain_transaction:is_valid_payment_txn(PaymentTxn) of
        true ->
            case blockchain_ledger:debit_account(Payer, Amount, Nonce, Ledger0) of
                {error, _Reason}=Error ->
                    Error;
                Ledger1 ->
                    Ledger2 = blockchain_ledger:credit_account(Payee, Amount, Ledger1),
                    absorb_transactions(Tail, Ledger2)
                end;
        false ->
            {error, bad_signature}
    end;
absorb_transactions([#genesis_consensus_group_txn{members=Members} | Tail], Ledger) ->
    absorb_transactions(Tail, blockchain_ledger:add_consensus_members(Members, Ledger)).
