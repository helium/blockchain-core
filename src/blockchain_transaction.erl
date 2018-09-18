%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_transaction).

-export([
    new_payment_txn/4
    ,new_add_gateway_txn/2
    ,new_coinbase_txn/2
    ,new_assert_location_txn/3
    ,new_genesis_consensus_group/1
    ,sign_payment_txn/2
    ,sign_add_gateway_request/2
    ,sign_add_gateway_txn/2
    ,sign_assert_location_txn/2
    ,is_valid_payment_txn/1
    ,gateway_address/1
    ,is_add_gateway_txn/1
    ,is_coinbase_txn/1
    ,is_payment_txn/1
    ,is_genesis_consensus_group_txn/1
    ,is_assert_location_txn/1
    ,payer/1
    ,payee/1
    ,amount/1
    ,payment_nonce/1
    ,signature/1
    ,assert_location_nonce/1
    ,coinbase_payee/1
    ,coinbase_amount/1
    ,genesis_consensus_group_members/1
    ,validate_transactions/2
    ,absorb_transactions/2
    ,sort/2
]).

-record(add_gateway_txn, {
    owner_address :: libp2p_crypto:address()
    ,gateway_address :: libp2p_crypto:address()
    ,owner_signature = <<>> :: binary()
    ,gateway_signature = <<>> :: binary()
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
    gateway_address :: libp2p_crypto:address()
    ,signature :: binary()
    ,location :: location()
    ,nonce = 0 :: non_neg_integer()
}).

-type location() :: non_neg_integer(). %% h3 index
-type assert_location_txn() :: #assert_location_txn{}.
-type add_gateway_txn() :: #add_gateway_txn{}.
-type payment_txn() :: #payment_txn{}.
-type coinbase_txn() :: #coinbase_txn{}.
-type genesis_consensus_group_txn() :: #genesis_consensus_group_txn{}.
-type transaction() :: add_gateway_txn() | payment_txn() | coinbase_txn()
                       | assert_location_txn() | genesis_consensus_group_txn().
-type transactions() :: [transaction()].

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
-spec new_add_gateway_txn(libp2p_crypto:address(), libp2p_crypto:address()) -> add_gateway_txn().
new_add_gateway_txn(OwnerAddress, GatewayAddress) ->
    #add_gateway_txn{
        owner_address=OwnerAddress
        ,gateway_address=GatewayAddress
        ,owner_signature = <<>>
        ,gateway_signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_coinbase_txn(libp2p_crypto:address(), non_neg_integer()) -> coinbase_txn().
new_coinbase_txn(Payee, Amount) ->
    #coinbase_txn{payee=Payee, amount=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_assert_location_txn(libp2p_crypto:address(), location(), non_neg_integer()) -> assert_location_txn().
new_assert_location_txn(Address, Location, Nonce) ->
    #assert_location_txn{
        gateway_address=Address
        ,location=Location
        ,signature = <<>>
        ,nonce=Nonce
    }.

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
    AddGwTxn#add_gateway_txn{gateway_signature=Signature};
sign_add_gateway_txn(AddGwTxn, PrivKey) ->
    Sign = libp2p_crypto:mk_sig_fun(PrivKey),
    AddGwTxn#add_gateway_txn{gateway_signature=Sign(erlang:term_to_binary(AddGwTxn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_add_gateway_request(add_gateway_txn(), pid()) -> add_gateway_txn().
sign_add_gateway_request(AddGwTxn, Swarm) ->
    {ok, _PubKey, Sigfun} = libp2p_swarm:keys(Swarm),
    Signature = Sigfun(erlang:term_to_binary(AddGwTxn#add_gateway_txn{gateway_signature = <<>>, owner_signature = <<>>})),
    AddGwTxn#add_gateway_txn{gateway_signature=Signature}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
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
is_valid_payment_txn(PaymentTxn=#payment_txn{payer=Payer, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Payer),
    libp2p_crypto:verify(erlang:term_to_binary(PaymentTxn#payment_txn{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(add_gateway_txn()) -> libp2p_crypto:address().
gateway_address(Transaction) when is_record(Transaction, add_gateway_txn) ->
    Transaction#add_gateway_txn.gateway_signature.

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
-spec is_assert_location_txn(assert_location_txn()) -> boolean().
is_assert_location_txn(Transaction) ->
    is_record(Transaction, assert_location_txn).

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
-spec payment_nonce(payment_txn()) -> non_neg_integer().
payment_nonce(PaymentTxn) ->
    PaymentTxn#payment_txn.nonce.

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
-spec assert_location_nonce(assert_location_txn()) -> non_neg_integer().
assert_location_nonce(AssertLocationTxn) ->
    AssertLocationTxn#assert_location_txn.nonce.

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
-spec genesis_consensus_group_members(genesis_consensus_group_txn()) -> [libp2p_crypto:address()].
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

validate_transactions([], Valid,  Invalid, _Ledger) ->
    lager:info("valid: ~p, invalid: ~p", [Valid, Invalid]),
    {Valid, Invalid};
validate_transactions([Txn | Tail], Valid, Invalid, Ledger) ->
    %% sort the new transaction in with the accumulated list
    SortedPaymentTxns = Valid ++ [Txn],
    %% check that these transactions are valid to apply in this order
    case absorb_transactions(SortedPaymentTxns, Ledger) of
        {ok, _NewLedger} ->
            validate_transactions(Tail, SortedPaymentTxns, Invalid, Ledger);
        {error, {bad_nonce, {_NonceType, Nonce, LedgerNonce}}} when Nonce > LedgerNonce + 1 ->
            %% we don't have enough context to decide if this transaction is valid yet, keep it
            %% but don't include it in the block (so it stays in the buffer)
            validate_transactions(Tail, Valid, Invalid, Ledger);
        _ ->
            %% any other error means we drop it
            validate_transactions(Tail, Valid, [Txn | Invalid], Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
% TODO: Simplify this & fix dialyzer some day...
-dialyzer({nowarn_function, absorb_transactions/2}).

-spec absorb_transactions(blockchain_transaction:transactions() | []
                          ,blockchain_ledger:ledger()) -> {ok, blockchain_ledger:ledger()}
                                                          | {error, any()}.
absorb_transactions([], Ledger) ->
    {ok, Ledger};
absorb_transactions([#add_gateway_txn{owner_address=OwnerAddress
                                      ,gateway_address=GatewayAddress
                                      ,owner_signature=OSig
                                      ,gateway_signature=GSig} | Tail], Ledger) ->
    BinTxn = erlang:term_to_binary(#add_gateway_txn{owner_address=OwnerAddress
                                                    ,gateway_address=GatewayAddress}),
    case libp2p_crypto:verify(BinTxn, OSig, libp2p_crypto:address_to_pubkey(OwnerAddress)) of
        true ->
            case libp2p_crypto:verify(BinTxn, GSig, libp2p_crypto:address_to_pubkey(GatewayAddress)) of
                true ->
                    case blockchain_ledger:add_gateway(OwnerAddress, GatewayAddress, Ledger) of
                        false ->
                            {error, gateway_already_registered};
                        NewLedger ->
                            absorb_transactions(Tail, NewLedger)
                    end;
                false ->
                    {error, bad_gateway_signature}
            end;
        false ->
            {error, bad_owner_signature}
    end;
absorb_transactions([#assert_location_txn{gateway_address=GatewayAddress
                                          ,location=Location
                                          ,nonce=Nonce} | Tail]
                    ,Ledger) when GatewayAddress /= undefined andalso Location /= undefined ->
    case assert_gateway_location(GatewayAddress, Location, Nonce, Ledger) of
        {error, Reason} ->
            {error, Reason};
        NewLedger ->
            absorb_transactions(Tail, NewLedger)
    end;
absorb_transactions([#coinbase_txn{payee=Address,
                                   amount=Amount} | Tail],
                    Ledger) when Amount > 0 ->
    absorb_transactions(Tail, blockchain_ledger:credit_account(Address, Amount, Ledger));
absorb_transactions([PaymentTxn = #payment_txn{amount=Amount} | _Tail], _Ledger) when Amount =< 0 ->
    lager:error("amount < 0 for PaymentTxn: ~p", [PaymentTxn]),
    {error, invalid_transaction};
absorb_transactions([PaymentTxn=#payment_txn{payer=Payer,
                                             payee=Payee,
                                             amount=Amount,
                                             nonce=Nonce} | Tail], Ledger) ->
    lager:notice("absorb_transactions: ~p", [PaymentTxn]),
    case blockchain_transaction:is_valid_payment_txn(PaymentTxn) of
        true ->
            case blockchain_ledger:credit_account(Payee, Amount, blockchain_ledger:debit_account(Payer, Amount, Nonce, Ledger)) of
                {error, Reason} ->
                    lager:error("paymentTxn: ~p, Error: ~p", [PaymentTxn, Reason]),
                    {error, Reason};
                NewLedger ->
                    absorb_transactions(Tail, NewLedger)
            end;
        false ->
            {error, bad_signature}
    end;
absorb_transactions([#genesis_consensus_group_txn{members=Members} | Tail], Ledger) ->
    absorb_transactions(Tail, blockchain_ledger:add_consensus_members(Members, Ledger));
absorb_transactions([Unknown|_Tail], _Ledger) ->
    lager:warning("unknown transaction ~p", [Unknown]),
    {error, unknown_transaction}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sort(transaction(), transaction()) -> boolean().
sort(TxnA, TxnB) ->
    {actor(TxnA), nonce(TxnA)} =< {actor(TxnB), nonce(TxnB)}.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(any()) -> integer().
nonce(#assert_location_txn{nonce=Nonce}) ->
    Nonce;
nonce(#payment_txn{nonce=Nonce}) ->
    Nonce;
nonce(_) ->
    %% other transactions sort first
    -1.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec actor(any()) -> libp2p_crypto:address() | <<>>.
actor(#assert_location_txn{gateway_address=Address}) ->
    Address;
actor(#add_gateway_txn{owner_address=Address}) ->
    Address;
actor(#payment_txn{payer=Payer}) ->
    Payer;
actor(#coinbase_txn{payee=Payee}) ->
    Payee;
actor(_) ->
    <<>>.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-dialyzer({nowarn_function, assert_gateway_location/4}).
assert_gateway_location(GatewayAddress, Location, Nonce, Ledger) ->
    case blockchain_ledger:find_gateway_info(GatewayAddress, Ledger) of
        undefined ->
            {error, {unknown_gateway, GatewayAddress, Ledger}};
        GwInfo ->
            lager:info("gw_info from ledger: ~p", [GwInfo]),
            LedgerNonce = blockchain_ledger:assert_location_nonce(GwInfo),
            lager:info("assert_gateway_location, gw_address: ~p, Nonce: ~p, LedgerNonce: ~p", [GatewayAddress, Nonce, LedgerNonce]),
            case Nonce == LedgerNonce + 1 of
                true ->
                    %% update the ledger with new gw_info
                    case blockchain_ledger:add_gateway_location(GatewayAddress, Location, Nonce, Ledger) of
                        false ->
                            Ledger;
                        NewLedger ->
                            NewLedger
                    end;
                false ->
                    {error, {bad_nonce, {assert_location, Nonce, LedgerNonce}}}
            end
    end.
