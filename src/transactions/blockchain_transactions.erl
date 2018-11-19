%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transactions ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_transactions).

-export([
    validate/2,
    absorb/2,
    sort/2,
    type/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type transaction() :: blockchain_txn_add_gateway_v1:txn_add_gateway()
                       | blockchain_txn_assert_location_v1:txn_assert_location()
                       | blockchain_txn_coinbase_v1:txn_coinbase()
                       | blockchain_txn_gen_consensus_group_v1:txn_genesis_consensus_group()
                       | blockchain_txn_gen_gateway_v1:txn_genesis_gateway()
                       | blockchain_txn_payment_v1:txn_payment()
                       | blockchain_txn_create_htlc_v1:txn_create_htlc()
                       | blockchain_txn_redeem_htlc_v1:txn_redeem_htlc()
                       | blockchain_txn_poc_request_v1:txn_poc_request()
                       | blockchain_txn_poc_receipts_v1:txn_poc_receipts().
-type transactions() :: [transaction()].
-export_type([transactions/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec validate(blockchain_transaction:transactions(),
               blockchain_ledger_v1:ledger()) -> {blockchain_transaction:transactions(),
                                               blockchain_transaction:transactions()}.
validate(Transactions, Ledger) ->
    validate(Transactions, [], [], Ledger).

validate([], Valid,  Invalid, _Ledger) ->
    lager:info("valid: ~p, invalid: ~p", [Valid, Invalid]),
    {Valid, Invalid};
validate([Txn | Tail], Valid, Invalid, Ledger) ->
    %% sort the new transaction in with the accumulated list
    SortedPaymentTxns = Valid ++ [Txn],
    %% check that these transactions are valid to apply in this order
    case absorb(SortedPaymentTxns, Ledger) of
        {ok, _NewLedger} ->
            validate(Tail, SortedPaymentTxns, Invalid, Ledger);
        {error, {bad_nonce, {_NonceType, Nonce, LedgerNonce}}} when Nonce > LedgerNonce + 1 ->
            %% we don't have enough context to decide if this transaction is valid yet, keep it
            %% but don't include it in the block (so it stays in the buffer)
            validate(Tail, Valid, Invalid, Ledger);
        _ ->
            %% any other error means we drop it
            validate(Tail, Valid, [Txn | Invalid], Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(transactions() | [], blockchain_ledger_v1:ledger()) -> {ok, blockchain_ledger_v1:ledger()}
                                                                 | {error, any()}.
absorb([], Ledger) ->
    Ledger1 = blockchain_ledger:update_transaction_fee(Ledger),
    %% TODO: probably not the correct place to be incrementing the height for the ledger?
    {ok, blockchain_ledger_v1:increment_height(Ledger1)};
absorb(Txns, Ledger) when map_size(Ledger) == 0 ->
    absorb(Txns, blockchain_ledger_v1:new());
absorb([Txn|Txns], Ledger0) ->
    Type = type(Txn),
    try Type:absorb(Txn, Ledger0) of
        {error, _Reason}=Error -> Error;
        {ok, Ledger1} -> absorb(Txns, Ledger1)
    catch
        What:Why -> {error, {type(Txn), What, Why}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sort(transaction(), transaction()) -> boolean().
sort(TxnA, TxnB) ->
    {actor(TxnA), nonce(TxnA)} =< {actor(TxnB), nonce(TxnB)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec type(transaction()) -> atom().
type(Txn) ->
    Types = [
        blockchain_txn_assert_location_v1, blockchain_txn_payment_v1
        ,blockchain_txn_create_htlc_v1, blockchain_txn_redeem_htlc_v1
        ,blockchain_txn_add_gateway_v1 ,blockchain_txn_coinbase_v1
        ,blockchain_txn_gen_consensus_group_v1 ,blockchain_txn_poc_request_v1
        ,blockchain_txn_poc_receipts_v1
    ],
    case lists:filter(fun(M) -> M:is(Txn) end, Types) of
        [Type] -> Type;
        _ -> undefined
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(transaction()) -> integer().
nonce(Txn) ->
    case ?MODULE:type(Txn) of
        blockchain_txn_assert_location_v1 ->
            blockchain_txn_assert_location_v1:nonce(Txn);
        blockchain_txn_payment_v1 ->
            blockchain_txn_payment_v1:nonce(Txn);
        _ ->
            -1 %% other transactions sort first
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec actor(transaction()) -> libp2p_crypto:address() | <<>>.
actor(Txn) ->
    case ?MODULE:type(Txn) of
        blockchain_txn_assert_location_v1 ->
            blockchain_txn_assert_location_v1:gateway_address(Txn);
        blockchain_txn_payment_v1 ->
            blockchain_txn_payment_v1:payer(Txn);
        blockchain_txn_create_htlc_v1 ->
            blockchain_txn_create_htlc_v1:payer(Txn);
        blockchain_txn_redeem_htlc_v1 ->
            blockchain_txn_redeem_htlc_v1:payee(Txn);
        blockchain_txn_poc_request_v1 ->
            blockchain_txn_poc_request_v1:gateway_address(Txn);
        blockchain_txn_add_gateway_v1 ->
            blockchain_txn_add_gateway_v1:owner_address(Txn);
        blockchain_txn_coinbase_v1 ->
            blockchain_txn_coinbase_v1:payee(Txn);
        blockchain_txn_poc_receipts_v1 ->
            blockchain_txn_poc_receipts_v1:challenger(Txn);
        _ ->
            <<>>
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-endif.
