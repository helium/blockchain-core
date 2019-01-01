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
%% NOTE: Called in the miner
-spec validate(blockchain_transaction:transactions(),
               blockchain_ledger_v1:ledger()) -> {blockchain_transaction:transactions(),
                                               blockchain_transaction:transactions()}.
%% TODO we should separate validation from absorbing transactions and validate transactions
%% before absorbing them.
validate(Transactions, Ledger) ->
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    validate(Transactions, [], [], Ledger1).

validate([], Valid,  Invalid, _Ledger) ->
    lager:info("valid: ~p, invalid: ~p", [Valid, Invalid]),
    {lists:reverse(Valid), Invalid};
validate([Txn | Tail], Valid, Invalid, Ledger) ->
    %% sort the new transaction in with the accumulated list
    Type = type(Txn),
    %% check that these transactions are valid to apply in this order
    case Type:absorb(Txn, Ledger) of
        ok ->
            validate(Tail, [Txn|Valid], Invalid, Ledger);
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
-spec absorb(blockchain_block:block(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Block, Ledger0) ->
   Ledger1 = blockchain_ledger_v1:new_context(Ledger0),
   Transactions = blockchain_block:transactions(Block),
   case absorb_(Transactions, Ledger1) of
       ok ->
           %% these should be all done atomically in the same context
           ok = blockchain_ledger_v1:update_transaction_fee(Ledger1),
           ok = blockchain_ledger_v1:increment_height(Block, Ledger1),
           ok = blockchain_ledger_v1:commit_context(Ledger1);
       Error ->
           blockchain_ledger_v1:delete_context(Ledger1),
           Error
   end.

-spec absorb_([blockchain_transactions:transactions()], blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb_([], _Ledger) ->
    ok;
absorb_([Txn|Txns], Ledger) ->
    Type = type(Txn),
    try Type:absorb(Txn,  Ledger) of
        {error, _Reason}=Error -> Error;
        ok -> absorb_(Txns, Ledger)
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
        ,blockchain_txn_add_gateway_v1, blockchain_txn_coinbase_v1
        ,blockchain_txn_gen_consensus_group_v1 ,blockchain_txn_poc_request_v1
        ,blockchain_txn_poc_receipts_v1, blockchain_txn_gen_gateway_v1
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
