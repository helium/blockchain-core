%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transactions ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_transactions).

-export([
    validate/2
    ,absorb/2
    ,sort/2
    ,type/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type transaction() :: blockchain_txn_add_gateway:txn_add_gateway()
                       | blockchain_txn_assert_location:txn_assert_location()
                       | blockchain_txn_coinbase:txn_coinbase()
                       | blockchain_txn_gen_consensus_group:txn_genesis_consensus_group()
                       | blockchain_txn_payment:txn_payment()
                       | blockchain_txn_create_htlc:txn_create_htlc()
                       | blockchain_txn_redeem_htlc:txn_redeem_htlc()
                       | blockchain_txn_poc_request:txn_poc_request()
                       | blockchain_txn_poc_receipts:txn_poc_receipts().
-type transactions() :: [transaction()].
-export_type([transactions/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec validate(blockchain_transaction:transactions()
                            ,blockchain_ledger:ledger()) -> {blockchain_transaction:transactions()
                                                             ,blockchain_transaction:transactions()}.
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


-spec absorb(transactions() | [], blockchain_ledger:ledger()) -> {ok, blockchain_ledger:ledger()}
                                                                 | {error, any()}.
absorb([], Ledger) ->    
    Ledger1 = blockchain_ledger:update_transaction_fee(Ledger),
    %% TODO: probably not the correct place to be incrementing the height for the ledger?
    {ok, blockchain_ledger:increment_height(Ledger1)};
absorb(Txns, Ledger) when map_size(Ledger) == 0 ->
    absorb(Txns, blockchain_ledger:new());
absorb([Txn|Txns], Ledger0) ->
    case absorb(type(Txn), Txn, Ledger0) of
        {error, _Reason}=Error -> Error;
        {ok, Ledger1} -> absorb(Txns, Ledger1)
    end.

% TODO: Fix dialyzer some day...
-dialyzer({nowarn_function, absorb/3}).
-spec absorb(atom(), transaction(), blockchain_ledger:ledger()) -> {ok, blockchain_ledger:ledger()}
                                                                   | {error, any()}.
absorb(blockchain_txn_coinbase, Txn, Ledger) ->
    Payee = blockchain_txn_coinbase:payee(Txn),
    Amount = blockchain_txn_coinbase:amount(Txn),
    case Amount > 0 of
        true ->
            {ok, blockchain_ledger:credit_account(Payee, Amount, Ledger)};
        false ->
            {ok, Ledger}
    end;
absorb(blockchain_txn_gen_consensus_group, Txn, Ledger0) ->
    Members = blockchain_txn_gen_consensus_group:members(Txn),
    {ok, blockchain_ledger:consensus_members(Members, Ledger0)};
absorb(blockchain_txn_add_gateway, Txn, Ledger0) ->
    case {blockchain_txn_add_gateway:is_valid_owner(Txn)
          ,blockchain_txn_add_gateway:is_valid_gateway(Txn)} of
        {false, _} ->
            {error, bad_owner_signature};
        {_, false} ->
            {error, bad_gateway_signature};
        {true, true} ->
            OwnerAddress = blockchain_txn_add_gateway:owner_address(Txn),
            GatewayAddress = blockchain_txn_add_gateway:gateway_address(Txn),
            case blockchain_ledger:add_gateway(OwnerAddress, GatewayAddress, Ledger0) of
                {error, _Reason}=Error ->
                    Error;
                Ledger1 ->
                    {ok, Ledger1}
            end
    end;
absorb(blockchain_txn_assert_location, Txn, Ledger0) ->
    GatewayAddress = blockchain_txn_assert_location:gateway_address(Txn),
    OwnerAddress = blockchain_txn_assert_location:owner_address(Txn),
    Location = blockchain_txn_assert_location:location(Txn),
    Nonce = blockchain_txn_assert_location:nonce(Txn),
    Fee = blockchain_txn_assert_location:fee(Txn),
    Entries = blockchain_ledger:entries(Ledger0),
    LastEntry = blockchain_ledger:find_entry(OwnerAddress, Entries),
    PaymentNonce = blockchain_ledger:payment_nonce(LastEntry) + 1,
    case blockchain_ledger:debit_account(OwnerAddress, Fee, PaymentNonce, Ledger0) of
        {error, _Reason}=Error -> Error;
        Ledger1 ->
            case assert_gateway_location(GatewayAddress, Location, Nonce, Ledger1) of
                {error, _}=Error2 -> Error2;
                Ledger2 ->
                    {ok, Ledger2}
            end
    end;
absorb(blockchain_txn_payment, Txn, Ledger0) ->
    Amount = blockchain_txn_payment:amount(Txn),
    Fee = blockchain_txn_payment:fee(Txn),
    MinerFee = blockchain_ledger:transaction_fee(Ledger0),
    case (Amount >= 0) andalso (Fee >= MinerFee) of
        false ->
            lager:error("amount < 0 for PaymentTxn: ~p", [Txn]),
            {error, invalid_transaction};
        true ->
            case blockchain_txn_payment:is_valid(Txn) of
                true ->
                    Payer = blockchain_txn_payment:payer(Txn),
                    Nonce = blockchain_txn_payment:nonce(Txn),                     
                    case blockchain_ledger:debit_account(Payer, Amount + Fee, Nonce, Ledger0) of
                        {error, _Reason}=Error ->
                            Error;
                        Ledger1 ->
                            Payee = blockchain_txn_payment:payee(Txn),
                            {ok, blockchain_ledger:credit_account(Payee, Amount, Ledger1)}
                    end;
                false ->
                    {error, bad_signature}
            end
    end;
absorb(blockchain_txn_create_htlc, Txn, Ledger0) ->
    Amount = blockchain_txn_create_htlc:amount(Txn),
    case Amount >= 0 of
        false ->
            lager:error("amount < 0 for CreateHTLCTxn: ~p", [Txn]),
            {error, invalid_transaction};
        true ->
            case blockchain_txn_create_htlc:is_valid(Txn) of
                true ->
                    Payer = blockchain_txn_create_htlc:payer(Txn),
                    Payee = blockchain_txn_create_htlc:payee(Txn),
                    Entry = blockchain_ledger:find_entry(Payer, blockchain_ledger:entries(Ledger0)),
                    Nonce = blockchain_ledger:payment_nonce(Entry) + 1,
                    case blockchain_ledger:debit_account(Payer, Amount, Nonce, Ledger0) of
                        {error, _Reason}=Error ->
                            Error;
                        Ledger1 ->
                            Address = blockchain_txn_create_htlc:address(Txn),
                            case blockchain_ledger:add_htlc(Address,
                                                            Payer,
                                                            Payee,
                                                            Amount,
                                                            blockchain_txn_create_htlc:hashlock(Txn),
                                                            blockchain_txn_create_htlc:timelock(Txn),
                                                            Ledger1) of
                                {error, _Reason}=Error ->
                                    Error;
                                Ledger2 ->
                                    {ok, Ledger2}
                            end
                    end;
                false ->
                    {error, bad_signature}
            end
    end;
absorb(blockchain_txn_redeem_htlc, Txn, Ledger0) ->
    case blockchain_txn_redeem_htlc:is_valid(Txn) of
        true ->
            Address = blockchain_txn_redeem_htlc:address(Txn),
            case blockchain_ledger:find_htlc(Address, blockchain_ledger:htlcs(Ledger0)) of
                {error, _Reason}=Error ->
                    Error;
                HTLC ->
                    Redeemer = blockchain_txn_redeem_htlc:payee(Txn),
                    Payer = blockchain_ledger:htlc_payer(HTLC),
                    Payee = blockchain_ledger:htlc_payee(HTLC),
                    %% if the Creator of the HTLC is not the redeemer, continue to check for pre-image
                    %% otherwise check that the timelock has expired which allows the Creator to redeem
                    case Payer =:= Redeemer of
                        false ->
                            %% check that the address trying to redeem matches the HTLC
                            case Redeemer =:= Payee of
                                true ->
                                    Hashlock = blockchain_ledger:htlc_hashlock(HTLC),
                                    Preimage = blockchain_txn_redeem_htlc:preimage(Txn),
                                    case (crypto:hash(sha256, Preimage) =:= Hashlock) of
                                        true ->
                                            {ok, blockchain_ledger:redeem_htlc(Address, Payee, Ledger0)};
                                        false ->
                                            {error, invalid_preimage}
                                    end;
                                false ->
                                    {error, invalid_payee}
                            end;
                        true ->
                            Timelock = blockchain_ledger:htlc_timelock(HTLC),
                            Height = blockchain_ledger:current_height(Ledger0),
                            case Timelock >= Height of
                                true ->
                                    {error, timelock_not_expired};
                                false ->
                                    {ok, blockchain_ledger:redeem_htlc(Address, Payee, Ledger0)}
                            end
                    end
            end;
        false ->
            {error, bad_signature}
    end;
absorb(blockchain_txn_poc_request, Txn, Ledger0) ->
    case blockchain_txn_poc_request:is_valid(Txn) of
        true ->
            GatewayAddress = blockchain_txn_poc_request:gateway_address(Txn),
            case blockchain_ledger:request_poc(GatewayAddress, Ledger0) of
                {error, _Reason}=Error ->
                    Error;
                Ledger1 ->
                    {ok, Ledger1}
            end;
        false ->
            {error, bad_signature}
    end;
absorb(blockchain_txn_oui, Txn, Ledger0) ->
    case blockchain_txn_oui:is_valid(Txn) of
        false ->
            {error, invalid_transaction};
        true ->
            Fee = blockchain_txn_oui:fee(Txn),
            Owner = blockchain_txn_oui:owner(Txn),
            Entries = blockchain_ledger:entries(Ledger0),
            LastEntry = blockchain_ledger:find_entry(Owner, Entries),
            Nonce = blockchain_ledger:payment_nonce(LastEntry) + 1,
            case blockchain_ledger:debit_account(Owner, Fee, Nonce, Ledger0) of
                {error, _Reason}=Error -> Error;
                Ledger1 -> {ok, Ledger1}
            end
    end;
absorb(blockchain_txn_poc_receipts, Txn, Ledger0) ->
    case blockchain_txn_poc_receipts:is_valid(Txn) of
        false ->
            {error, invalid_transaction};
        true ->
            % TODO: Update score and last_poc_challenge
            {ok, Ledger0}
    end;
absorb(_, Unknown, _Ledger) ->
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
-spec nonce(transaction()) -> integer().
nonce(Txn) ->
    case type(Txn) of
        blockchain_txn_assert_location ->
            blockchain_txn_assert_location:nonce(Txn);
        blockchain_txn_payment ->
            blockchain_txn_payment:nonce(Txn);
        _ ->
            -1 %% other transactions sort first
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec actor(transaction()) -> libp2p_crypto:address() | <<>>.
actor(Txn) ->
    case type(Txn) of
        blockchain_txn_assert_location ->
            blockchain_txn_assert_location:gateway_address(Txn);
        blockchain_txn_payment ->
            blockchain_txn_payment:payer(Txn);
        blockchain_txn_create_htlc ->
            blockchain_txn_create_htlc:payer(Txn);
        blockchain_txn_redeem_htlc ->
            blockchain_txn_redeem_htlc:payee(Txn);
        blockchain_txn_poc_request ->
            blockchain_txn_poc_request:gateway_address(Txn);
        blockchain_txn_add_gateway ->
            blockchain_txn_add_gateway:owner_address(Txn);
        blockchain_txn_coinbase ->
            blockchain_txn_coinbase:payee(Txn);
        blockchain_txn_poc_receipts ->
            blockchain_txn_poc_receipts:challenger(Txn);
        _ ->
            <<>>
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-dialyzer({nowarn_function, assert_gateway_location/4}).
assert_gateway_location(GatewayAddress, Location, Nonce, Ledger0) ->
    case blockchain_ledger:find_gateway_info(GatewayAddress, Ledger0) of
        undefined ->
            {error, {unknown_gateway, GatewayAddress, Ledger0}};
        GwInfo ->
            lager:info("gw_info from ledger: ~p", [GwInfo]),
            LedgerNonce = blockchain_ledger:assert_location_nonce(GwInfo),
            lager:info("assert_gateway_location, gw_address: ~p, Nonce: ~p, LedgerNonce: ~p", [GatewayAddress, Nonce, LedgerNonce]),
            case Nonce == LedgerNonce + 1 of
                true ->
                    %% update the ledger with new gw_info
                    case blockchain_ledger:add_gateway_location(GatewayAddress, Location, Nonce, Ledger0) of
                        false ->
                            Ledger0;
                        Ledger1 ->
                            Ledger1
                    end;
                false ->
                    {error, {bad_nonce, {assert_location, Nonce, LedgerNonce}}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec type(transaction()) -> atom().
type(Txn) ->
    Types = [
        blockchain_txn_assert_location, blockchain_txn_payment
        ,blockchain_txn_create_htlc, blockchain_txn_redeem_htlc
        ,blockchain_txn_add_gateway ,blockchain_txn_coinbase
        ,blockchain_txn_gen_consensus_group ,blockchain_txn_poc_request
        ,blockchain_txn_poc_receipts
    ],
    case lists:filter(fun(M) -> M:is(Txn) end, Types) of
        [Type] -> Type;
        _ -> undefined
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-endif.
