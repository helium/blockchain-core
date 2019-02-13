%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Behavior ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn).

%% The union type of all transactions is defined in
%% blockchain_txn.proto. The txn() type below should reflec that
%% union.
-include("pb/blockchain_txn_pb.hrl").

-type hash() :: <<_:256>>. %% SHA256 digest
-type txn() :: blockchain_txn_add_gateway_v1:txn_add_gateway()
             | blockchain_txn_assert_location_v1:txn_assert_location()
             | blockchain_txn_coinbase_v1:txn_coinbase()
             | blockchain_txn_consensus_group_v1:txn_consensus_group()
             | blockchain_txn_gen_gateway_v1:txn_genesis_gateway()
             | blockchain_txn_payment_v1:txn_payment()
             | blockchain_txn_oui_v1:txn_oui()
             | blockchain_txn_create_htlc_v1:txn_create_htlc()
             | blockchain_txn_redeem_htlc_v1:txn_redeem_htlc()
             | blockchain_txn_poc_request_v1:txn_poc_request()
             | blockchain_txn_poc_receipts_v1:txn_poc_receipts().
-type txns() :: [txn()].
-export_type([hash/0, txn/0, txns/0]).

-callback hash(State::any()) -> hash().
-callback absorb(txn(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
-callback sign(txn(), libp2p_crypto:sig_fun()) -> txn().
-callback is_valid(txn(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.

-export([
    hash/1,
    validate/2,
    absorb/2,
    sign/2,
    absorb_and_commit/2,
    absorb_block/2,
    sort/2,
    type/1,
    serialize/1,
    deserialize/1,
    wrap_txn/1,
    unwrap_txn/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(BLOCK_DELAY, 50).

hash(Txn) ->
    (type(Txn)):hash(Txn).

sign(Txn, SigFun) ->
    (type(Txn)):sign(Txn, SigFun).

serialize(Txn) ->
    blockchain_txn_pb:encode_msg(wrap_txn(Txn)).

deserialize(Bin) ->
    unwrap_txn(blockchain_txn_pb:decode_msg(Bin, blockchain_txn_pb)).

%% Since the proto file for the transaction union includes the
%% definitions of the underlying protobufs for each transaction we
%% break encapsulation here and do no tuse the txn modules themselves.
-spec wrap_txn(blockchain_txn:txn()) -> #blockchain_txn_pb{}.
wrap_txn(#blockchain_txn_assert_location_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={assert_location, Txn}};
wrap_txn(#blockchain_txn_payment_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={payment, Txn}};
wrap_txn(#blockchain_txn_create_htlc_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={create_htlc, Txn}};
wrap_txn(#blockchain_txn_redeem_htlc_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={redeem_htlc, Txn}};
wrap_txn(#blockchain_txn_add_gateway_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={add_gateway, Txn}};
wrap_txn(#blockchain_txn_coinbase_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={coinbase, Txn}};
wrap_txn(#blockchain_txn_consensus_group_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={consensus_group, Txn}};
wrap_txn(#blockchain_txn_poc_request_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={poc_request, Txn}};
wrap_txn(#blockchain_txn_poc_receipts_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={poc_receipts, Txn}};
wrap_txn(#blockchain_txn_gen_gateway_v1_pb{}=Txn) ->
    #blockchain_txn_pb{txn={gen_gateway, Txn}}.

-spec unwrap_txn(#blockchain_txn_pb{}) -> blockchain_txn:txn().
unwrap_txn(#blockchain_txn_pb{txn={_, Txn}}) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% Called in the miner
%% @end
%%--------------------------------------------------------------------
-spec validate(blockchain_txn:txns(),
               blockchain_ledger_v1:ledger()) -> {blockchain_txn:txns(), blockchain_txn:txns()}.
validate(Transactions, Ledger0) ->
    Ledger1 = blockchain_ledger_v1:new_context(Ledger0),
    validate(Transactions, [], [], Ledger1).

validate([], Valid,  Invalid, Ledger) ->
    blockchain_ledger_v1:delete_context(Ledger),
    lager:info("valid: ~p, invalid: ~p", [Valid, Invalid]),
    {lists:reverse(Valid), Invalid};
validate([Txn | Tail], Valid, Invalid, Ledger) ->
    Type = ?MODULE:type(Txn),
    case Type:is_valid(Txn, Ledger) of
        ok ->
            case ?MODULE:absorb(Txn, Ledger) of
                ok ->
                    validate(Tail, [Txn|Valid], Invalid, Ledger);
                {error, _Reason} ->
                    lager:error("invalid txn while absorbing ~p : ~p / ~p", [Type, _Reason, Txn]),
                    validate(Tail, Valid, [Txn | Invalid], Ledger)
            end;
        {error, {bad_nonce, {_NonceType, Nonce, LedgerNonce}}} when Nonce > LedgerNonce + 1 ->
            %% we don't have enough context to decide if this transaction is valid yet, keep it
            %% but don't include it in the block (so it stays in the buffer)
            validate(Tail, Valid, Invalid, Ledger);
        Error ->
            lager:error("invalid txn ~p : ~p / ~p", [Type, Error, Txn]),
            %% any other error means we drop it
            validate(Tail, Valid, [Txn | Invalid], Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_and_commit(blockchain_block:block(), blockchain:blockchain()) -> ok | {error, any()}.
absorb_and_commit(Block, Blockchain) ->
    Ledger0 = blockchain:ledger(Blockchain),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger0),
    Transactions = blockchain_block:transactions(Block),
    case ?MODULE:validate(Transactions, Ledger1) of
        {_ValidTxns, []} ->
            case ?MODULE:absorb_block(Block, Ledger1) of
                {ok, Ledger2} ->
                    ok = blockchain_ledger_v1:commit_context(Ledger2),
                    absorb_delayed(Block, Blockchain);
                Error ->
                    blockchain_ledger_v1:delete_context(Ledger1),
                    Error
            end;
        {[], _InvalidTxns} ->
            lager:error("found all transactions invalid: ~p", [Block]),
            {error, invalid_txns};
        {_ValidTxns, InvalidTxns} ->
            lager:error("found invalid transactions: ~p", [InvalidTxns]),
            {error, invalid_txns}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_block(blockchain_block:block(), blockchain_ledger_v1:ledger())
                  -> {ok, blockchain_ledger_v1:ledger()} | {error, any()}.
absorb_block(Block, Ledger) ->
    Transactions = blockchain_block:transactions(Block),
   case absorb_txns(Transactions, Ledger) of
        ok ->
            ok = blockchain_ledger_v1:update_transaction_fee(Ledger),
            ok = blockchain_ledger_v1:increment_height(Block, Ledger),
            {ok, Ledger};
        Error ->
            Error
    end.
    
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    Type = ?MODULE:type(Txn),
    try Type:absorb(Txn,  Ledger) of
        {error, _Reason}=Error -> Error;
        ok -> ok
    catch
        What:Why:Stack ->
            {error, {Type, What, {Why, Stack}}}
    end.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sort(txn(), txn()) -> boolean().
sort(TxnA, TxnB) ->
    {actor(TxnA), nonce(TxnA)} =< {actor(TxnB), nonce(TxnB)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec type(txn()) -> atom().
type(#blockchain_txn_assert_location_v1_pb{}) ->
    blockchain_txn_assert_location_v1;
type(#blockchain_txn_payment_v1_pb{}) ->
    blockchain_txn_payment_v1;
type(#blockchain_txn_create_htlc_v1_pb{}) ->
    blockchain_txn_create_htlc_v1;
type(#blockchain_txn_redeem_htlc_v1_pb{}) ->
    blockchain_txn_redeem_htlc_v1;
type(#blockchain_txn_add_gateway_v1_pb{}) ->
    blockchain_txn_add_gateway_v1;
type(#blockchain_txn_coinbase_v1_pb{}) ->
    blockchain_txn_coinbase_v1;
type(#blockchain_txn_consensus_group_v1_pb{}) ->
    blockchain_txn_consensus_group_v1;
type(#blockchain_txn_poc_request_v1_pb{}) ->
    blockchain_txn_poc_request_v1;
type(#blockchain_txn_poc_receipts_v1_pb{}) ->
    blockchain_txn_poc_receipts_v1;
type(#blockchain_txn_gen_gateway_v1_pb{}) ->
    blockchain_txn_gen_gateway_v1.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_txns(txns(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb_txns([], _Ledger) ->
    ok;
absorb_txns([Txn|Txns], Ledger) ->
    case ?MODULE:absorb(Txn, Ledger) of
        {error, _Reason}=Error -> Error;
        ok -> absorb_txns(Txns, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb_delayed(blockchain_block:block(), blockchain:blockchain()) -> ok | {error, any()}.
absorb_delayed(Block0, Blockchain) ->
    Ledger0 = blockchain:ledger(Blockchain),
    DelayedLedger0 = blockchain_ledger_v1:mode(delayed, Ledger0),
    DelayedLedger1 = blockchain_ledger_v1:new_context(DelayedLedger0),
    case blockchain_ledger_v1:current_height(Ledger0) of
        % This is so it absosbs genesis
        {ok, H} when H < 2 ->
            absorb_delayed_(Block0, DelayedLedger1);
        {ok, CurrentHeight} ->
            {ok, DelayedHeight} = blockchain_ledger_v1:current_height(DelayedLedger1),
            % Then we absorb if minimum limit is there
            case CurrentHeight - DelayedHeight > ?BLOCK_DELAY of
                false ->
                    ok;
                true ->
                    {ok, Block1} = blockchain:get_block(DelayedHeight+1, Blockchain),
                    absorb_delayed_(Block1, DelayedLedger1)
            end;
        _Any ->
            _Any
    end.

absorb_delayed_(Block, DelayedLedger0) ->
    case ?MODULE:absorb_block(Block, DelayedLedger0) of
        {ok, DelayedLedger1} ->
            ok = blockchain_ledger_v1:commit_context(DelayedLedger1);
        Error ->
            blockchain_ledger_v1:delete_context(DelayedLedger0),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn()) -> integer().
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
-spec actor(txn()) -> libp2p_crypto:pubkey_bin() | <<>>.
actor(Txn) ->
    case ?MODULE:type(Txn) of
        blockchain_txn_assert_location_v1 ->
            blockchain_txn_assert_location_v1:gateway(Txn);
        blockchain_txn_payment_v1 ->
            blockchain_txn_payment_v1:payer(Txn);
        blockchain_txn_create_htlc_v1 ->
            blockchain_txn_create_htlc_v1:payer(Txn);
        blockchain_txn_redeem_htlc_v1 ->
            blockchain_txn_redeem_htlc_v1:payee(Txn);
        blockchain_txn_poc_request_v1 ->
            blockchain_txn_poc_request_v1:gateway(Txn);
        blockchain_txn_add_gateway_v1 ->
            blockchain_txn_add_gateway_v1:owner(Txn);
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
