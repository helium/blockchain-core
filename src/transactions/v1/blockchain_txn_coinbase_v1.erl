%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Conbase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_coinbase_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_coinbase_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    payee/1,
    amount/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_coinbase() :: #blockchain_txn_coinbase_v1_pb{}.
-export_type([txn_coinbase/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer()) -> txn_coinbase().
new(Payee, Amount) ->
    #blockchain_txn_coinbase_v1_pb{payee=Payee, amount=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_coinbase()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_coinbase_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_coinbase()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_coinbase_v1_pb.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_coinbase()) -> non_neg_integer().
amount(Txn) ->
    Txn#blockchain_txn_coinbase_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_coinbase(),  blockchain_ledger_v1:ledger()) -> ok
                                                               | {error, not_in_genesis_block}
                                                               | {error, zero_or_negative_amount}.
absorb(Txn, Ledger) ->
    %% NOTE: This transaction is only allowed in the genesis block
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, 0} ->
            Payee = ?MODULE:payee(Txn),
            Amount = ?MODULE:amount(Txn),
            case Amount > 0 of
                true ->
                    blockchain_ledger_v1:credit_account(Payee, Amount, Ledger);
                false ->
                    {error, zero_or_negative_amount}
            end;
        _ ->
            {error, not_in_genesis_block}
    end.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_coinbase_v1_pb{payee= <<"payee">>, amount=666},
    ?assertEqual(Tx, new(<<"payee">>, 666)).

payee_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(<<"payee">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(666, amount(Tx)).

-endif.
