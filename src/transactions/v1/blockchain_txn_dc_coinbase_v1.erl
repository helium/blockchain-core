%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Data Credits Coinbase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_dc_coinbase_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_dc_coinbase_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    payee/1,
    amount/1,
    fee/1,
    is_valid/2,
    absorb/2,
    sign/2,
    print/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_dc_coinbase() :: #blockchain_txn_dc_coinbase_v1_pb{}.
-export_type([txn_dc_coinbase/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer()) -> txn_dc_coinbase().
new(Payee, Amount) ->
    #blockchain_txn_dc_coinbase_v1_pb{payee=Payee, amount=Amount}.



%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_dc_coinbase()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_dc_coinbase_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_dc_coinbase(), libp2p_crypto:sig_fun()) -> txn_dc_coinbase().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_dc_coinbase()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_dc_coinbase_v1_pb.payee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_dc_coinbase()) -> non_neg_integer().
amount(Txn) ->
    Txn#blockchain_txn_dc_coinbase_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_dc_coinbase()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% This transaction is only allowed in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_dc_coinbase(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, 0} ->
            Amount = ?MODULE:amount(Txn),
            case Amount > 0 of
                true ->
                    ok;
                false ->
                    {error, zero_or_negative_amount}
            end;
        _ ->
            {error, not_in_genesis_block}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_dc_coinbase(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payee = ?MODULE:payee(Txn),
    Amount = ?MODULE:amount(Txn),
    blockchain_ledger_v1:credit_dc(Payee, Amount, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_dc_coinbase()) -> iodata().
print(undefined) -> <<"type=dc_coinbase, undefined">>;
print(#blockchain_txn_dc_coinbase_v1_pb{
         payee=Payee, amount=Amount}) ->
    io_lib:format("type=dc_coinbase payee=~p, amount=~p",
                  [Payee, Amount]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_dc_coinbase_v1_pb{payee= <<"payee">>, amount=666},
    ?assertEqual(Tx, new(<<"payee">>, 666)).

payee_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(<<"payee">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(666, amount(Tx)).

-endif.
