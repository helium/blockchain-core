%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction TOken Burn Exchange Rate ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_token_burn_exchange_rate_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_token_burn_exchange_rate_v1_pb.hrl").

-export([
    new/1,
    hash/1,
    rate/1,
    fee/1,
    is_valid/2,
    absorb/2,
    sign/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_token_burn_exchange_rate() :: #blockchain_txn_token_burn_exchange_rate_v1_pb{}.
-export_type([txn_token_burn_exchange_rate/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(non_neg_integer()) -> txn_token_burn_exchange_rate().
new(Amount) ->
    #blockchain_txn_token_burn_exchange_rate_v1_pb{rate=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_token_burn_exchange_rate()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_token_burn_exchange_rate_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_token_burn_exchange_rate(), libp2p_crypto:sig_fun()) -> txn_token_burn_exchange_rate().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rate(txn_token_burn_exchange_rate()) -> non_neg_integer().
rate(Txn) ->
    Txn#blockchain_txn_token_burn_exchange_rate_v1_pb.rate.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_token_burn_exchange_rate()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% This transaction is only allowed in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_token_burn_exchange_rate(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, _Chain) ->
    Amount = ?MODULE:rate(Txn),
    case Amount > 0 of
        true ->
            ok;
        false ->
            {error, zero_or_negative_rate}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_token_burn_exchange_rate(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Rate = ?MODULE:rate(Txn),
    blockchain_ledger_v1:token_burn_exchange_rate(Rate, Ledger).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_token_burn_exchange_rate_v1_pb{rate=666},
    ?assertEqual(Tx, new(666)).

rate_test() ->
    Tx = new(666),
    ?assertEqual(666, rate(Tx)).

-endif.
