%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Conbase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_coinbase_v1).

-export([
    new/2,
    payee/1,
    amount/1,
    is/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_coinbase_v1, {
    payee :: libp2p_crypto:address(),
    amount :: integer()
}).

-type txn_coinbase() :: #txn_coinbase_v1{}.
-export_type([txn_coinbase/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), integer()) -> txn_coinbase().
new(Payee, Amount) ->
    #txn_coinbase_v1{payee=Payee, amount=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_coinbase()) -> libp2p_crypto:address().
payee(Txn) ->
    Txn#txn_coinbase_v1.payee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_coinbase()) -> integer().
amount(Txn) ->
    Txn#txn_coinbase_v1.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_coinbase_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_coinbase(), blockchain_ledger:ledger()) -> {ok, blockchain_ledger:ledger()}
                                                            | {error, any()}.
absorb(Txn, Ledger) ->
    Payee = ?MODULE:payee(Txn),
    Amount = ?MODULE:amount(Txn),
    case Amount > 0 of
        true ->
            {ok, blockchain_ledger:credit_account(Payee, Amount, Ledger)};
        false ->
            {ok, Ledger}
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_coinbase_v1{payee= <<"payee">>, amount=666},
    ?assertEqual(Tx, new(<<"payee">>, 666)).

payee_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(<<"payee">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(666, amount(Tx)).

is_test() ->
    Tx0 = new(<<"payee">>, 666),
    ?assert(is(Tx0)).

-endif.
