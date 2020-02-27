%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Payment V2 ==
%%%-------------------------------------------------------------------
-module(blockchain_payment_v2).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v2_pb.hrl").

-export([
         new/2,
         payee/1,
         amount/1,
         print/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type payment() :: #payment_pb{}.
-type payments() :: [payment()].

-export_type([payment/0, payments/0]).

-spec new(Payee :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer()) -> payment().
new(Payee, Amount) when Amount > 0 ->
    #payment_pb{
       payee=Payee,
       amount=Amount
      }.

-spec payee(Payment :: payment()) -> libp2p_crypto:pubkey_bin().
payee(Payment) ->
    Payment#payment_pb.payee.

-spec amount(Payment :: payment()) -> non_neg_integer().
amount(Payment) ->
    Payment#payment_pb.amount.

print(undefined) ->
    <<"type=payment undefined">>;
print(#payment_pb{payee=Payee, amount=Amount}) ->
    io_lib:format("type=payment payee: ~p amount: ~p", [?TO_B58(Payee), Amount]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Payment = #payment_pb{payee= <<"payee">>, amount= 100},
    ?assertEqual(Payment, new(<<"payee">>, 100)).

payee_test() ->
    Payment = new(<<"payee">>, 100),
    ?assertEqual(<<"payee">>, ?MODULE:payee(Payment)).

amount_test() ->
    Payment = new(<<"payee">>, 100),
    ?assertEqual(100, ?MODULE:amount(Payment)).

-endif.
