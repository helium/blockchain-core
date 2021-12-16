%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Payment V2 ==
%%%-------------------------------------------------------------------
-module(blockchain_payment_v2).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v2_pb.hrl").

-export([
         new/2, new/3,
         payee/1,
         amount/1,
         memo/1, memo/2,
         is_valid_memo/1,
         print/1,
         json_type/0,
         to_json/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type payment() :: #payment_pb{}.
-type payments() :: [payment()].

-export_type([payment/0, payments/0]).

-spec new(Payee :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer()) -> payment().
new(Payee, Amount) ->
    #payment_pb{
       payee=Payee,
       amount=Amount,
       memo=0
      }.

-spec new(Payee :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer(),
          Memo :: non_neg_integer()) -> payment().
new(Payee, Amount, Memo) ->
    #payment_pb{
       payee=Payee,
       amount=Amount,
       memo=Memo
      }.

-spec payee(Payment :: payment()) -> libp2p_crypto:pubkey_bin().
payee(Payment) ->
    Payment#payment_pb.payee.

-spec amount(Payment :: payment()) -> non_neg_integer().
amount(Payment) ->
    Payment#payment_pb.amount.

-spec memo(Payment :: payment()) -> undefined | non_neg_integer().
memo(Payment) ->
    Payment#payment_pb.memo.

-spec memo(Payment :: payment(), Memo :: non_neg_integer()) -> payment().
memo(Payment, Memo) ->
    Payment#payment_pb{memo=Memo}.

-spec is_valid_memo(Payment :: payment()) -> boolean().
is_valid_memo(#payment_pb{memo = Memo}) ->
    try
        Bin = binary:encode_unsigned(Memo, big),
        bit_size(Bin) =< 64
    catch _:_ ->
            %% we can't do this, invalid
            false
    end.

print(undefined) ->
    <<"type=payment undefined">>;
print(#payment_pb{payee=Payee, amount=Amount, memo=Memo}) ->
    io_lib:format("type=payment payee: ~p amount: ~p, memo: ~p", [?TO_B58(Payee), Amount, Memo]).

json_type() ->
    undefined.

-spec to_json(payment(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Payment, _Opts) ->
    #{
      payee => ?BIN_TO_B58(payee(Payment)),
      amount => amount(Payment),
      memo => ?MAYBE_FN(fun (V) -> base64:encode(<<(V):64/unsigned-little-integer>>) end, memo(Payment))
     }.

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

to_json_test() ->
    Payment = new(<<"payee">>, 100),
    Json = to_json(Payment, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [payee, amount])).

-endif.
