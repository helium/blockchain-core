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
         new/2, new/3, new/4,
         payee/1,
         amount/1,
         memo/1, memo/2,
         max/1,
         is_valid_memo/1,
         is_valid_max/1,
         token_type/1, token_type/2,
         is_valid_token_type/1,
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
          Amount :: non_neg_integer() | max) -> payment().
new(Payee, Amount) when is_integer(Amount) ->
    #payment_pb{
       payee=Payee,
       amount=Amount,
       memo=0,
       max=false
      };
new(Payee, max) ->
    #payment_pb{
       payee=Payee,
       amount=0,
       memo=0,
       max=true
      }.

-spec new(Payee :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer() | max,
          MemoOrTT :: non_neg_integer() | blockchain_token_v1:type()) -> payment().
new(Payee, Amount, Memo) when is_integer(Memo) andalso is_integer(Amount) ->
    #payment_pb{
       payee=Payee,
       amount=Amount,
       memo=Memo,
       max=false
      };
new(Payee, max, Memo) when is_integer(Memo) ->
    #payment_pb{
       payee=Payee,
       amount=0,
       memo=Memo,
       max=true
      };
new(Payee, Amount, TT) when is_integer(Amount) andalso is_atom(TT) ->
    #payment_pb{
       payee=Payee,
       amount=Amount,
       memo=0,
       max=false,
       token_type=TT
      };
new(Payee, max, TT) when is_atom(TT) ->
    #payment_pb{
       payee=Payee,
       amount=0,
       memo=0,
       max=true,
       token_type=TT
      }.

-spec new(Payee :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer() | max,
          Memo :: non_neg_integer(),
          TT :: blockchain_token_v1:type()) -> payment().
new(Payee, Amount, Memo, TT) when is_integer(Amount) ->
    #payment_pb{
       payee=Payee,
       amount=Amount,
       memo=Memo,
       max=false,
       token_type=TT
      };
new(Payee, max, Memo, TT) ->
    #payment_pb{
       payee=Payee,
       amount=0,
       memo=Memo,
       max=true,
       token_type=TT
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

-spec max(Payment :: payment()) -> boolean().
max(Payment) ->
    Payment#payment_pb.max.

-spec token_type(Payment :: payment()) -> blockchain_token_v1:type().
token_type(Payment) ->
    Payment#payment_pb.token_type.

-spec token_type(Payment :: payment(), TT :: blockchain_token_v1:type()) -> payment().
token_type(Payment, TT) ->
    Payment#payment_pb{token_type=TT}.

-spec is_valid_memo(Payment :: payment()) -> boolean().
is_valid_memo(#payment_pb{memo = Memo}) ->
    try
        Bin = binary:encode_unsigned(Memo, big),
        bit_size(Bin) =< 64
    catch _:_ ->
            %% we can't do this, invalid
            false
    end.

-spec is_valid_max(Payment :: payment()) -> boolean().
is_valid_max(#payment_pb{amount=Amount, max=false}) when Amount > 0 -> true;
is_valid_max(#payment_pb{amount=0, max=true}) -> true;
is_valid_max(_) -> false.

-spec is_valid_token_type(Payment :: payment()) -> boolean().
is_valid_token_type(#payment_pb{token_type = TT}) ->
    try
        lists:member(TT, blockchain_token_v1:supported_tokens())
    catch _:_ ->
            %% we can't do this, invalid
            false
    end.

print(undefined) ->
    <<"type=payment undefined">>;
print(#payment_pb{payee=Payee, amount=Amount, memo=Memo, max=Max, token_type=TT}) ->
    io_lib:format("type=payment payee: ~p amount: ~p, memo: ~p, max: ~p, token_type: ~p", [?TO_B58(Payee), Amount, Memo, Max, TT]).

json_type() ->
    undefined.

-spec to_json(payment(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Payment, Opts) ->
    Amount = case proplists:get_value(amount, Opts, undefined) of
                 undefined -> amount(Payment);
                 Amt -> Amt
             end,
    #{
      payee => ?BIN_TO_B58(payee(Payment)),
      amount => Amount,
      memo => ?MAYBE_FN(fun (V) -> base64:encode(<<(V):64/unsigned-little-integer>>) end, memo(Payment)),
      max => ?MODULE:max(Payment),
      token_type => ?MAYBE_ATOM_TO_BINARY(token_type(Payment))
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

max_test() ->
    Payment1 = new(<<"payee">>, 100),
    ?assertEqual(false, ?MODULE:max(Payment1)),
    Payment2 = new(<<"payee">>, max),
    ?assertEqual(true, ?MODULE:max(Payment2)).

is_valid_max_test() ->
    Payment1 = new(<<"payee1">>, max),
    ?assertEqual(true, ?MODULE:is_valid_max(Payment1)),
    Payment2 = #payment_pb{payee= <<"payee2">>, amount= 100, max= true},
    ?assertEqual(false, ?MODULE:is_valid_max(Payment2)).

memo_test() ->
    Payment = new(<<"payee">>, 100, 3),
    ?assertEqual(3, ?MODULE:memo(Payment)).

token_type_test() ->
    P1 = new(<<"payee">>, 100, 3),
    ?assertEqual(hnt, ?MODULE:token_type(P1)),
    P2 = new(<<"payee">>, 100, hst),
    ?assertEqual(hst, ?MODULE:token_type(P2)).

is_valid_token_type_test() ->
    P1 = new(<<"payee">>, 100, mobile),
    ?assertEqual(true, ?MODULE:is_valid_token_type(P1)),
    P2 = new(<<"payee">>, 100, doge),
    ?assertEqual(false, ?MODULE:is_valid_token_type(P2)).

to_json_test() ->
    Payment = new(<<"payee">>, 100),
    Json = to_json(Payment, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [payee, amount, memo, max, token_type])),
    ?assertEqual(<<"mobile">>, maps:get(token_type, to_json(new(<<"payee">>, 200, mobile), []))),
    ok.

-endif.
