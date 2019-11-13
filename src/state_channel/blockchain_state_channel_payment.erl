%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_payment).

-export([
    new/4,
    payer/1, payee/1, amount/1, packet/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type payment() :: #helium_state_channel_payment_v1_pb{}.

-spec new(binary(), binary(), non_neg_integer(), binary()) -> payment().
new(Payer, Payee, Amount, Packet) -> 
    #helium_state_channel_payment_v1_pb{
        payer=Payer,
        payee=Payee,
        amount=Amount,
        packet=Packet
    }.

-spec payer(payment()) -> binary().
payer(#helium_state_channel_payment_v1_pb{payer=Payer}) ->
    Payer.

-spec payee(payment()) -> binary().
payee(#helium_state_channel_payment_v1_pb{payee=Payee}) ->
    Payee.

-spec amount(payment()) -> non_neg_integer().
amount(#helium_state_channel_payment_v1_pb{amount=Amount}) ->
    Amount.

-spec packet(payment()) -> binary().
packet(#helium_state_channel_payment_v1_pb{packet=Packets}) ->
    Packets.

-spec encode(payment()) -> binary().
encode(#helium_state_channel_payment_v1_pb{}=Payment) ->
    helium_state_channel_v1_pb:encode_msg(Payment).

-spec decode(binary()) -> payment().
decode(BinaryPayment) ->
    helium_state_channel_v1_pb:decode_msg(BinaryPayment, helium_state_channel_payment_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Payment = #helium_state_channel_payment_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        amount=10,
        packet= <<>>
    },
    ?assertEqual(Payment, new(<<"payer">>, <<"payee">>, 10, <<>>)).

payer_test() ->
    Payment = new(<<"payer">>, <<"payee">>, 10, <<>>),
    ?assertEqual(<<"payer">>, payer(Payment)).

payee_test() ->
    Payment = new(<<"payer">>, <<"payee">>, 10, <<>>),
    ?assertEqual(<<"payee">>, payee(Payment)).

amount_test() ->
    Payment = new(<<"payer">>, <<"payee">>, 10, <<>>),
    ?assertEqual(10, amount(Payment)).

packet_test() ->
    Payment = new(<<"payer">>, <<"payee">>, 10, <<>>),
    ?assertEqual(<<>>, packet(Payment)).

encode_decode_test() ->
    Payment = new(<<"payer">>, <<"payee">>, 10, <<>>),
    ?assertEqual(Payment, decode(encode(Payment))).

-endif.