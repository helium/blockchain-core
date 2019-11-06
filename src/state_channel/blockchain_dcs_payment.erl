%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_payment).

-export([
    new/5,
    payer/1, payee/1, amount/1, packets/1, nonce/1, signature/1,
    sign/2,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_dcs_payment_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type dcs_payment() ::#helium_dcs_payment_v1_pb{}.

-spec new(binary(), binary(), non_neg_integer(), binary(), non_neg_integer()) -> dcs_payment().
new(Payer, Payee, Amount, Packets, Nonce) -> 
    #helium_dcs_payment_v1_pb{
        payer=Payer,
        payee=Payee,
        amount=Amount,
        packets=Packets,
        nonce=Nonce
    }.

-spec payer(dcs_payment()) -> binary().
payer(#helium_dcs_payment_v1_pb{payer=Payer}) ->
    Payer.

-spec payee(dcs_payment()) -> binary().
payee(#helium_dcs_payment_v1_pb{payee=Payee}) ->
    Payee.

-spec amount(dcs_payment()) -> non_neg_integer().
amount(#helium_dcs_payment_v1_pb{amount=Amount}) ->
    Amount.

-spec packets(dcs_payment()) -> binary().
packets(#helium_dcs_payment_v1_pb{packets=Packets}) ->
    Packets.

-spec nonce(dcs_payment()) -> non_neg_integer().
nonce(#helium_dcs_payment_v1_pb{nonce=Nonce}) ->
    Nonce.

-spec signature(dcs_payment()) -> binary().
signature(#helium_dcs_payment_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(dcs_payment(), function()) -> dcs_payment().
sign(Payment, SigFun) ->
    EncodedPayment = ?MODULE:encode(Payment#helium_dcs_payment_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedPayment),
    Payment#helium_dcs_payment_v1_pb{signature=Signature}.

-spec encode(dcs_payment()) -> binary().
encode(#helium_dcs_payment_v1_pb{}=Payment) ->
    helium_dcs_payment_v1_pb:encode_msg(Payment).

-spec decode(binary()) -> dcs_payment().
decode(BinaryPayment) ->
    helium_dcs_payment_v1_pb:decode_msg(BinaryPayment, helium_dcs_payment_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.