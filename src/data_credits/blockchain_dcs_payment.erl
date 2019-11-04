%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_payment).

-export([
    new/5,
    payer/1, payee/1, amount/1, packets/1, nonce/1, signature/1,
    sign/2, store/3, get_all/3,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include("blockchain_dcs.hrl").
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

-spec store(rocksdb:db_handle(), rocksdb:cf_handle(), dcs_payment()) -> ok | {error, any()}.
store(DB, CF, #helium_dcs_payment_v1_pb{nonce=Nonce, amount=Amount}=Payment) ->
    Encoded = ?MODULE:encode(Payment),
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CF, <<Nonce>>, Encoded),
    ok = rocksdb:batch_put(Batch, CF, ?NONCE_KEY, <<Nonce>>),
    case Nonce == 0 of
        true ->
            ok = rocksdb:batch_put(Batch, CF, ?CREDITS_KEY, <<Amount>>);
        false ->
            case rocksdb:get(DB, CF, ?CREDITS_KEY, [{sync, true}]) of
                {ok, <<Credits/integer>>} ->
                    Total = Credits - Amount,
                    ok = rocksdb:batch_put(Batch, CF, ?CREDITS_KEY, <<Total>>);
                _Error ->
                    lager:error("failed to get ~p: ~p", [?CREDITS_KEY, _Error]),
                    _Error
            end
    end,
    ok = rocksdb:write_batch(DB, Batch, []).

-spec get_all(rocksdb:db_handle(), rocksdb:cf_handle(), non_neg_integer()) -> [binary()].
get_all(DB, CF, Height) ->
    get_all(DB, CF, Height, 0, []).

-spec get_all(rocksdb:db_handle(), rocksdb:cf_handle(), non_neg_integer(),
                   non_neg_integer(), [binary()]) -> [binary()].
get_all(_DB, _CF, Height, I, Payments) when Height < I ->
    lists:reverse(Payments);
get_all(DB, CF, Height, I, Payments) ->
    case rocksdb:get(DB, CF, <<I>>, [{sync, true}]) of
        {ok, Payment} ->
            get_all(DB, CF, Height, I+1, [Payment|Payments]);
        _Error ->
            lager:error("failed to get ~p: ~p", [<<Height>>, _Error]),
            get_all(DB, CF, Height, I+1, Payments)
    end.

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