%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_payment).

-export([
    payee/1, amount/1, nonce/1, signature/1,
    new/3, sign/2, store/3, get_all/3,
    encode/1,
    decode/1
]).

-include("blockchain.hrl").
-include("blockchain_dcs.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(dcs_payment, {
    payee :: binary(),
    amount :: integer(),
    nonce :: integer(),
    signature :: binary() | undefined
}).

-type dcs_payment() ::#dcs_payment{}.

-spec payee(dcs_payment()) -> binary().
payee(#dcs_payment{payee=Payee}) ->
    Payee.

-spec amount(dcs_payment()) -> integer().
amount(#dcs_payment{amount=Amount}) ->
    Amount.

-spec nonce(dcs_payment()) -> integer().
nonce(#dcs_payment{nonce=Nonce}) ->
    Nonce.

-spec signature(dcs_payment()) -> binary().
signature(#dcs_payment{signature=Signature}) ->
    Signature.

-spec new(binary(), non_neg_integer(), non_neg_integer()) -> dcs_payment().
new(Payee, Amount, Nonce) -> 
    #dcs_payment{
        payee=Payee,
        amount=Amount,
        nonce=Nonce
    }.

-spec sign(dcs_payment(), function()) -> dcs_payment().
sign(Payment, SigFun) ->
    EncodedPayment = ?MODULE:encode(Payment#dcs_payment{signature=undefined}),
    Signature = SigFun(EncodedPayment),
    Payment#dcs_payment{signature=Signature}.

-spec store(rocksdb:db_handle(), rocksdb:cf_handle(), dcs_payment()) -> ok | {error, any()}.
store(DB, CF, #dcs_payment{nonce=Nonce, amount=Amount}=Payment) ->
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
encode(#dcs_payment{}=Payment) ->
    erlang:term_to_binary(Payment).

-spec decode(binary()) -> dcs_payment().
decode(BinaryPayment) ->
    erlang:binary_to_term(BinaryPayment).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.