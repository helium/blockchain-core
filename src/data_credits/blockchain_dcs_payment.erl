%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Payment ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_payment).

-export([
    id/1, key/1, height/1, payer/1, payee/1, amount/1, signature/1,
    new/6, store/3, get_all/3,
    encode/1,
    decode/1
]).

-include("blockchain.hrl").
-include("blockchain_dcs.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(dcs_payment, {
    id :: binary(),
    key :: binary(),
    height :: integer(),
    payer :: binary(),
    payee :: binary(),
    amount :: integer(),
    signature :: binary() | undefined
}).

-type dcs_payment() ::#dcs_payment{}.

-spec id(dcs_payment()) -> binary().
id(#dcs_payment{id=Id}) ->
    Id.

-spec key(dcs_payment()) -> binary().
key(#dcs_payment{key=Key}) ->
    Key.

-spec height(dcs_payment()) -> integer().
height(#dcs_payment{height=Height}) ->
    Height.

-spec payer(dcs_payment()) -> binary().
payer(#dcs_payment{payer=Payer}) ->
    Payer.

-spec payee(dcs_payment()) -> binary().
payee(#dcs_payment{payee=Payee}) ->
    Payee.

-spec amount(dcs_payment()) -> integer().
amount(#dcs_payment{amount=Amount}) ->
    Amount.

-spec signature(dcs_payment()) -> binary().
signature(#dcs_payment{signature=Signature}) ->
    Signature.

-spec new(binary(), map(), non_neg_integer(), libp2p_crypto:pubkey_bin(),
                  libp2p_crypto:pubkey_bin(), non_neg_integer()) -> dcs_payment().
new(ID, #{secret := PrivKey, public := PubKey}, Height, Payer, Payee, Amount) -> 
    Payment = #dcs_payment{
        id=ID,
        key=libp2p_crypto:pubkey_to_bin(PubKey),
        height=Height,
        payer=Payer,
        payee=Payee,
        amount=Amount
    },
    EncodedPayment = ?MODULE:encode(Payment),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Signature = SigFun(EncodedPayment),
    Payment#dcs_payment{signature=Signature}.

-spec store(rocksdb:db_handle(), rocksdb:cf_handle(), dcs_payment()) -> ok | {error, any()}.
store(DB, CF, #dcs_payment{height=Height, amount=Amount}=Payment) ->
    Encoded = ?MODULE:encode(Payment),
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CF, <<Height>>, Encoded),
    ok = rocksdb:batch_put(Batch, CF, ?HEIGHT_KEY, <<Height>>),
    case Height == 0 of
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