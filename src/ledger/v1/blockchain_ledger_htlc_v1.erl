%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger HTLC ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_htlc_v1).

-export([
    new/0, new/6,
    nonce/1, nonce/2,
    payer/1, payer/2,
    payee/1, payee/2,
    balance/1, balance/2,
    hashlock/1, hashlock/2,
    timelock/1, timelock/2,
    serialize/1, deserialize/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(htlc_v1, {
    nonce = 0 :: non_neg_integer(),
    payer :: undefined | libp2p_crypto:pubkey_bin(),
    payee :: undefined | libp2p_crypto:pubkey_bin(),
    balance = 0 :: non_neg_integer(),
    hashlock :: undefined | binary(),
    timelock :: undefined | non_neg_integer()
}).

-type htlc() :: #htlc_v1{}.

-export_type([htlc/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new() -> htlc().
new() ->
    #htlc_v1{}.

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), non_neg_integer(),
          non_neg_integer(), binary(), non_neg_integer()) -> htlc().
new(Payer, Payee, Balance, Nonce, Hashlock, Timelock) when Balance /= undefined ->
    #htlc_v1{
        payer=Payer,
        payee=Payee,
        balance=Balance,
        nonce=Nonce,
        hashlock=Hashlock,
        timelock=Timelock
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(htlc()) -> non_neg_integer().
nonce(#htlc_v1{nonce=Nonce}) ->
    Nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(non_neg_integer(), htlc()) -> htlc().
nonce(Nonce, HTLC) ->
    HTLC#htlc_v1{nonce=Nonce}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(htlc()) -> undefined | libp2p_crypto:pubkey_bin().
payer(#htlc_v1{payer=Payer}) ->
    Payer.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(libp2p_crypto:pubkey_bin(), htlc()) -> htlc().
payer(Payer, HTLC) ->
    HTLC#htlc_v1{payer=Payer}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(htlc()) -> undefined | libp2p_crypto:pubkey_bin().
payee(#htlc_v1{payee=Payee}) ->
    Payee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(libp2p_crypto:pubkey_bin(), htlc()) -> htlc().
payee(Payee, HTLC) ->
    HTLC#htlc_v1{payee=Payee}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec balance(htlc()) -> non_neg_integer().
balance(#htlc_v1{balance=Balance}) ->
    Balance.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec balance(non_neg_integer(), htlc()) -> htlc().
balance(Balance, HTLC) ->
    HTLC#htlc_v1{balance=Balance}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hashlock(htlc()) -> binary().
hashlock(#htlc_v1{hashlock=Hashlock}) ->
    Hashlock.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hashlock(binary(), htlc()) -> htlc().
hashlock(Hashlock, HTLC) ->
    HTLC#htlc_v1{hashlock=Hashlock}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec timelock(htlc()) -> non_neg_integer().
timelock(#htlc_v1{timelock=Timelock}) ->
    Timelock.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec timelock(non_neg_integer(), htlc()) -> htlc().
timelock(Timelock, HTLC) ->
    HTLC#htlc_v1{timelock=Timelock}.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(htlc()) -> binary().
serialize(HTLC) ->
    BinHTLC = erlang:term_to_binary(HTLC, [compressed]),
    <<1, BinHTLC/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> htlc().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    HTLC0 = #htlc_v1{
        nonce=0,
        payer=undefined,
        payee=undefined,
        balance=0,
        hashlock=undefined,
        timelock=undefined
    },
    ?assertEqual(HTLC0, new()),
    HTLC1 = #htlc_v1{
        nonce=0,
        payer= <<"payer">>,
        payee= <<"payee">>,
        balance=12,
        hashlock= <<"hashlock">>,
        timelock=13
    },
    ?assertEqual(HTLC1, new(<<"payer">>, <<"payee">>, 12, 0, <<"hashlock">>, 13)).

nonce_test() ->
    HTLC = new(),
    ?assertEqual(0, nonce(HTLC)),
    ?assertEqual(1, nonce(nonce(1, HTLC))).

payer_test() ->
    HTLC = new(),
    ?assertEqual(undefined, payer(HTLC)),
    ?assertEqual(<<"payer">>, payer(payer(<<"payer">>, HTLC))).

payee_test() ->
    HTLC = new(),
    ?assertEqual(undefined, payee(HTLC)),
    ?assertEqual(<<"payee">>, payee(payee(<<"payee">>, HTLC))).

balance_test() ->
    HTLC = new(),
    ?assertEqual(0, balance(HTLC)),
    ?assertEqual(1, balance(balance(1, HTLC))).

hashlock_test() ->
    HTLC = new(),
    ?assertEqual(undefined, hashlock(HTLC)),
    ?assertEqual(<<"hashlock">>, hashlock(hashlock(<<"hashlock">>, HTLC))).

timelock_test() ->
    HTLC = new(),
    ?assertEqual(undefined, timelock(HTLC)),
    ?assertEqual(1, timelock(timelock(1, HTLC))).

-endif.
