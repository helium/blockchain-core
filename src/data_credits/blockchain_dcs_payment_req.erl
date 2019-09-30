%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Payment Request ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_payment_req).

-export([
    id/1, payee/1, amount/1,
    new/3,
    encode/1,
    decode/1
]).

-include("blockchain.hrl").
-include("blockchain_dcs.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(dcs_payment_req, {
    id :: binary(),
    payee :: binary(),
    amount :: integer()
}).

-type dcs_payment_req() ::#dcs_payment_req{}.

-spec id(dcs_payment_req()) -> binary().
id(#dcs_payment_req{id=Id}) ->
    Id.

-spec payee(dcs_payment_req()) -> binary().
payee(#dcs_payment_req{payee=Payee}) ->
    Payee.

-spec amount(dcs_payment_req()) -> integer().
amount(#dcs_payment_req{amount=Amount}) ->
    Amount.

-spec new(binary(),libp2p_crypto:pubkey_bin(), non_neg_integer()) -> dcs_payment_req().
new(ID, Payee, Amount) -> 
    #dcs_payment_req{
        id=ID,
        payee=Payee,
        amount=Amount
    }.

-spec encode(dcs_payment_req()) -> binary().
encode(#dcs_payment_req{}=Payment) ->
    erlang:term_to_binary(Payment).

-spec decode(binary()) -> dcs_payment_req().
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