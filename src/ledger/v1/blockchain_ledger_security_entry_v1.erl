%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Security Entry ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_security_entry_v1).

-export([
    new/0, new/2,
    nonce/1, nonce/2,
    balance/1, balance/2,
    serialize/1, deserialize/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(security_entry_v1, {
    nonce = 0 :: non_neg_integer(),
    balance = 0 :: non_neg_integer()
}).

-type security_entry() :: #security_entry_v1{}.

-export_type([security_entry/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new() -> security_entry().
new() ->
    #security_entry_v1{}.

-spec new(non_neg_integer(), non_neg_integer()) -> security_entry().
new(Nonce, Balance) when Nonce /= undefined andalso Balance /= undefined ->
    #security_entry_v1{nonce=Nonce, balance=Balance}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(security_entry()) -> non_neg_integer().
nonce(#security_entry_v1{nonce=Nonce}) ->
    Nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(non_neg_integer(), security_entry()) -> security_entry().
nonce(Nonce, Entry) ->
    Entry#security_entry_v1{nonce=Nonce}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec balance(security_entry()) -> non_neg_integer().
balance(#security_entry_v1{balance=Balance}) ->
    Balance.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec balance(non_neg_integer(), security_entry()) -> security_entry().
balance(Balance, Entry) ->
    Entry#security_entry_v1{balance=Balance}.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(security_entry()) -> binary().
serialize(Entry) ->
    BinEntry = erlang:term_to_binary(Entry),
    <<1, BinEntry/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> security_entry().
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
    Entry0 = #security_entry_v1{
        nonce = 0,
        balance = 0
    },
    ?assertEqual(Entry0, new()),
    Entry1 = #security_entry_v1{
        nonce = 1,
        balance = 1
    },
    ?assertEqual(Entry1, new(1, 1)).

nonce_test() ->
    Entry = new(),
    ?assertEqual(0, nonce(Entry)),
    ?assertEqual(1, nonce(nonce(1, Entry))).

balance_test() ->
    Entry = new(),
    ?assertEqual(0, balance(Entry)),
    ?assertEqual(1, balance(balance(1, Entry))).

-endif.
