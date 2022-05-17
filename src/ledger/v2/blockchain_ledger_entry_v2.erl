%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Entry V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_entry_v2).

-export([
    new/2, new/3,
    nonce/1, nonce/2,
    balance/1, balance/2,
    token_type/1, token_type/2,

    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("helium_proto/include/blockchain_ledger_entries_v2_pb.hrl").

-type entry() :: #blockchain_ledger_entry_v2_pb{}.
-type entries() :: [entry()].

-export_type([entry/0, entries/0]).

-spec new(Nonce :: non_neg_integer(), Balance :: non_neg_integer()) -> entry().
new(Nonce, Balance) when Nonce /= undefined andalso Balance /= undefined ->
    #blockchain_ledger_entry_v2_pb{nonce = Nonce, balance = Balance, token_type = hnt}.

-spec new(
    Nonce :: non_neg_integer(),
    Balance :: non_neg_integer(),
    TT :: blockchain_token_type_v1:token_type()
) -> entry().
new(Nonce, Balance, TT) when
    Nonce /= undefined andalso Balance /= undefined andalso TT /= undefined
->
    #blockchain_ledger_entry_v2_pb{nonce = Nonce, balance = Balance, token_type = TT}.

-spec nonce(Entry :: entry()) -> non_neg_integer().
nonce(#blockchain_ledger_entry_v2_pb{nonce = Nonce}) ->
    Nonce.

-spec nonce(Nonce :: non_neg_integer(), Entry :: entry()) -> entry().
nonce(Nonce, Entry) ->
    Entry#blockchain_ledger_entry_v2_pb{nonce = Nonce}.

-spec balance(Entry :: entry()) -> non_neg_integer().
balance(#blockchain_ledger_entry_v2_pb{balance = Balance}) ->
    Balance.

-spec balance(Balance :: non_neg_integer(), Entry :: entry()) -> entry().
balance(Balance, Entry) ->
    Entry#blockchain_ledger_entry_v2_pb{balance = Balance}.

-spec token_type(Entry :: entry()) -> non_neg_integer().
token_type(#blockchain_ledger_entry_v2_pb{token_type = TT}) ->
    TT.

-spec token_type(TT :: blockchain_token_type_v1:token_type(), Entry :: entry()) -> entry().
token_type(TT, Entry) ->
    Entry#blockchain_ledger_entry_v2_pb{token_type = TT}.

-spec print(undefined | entry()) -> iodata().
print(undefined) ->
    <<"type=entry_v2 undefined">>;
print(#blockchain_ledger_entry_v2_pb{nonce = Nonce, balance = Balance, token_type = TT}) ->
    io_lib:format("type=entry_v2 nonce: ~p balance: ~p, token_type: ~p", [Nonce, Balance, TT]).

-spec json_type() -> binary().
json_type() ->
    <<"blockchain_ledger_entry_v2">>.

-spec to_json(Entry :: entry(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Entry, _Opts) ->
    #{
        nonce => nonce(Entry),
        balance => balance(Entry),
        token_type => atom_to_list(token_type(Entry))
    }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Entry = #blockchain_ledger_entry_v2_pb{nonce=0, balance=100},
    ?assertEqual(Entry, new(0, 100)),
    ?assertEqual(hnt, ?MODULE:token_type(Entry)),

new2_test() ->
    Entry = #blockchain_ledger_entry_v2_pb{nonce=0, balance=100, token_type=hst},
    ?assertEqual(Entry, new(0, 100, hst)),
    ?assertEqual(hst, ?MODULE:token_type(Entry)),

nonce_test() ->
    Entry = #blockchain_ledger_entry_v2_pb{nonce=0, balance=100},
    ?assertEqual(0, ?MODULE:nonce(Entry)).

balance_test() ->
    Entry = #blockchain_ledger_entry_v2_pb{nonce = 0, balance = 100},
    ?assertEqual(100, ?MODULE:balance(Entry)).

token_type_test() ->
    Entry = #blockchain_ledger_entry_v2_pb{nonce = 0, balance = 100, token_type = hlt},
    ?assertEqual(hlt, ?MODULE:token_type(Entry)).

to_json_test() ->
    Entry = #blockchain_ledger_entry_v2_pb{nonce = 0, balance = 100},
    Json = to_json(Entry, []),
    ?assert(
        lists:all(
            fun(K) -> maps:is_key(K, Json) end,
            [nonce, balance, token_type]
        )
    ).

-endif.
