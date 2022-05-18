%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Entry V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_entry_v2).

-export([
    new/3,
    new_hnt/2,
    new_hst/2,
    new_hgt/2,
    new_hlt/2,

    credit/3,

    hnt_balance/1, hnt_balance/2,
    hst_balance/1, hst_balance/2,
    hgt_balance/1, hgt_balance/2,
    hlt_balance/1, hlt_balance/2,

    hnt_nonce/1, hnt_nonce/2,
    hst_nonce/1, hst_nonce/2,
    hgt_nonce/1, hgt_nonce/2,
    hlt_nonce/1, hlt_nonce/2,

    serialize/1,
    deserialize/1,

    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("helium_proto/include/blockchain_ledger_entry_v2_pb.hrl").

-type entry() :: #blockchain_ledger_entry_v2_pb{}.
-type entries() :: [entry()].

-export_type([entry/0, entries/0]).

-spec new(
    Nonce :: non_neg_integer(),
    Balance :: non_neg_integer(),
    TT :: blockchain_token_type_v1:token_type()
) -> entry().
new(Nonce, Balance, hnt) when Nonce /= undefined andalso Balance /= undefined ->
    new_hnt(Nonce, Balance);
new(Nonce, Balance, hst) when Nonce /= undefined andalso Balance /= undefined ->
    new_hst(Nonce, Balance);
new(Nonce, Balance, hgt) when Nonce /= undefined andalso Balance /= undefined ->
    new_hgt(Nonce, Balance);
new(Nonce, Balance, hlt) when Nonce /= undefined andalso Balance /= undefined ->
    new_hlt(Nonce, Balance).

-spec credit(
    Entry :: entry(),
    Amount :: non_neg_integer(),
    TT :: blockchain_token_type_v1:token_type()
) -> entry().
credit(Entry, Amount, hnt) ->
    credit_hnt(Entry, Amount);
credit(Entry, Amount, hst) ->
    credit_hst(Entry, Amount);
credit(Entry, Amount, hgt) ->
    credit_hgt(Entry, Amount);
credit(Entry, Amount, hlt) ->
    credit_hlt(Entry, Amount).

-spec credit_hnt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hnt(Entry, Amount) ->
    ?MODULE:hnt_balance(Entry, ?MODULE:hnt_balance(Entry) + Amount).

-spec credit_hst(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hst(Entry, Amount) ->
    ?MODULE:hst_balance(Entry, ?MODULE:hst_balance(Entry) + Amount).

-spec credit_hgt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hgt(Entry, Amount) ->
    ?MODULE:hgt_balance(Entry, ?MODULE:hgt_balance(Entry) + Amount).

-spec credit_hlt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hlt(Entry, Amount) ->
    ?MODULE:hlt_balance(Entry, ?MODULE:hlt_balance(Entry) + Amount).

-spec new_hnt(Nonce :: non_neg_integer(), non_neg_integer()) -> entry().
new_hnt(Nonce, Balance) ->
    #blockchain_ledger_entry_v2_pb{hnt_nonce = Nonce, hnt_balance = Balance}.

-spec new_hst(Nonce :: non_neg_integer(), non_neg_integer()) -> entry().
new_hst(Nonce, Balance) ->
    #blockchain_ledger_entry_v2_pb{hst_nonce = Nonce, hst_balance = Balance}.

-spec new_hgt(Nonce :: non_neg_integer(), non_neg_integer()) -> entry().
new_hgt(Nonce, Balance) ->
    #blockchain_ledger_entry_v2_pb{hgt_nonce = Nonce, hgt_balance = Balance}.

-spec new_hlt(Nonce :: non_neg_integer(), non_neg_integer()) -> entry().
new_hlt(Nonce, Balance) ->
    #blockchain_ledger_entry_v2_pb{hlt_nonce = Nonce, hlt_balance = Balance}.

-spec hnt_nonce(entry()) -> non_neg_integer().
hnt_nonce(#blockchain_ledger_entry_v2_pb{hnt_nonce = Nonce}) ->
    Nonce.

-spec hnt_nonce(entry(), non_neg_integer()) -> entry().
hnt_nonce(Entry, Nonce) ->
    Entry#blockchain_ledger_entry_v2_pb{hnt_nonce = Nonce}.

-spec hst_nonce(entry()) -> non_neg_integer().
hst_nonce(#blockchain_ledger_entry_v2_pb{hst_nonce = Nonce}) ->
    Nonce.

-spec hst_nonce(entry(), non_neg_integer()) -> entry().
hst_nonce(Entry, Nonce) ->
    Entry#blockchain_ledger_entry_v2_pb{hst_nonce = Nonce}.

-spec hgt_nonce(entry()) -> non_neg_integer().
hgt_nonce(#blockchain_ledger_entry_v2_pb{hgt_nonce = Nonce}) ->
    Nonce.

-spec hgt_nonce(entry(), non_neg_integer()) -> entry().
hgt_nonce(Entry, Nonce) ->
    Entry#blockchain_ledger_entry_v2_pb{hgt_nonce = Nonce}.

-spec hlt_nonce(entry()) -> non_neg_integer().
hlt_nonce(#blockchain_ledger_entry_v2_pb{hlt_nonce = Nonce}) ->
    Nonce.

-spec hlt_nonce(entry(), non_neg_integer()) -> entry().
hlt_nonce(Entry, Nonce) ->
    Entry#blockchain_ledger_entry_v2_pb{hlt_nonce = Nonce}.

-spec hnt_balance(entry()) -> non_neg_integer().
hnt_balance(#blockchain_ledger_entry_v2_pb{hnt_balance = Balance}) ->
    Balance.

-spec hnt_balance(entry(), non_neg_integer()) -> entry().
hnt_balance(Entry, Balance) ->
    Entry#blockchain_ledger_entry_v2_pb{hnt_balance = Balance}.

-spec hst_balance(entry()) -> non_neg_integer().
hst_balance(#blockchain_ledger_entry_v2_pb{hst_balance = Balance}) ->
    Balance.

-spec hst_balance(entry(), non_neg_integer()) -> entry().
hst_balance(Entry, Balance) ->
    Entry#blockchain_ledger_entry_v2_pb{hst_balance = Balance}.

-spec hgt_balance(entry()) -> non_neg_integer().
hgt_balance(#blockchain_ledger_entry_v2_pb{hgt_balance = Balance}) ->
    Balance.

-spec hgt_balance(entry(), non_neg_integer()) -> entry().
hgt_balance(Entry, Balance) ->
    Entry#blockchain_ledger_entry_v2_pb{hgt_balance = Balance}.

-spec hlt_balance(entry()) -> non_neg_integer().
hlt_balance(#blockchain_ledger_entry_v2_pb{hlt_balance = Balance}) ->
    Balance.

-spec hlt_balance(entry(), non_neg_integer()) -> entry().
hlt_balance(Entry, Balance) ->
    Entry#blockchain_ledger_entry_v2_pb{hlt_balance = Balance}.

-spec serialize(Entry :: entry()) -> binary().
serialize(Entry) ->
    blockchain_ledger_entry_v2_pb:encode_msg(Entry).

-spec deserialize(EntryBin :: binary()) -> entry().
deserialize(EntryBin) ->
    blockchain_ledger_entry_v2_pb:decode_msg(EntryBin, blockchain_ledger_entry_v2_pb).

-spec print(entry()) -> iodata().
print(#blockchain_ledger_entry_v2_pb{
    hnt_nonce = HNTNonce,
    hnt_balance = HNTBal,
    hst_nonce = HSTNonce,
    hst_balance = HSTBal,
    hgt_nonce = HGTNonce,
    hgt_balance = HGTBal,
    hlt_nonce = HLTNonce,
    hlt_balance = HLTBal
}) ->
    io_lib:format(
        "type=entry_v2 hnt_nonce: ~p hnt_balance: ~p hst_nonce: ~p hst_balance: ~p hgt_nonce: ~p hgt_balance: ~p hlt_nonce: ~p hlt_balance: ~p",
        [HNTNonce, HNTBal, HSTNonce, HSTBal, HGTNonce, HGTBal, HLTNonce, HLTBal]
    ).

-spec json_type() -> binary().
json_type() ->
    <<"blockchain_ledger_entry_v2">>.

-spec to_json(Entry :: entry(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(
    #blockchain_ledger_entry_v2_pb{
        hnt_nonce = HNTNonce,
        hnt_balance = HNTBal,
        hst_nonce = HSTNonce,
        hst_balance = HSTBal,
        hgt_nonce = HGTNonce,
        hgt_balance = HGTBal,
        hlt_nonce = HLTNonce,
        hlt_balance = HLTBal
    },
    _Opts
) ->
    #{
        hnt_nonce => HNTNonce,
        hnt_balance => HNTBal,
        hst_nonce => HSTNonce,
        hst_balance => HSTBal,
        hgt_nonce => HGTNonce,
        hgt_balance => HGTBal,
        hlt_nonce => HLTNonce,
        hlt_balance => HLTBal
    }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

test_new() ->
    E0 = new(1, 10000, hnt),
    E1 = hst_nonce(hst_balance(E0, 20000), 2),
    E2 = hgt_nonce(hgt_balance(E1, 30000), 3),
    E3 = hlt_nonce(hlt_balance(E2, 40000), 4),
    E3.

new_test() ->
    Entry = #blockchain_ledger_entry_v2_pb{
        hnt_nonce = 1,
        hnt_balance = 10000,
        hst_nonce = 2,
        hst_balance = 20000,
        hgt_nonce = 3,
        hgt_balance = 30000,
        hlt_nonce = 4,
        hlt_balance = 40000
    },
    ?assertEqual(Entry, test_new()).

serde_test() ->
    Entry = test_new(),
    SerEntry = ?MODULE:serialize(Entry),
    ?assertEqual(Entry, ?MODULE:deserialize(SerEntry)).

-endif.
