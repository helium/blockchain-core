%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Entry V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_entry_v2).

-export([new/0, nonce/2, balance/2, credit/3, debit/3, serialize/1, deserialize/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("helium_proto/include/blockchain_ledger_entry_v2_pb.hrl").

-type hnt_entry() :: #hnt_entry_pb{}.
-type hst_entry() :: #hst_entry_pb{}.
-type hgt_entry() :: #hgt_entry_pb{}.
-type hlt_entry() :: #hlt_entry_pb{}.
-type entry() :: #blockchain_ledger_entry_v2_pb{}.

-export_type([entry/0, hnt_entry/0, hst_entry/0, hgt_entry/0, hlt_entry/0]).

%% ==================================================================
%% API Functions
%% ==================================================================

-spec new() -> entry().
new() ->
    HNT = new_hnt(0, 0),
    HST = new_hst(0, 0),
    HGT = new_hgt(0, 0),
    HLT = new_hlt(0, 0),
    #blockchain_ledger_entry_v2_pb{
        hnt_entry = HNT,
        hst_entry = HST,
        hgt_entry = HGT,
        hlt_entry = HLT
    }.

-spec nonce(Entry :: entry(), TT :: blockchain_token_type_v1:token_type()) -> non_neg_integer().
nonce(Entry, hnt) ->
    hnt_nonce(hnt_entry(Entry));
nonce(Entry, hst) ->
    hst_nonce(hst_entry(Entry));
nonce(Entry, hgt) ->
    hgt_nonce(hgt_entry(Entry));
nonce(Entry, hlt) ->
    hlt_nonce(hlt_entry(Entry)).

-spec balance(Entry :: entry(), TT :: blockchain_token_type_v1:token_type()) -> non_neg_integer().
balance(Entry, hnt) ->
    hnt_balance(hnt_entry(Entry));
balance(Entry, hst) ->
    hst_balance(hst_entry(Entry));
balance(Entry, hgt) ->
    hgt_balance(hgt_entry(Entry));
balance(Entry, hlt) ->
    hlt_balance(hlt_entry(Entry)).

-spec credit(
    Entry :: entry(),
    Amount :: non_neg_integer(),
    TT :: blockchain_token_type_v1:token_type()
) -> entry().
credit(Entry, Amount, hnt) ->
    hnt_entry(Entry, credit_hnt(hnt_entry(Entry), Amount));
credit(Entry, Amount, hst) ->
    hst_entry(Entry, credit_hst(hst_entry(Entry), Amount));
credit(Entry, Amount, hgt) ->
    hgt_entry(Entry, credit_hgt(hgt_entry(Entry), Amount));
credit(Entry, Amount, hlt) ->
    hlt_entry(Entry, credit_hlt(hlt_entry(Entry), Amount)).

-spec debit(
    Entry :: entry(),
    Amount :: non_neg_integer(),
    TT :: blockchain_token_type_v1:token_type()
) -> entry().
debit(Entry, Amount, hnt) ->
    hnt_entry(Entry, debit_hnt(hnt_entry(Entry), Amount));
debit(Entry, Amount, hst) ->
    hst_entry(Entry, debit_hst(hst_entry(Entry), Amount));
debit(Entry, Amount, hgt) ->
    hgt_entry(Entry, debit_hgt(hgt_entry(Entry), Amount));
debit(Entry, Amount, hlt) ->
    hlt_entry(Entry, debit_hlt(hlt_entry(Entry), Amount)).

-spec serialize(Entry :: entry()) -> binary().
serialize(Entry) ->
    blockchain_ledger_entry_v2_pb:encode_msg(Entry).

-spec deserialize(EntryBin :: binary()) -> entry().
deserialize(EntryBin) ->
    blockchain_ledger_entry_v2_pb:decode_msg(EntryBin, blockchain_ledger_entry_v2_pb).

%% ==================================================================
%% Internal Functions
%% ==================================================================

hnt_entry(#blockchain_ledger_entry_v2_pb{hnt_entry = HNT}) ->
    HNT.
hnt_entry(Entry, HNT) ->
    Entry#blockchain_ledger_entry_v2_pb{hnt_entry = HNT}.

hst_entry(#blockchain_ledger_entry_v2_pb{hst_entry = HST}) ->
    HST.
hst_entry(Entry, HST) ->
    Entry#blockchain_ledger_entry_v2_pb{hst_entry = HST}.

hgt_entry(#blockchain_ledger_entry_v2_pb{hgt_entry = HGT}) ->
    HGT.
hgt_entry(Entry, HGT) ->
    Entry#blockchain_ledger_entry_v2_pb{hgt_entry = HGT}.

hlt_entry(#blockchain_ledger_entry_v2_pb{hlt_entry = HLT}) ->
    HLT.
hlt_entry(Entry, HLT) ->
    Entry#blockchain_ledger_entry_v2_pb{hlt_entry = HLT}.

new_hnt(Nonce, Balance) ->
    #hnt_entry_pb{nonce = Nonce, balance = Balance}.

new_hst(Nonce, Balance) ->
    #hst_entry_pb{nonce = Nonce, balance = Balance}.

new_hgt(Nonce, Balance) ->
    #hgt_entry_pb{nonce = Nonce, balance = Balance}.

new_hlt(Nonce, Balance) ->
    #hlt_entry_pb{nonce = Nonce, balance = Balance}.

credit_hnt(HNT, Amount) ->
    HNT#hnt_entry_pb{balance = hnt_balance(HNT) + Amount}.

credit_hst(HST, Amount) ->
    HST#hst_entry_pb{balance = hst_balance(HST) + Amount}.

credit_hlt(HLT, Amount) ->
    HLT#hlt_entry_pb{balance = hlt_balance(HLT) + Amount}.

credit_hgt(HGT, Amount) ->
    HGT#hgt_entry_pb{balance = hgt_balance(HGT) + Amount}.

debit_hnt(HNT, Amount) ->
    HNT#hnt_entry_pb{balance = hnt_balance(HNT) - Amount}.

debit_hst(HST, Amount) ->
    HST#hst_entry_pb{balance = hst_balance(HST) - Amount}.

debit_hlt(HLT, Amount) ->
    HLT#hlt_entry_pb{balance = hlt_balance(HLT) - Amount}.

debit_hgt(HGT, Amount) ->
    HGT#hgt_entry_pb{balance = hgt_balance(HGT) - Amount}.

hnt_nonce(#hnt_entry_pb{nonce = Nonce}) ->
    Nonce.
hst_nonce(#hst_entry_pb{nonce = Nonce}) ->
    Nonce.
hgt_nonce(#hgt_entry_pb{nonce = Nonce}) ->
    Nonce.
hlt_nonce(#hlt_entry_pb{nonce = Nonce}) ->
    Nonce.

hnt_balance(#hnt_entry_pb{balance = Balance}) ->
    Balance.
hst_balance(#hst_entry_pb{balance = Balance}) ->
    Balance.
hgt_balance(#hgt_entry_pb{balance = Balance}) ->
    Balance.
hlt_balance(#hlt_entry_pb{balance = Balance}) ->
    Balance.

%% increment_hnt_nonce(#hnt_entry_pb{nonce = Nonce} = HNT) ->
%%     HNT#hnt_entry_pb{nonce = Nonce + 1}.
%% 
%% increment_hst_nonce(#hst_entry_pb{nonce = Nonce} = HST) ->
%%     HST#hst_entry_pb{nonce = Nonce + 1}.
%% 
%% increment_hgt_nonce(#hgt_entry_pb{nonce = Nonce} = HGT) ->
%%     HGT#hgt_entry_pb{nonce = Nonce + 1}.
%% 
%% increment_hlt_nonce(#hlt_entry_pb{nonce = Nonce} = HLT) ->
%%     HLT#hlt_entry_pb{nonce = Nonce + 1}.
