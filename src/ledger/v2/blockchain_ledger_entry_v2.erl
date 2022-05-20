%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Entry V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_entry_v2).

-export([
    new/0,
    nonce/1, nonce/2,
    balance/1, balance/2,
    credit/3,
    debit/3,
    serialize/1,
    deserialize/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("helium_proto/include/blockchain_ledger_entry_v2_pb.hrl").

-type entry() :: #blockchain_ledger_entry_v2_pb{}.

-export_type([entry/0]).

%% ==================================================================
%% API Functions
%% ==================================================================

-spec new() -> entry().
new() ->
    #blockchain_ledger_entry_v2_pb{
        nonce = 0,
        hnt_balance = 0,
        hst_balance = 0,
        hgt_balance = 0,
        hlt_balance = 0
    }.

-spec nonce(Entry :: entry()) -> non_neg_integer().
nonce(#blockchain_ledger_entry_v2_pb{nonce = Nonce}) ->
    Nonce.

-spec nonce(Entry :: entry(), Nonce :: non_neg_integer()) -> entry().
nonce(Entry, Nonce) ->
    Entry#blockchain_ledger_entry_v2_pb{nonce = Nonce}.

%%--------------------------------------------------------------------
%% @doc Return hnt balance as default.
%% @end
%%--------------------------------------------------------------------
-spec balance(Entry :: entry()) -> non_neg_integer().
balance(Entry) ->
    hnt_balance(Entry).

%%--------------------------------------------------------------------
%% @doc Return requested token balance.
%% @end
%%--------------------------------------------------------------------
-spec balance(Entry :: entry(), TT :: blockchain_token_type_v1:token_type()) -> non_neg_integer().
balance(Entry, hnt) ->
    hnt_balance(Entry);
balance(Entry, hst) ->
    hst_balance(Entry);
balance(Entry, hgt) ->
    hgt_balance(Entry);
balance(Entry, hlt) ->
    hlt_balance(Entry).

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

-spec debit(
    Entry :: entry(),
    Amount :: non_neg_integer(),
    TT :: blockchain_token_type_v1:token_type()
) -> entry().
debit(Entry, Amount, hnt) ->
    debit_hnt(Entry, Amount);
debit(Entry, Amount, hst) ->
    debit_hst(Entry, Amount);
debit(Entry, Amount, hgt) ->
    debit_hgt(Entry, Amount);
debit(Entry, Amount, hlt) ->
    debit_hlt(Entry, Amount).

-spec serialize(Entry :: entry()) -> binary().
serialize(Entry) ->
    blockchain_ledger_entry_v2_pb:encode_msg(Entry).

-spec deserialize(EntryBin :: binary()) -> entry().
deserialize(EntryBin) ->
    blockchain_ledger_entry_v2_pb:decode_msg(EntryBin, blockchain_ledger_entry_v2_pb).

%% ==================================================================
%% Internal Functions
%% ==================================================================

-spec credit_hnt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hnt(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hnt_balance = hnt_balance(Entry) + Amount}.

-spec credit_hst(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hst(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hst_balance = hst_balance(Entry) + Amount}.

-spec credit_hlt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hlt(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hlt_balance = hlt_balance(Entry) + Amount}.

-spec credit_hgt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
credit_hgt(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hgt_balance = hgt_balance(Entry) + Amount}.

-spec debit_hnt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
debit_hnt(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hnt_balance = hnt_balance(Entry) - Amount}.

-spec debit_hst(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
debit_hst(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hst_balance = hst_balance(Entry) - Amount}.

-spec debit_hgt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
debit_hgt(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hgt_balance = hgt_balance(Entry) - Amount}.

-spec debit_hlt(Entry :: entry(), Amount :: non_neg_integer()) -> entry().
debit_hlt(Entry, Amount) ->
    Entry#blockchain_ledger_entry_v2_pb{hlt_balance = hlt_balance(Entry) - Amount}.

-spec hnt_balance(Entry :: entry()) -> non_neg_integer().
hnt_balance(#blockchain_ledger_entry_v2_pb{hnt_balance = Balance}) ->
    Balance.

-spec hst_balance(Entry :: entry()) -> non_neg_integer().
hst_balance(#blockchain_ledger_entry_v2_pb{hst_balance = Balance}) ->
    Balance.

-spec hgt_balance(Entry :: entry()) -> non_neg_integer().
hgt_balance(#blockchain_ledger_entry_v2_pb{hgt_balance = Balance}) ->
    Balance.

-spec hlt_balance(Entry :: entry()) -> non_neg_integer().
hlt_balance(#blockchain_ledger_entry_v2_pb{hlt_balance = Balance}) ->
    Balance.
