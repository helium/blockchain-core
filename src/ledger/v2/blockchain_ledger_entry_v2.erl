%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Entry V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_entry_v2).

-export([
    new/0, new/2,
    nonce/1, nonce/2,
    balance/1, balance/2,
    credit/3,
    debit/3,
    serialize/1,
    deserialize/1,

    from_v1/2
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
        mobile_balance = 0,
        iot_balance = 0
    }.

%% NOTE: This function is for 1:1 correspondence entry_v1
-spec new(Nonce :: non_neg_integer(), HNTBalance :: non_neg_integer()) -> entry().
new(Nonce, HNTBalance) when Nonce /= undefined andalso HNTBalance /= undefined ->
    #blockchain_ledger_entry_v2_pb{nonce = Nonce, hnt_balance = HNTBalance}.

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
-spec balance(Entry :: entry(), TT :: blockchain_token_v1:type()) -> non_neg_integer().
balance(Entry, hnt) ->
    hnt_balance(Entry);
balance(Entry, hst) ->
    hst_balance(Entry);
balance(Entry, mobile) ->
    mobile_balance(Entry);
balance(Entry, iot) ->
    iot_balance(Entry).

-spec credit(
    Entry :: entry(),
    Amount :: non_neg_integer(),
    TT :: blockchain_token_v1:type()
) -> entry().
credit(Entry, Amount, hnt) ->
    Entry#blockchain_ledger_entry_v2_pb{hnt_balance = hnt_balance(Entry) + Amount};
credit(Entry, Amount, hst) ->
    Entry#blockchain_ledger_entry_v2_pb{hst_balance = hst_balance(Entry) + Amount};
credit(Entry, Amount, mobile) ->
    Entry#blockchain_ledger_entry_v2_pb{mobile_balance = mobile_balance(Entry) + Amount};
credit(Entry, Amount, iot) ->
    Entry#blockchain_ledger_entry_v2_pb{iot_balance = iot_balance(Entry) + Amount}.

-spec debit(
    Entry :: entry(),
    Amount :: non_neg_integer(),
    TT :: blockchain_token_v1:type()
) -> entry().
debit(Entry, Amount, hnt) ->
    Entry#blockchain_ledger_entry_v2_pb{hnt_balance = hnt_balance(Entry) - Amount};
debit(Entry, Amount, hst) ->
    Entry#blockchain_ledger_entry_v2_pb{hst_balance = hst_balance(Entry) - Amount};
debit(Entry, Amount, mobile) ->
    Entry#blockchain_ledger_entry_v2_pb{mobile_balance = mobile_balance(Entry) - Amount};
debit(Entry, Amount, iot) ->
    Entry#blockchain_ledger_entry_v2_pb{iot_balance = iot_balance(Entry) - Amount}.

-spec serialize(Entry :: entry()) -> binary().
serialize(Entry) ->
    blockchain_ledger_entry_v2_pb:encode_msg(Entry).

-spec deserialize(EntryBin :: binary()) -> entry().
deserialize(EntryBin) ->
    blockchain_ledger_entry_v2_pb:decode_msg(EntryBin, blockchain_ledger_entry_v2_pb).

-spec from_v1(
    V1 :: blockchain_ledger_entry_v1:entry() | blockchain_ledger_security_entry_v1:entry(),
    Type :: entry | security
) -> entry().
from_v1(V1, entry) ->
    from_entry_v1(V1);
from_v1(V1, security) ->
    from_sec_v1(V1).

-spec from_entry_v1(EntryV1 :: blockchain_ledger_entry_v1:entry()) -> entry().
from_entry_v1(EntryV1) ->
    Balance = blockchain_ledger_entry_v1:balance(EntryV1),
    Nonce = blockchain_ledger_entry_v1:nonce(EntryV1),
    Entry0 = new(),
    nonce(credit(Entry0, Balance, hnt), Nonce).

-spec from_sec_v1(SecEntryV1 :: blockchain_ledger_security_entry_v1:entry()) -> entry().
from_sec_v1(SecEntryV1) ->
    Balance = blockchain_ledger_security_entry_v1:balance(SecEntryV1),
    Nonce = blockchain_ledger_security_entry_v1:nonce(SecEntryV1),
    Entry0 = new(),
    nonce(credit(Entry0, Balance, hst), Nonce).

%% ==================================================================
%% Internal Functions
%% ==================================================================

-spec hnt_balance(Entry :: entry()) -> non_neg_integer().
hnt_balance(#blockchain_ledger_entry_v2_pb{hnt_balance = Balance}) ->
    Balance.

-spec hst_balance(Entry :: entry()) -> non_neg_integer().
hst_balance(#blockchain_ledger_entry_v2_pb{hst_balance = Balance}) ->
    Balance.

-spec mobile_balance(Entry :: entry()) -> non_neg_integer().
mobile_balance(#blockchain_ledger_entry_v2_pb{mobile_balance = Balance}) ->
    Balance.

-spec iot_balance(Entry :: entry()) -> non_neg_integer().
iot_balance(#blockchain_ledger_entry_v2_pb{iot_balance = Balance}) ->
    Balance.
