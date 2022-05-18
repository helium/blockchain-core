%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Entries V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_entries_v2).

-export([
    new/2,
    owner/1,
    entries/1, entries/2,

    serialize/1,
    deserialize/1,

    print/1,
    json_type/0,
    to_json/2
]).

-include("blockchain_json.hrl").
-include_lib("helium_proto/include/blockchain_ledger_entries_v2_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type entries() :: #blockchain_ledger_entries_v2_pb{}.

-spec new(
    Owner :: libp2p_crypto:pubkey_bin(),
    Entries :: blockchain_ledger_entry_v2:entries()
) -> entries().
new(Owner, Entries) ->
    #blockchain_ledger_entries_v2_pb{owner = Owner, entries = Entries}.

-spec owner(Entries :: entries()) -> libp2p_crypto:pubkey_bin().
owner(#blockchain_ledger_entries_v2_pb{owner = Owner}) ->
    Owner.

-spec entries(Entries :: entries()) -> blockchain_ledger_entry_v2:entries().
entries(#blockchain_ledger_entries_v2_pb{entries = Entries}) ->
    Entries.

-spec entries(Entries :: entries(), LedgerEntries :: blockchain_ledger_entry_v2:entries()) ->
    entries().
entries(Entries, LedgerEntries) ->
    Entries#blockchain_ledger_entries_v2_pb{entries = LedgerEntries}.

-spec serialize(Entries :: entries()) -> binary().
serialize(Entries) ->
    blockchain_ledger_entries_v2_pb:encode_msg(Entries).

-spec deserialize(EntriesBin :: binary()) -> entries().
deserialize(EntriesBin) ->
    blockchain_ledger_entries_v2_pb:decode_msg(EntriesBin, blockchain_ledger_entries_v2_pb).

-spec print(entries()) -> iodata().
print(#blockchain_ledger_entries_v2_pb{owner = Owner, entries = Entries}) ->
    io_lib:format(
        "type=entries_v2 owner: ~p entries: ~p",
        [libp2p_crypto:bin_to_b58(Owner), [blockchain_ledger_entry_v2:print(E) || E <- Entries]]
    ).

-spec json_type() -> binary().
json_type() ->
    <<"blockchain_ledger_entries_v2">>.

-spec to_json(Entries :: entries(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Entries, _Opts) ->
    #{
        owner => ?BIN_TO_B58(owner(Entries)),
        entries => [blockchain_ledger_entry_v2:to_json(E, []) || E <- ?MODULE:entries(Entries)]
    }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    E1 = #blockchain_ledger_entry_v2_pb{nonce = 0, balance = 100, token_type = hnt},
    E2 = #blockchain_ledger_entry_v2_pb{nonce = 1, balance = 200, token_type = hst},
    E3 = #blockchain_ledger_entry_v2_pb{nonce = 1, balance = 300, token_type = hgt},
    E4 = #blockchain_ledger_entry_v2_pb{nonce = 0, balance = 50, token_type = hlt},
    LedgerEntries = [E1, E2, E3, E4],
    Entries = #blockchain_ledger_entries_v2_pb{owner = <<"owner">>, entries = LedgerEntries},
    ?assertEqual(Entries, new(<<"owner">>, LedgerEntries)).

owner_test() ->
    Entries = new(<<"owner">>, [
        blockchain_ledger_entry_v2:new(0, 100),
        blockchain_ledger_entry_v2:new(1, 20, hlt)
    ]),
    ?assertEqual(<<"owner">>, ?MODULE:owner(Entries)).

entries_test() ->
    LedgerEntries = [
        blockchain_ledger_entry_v2:new(0, 100),
        blockchain_ledger_entry_v2:new(1, 20, hlt)
    ],
    Entries = new(<<"owner">>, LedgerEntries),
    ?assertEqual(LedgerEntries, ?MODULE:entries(Entries)).

serde_test() ->
    LedgerEntries = [
        blockchain_ledger_entry_v2:new(0, 100),
        blockchain_ledger_entry_v2:new(1, 20, hlt)
    ],
    Entries = new(<<"owner">>, LedgerEntries),
    SerEntries = ?MODULE:serialize(Entries),
    ?assertEqual(Entries, ?MODULE:deserialize(SerEntries)).

-endif.
