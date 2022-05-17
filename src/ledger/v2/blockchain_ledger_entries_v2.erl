%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Entry V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_entries_v2).

-export([
    new/2,
    owner/1,
    entries/1,

    serialize/1,
    deserialize/1,

    print/1,
    json_type/0,
    to_json/2
]).

-include("blockchain_json.hrl").
-include_lib("helium_proto/include/blockchain_ledger_entries_v2_pb.hrl").

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

-spec entries(Entries :: entries(), LedgerEntries :: blockchain_ledger_entry_v2:entries()) -> blockchain_ledger_entry_v2:entries().
entries(Entries, LedgerEntries) ->
    Entries.

-spec serialize(Entries :: entries()) -> binary().
serialize(Entries) ->
    blockchain_ledger_entries_v2_pb:encode_msg(Entries).

-spec deserialize(EntriesBin :: binary()) -> entries().
deserialize(EntriesBin) ->
    blockchain_ledger_entries_v2_pb:decode_msg(EntriesBin, blockchain_ledger_entries_v2_pb).

-spec print(undefined | entries()) -> iodata().
print(undefined) ->
    <<"type=entries_v2 undefined">>;
print(#blockchain_ledger_entries_v2_pb{owner = Owner, entries = Entries}) ->
    io_lib:format(
        "type=entries_v2 owner: ~p entries: ~p",
        [
            libp2p_crypto:bin_to_b58(Owner),
            [blockchain_ledger_entry_v2:print(E) || E <- ?MODULE:entries(Entries)]
        ]
    ).

-spec json_type() -> binary().
json_type() ->
    <<"blockchain_ledger_entries_v2">>.

-spec to_json(Entries :: entries(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Entries, _Opts) ->
    #{
        owner => ?BIN_TO_B58(owner(Entries)),
        entries => [blockchain_ledger_entry_v2:to_json(E) || E <- ?MODULE:entries(Entries)]
    }.
