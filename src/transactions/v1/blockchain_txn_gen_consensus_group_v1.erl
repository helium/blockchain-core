%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Consensur Group ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_consensus_group_v1).

-behavior(blockchain_txn).

-export([
    new/1,
    hash/1,
    members/1,
    is/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_genesis_consensus_group_v1, {
    members = [] :: [libp2p_crypto:pubkey_bin()]
}).

-type txn_genesis_consensus_group() :: #txn_genesis_consensus_group_v1{}.
-export_type([txn_genesis_consensus_group/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new([libp2p_crypto:pubkey_bin()]) -> txn_genesis_consensus_group().
new(Members) ->
    #txn_genesis_consensus_group_v1{members=Members}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_genesis_consensus_group()) -> blockchain_txn:hash().
hash(Txn) ->
    crypto:hash(sha256, erlang:term_to_binary(Txn)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec members(txn_genesis_consensus_group()) -> [libp2p_crypto:pubkey_bin()].
members(Txn) ->
    Txn#txn_genesis_consensus_group_v1.members.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_genesis_consensus_group_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_genesis_consensus_group(),
             blockchain_ledger_v1:ledger()) -> ok| {error, any()}.
absorb(Txn, Ledger) ->
    Members = ?MODULE:members(Txn),
    blockchain_ledger_v1:consensus_members(Members, Ledger).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_genesis_consensus_group_v1{members=[<<"1">>]},
    ?assertEqual(Tx, new([<<"1">>])).

members_test() ->
    Tx = new([<<"1">>]),
    ?assertEqual([<<"1">>], members(Tx)).

is_test() ->
    Tx0 = new([<<"1">>]),
    ?assert(is(Tx0)).

-endif.
