%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Consensur Group ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_consensus_group_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_consensus_group_v1_pb.hrl").

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

-type txn_consensus_group() :: #blockchain_txn_consensus_group_v1_pb{}.
-export_type([txn_consensus_group/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new([libp2p_crypto:pubkey_bin()]) -> txn_consensus_group().
new(Members) ->
    #blockchain_txn_consensus_group_v1_pb{members=Members}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_consensus_group()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_consensus_group_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec members(txn_consensus_group()) -> [libp2p_crypto:pubkey_bin()].
members(Txn) ->
    Txn#blockchain_txn_consensus_group_v1_pb.members.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, blockchain_txn_consensus_group_v1_pb).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_consensus_group(),
             blockchain_ledger_v1:ledger()) -> ok| {error, any()}.
absorb(Txn, Ledger) ->
    Members = ?MODULE:members(Txn),
    blockchain_ledger_v1:consensus_members(Members, Ledger).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_consensus_group_v1_pb{members=[<<"1">>]},
    ?assertEqual(Tx, new([<<"1">>])).

members_test() ->
    Tx = new([<<"1">>]),
    ?assertEqual([<<"1">>], members(Tx)).

is_test() ->
    Tx0 = new([<<"1">>]),
    ?assert(is(Tx0)).

-endif.
