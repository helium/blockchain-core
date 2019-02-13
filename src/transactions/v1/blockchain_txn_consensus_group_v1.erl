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
    sign/2,
    members/1,
    is_valid/2,
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
-spec sign(txn_consensus_group(), libp2p_crypto:sig_fun()) -> txn_consensus_group().
sign(Txn, _SigFun) ->
    Txn.

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
-spec is_valid(txn_consensus_group(), blockchain_ledger_v1:ledger()) -> ok.
is_valid(Txn, _Ledger) ->
    case ?MODULE:members(Txn) of
        [] -> {error, no_members};
        _ -> ok
    end.cleanup

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

-endif.
