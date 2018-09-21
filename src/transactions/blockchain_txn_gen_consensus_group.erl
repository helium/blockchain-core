%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Consensur Group ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_consensus_group).

-export([
    new/1
    ,members/1
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_genesis_consensus_group, {
    members = [] :: [libp2p_crypto:address()]
}).

-type txn_genesis_consensus_group() :: #txn_genesis_consensus_group{}.
-export_type([txn_genesis_consensus_group/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new([libp2p_crypto:address()]) -> txn_genesis_consensus_group().
new(Members) ->
    #txn_genesis_consensus_group{members=Members}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec members(txn_genesis_consensus_group()) -> [libp2p_crypto:address()].
members(Txn) ->
    Txn#txn_genesis_consensus_group.members.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_genesis_consensus_group).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_genesis_consensus_group{members=[<<"1">>]},
    ?assertEqual(Tx, new([<<"1">>])).

members_test() ->
    Tx = new([<<"1">>]),
    ?assertEqual([<<"1">>], members(Tx)).

is_test() ->
    Tx0 = new([<<"1">>]),
    ?assert(is(Tx0)).

-endif.
