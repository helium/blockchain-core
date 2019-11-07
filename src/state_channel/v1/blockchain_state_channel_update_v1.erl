%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Update ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_update_v1).

-export([
    new/2,
    state_channel/1,
    hash/1
]).

-include("blockchain.hrl").
-include_lib("pb/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type state_channel_update() :: #blockchain_state_channel_update_v1_pb{}.
-export_type([state_channel_update/0]).

-spec new(blockchain_state_channel_v1:state_channel(), skewed:hash()) -> state_channel_update().
new(SC, Hash) -> 
    #blockchain_state_channel_update_v1_pb{
        state_channel=SC,
        hash=Hash
    }.

-spec state_channel(state_channel_update()) -> blockchain_state_channel_v1:state_channel().
state_channel(#blockchain_state_channel_update_v1_pb{state_channel=SC}) ->
    SC.

-spec hash(state_channel_update()) -> skewed:hash().
hash(#blockchain_state_channel_update_v1_pb{hash=Hash}) ->
    Hash.
%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    Hash = <<3>>,
    Update = #blockchain_state_channel_update_v1_pb{
        state_channel=SC,
        hash=Hash
    },
    ?assertEqual(Update, new(SC, Hash)).

state_channel_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    Hash = <<3>>,
    Update = new(SC, Hash),
    ?assertEqual(SC, state_channel(Update)).

hash_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    Hash = <<3>>,
    Update = new(SC, Hash),
    ?assertEqual(Hash, hash(Update)).

-endif.