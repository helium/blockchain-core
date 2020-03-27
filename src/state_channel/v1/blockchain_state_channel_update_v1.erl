%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Update ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_update_v1).

-export([
    new/2,
    state_channel/1,
    previous_hash/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type state_channel_update() :: #blockchain_state_channel_update_v1_pb{}.
-export_type([state_channel_update/0]).

-spec new(SC :: blockchain_state_channel_v1:state_channel(),
          Hash :: skewed:hash()) -> state_channel_update().
new(SC, Hash) ->
    #blockchain_state_channel_update_v1_pb{
        state_channel=SC,
        previous_hash=Hash
    }.

-spec state_channel(state_channel_update()) -> blockchain_state_channel_v1:state_channel().
state_channel(#blockchain_state_channel_update_v1_pb{state_channel=SC}) ->
    SC.

-spec previous_hash(state_channel_update()) -> skewed:hash().
previous_hash(#blockchain_state_channel_update_v1_pb{previous_hash=Hash}) ->
    Hash.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    Hash = <<3>>,
    Update = #blockchain_state_channel_update_v1_pb{
        state_channel=SC,
        previous_hash=Hash
    },
    ?assertEqual(Update, new(SC, Hash)).

state_channel_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    Hash = <<3>>,
    Update = new(SC, Hash),
    ?assertEqual(SC, state_channel(Update)).

previous_hash_test() ->
    SC = blockchain_state_channel_v1:new(<<1>>, <<2>>),
    Hash = <<3>>,
    Update = new(SC, Hash),
    ?assertEqual(Hash, previous_hash(Update)).

-endif.
