%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Purchase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_purchase_v1).

-export([
    new/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type purchase() :: #blockchain_state_channel_purchase_v1_pb{}.
-export_type([purchase/0]).

-spec new(StateChannel :: blockchain_state_channel_v1:state_channel()) -> purchase().
new(StateChannel) ->
    #blockchain_state_channel_purchase_v1_pb{
       sc=StateChannel
    }.

-spec encode(purchase()) -> binary().
encode(#blockchain_state_channel_purchase_v1_pb{}=Purchase) ->
    blockchain_state_channel_v1_pb:encode_msg(Purchase).

-spec decode(binary()) -> purchase().
decode(BinaryPurchase) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryPurchase, blockchain_state_channel_purchase_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>),
    Purchase = #blockchain_state_channel_purchase_v1_pb{sc = SC},
    ?assertEqual(Purchase, new(SC)).

encode_decode_test() ->
    Purchase = new(blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>)),
    ?assertEqual(Purchase, decode(encode(Purchase))).

-endif.
