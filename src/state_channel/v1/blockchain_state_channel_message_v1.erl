%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Message ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_message_v1).

-export([encode/1, decode/1]).

-include("blockchain.hrl").
-include_lib("pb/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type message() :: #blockchain_state_channel_message_v1_pb{}.
-type oneof() :: blockchain_state_channel_request_v1:request() |
                 blockchain_state_channel_v1:state_channel() |
                 blockchain_state_channel_packet_v1:packet().

-export_type([message/0]).

-spec encode(oneof()) -> binary().
encode(Msg) ->
     blockchain_state_channel_v1_pb:encode_msg(wrap_msg(Msg)).

-spec decode(binary()) -> {atom(), oneof()}.
decode(Bin) ->
    unwrap_msg(blockchain_state_channel_v1_pb:decode_msg(Bin, blockchain_state_channel_message_v1_pb)).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec wrap_msg(oneof()) -> message().
wrap_msg(#blockchain_state_channel_v1_pb{}=SC) ->
    #blockchain_state_channel_message_v1_pb{msg={state_channel, SC}};
wrap_msg(#blockchain_state_channel_request_v1_pb{}=Req) ->
    #blockchain_state_channel_message_v1_pb{msg={request, Req}};
wrap_msg(#blockchain_state_channel_packet_v1_pb{}=Packet) ->
    #blockchain_state_channel_message_v1_pb{msg={packet, Packet}}.

-spec unwrap_msg(message()) -> {atom(), oneof()}.
unwrap_msg(#blockchain_state_channel_message_v1_pb{msg={Type, Msg}}) ->
    {Type, Msg}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

encode_decode_test() ->
    Req = blockchain_state_channel_request_v1:new(<<"payee">>, 1, 12),
    ?assertEqual({request, Req}, decode(encode(Req))).

-endif.