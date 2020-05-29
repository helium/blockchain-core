%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Message ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_message_v1).

-export([encode/1, decode/1]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type message() :: #blockchain_state_channel_message_v1_pb{}.
-type oneof() :: blockchain_state_channel_response_v1:response() |
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
wrap_msg(#blockchain_state_channel_response_v1_pb{}=Packet) ->
    #blockchain_state_channel_message_v1_pb{msg={response, Packet}};
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
    Packet = blockchain_helium_packet_v1:new({devaddr, 16#deadbeef}, <<"yolo">>),
    SCPacket = blockchain_state_channel_packet_v1:new(Packet, <<"hotspot">>, <<"US902-928">>),
    ?assertEqual({packet, SCPacket}, decode(encode(SCPacket))).

-endif.
