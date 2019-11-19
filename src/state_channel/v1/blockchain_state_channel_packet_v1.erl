%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_packet_v1).

-export([
    new/2,
    packet/1, req_id/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type packet() :: #helium_state_channel_packet_v1_pb{}.
-export_type([packet/0]).

-spec new(binary(), blockchain_state_channel_payment_req_v1:id()) -> packet().
new(Packet, ReqID) -> 
    #helium_state_channel_packet_v1_pb{
        packet=Packet,
        req_id=ReqID
    }.

-spec packet(packet()) -> binary().
packet(#helium_state_channel_packet_v1_pb{packet=Packet}) ->
    Packet.

-spec req_id(packet()) -> blockchain_state_channel_payment_req_v1:id().
req_id(#helium_state_channel_packet_v1_pb{req_id=ReqID}) ->
    ReqID.

-spec encode(packet()) -> binary().
encode(#helium_state_channel_packet_v1_pb{}=Packet) ->
    helium_state_channel_v1_pb:encode_msg(Packet).

-spec decode(binary()) -> packet().
decode(BinaryPacket) ->
    helium_state_channel_v1_pb:decode_msg(BinaryPacket, helium_state_channel_packet_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Packet = #helium_state_channel_packet_v1_pb{
        packet= <<"data">>,
        req_id= <<"req_id">>
    },
    ?assertEqual(Packet, new(<<"data">>, <<"req_id">>)).

packet_test() ->
    Packet = new(<<"data">>, <<"req_id">>),
    ?assertEqual(<<"data">>, packet(Packet)).

req_id_test() ->
    Packet = new(<<"data">>, <<"req_id">>),
    ?assertEqual(<<"req_id">>, req_id(Packet)).

encode_decode_test() ->
    Packet = new(<<"data">>, <<"req_id">>),
    ?assertEqual(Packet, decode(encode(Packet))).

-endif.