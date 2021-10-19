%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Message ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_message_v1).

-export([encode/1, decode/1, wrap_msg/1]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type message() :: #blockchain_state_channel_message_v1_pb{}.
-type oneof() :: blockchain_state_channel_response_v1:response() |
                 blockchain_state_channel_packet_v1:packet() |
                 blockchain_state_channel_offer_v1:offer() |
                 blockchain_state_channel_purchase_v1:purchase() |
                 blockchain_state_channel_banner_v1:banner() |
                 blockchain_state_channel_rejection_v1:rejection()|
                 blockchain_state_channel_purchase_diff_v1:purchase_diff().

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
    #blockchain_state_channel_message_v1_pb{msg={packet, Packet}};
wrap_msg(#blockchain_state_channel_offer_v1_pb{}=Offer) ->
    #blockchain_state_channel_message_v1_pb{msg={offer, Offer}};
wrap_msg(#blockchain_state_channel_purchase_v1_pb{}=Purchase) ->
    #blockchain_state_channel_message_v1_pb{msg={purchase, Purchase}};
wrap_msg(#blockchain_state_channel_banner_v1_pb{}=Banner) ->
    #blockchain_state_channel_message_v1_pb{msg={banner, Banner}};
wrap_msg(#blockchain_state_channel_rejection_v1_pb{}=Rejection) ->
    #blockchain_state_channel_message_v1_pb{msg={reject, Rejection}};
wrap_msg(#blockchain_state_channel_purchase_diff_v1_pb{}=PurchaseDiff) ->
    #blockchain_state_channel_message_v1_pb{msg={purchase_diff, PurchaseDiff}}.

-spec unwrap_msg(message()) -> {atom(), oneof()}.
unwrap_msg(#blockchain_state_channel_message_v1_pb{msg={Type, Msg}}) ->
    {Type, Msg}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

encode_decode_test() ->
    Packet = blockchain_helium_packet_v1:new({devaddr, 16#deadbeef}, <<"yolo">>),
    SCPacket = blockchain_state_channel_packet_v1:new(Packet, <<"hotspot">>, 'US915'),
    ?assertEqual({packet, SCPacket}, decode(encode(SCPacket))).

-endif.
