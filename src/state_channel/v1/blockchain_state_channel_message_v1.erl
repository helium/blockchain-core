%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Message ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_message_v1).

-export([encode/1, decode/1]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type message() :: #helium_state_channel_message_v1_pb{}.
-type oneof() :: blockchain_state_channel_payment_req_v1:payment_req() |
                 blockchain_state_channel_payment_v1:payment() |
                blockchain_state_channel_packet_v1:packet().

-export_type([message/0]).

-spec encode(oneof()) -> binary().
encode(Msg) ->
     helium_state_channel_v1_pb:encode_msg(wrap_msg(Msg)).

-spec decode(binary()) -> {atom(), oneof()}.
decode(Bin) ->
    unwrap_msg(helium_state_channel_v1_pb:decode_msg(Bin, helium_state_channel_message_v1_pb)).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec wrap_msg(oneof()) -> message().
wrap_msg(#helium_state_channel_v1_pb{}=SC) ->
    #helium_state_channel_message_v1_pb{msg={state_channel, SC}};
wrap_msg(#helium_state_channel_payment_req_v1_pb{}=Req) ->
    #helium_state_channel_message_v1_pb{msg={payment_req, Req}};
wrap_msg(#helium_state_channel_packet_v1_pb{}=Packet) ->
    #helium_state_channel_message_v1_pb{msg={packet, Packet}}.

-spec unwrap_msg(message()) -> {atom(), oneof()}.
unwrap_msg(#helium_state_channel_message_v1_pb{msg={Type, Msg}}) ->
    {Type, Msg}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

encode_decode_test() ->
    Req = blockchain_state_channel_payment_req_v1:new(<<"id">>, <<"payee">>, 1, 12),
    ?assertEqual({payment_req, Req}, decode(encode(Req))).

-endif.