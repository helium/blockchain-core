%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Rejection ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_rejection_v1).

-export([
    new/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type rejection() :: #blockchain_state_channel_rejection_v1_pb{}.
-export_type([rejection/0]).

-spec new(PacketHash :: binary()) -> rejection().
new(PacketHash) ->
    #blockchain_state_channel_rejection_v1_pb{ packet_hash = PacketHash}.

-spec encode(rejection()) -> binary().
encode(#blockchain_state_channel_rejection_v1_pb{}=Rejection) ->
    blockchain_state_channel_v1_pb:encode_msg(Rejection).

-spec decode(binary()) -> rejection().
decode(BinaryRejection) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryRejection, blockchain_state_channel_rejection_v1_pb).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

encode_decode_test() ->
    Rejection = new(<<"packet_hash">>),
    ?assertEqual(Rejection, decode(encode(Rejection))).

-endif.
