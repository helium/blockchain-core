%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Offer ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_offer_v1).

-export([
    new/3,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type offer() :: #blockchain_state_channel_offer_v1_pb{}.
-export_type([offer/0]).

-spec new(DevAddr :: pos_integer(),
          PacketHash :: binary(),
          SeqNum :: pos_integer()) -> offer().
new(DevAddr, PacketHash, SeqNum) ->
    #blockchain_state_channel_offer_v1_pb{
       devaddr=DevAddr,
       packet_hash=PacketHash,
       seqnum=SeqNum
    }.

-spec encode(offer()) -> binary().
encode(#blockchain_state_channel_offer_v1_pb{}=Offer) ->
    blockchain_state_channel_v1_pb:encode_msg(Offer).

-spec decode(binary()) -> offer().
decode(BinaryOffer) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryOffer, blockchain_state_channel_offer_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Offer = #blockchain_state_channel_offer_v1_pb{
        devaddr= 1,
        packet_hash = <<"packet_hash">>,
        seqnum=10
    },
    ?assertEqual(Offer, new(1, <<"packet_hash">>, 10)).

encode_decode_test() ->
    Offer = new(1, <<"packet_hash">>, 10),
    ?assertEqual(Offer, decode(encode(Offer))).

-endif.
