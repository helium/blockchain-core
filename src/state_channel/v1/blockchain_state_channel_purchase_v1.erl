%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Purchase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_purchase_v1).

-export([
    new/4,
    hotspot/1, packet_hash/1, region/1, sc/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type purchase() :: #blockchain_state_channel_purchase_v1_pb{}.
-export_type([purchase/0]).

-spec new(SC :: blockchain_state_channel_v1:state_channel(),
          Hotspot :: libp2p_crypto:pubkey_bin(),
          PacketHash :: binary(),
          Region :: atom()) -> purchase().
new(SC, Hotspot, PacketHash, Region) ->
    #blockchain_state_channel_purchase_v1_pb{
       sc=SC,
       hotspot=Hotspot,
       packet_hash=PacketHash,
       region=Region
    }.

-spec hotspot(purchase()) -> libp2p_crypto:pubkey_bin().
hotspot(#blockchain_state_channel_purchase_v1_pb{hotspot=Hotspot}) ->
    Hotspot.

-spec region(purchase()) -> atom().
region(#blockchain_state_channel_purchase_v1_pb{region=Region}) ->
    Region.

-spec packet_hash(purchase()) -> binary().
packet_hash(#blockchain_state_channel_purchase_v1_pb{packet_hash=PacketHash}) ->
    PacketHash.

-spec sc(purchase()) -> blockchain_state_channel_v1:state_channel().
sc(#blockchain_state_channel_purchase_v1_pb{sc=SC}) ->
    SC.

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
    SC = blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>, 10),
    Hotspot = <<"hotspot">>,
    Region = 'US915',
    PacketHash = <<"packet_hash">>,
    Purchase = #blockchain_state_channel_purchase_v1_pb{sc = SC,
                                                        hotspot = Hotspot,
                                                        packet_hash = PacketHash,
                                                        region = Region},
    ?assertEqual(Purchase, new(SC, Hotspot, PacketHash, Region)).

encode_decode_test() ->
    SC = blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>, 10),
    Purchase = new(SC, <<"hotspot">>, <<"packet_hash">>, 'US915'),
    ?assertEqual(Purchase, decode(encode(Purchase))).

-endif.
