%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Purchase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_purchase_v1).

-export([
    new/4, new_diff/4,
    sc/1,
    hotspot/1,
    packet_hash/1,
    region/1,
    sc_diff/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type purchase() :: #blockchain_state_channel_purchase_v1_pb{}.
-export_type([purchase/0]).

-spec new(
    SC :: blockchain_state_channel_v1:state_channel(),
    Hotspot :: libp2p_crypto:pubkey_bin(),
    PacketHash :: binary(),
    Region :: atom()
) -> purchase().
new(SC, Hotspot, PacketHash, Region) ->
    #blockchain_state_channel_purchase_v1_pb{
       sc=SC,
       hotspot=Hotspot,
       packet_hash=PacketHash,
       region=Region
    }.

-spec new_diff(
    SCDiff :: blockchain_state_channel_diff_v1:diff(),
    Hotspot :: libp2p_crypto:pubkey_bin(),
    PacketHash :: binary(),
    Region :: atom()
) -> purchase().
new_diff(SCDiff, Hotspot, PacketHash, Region) ->
    #blockchain_state_channel_purchase_v1_pb{
       hotspot=Hotspot,
       packet_hash=PacketHash,
       region=Region,
       sc_diff=SCDiff
    }.

-spec sc(purchase()) -> blockchain_state_channel_v1:state_channel().
sc(#blockchain_state_channel_purchase_v1_pb{sc=SC}) ->
    SC.

-spec hotspot(purchase()) -> libp2p_crypto:pubkey_bin().
hotspot(#blockchain_state_channel_purchase_v1_pb{hotspot=Hotspot}) ->
    Hotspot.

-spec region(purchase()) -> atom().
region(#blockchain_state_channel_purchase_v1_pb{region=Region}) ->
    Region.

-spec packet_hash(purchase()) -> binary().
packet_hash(#blockchain_state_channel_purchase_v1_pb{packet_hash=PacketHash}) ->
    PacketHash.

-spec sc_diff(purchase()) -> blockchain_state_channel_diff_v1:diff().
sc_diff(#blockchain_state_channel_purchase_v1_pb{sc_diff=SCDiff}) ->
    SCDiff.

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
