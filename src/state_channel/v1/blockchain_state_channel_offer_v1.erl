%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Offer ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_offer_v1).

-export([
    from_packet/3,
    hotspot/1,
    routing/1,
    region/1,
    packet_hash/1,
    payload_size/1,
    signature/1, sign/2,
    validate/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type offer() :: #blockchain_state_channel_offer_v1_pb{}.
-export_type([offer/0]).

-spec from_packet(Packet :: blockchain_helium_packet_v1:packet(),
                  Hotspot :: libp2p_crypto:pubkey_bin(),
                  Region :: atom()) -> offer().
from_packet(Packet, Hotspot, Region) ->
    case blockchain_helium_packet_v1:routing_info(Packet) of
        {eui, _, _}=Routing ->
            #blockchain_state_channel_offer_v1_pb{
               routing=blockchain_helium_packet_v1:make_routing_info(Routing),
               packet_hash=blockchain_helium_packet_v1:packet_hash(Packet),
               payload_size=byte_size(blockchain_helium_packet_v1:payload(Packet)),
               hotspot=Hotspot,
               signature = <<>>,
               region=Region
              };
        {devaddr, _}=Routing ->
            #blockchain_state_channel_offer_v1_pb{
               routing=blockchain_helium_packet_v1:make_routing_info(Routing),
               packet_hash=blockchain_helium_packet_v1:packet_hash(Packet),
               payload_size=byte_size(blockchain_helium_packet_v1:payload(Packet)),
               hotspot=Hotspot,
               signature = <<>>,
               region=Region
              }
    end.

-spec hotspot(offer()) -> libp2p_crypto:pubkey_bin().
hotspot(#blockchain_state_channel_offer_v1_pb{hotspot=Hotspot}) ->
    Hotspot.

-spec routing(offer()) -> blockchain_helium_packet_v1:routing_information().
routing(#blockchain_state_channel_offer_v1_pb{routing=Routing}) ->
    Routing.

-spec region(offer()) -> atom().
region(#blockchain_state_channel_offer_v1_pb{region=Region}) ->
    Region.

-spec packet_hash(offer()) -> binary().
packet_hash(#blockchain_state_channel_offer_v1_pb{packet_hash=PacketHash}) ->
    PacketHash.

-spec payload_size(offer()) -> pos_integer().
payload_size(#blockchain_state_channel_offer_v1_pb{payload_size=PayloadSize}) ->
    PayloadSize.

-spec signature(offer()) -> binary().
signature(#blockchain_state_channel_offer_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(offer(), function()) -> offer().
sign(Offer, SigFun) ->
    EncodedReq = ?MODULE:encode(Offer#blockchain_state_channel_offer_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedReq),
    Offer#blockchain_state_channel_offer_v1_pb{signature=Signature}.

-spec validate(offer()) -> true | {error, any()}.
validate(Offer) ->
    %% TODO: enhance
    BaseOffer = Offer#blockchain_state_channel_offer_v1_pb{signature = <<>>},
    EncodedOffer = ?MODULE:encode(BaseOffer),
    Signature = ?MODULE:signature(Offer),
    PubKeyBin = ?MODULE:hotspot(Offer),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    case libp2p_crypto:verify(EncodedOffer, Signature, PubKey) of
        false -> {error, bad_signature};
        true -> true
    end.

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

from_packet_test() ->
    %% TODO
    ok.

encode_decode_test() ->
    %% TODO
    ok.

-endif.
