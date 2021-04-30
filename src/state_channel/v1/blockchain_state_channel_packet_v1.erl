%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_packet_v1).

-export([
    new/3, new/4,
    packet/1, hotspot/1, region/1, signature/1, hold_time/1,
    sign/2, validate/2,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type packet() :: #blockchain_state_channel_packet_v1_pb{}.
-export_type([packet/0]).

-spec new(blockchain_helium_packet_v1:packet(), libp2p_crypto:pubkey_bin(), atom()) -> packet().
new(Packet, Hotspot, Region) ->
    new(Packet, Hotspot, Region, 0).

-spec new(blockchain_helium_packet_v1:packet(), libp2p_crypto:pubkey_bin(), atom(), non_neg_integer()) -> packet().
new(Packet, Hotspot, Region, HoldTime) ->
    #blockchain_state_channel_packet_v1_pb{
        packet=Packet,
        hotspot=Hotspot,
        region=Region,
        hold_time=HoldTime
    }.

-spec packet(packet()) -> blockchain_helium_packet_v1:packet().
packet(#blockchain_state_channel_packet_v1_pb{packet=Packet}) ->
    Packet.

-spec hotspot(packet()) -> libp2p_crypto:pubkey_bin().
hotspot(#blockchain_state_channel_packet_v1_pb{hotspot=Hotspot}) ->
    Hotspot.

-spec region(packet()) -> atom().
region(#blockchain_state_channel_packet_v1_pb{region=Region}) ->
    Region.

-spec signature(packet()) -> binary().
signature(#blockchain_state_channel_packet_v1_pb{signature=Signature}) ->
    Signature.

-spec hold_time(packet()) -> non_neg_integer().
hold_time(#blockchain_state_channel_packet_v1_pb{hold_time=HoldTime}) ->
    HoldTime.

-spec sign(packet(), function()) -> packet().
sign(Packet, SigFun) ->
    EncodedReq = ?MODULE:encode(Packet#blockchain_state_channel_packet_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedReq),
    Packet#blockchain_state_channel_packet_v1_pb{signature=Signature}.

-spec validate(packet(), blockchain_state_channel_offer_v1:offer()) -> true | {error, any()}.
validate(Packet, Offer) ->
    BasePacket = Packet#blockchain_state_channel_packet_v1_pb{signature = <<>>},
    EncodedPacket = ?MODULE:encode(BasePacket),
    Signature = ?MODULE:signature(Packet),
    PubKeyBin = ?MODULE:hotspot(Packet),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    case libp2p_crypto:verify(EncodedPacket, Signature, PubKey) of
        false -> {error, bad_signature};
        true ->
            %% compute the offer from the packet and check it compares to the original offer
            ExpectedOffer = blockchain_state_channel_offer_v1:from_packet(packet(Packet), hotspot(Packet), region(Packet)),
            %% signatures won't be the same, but we can compare everything else
            lager:info("Original offer ~p, calculated offer ~p", [Offer, ExpectedOffer]),
            case blockchain_state_channel_offer_v1:packet_hash(Offer) == blockchain_state_channel_offer_v1:packet_hash(ExpectedOffer) andalso
                 blockchain_state_channel_offer_v1:payload_size(Offer) == blockchain_state_channel_offer_v1:payload_size(ExpectedOffer) andalso
                 blockchain_state_channel_offer_v1:routing(Offer) == blockchain_state_channel_offer_v1:routing(ExpectedOffer) andalso
                 blockchain_state_channel_offer_v1:region(Offer) == blockchain_state_channel_offer_v1:region(ExpectedOffer) andalso
                 blockchain_state_channel_offer_v1:hotspot(Offer) == blockchain_state_channel_offer_v1:hotspot(ExpectedOffer) of
                false -> {error, packet_offer_mismatch};
                true -> true
            end
    end.

-spec encode(packet()) -> binary().
encode(#blockchain_state_channel_packet_v1_pb{}=Packet) ->
    blockchain_state_channel_v1_pb:encode_msg(Packet).

-spec decode(binary()) -> packet().
decode(BinaryPacket) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryPacket, blockchain_state_channel_packet_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Packet = #blockchain_state_channel_packet_v1_pb{
        packet= blockchain_helium_packet_v1:new(),
        hotspot = <<"hotspot">>
    },
    ?assertEqual(Packet, new(blockchain_helium_packet_v1:new(), <<"hotspot">>, 'US915')).

hotspot_test() ->
    Packet = new(blockchain_helium_packet_v1:new(), <<"hotspot">>, 'US915'),
    ?assertEqual(<<"hotspot">>, hotspot(Packet)).

packet_test() ->
    Packet = new(blockchain_helium_packet_v1:new(), <<"hotspot">>, 'US915'),
    ?assertEqual(blockchain_helium_packet_v1:new(), packet(Packet)).

signature_test() ->
    Packet = new(blockchain_helium_packet_v1:new(), <<"hotspot">>, 'US915'),
    ?assertEqual(<<>>, signature(Packet)).

sign_test() ->
    #{secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Packet = new(blockchain_helium_packet_v1:new(), <<"hotspot">>, 'US915'),
    ?assertNotEqual(<<>>, signature(sign(Packet, SigFun))).

validate_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    P = blockchain_helium_packet_v1:new({devaddr, 1234}, <<"hello">>),
    Packet0 = new(P, PubKeyBin, 'US915'),
    Packet1 = sign(Packet0, SigFun),
    Offer = blockchain_state_channel_offer_v1:from_packet(P, PubKeyBin, 'US915'),
    ?assertEqual(true, validate(Packet1, Offer)).

encode_decode_test() ->
    Packet = new(blockchain_helium_packet_v1:new(), <<"hotspot">>, 'US915'),
    ?assertEqual(Packet, decode(encode(Packet))).

-endif.
