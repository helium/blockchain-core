%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_packet_v1).

-export([
    new/2,
    packet/1, hotspot/1, signature/1,
    sign/2, validate/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").
-include_lib("helium_proto/include/packet_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type helium_packet() :: #packet_pb{}.
-type packet() :: #blockchain_state_channel_packet_v1_pb{}.
-export_type([packet/0]).

-spec new(helium_packet(), libp2p_crypto:pubkey_bin()) -> packet().
new(Packet, Hotspot) -> 
    #blockchain_state_channel_packet_v1_pb{
        packet=Packet,
        hotspot=Hotspot
    }.

-spec packet(packet()) -> helium_packet().
packet(#blockchain_state_channel_packet_v1_pb{packet=Packet}) ->
    Packet.

-spec hotspot(packet()) -> libp2p_crypto:pubkey_bin().
hotspot(#blockchain_state_channel_packet_v1_pb{hotspot=Hotspot}) ->
    Hotspot.

-spec signature(packet()) -> binary().
signature(#blockchain_state_channel_packet_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(packet(), function()) -> packet().
sign(Packet, SigFun) ->
    EncodedReq = ?MODULE:encode(Packet#blockchain_state_channel_packet_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedReq),
    Packet#blockchain_state_channel_packet_v1_pb{signature=Signature}.

-spec validate(packet()) -> true | {error, any()}.
validate(Packet) ->
    BasePacket = Packet#blockchain_state_channel_packet_v1_pb{signature = <<>>},
    EncodedPacket = ?MODULE:encode(BasePacket),
    Signature = ?MODULE:signature(Packet),
    PubKeyBin = ?MODULE:hotspot(Packet),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    case libp2p_crypto:verify(EncodedPacket, Signature, PubKey) of
        false -> {error, bad_signature};
        true -> true
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
        packet= #packet_pb{},
        hotspot = <<"hotspot">>
    },
    ?assertEqual(Packet, new(#packet_pb{}, <<"hotspot">>)).

hotspot_test() ->
    Packet = new(#packet_pb{}, <<"hotspot">>),
    ?assertEqual(<<"hotspot">>, hotspot(Packet)).

packet_test() ->
    Packet = new(#packet_pb{}, <<"hotspot">>),
    ?assertEqual(#packet_pb{}, packet(Packet)).

signature_test() ->
    Packet = new(#packet_pb{}, <<"hotspot">>),
    ?assertEqual(<<>>, signature(Packet)).

sign_test() ->
    #{secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Packet = new(#packet_pb{}, <<"hotspot">>),
    ?assertNotEqual(<<>>, signature(sign(Packet, SigFun))).

validate_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Packet0 = new(#packet_pb{}, PubKeyBin),
    Packet1 = sign(Packet0, SigFun),
    ?assertEqual(true, validate(Packet1)).

encode_decode_test() ->
    Packet = new(#packet_pb{}, <<"hotspot">>),
    ?assertEqual(Packet, decode(encode(Packet))).

-endif.
