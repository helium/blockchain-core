%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_packet_v1).

-export([
    new/1,
    packet/1, signature/1,
    sign/2, validate/2,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type packet() :: #helium_state_channel_packet_v1_pb{}.
-export_type([packet/0]).

-spec new(binary()) -> packet().
new(Packet) -> 
    #helium_state_channel_packet_v1_pb{
        packet=Packet
    }.

-spec packet(packet()) -> binary().
packet(#helium_state_channel_packet_v1_pb{packet=Packet}) ->
    Packet.

-spec signature(packet()) -> binary().
signature(#helium_state_channel_packet_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(packet(), function()) -> packet().
sign(Packet, SigFun) ->
    EncodedReq = ?MODULE:encode(Packet#helium_state_channel_packet_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedReq),
    Packet#helium_state_channel_packet_v1_pb{signature=Signature}.

-spec validate(packet(), libp2p_crypto:pubkey_bin()) -> true | {error, any()}.
validate(Packet, PubKeyBin) ->
    BasePacket = Packet#helium_state_channel_packet_v1_pb{signature = <<>>},
    EncodedPacket = ?MODULE:encode(BasePacket),
    Signature = ?MODULE:signature(Packet),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    case libp2p_crypto:verify(EncodedPacket, Signature, PubKey) of
        false -> {error, bad_signature};
        true -> true
    end.

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
        packet= <<"data">>
    },
    ?assertEqual(Packet, new(<<"data">>)).

packet_test() ->
    Packet = new(<<"data">>),
    ?assertEqual(<<"data">>, packet(Packet)).

signature_test() ->
    Packet = new(<<"data">>),
    ?assertEqual(<<>>, signature(Packet)).

sign_test() ->
    #{secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Packet = new(<<"data">>),
    ?assertNotEqual(<<>>, signature(sign(Packet, SigFun))).

validate_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Packet0 = new(<<"data">>),
    Packet1 = sign(Packet0, SigFun),
    ?assertEqual(true, validate(Packet1, PubKeyBin)).

encode_decode_test() ->
    Packet = new(<<"data">>),
    ?assertEqual(Packet, decode(encode(Packet))).

-endif.