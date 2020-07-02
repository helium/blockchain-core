%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Helium Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_helium_packet_v1).

-export([
         new/0, new/2, %% only for testing, where we set only the oui and payload
         new/8,
         new_downlink/5,
         routing_info/1,
         type/1,
         payload/1,
         timestamp/1,
         signal_strength/1,
         frequency/1,
         datarate/1,
         snr/1,
         packet_hash/1,

         encode/1, decode/1,
         make_routing_info/1
        ]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/packet_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type routing_information() :: #routing_information_pb{}.
-type routing_info() :: {devaddr, DevAddr::non_neg_integer()} | {eui, DevEUI::non_neg_integer(), AppEUI::non_neg_integer()}.
-type packet() :: #packet_pb{}.
-export_type([packet/0, routing_info/0, routing_information/0]).

-spec new() -> packet().
new() ->
    #packet_pb{}.

new_downlink(Payload, TransmitTime, TransmitPower, Frequency, DataRate) ->
    #packet_pb{
       type=lorawan,
       payload=Payload,
       timestamp=TransmitTime,
       signal_strength=TransmitPower,
       frequency=Frequency,
       datarate=DataRate}.

-spec new(RoutingInfo :: routing_info(), Payload :: binary()) -> packet().
new(RoutingInfo, Payload) ->
    #packet_pb{routing=make_routing_info(RoutingInfo), payload=Payload}.

-spec new(RoutingInfo :: routing_info(),
          Type :: longfi | lorawan,
          Payload :: binary(),
          TimeStamp :: non_neg_integer(),
          SignalStrength :: float(),
          Frequency :: float(),
          DataRate :: string(),
          SNR :: float()) -> packet().
new(RoutingInfo, Type, Payload, TimeStamp, SignalStrength, Frequency, DataRate, SNR) ->
    #packet_pb{
       routing=make_routing_info(RoutingInfo),
       type=Type,
       payload=Payload,
       timestamp=TimeStamp,
       signal_strength=SignalStrength,
       frequency=Frequency,
       datarate=DataRate,
       snr=SNR}.

-spec routing_info(packet()) -> routing_info().
routing_info(#packet_pb{routing=RoutingInfo}) ->
    case RoutingInfo of
        #routing_information_pb{data={devaddr, DevAddr}} ->
            {devaddr, DevAddr};
        #routing_information_pb{data={eui, #eui_pb{deveui=DevEUI, appeui=AppEUI}}} ->
            {eui, DevEUI, AppEUI}
    end.

-spec type(packet()) -> lorawan | longfi.
type(#packet_pb{type=Type}) ->
    Type.

-spec payload(packet()) -> binary().
payload(#packet_pb{payload=Payload}) ->
    Payload.

-spec timestamp(packet()) -> non_neg_integer().
timestamp(#packet_pb{timestamp=TS}) ->
    TS.

-spec signal_strength(packet()) -> float().
signal_strength(#packet_pb{signal_strength=SS}) ->
    SS.

-spec frequency(packet()) -> float().
frequency(#packet_pb{frequency=Freq}) ->
    Freq.

-spec datarate(packet()) -> string().
datarate(#packet_pb{datarate=DR}) ->
    DR.

-spec snr(packet()) -> float().
snr(#packet_pb{snr=SNR}) ->
    SNR.

-spec packet_hash(packet()) -> binary().
packet_hash(Packet) ->
    crypto:hash(sha256, term_to_binary(Packet)).

-spec encode(packet()) -> binary().
encode(#packet_pb{}=Packet) ->
    packet_pb:encode_msg(Packet).

-spec decode(binary()) -> packet().
decode(BinaryPacket) ->
    packet_pb:decode_msg(BinaryPacket, packet_pb).

-spec make_routing_info(routing_info()) -> routing_information().
make_routing_info({devaddr, DevAddr}) ->
    #routing_information_pb{data={devaddr, DevAddr}};
make_routing_info({eui, DevEUI, AppEUI}) ->
    #routing_information_pb{data={eui, #eui_pb{deveui=DevEUI, appeui=AppEUI}}}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Packet = #packet_pb{routing=#routing_information_pb{data={devaddr, 16#deadbeef}}, payload= <<"payload">>},
    ?assertEqual(Packet, new({devaddr, 16#deadbeef}, <<"payload">>)).

oui_test() ->
    Packet = new({devaddr, 16#deadbeef}, <<"payload">>),
    ?assertEqual({devaddr, 16#deadbeef}, routing_info(Packet)),
    Packet1 = new({eui, 16#deadbeef, 16#DEADC0DE}, <<"payload">>),
    ?assertEqual({eui, 16#deadbeef, 16#deadc0de}, routing_info(Packet1)).

payload_test() ->
    Packet = new({devaddr, 16#deadbeef}, <<"payload">>),
    ?assertEqual(<<"payload">>, payload(Packet)).

type_test() ->
    Packet = new({devaddr, 16#deadbeef}, lorawan, <<"payload">>, 1000, 0.0, 0.0, "dr", 0.0),
    ?assertEqual(lorawan, type(Packet)).

timestamp_test() ->
    Packet = new({devaddr, 16#deadbeef}, lorawan, <<"payload">>, 1000, 0.0, 0.0, "dr", 0.0),
    ?assertEqual(1000, timestamp(Packet)).

signal_strength_test() ->
    Packet = new({devaddr, 16#deadbeef}, lorawan, <<"payload">>, 1000, 0.0, 0.0, "dr", 0.0),
    ?assertEqual(0.0, signal_strength(Packet)).

frequency_test() ->
    Packet = new({devaddr, 16#deadbeef}, lorawan, <<"payload">>, 1000, 0.0, 0.0, "dr", 0.0),
    ?assertEqual(0.0, frequency(Packet)).

datarate_test() ->
    Packet = new({devaddr, 16#deadbeef}, lorawan, <<"payload">>, 1000, 0.0, 0.0, "dr", 0.0),
    ?assertEqual("dr", datarate(Packet)).

snr_test() ->
    Packet = new({devaddr, 16#deadbeef}, lorawan, <<"payload">>, 1000, 0.0, 0.0, "dr", 0.0),
    ?assertEqual(0.0, snr(Packet)).

encode_decode_test() ->
    Packet = new({devaddr, 16#deadbeef}, lorawan, <<"payload">>, 1000, 0.0, 0.0, "dr", 0.0),
    ?assertEqual(Packet, decode(encode(Packet))).

-endif.
