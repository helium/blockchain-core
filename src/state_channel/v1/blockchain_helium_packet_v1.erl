%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Helium Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_helium_packet_v1).

-export([new/0, new/2, new/8,
         new_downlink/5, new_downlink/6,
         make_routing_info/1,
         type/1,
         payload/1,
         timestamp/1,
         signal_strength/1,
         frequency/1,
         datarate/1,
         snr/1,
         routing_info/1,
         rx2_window/1,
         window/3,
         packet_hash/1,
         encode/1, decode/1
        ]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/packet_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type routing_information() :: #routing_information_pb{}.
-type routing_info() :: {devaddr, DevAddr::non_neg_integer()} | {eui, DevEUI::non_neg_integer(), AppEUI::non_neg_integer()}.
-type window() :: #window_pb{}.
-type packet() :: #packet_pb{}.
-export_type([window/0, packet/0, routing_info/0, routing_information/0]).

-spec new() -> packet().
new() ->
    #packet_pb{}.

-spec new(RoutingInfo :: routing_info(), Payload :: binary()) -> packet().
new(RoutingInfo, Payload) ->
    #packet_pb{routing=?MODULE:make_routing_info(RoutingInfo), payload=Payload}.

-spec new(Type :: longfi | lorawan,
          Payload :: binary(),
          Timestamp :: non_neg_integer(),
          SignalStrength :: float(),
          Frequency :: float(),
          DataRate :: string(),
          SNR :: float(),
          RoutingInfo :: routing_info()) -> packet().
new(Type, Payload, Timestamp, SignalStrength, Frequency, DataRate, SNR, RoutingInfo) ->
    #packet_pb{
       type=Type,
       payload=Payload,
       timestamp=Timestamp,
       signal_strength=SignalStrength,
       frequency=Frequency,
       datarate=DataRate,
       snr=SNR,
       routing=?MODULE:make_routing_info(RoutingInfo)}.

-spec new_downlink(Payload :: binary(), SignalStrength :: integer(), Timestamp :: non_neg_integer(),
                   Frequency :: float(), DataRate :: string()) -> packet().
new_downlink(Payload, SignalStrength, Timestamp, Frequency, DataRate) ->
    ?MODULE:new_downlink(Payload, SignalStrength, Timestamp, Frequency, DataRate, undefined).

-spec new_downlink(Payload :: binary(), SignalStrength :: integer(), Timestamp :: non_neg_integer(),
                   Frequency :: float(), DataRate :: string(), Rx2 :: window() | undefined) -> packet().
new_downlink(Payload, SignalStrength, Timestamp, Frequency, DataRate, Rx2) ->
    #packet_pb{
       type=lorawan,
       payload=Payload,
       timestamp=Timestamp,
       signal_strength=SignalStrength,
       frequency=Frequency,
       datarate=DataRate,
       rx2_window=Rx2
    }.

-spec make_routing_info(routing_info()) -> routing_information().
make_routing_info({devaddr, DevAddr}) ->
    #routing_information_pb{data={devaddr, DevAddr}};
make_routing_info({eui, DevEUI, AppEUI}) ->
    #routing_information_pb{data={eui, #eui_pb{deveui=DevEUI, appeui=AppEUI}}}.

-spec type(packet()) -> lorawan | longfi.
type(#packet_pb{type=Type}) ->
    Type.

-spec payload(packet()) -> binary().
payload(#packet_pb{payload=Payload}) ->
    Payload.

-spec timestamp(packet() | window()) -> non_neg_integer().
timestamp(#packet_pb{timestamp=TS})  ->
    TS;
timestamp(#window_pb{timestamp=TS}) ->
    TS.

-spec signal_strength(packet()) -> float().
signal_strength(#packet_pb{signal_strength=SS}) ->
    SS.

-spec frequency(packet() | window()) -> float().
frequency(#packet_pb{frequency=Freq})  ->
    Freq;
frequency(#window_pb{frequency=Freq}) ->
    Freq.

-spec datarate(packet() | window()) -> string().
datarate(#packet_pb{datarate=DataRate})  ->
    DataRate;
datarate(#window_pb{datarate=DataRate}) ->
    DataRate.

-spec snr(packet()) -> float().
snr(#packet_pb{snr=SNR}) ->
    SNR.

-spec routing_info(packet()) -> routing_info().
routing_info(#packet_pb{routing=RoutingInfo}) ->
    case RoutingInfo of
        #routing_information_pb{data={devaddr, DevAddr}} ->
            {devaddr, DevAddr};
        #routing_information_pb{data={eui, #eui_pb{deveui=DevEUI, appeui=AppEUI}}} ->
            {eui, DevEUI, AppEUI}
    end.

-spec rx2_window(packet()) -> window() | undefined.
rx2_window(#packet_pb{rx2_window=Window}) ->
    Window.

-spec window(non_neg_integer(), float(), string()) -> window().
window(TS, Freq, DataRate) ->
    #window_pb{timestamp=TS, frequency=Freq, datarate=DataRate}.

-spec packet_hash(packet()) -> binary().
packet_hash(Packet) ->
    crypto:hash(sha256, ?MODULE:payload(Packet)).

-spec encode(packet()) -> binary().
encode(#packet_pb{}=Packet) ->
    packet_pb:encode_msg(Packet).

-spec decode(binary()) -> packet().
decode(BinaryPacket) ->
    packet_pb:decode_msg(BinaryPacket, packet_pb).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new0_test() ->
    ?assertEqual(#packet_pb{}, new()).

new2_test() ->
    RoutingInfo = #routing_information_pb{data={devaddr, 16#deadbeef}},
    Packet = #packet_pb{payload= <<"payload">>, routing=RoutingInfo},
    ?assertEqual(Packet, new({devaddr, 16#deadbeef}, <<"payload">>)).

new8_test() ->
    RoutingInfo = #routing_information_pb{data={devaddr, 16#deadbeef}},
    Packet = #packet_pb{type=lorawan,
                        payload= <<"payload">>,
                        timestamp=12,
                        signal_strength=0.1,
                        frequency=1.0,
                        datarate="DR",
                        snr=0.2,
                        routing=RoutingInfo},
    ?assertEqual(Packet, new(lorawan, <<"payload">>, 12, 0.1, 1.0, "DR", 0.2, {devaddr, 16#deadbeef})).

new_downlink_test() ->
    Packet1 = #packet_pb{type=lorawan,
                         payload= <<"payload">>,
                         timestamp=100,
                         signal_strength=0.1,
                         frequency=0.2,
                         datarate="DR"},
    ?assertEqual(Packet1, new_downlink(<<"payload">>, 0.1, 100, 0.2, "DR")),
    Window2 = #window_pb{timestamp=22, frequency=2.0, datarate="DR2"},
    Packet2 = Packet1#packet_pb{rx2_window=Window2},
    ?assertEqual(Packet2, new_downlink(<<"payload">>, 0.1, 100, 0.2, "DR", Window2)).

make_routing_info_test() ->
    ?assertEqual(#routing_information_pb{data={devaddr, 16#deadbeef}}, make_routing_info({devaddr, 16#deadbeef})),
    ?assertEqual(#routing_information_pb{data={eui, #eui_pb{deveui=16#deadbeef, appeui=16#deadbeef}}}, make_routing_info({eui, 16#deadbeef, 16#deadbeef})).

type_test() ->
    Packet = new(lorawan, <<"payload">>, 12, 0.1, 1.0, "DR", 0.2, {devaddr, 16#deadbeef}),
    ?assertEqual(lorawan, type(Packet)).

payload_test() ->
    Packet = new(lorawan, <<"payload">>, 12, 0.1, 1.0, "DR", 0.2, {devaddr, 16#deadbeef}),
    ?assertEqual(<<"payload">>, payload(Packet)).

timestamp_test() ->
    Window = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    Packet = new_downlink(<<"payload">>, 0.1, 12, 0.2, "DR"),
    ?assertEqual(12, timestamp(Window)),
    ?assertEqual(12, timestamp(Packet)).

signal_strength_test() ->
    Packet = new(lorawan, <<"payload">>, 12, 0.1, 1.0, "DR", 0.2, {devaddr, 16#deadbeef}),
    ?assertEqual(0.1, signal_strength(Packet)).

frequency_test() ->
    Window = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    Packet = new_downlink(<<"payload">>, 0.1, 12, 1.0, "DR"),
    ?assertEqual(1.0, frequency(Window)),
    ?assertEqual(1.0, frequency(Packet)).

datarate_test() ->
    Window = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    Packet = new_downlink(<<"payload">>, 0.1, 12, 1.0, "DR"),
    ?assertEqual("DR", datarate(Window)),
    ?assertEqual("DR", datarate(Packet)).

snr_test() ->
    Packet = new(lorawan, <<"payload">>, 12, 0.1, 1.0, "DR", 0.2, {devaddr, 16#deadbeef}),
    ?assertEqual(0.2, snr(Packet)).

rx2_window_test() ->
    Packet = new(lorawan, <<"payload">>, 12, 0.1, 1.0, "DR", 0.2, {devaddr, 16#deadbeef}),
    ?assertEqual(undefined, rx2_window(Packet)).

routing_info_test() ->
    Packet = new(lorawan, <<"payload">>, 12, 0.1, 1.0, "DR", 0.2, {devaddr, 16#deadbeef}),
    ?assertEqual({devaddr, 16#deadbeef}, routing_info(Packet)).

window_test() ->
    Window = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    ?assertEqual(Window, window(12, 1.0, "DR")).

packet_hash_test() ->
    Packet = new(lorawan, <<"payload">>, 1500, 2.0, 1.0, "DR", 1.0, {devaddr, 16#deadbeef}),
    ?assertEqual(crypto:hash(sha256, <<"payload">>), packet_hash(Packet)).

encode_decode_test() ->
    Packet = new(lorawan, <<"payload">>, 1500, 2.0, 1.0, "DR", 1.0, {devaddr, 16#deadbeef}),
    ?assertEqual(Packet, decode(encode(Packet))).

-endif.
