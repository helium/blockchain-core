%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Helium Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_helium_packet_v1).

-export([new/0, new/2, new/8,
         new_downlink/3, new_downlink/4,
         make_routing_info/1,
         type/1,
         payload/1,
         signal_strength/1,
         snr/1,
         rx1_window/1,
         rx2_window/1,
         routing/1,
         window/1, window/3,
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
-export_type([packet/0, routing_info/0, routing_information/0]).

-spec new() -> packet().
new() ->
    #packet_pb{}.

-spec new(RoutingInfo :: routing_info(), Payload :: binary()) -> packet().
new(RoutingInfo, Payload) ->
    #packet_pb{routing=?MODULE:make_routing_info(RoutingInfo), payload=Payload}.

-spec new(Type :: longfi | lorawan,
          Payload :: binary(),
          SignalStrength :: float(),
          SNR :: float(),
          TimeStamp :: non_neg_integer(),
          Frequency :: float(),
          DataRate :: string(),
          RoutingInfo :: routing_info()) -> packet().
new(Type, Payload, SignalStrength, SNR, TimeStamp, Frequency, DataRate, RoutingInfo) ->
    #packet_pb{
       type=Type,
       payload=Payload,
       signal_strength=SignalStrength,
       snr=SNR,
       rx1_window=?MODULE:window(TimeStamp, Frequency, DataRate),
       routing=?MODULE:make_routing_info(RoutingInfo)}.

-spec new_downlink(Payload :: binary(), SignalStrength :: integer(), Rx1 :: window()) -> packet().
new_downlink(Payload, SignalStrength, Rx1) ->
    ?MODULE:new_downlink(Payload, SignalStrength, Rx1, undefined).

-spec new_downlink(Payload :: binary(), SignalStrength :: integer(),
                  Rx1 :: window(), Rx1 :: window() | undefined) -> packet().
new_downlink(Payload, SignalStrength, Rx1, Rx2) ->
    #packet_pb{
       type=lorawan,
       payload=Payload,
       signal_strength=SignalStrength,
       rx1_window=Rx1,
       rx2_window=Rx2}.

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

-spec signal_strength(packet()) -> float().
signal_strength(#packet_pb{signal_strength=SS}) ->
    SS.

-spec snr(packet()) -> float().
snr(#packet_pb{snr=SNR}) ->
    SNR.

-spec rx1_window(packet()) ->  window() | undefined.
rx1_window(#packet_pb{rx1_window=Window}) ->
    Window.

-spec rx2_window(packet()) -> window() | undefined.
rx2_window(#packet_pb{rx2_window=Window}) ->
    Window.

-spec routing(packet()) -> routing_info().
routing(#packet_pb{routing=RoutingInfo}) ->
    case RoutingInfo of
        #routing_information_pb{data={devaddr, DevAddr}} ->
            {devaddr, DevAddr};
        #routing_information_pb{data={eui, #eui_pb{deveui=DevEUI, appeui=AppEUI}}} ->
            {eui, DevEUI, AppEUI}
    end.

-spec window(window()) -> {non_neg_integer(), float(), string()}.
window(#window_pb{timestamp=TS, frequency=Freq, datarate=DataRate}) ->
    {TS, Freq, DataRate}.

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
    Window = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    RoutingInfo = #routing_information_pb{data={devaddr, 16#deadbeef}},
    Packet = #packet_pb{type=lorawan, payload= <<"payload">>, signal_strength=0.1, snr=0.2,
                        rx1_window=Window, routing=RoutingInfo},
    ?assertEqual(Packet, new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef})).

new_downlink_test() ->
    Window1 = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    Packet1 = #packet_pb{type=lorawan, payload= <<"payload">>, signal_strength=0.1, rx1_window=Window1},
    ?assertEqual(Packet1, new_downlink(<<"payload">>, 0.1, Window1)),
    Window2 = #window_pb{timestamp=22, frequency=2.0, datarate="DR2"},
    Packet2 = #packet_pb{type=lorawan, payload= <<"payload">>, signal_strength=0.1, rx1_window=Window1, rx2_window=Window2},
    ?assertEqual(Packet2, new_downlink(<<"payload">>, 0.1, Window1, Window2)).

make_routing_info_test() ->
    ?assertEqual(#routing_information_pb{data={devaddr, 16#deadbeef}}, make_routing_info({devaddr, 16#deadbeef})),
    ?assertEqual(#routing_information_pb{data={eui, #eui_pb{deveui=16#deadbeef, appeui=16#deadbeef}}}, make_routing_info({eui, 16#deadbeef, 16#deadbeef})).

type_test() ->
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(lorawan, type(Packet)).

payload_test() ->
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(<<"payload">>, payload(Packet)).

signal_strength_test() ->
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(0.1, signal_strength(Packet)).

snr_test() ->
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(0.2, snr(Packet)).

rx1_window_test() ->
    Window = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(Window, rx1_window(Packet)).

rx2_window_test() ->
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(undefined, rx2_window(Packet)).

routing_test() ->
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual({devaddr, 16#deadbeef}, routing(Packet)).

window_test() ->
    Window = #window_pb{timestamp=12, frequency=1.0, datarate="DR"},
    ?assertEqual({12, 1.0, "DR"}, window(Window)),
    ?assertEqual(Window, window(12, 1.0, "DR")).

packet_hash_test() ->
    Packet = new(lorawan, <<"payload">>, 0.1, 0.2, 12, 1.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(crypto:hash(sha256, <<"payload">>), packet_hash(Packet)).

encode_decode_test() ->
    Packet = new(lorawan, <<"payload">>, 0.0, 0.0, 12, 0.0, "DR", {devaddr, 16#deadbeef}),
    ?assertEqual(Packet, decode(encode(Packet))).

-endif.
