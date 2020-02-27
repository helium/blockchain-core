%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Helium Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_helium_packet_v1).

-export([
         new/0, new/2, %% only for testing, where we set only the oui and payload
         new/8,
         oui/1,
         type/1,
         payload/1,
         timestamp/1,
         signal_strength/1,
         frequency/1,
         datarate/1,
         snr/1
        ]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/packet_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type packet() :: #packet_pb{}.
-export_type([packet/0]).

-spec new() -> packet().
new() ->
    #packet_pb{}.

-spec new(OUI :: non_neg_integer(), Payload :: binary()) -> packet().
new(OUI, Payload) ->
    #packet_pb{oui=OUI, payload=Payload}.

-spec new(OUI :: non_neg_integer(),
          Type :: longfi | lorawan,
          Payload :: binary(),
          TimeStamp :: non_neg_integer(),
          SignalStrength :: float(),
          Frequency :: float(),
          DataRate :: string(),
          SNR :: float()) -> packet().
new(OUI, Type, Payload, TimeStamp, SignalStrength, Frequency, DataRate, SNR) ->
    #packet_pb{
       oui=OUI,
       type=Type,
       payload=Payload,
       timestamp=TimeStamp,
       signal_strength=SignalStrength,
       frequency=Frequency,
       datarate=DataRate,
       snr=SNR}.

-spec oui(packet()) -> non_neg_integer().
oui(#packet_pb{oui=OUI}) ->
    OUI.

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

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Packet = #packet_pb{oui=1, payload= <<"hello">>},
    ?assertEqual(Packet, new(1, <<"hello">>)).

-endif.
