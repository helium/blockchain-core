-module(sc_packet_test_handler).

-export([handle_packet/3, handle_offer/2]).

handle_packet(Packet, _PacketTime, HandlerPid) ->
    lager:info("Packet: ~p, HandlerPid: ~p", [Packet, HandlerPid]),
    ok.

handle_offer(Offer, HandlerPid) ->
    lager:info("Offer: ~p, HandlerPid: ~p", [Offer, HandlerPid]),
    ok.
