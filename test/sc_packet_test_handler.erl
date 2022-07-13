-module(sc_packet_test_handler).

-export([handle_packet/3, handle_offer/2]).

handle_packet(Packet, _PacketTime, HandlerPid) ->
    F = application:get_env(blockchain, sc_packet_handler_packet_fun, fun(_Packet, _Handler) -> ok end),
    lager:info("Packet: HandlerPid: ~p", [HandlerPid]),
    F(Packet, HandlerPid).

handle_offer(Offer, HandlerPid) ->
    F = application:get_env(blockchain, sc_packet_handler_offer_fun, fun(_Packet, _Handler) -> ok end),
    lager:info("Offer: ~p, HandlerPid: ~p", [Offer, HandlerPid]),
    F(Offer, HandlerPid).
