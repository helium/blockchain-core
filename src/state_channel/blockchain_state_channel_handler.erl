%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_handler).

-behavior(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2,
    dial/3,
    send_packet/2,
    send_offer/2,
    send_purchase/5,
    send_response/2,
    send_banner/2,
    send_rejection/2
]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/3,
    handle_data/3,
    handle_info/3
]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-record(state, {
          ledger :: undefined | blockchain_ledger_v1:ledger(),
          pending_packet_offers = #{} :: #{binary() => blockchain_state_channel_packet_offer_v1:offer()},
          offer_queue = [] :: [blockchain_state_channel_packet_offer_v1:offer()],
          handler_mod :: atom(),
          pending_offer_limit = undefined :: undefined | pos_integer()
         }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

dial(Swarm, Peer, Opts) ->
    libp2p_swarm:dial_framed_stream(Swarm,
                                    Peer,
                                    ?STATE_CHANNEL_PROTOCOL_V1,
                                    ?MODULE,
                                    Opts).


-spec send_packet(pid(), blockchain_state_channel_packet_v1:packet()) -> ok.
send_packet(Pid, Packet) ->
    Pid ! {send_packet, Packet},
    ok.

-spec send_offer(pid(), blockchain_state_channel_packet_offer_v1:offer()) -> ok.
send_offer(Pid, Offer) ->
    lager:info("sending offer: ~p, pid: ~p", [Offer, Pid]),
    Pid ! {send_offer, Offer},
    ok.

%-spec send_purchase(pid(), blockchain_state_channel_purchase_v1:purchase()) -> ok.
send_purchase(Pid, NewPurchaseSC, Hotspot, PacketHash, Region) ->
    %lager:info("sending purchase: ~p, pid: ~p", [Purchase, Pid]),
    Pid ! {send_purchase, NewPurchaseSC, Hotspot, PacketHash, Region},
    ok.

-spec send_banner(pid(), blockchain_state_channel_banner_v1:banner()) -> ok.
send_banner(Pid, Banner) ->
    lager:info("sending banner: ~p, pid: ~p", [Banner, Pid]),
    Pid ! {send_banner, Banner},
    ok.

-spec send_rejection(pid(), blockchain_state_channel_rejection_v1:rejection()) -> ok.
send_rejection(Pid, Rejection) ->
    lager:info("sending rejection: ~p, pid: ~p", [Rejection, Pid]),
    Pid ! {send_rejection, Rejection},
    ok.

-spec send_response(pid(), blockchain_state_channel_response_v1:response()) -> ok.
send_response(Pid, Resp) ->
    Pid ! {send_response, Resp},
    ok.

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _) ->
    {ok, #state{}};
init(server, _Conn, [_Path, Blockchain]) ->
    Ledger = blockchain:ledger(Blockchain),
    HandlerMod = application:get_env(blockchain, sc_packet_handler, undefined),
    OfferLimit = application:get_env(blockchain, sc_pending_offer_limit, 5),
    case blockchain:config(?sc_version, Ledger) of
        {ok, N} when N > 1 ->
            case blockchain_state_channels_server:active_sc() of
                undefined ->
                    %% Send empty banner
                    SCBanner = blockchain_state_channel_banner_v1:new(),
                    lager:info("sc_handler, empty banner: ~p", [SCBanner]),
                    {ok, #state{ledger=Ledger, handler_mod=HandlerMod, pending_offer_limit=OfferLimit},
                     blockchain_state_channel_message_v1:encode(SCBanner)};
                ActiveSC ->
                    %lager:info("sc_handler, active_sc: ~p", [ActiveSC]),
                    SCBanner = blockchain_state_channel_banner_v1:new(ActiveSC),
                    %lager:info("sc_handler, banner: ~p", [SCBanner]),
                    {ok, #state{ledger=Ledger, handler_mod=HandlerMod, pending_offer_limit=OfferLimit},
                     blockchain_state_channel_message_v1:encode(SCBanner)}
            end;
        _ -> {ok, #state{handler_mod=HandlerMod, pending_offer_limit=OfferLimit}}
    end.

handle_data(client, Data, State) ->
    case blockchain_state_channel_message_v1:decode(Data) of
        {banner, Banner} ->
            lager:info("sc_handler client got banner: ~p", [Banner]),
            blockchain_state_channels_client:banner(Banner, self());
        {purchase, Purchase} ->
            lager:info("sc_handler client got purchase: ~p", [Purchase]),
            blockchain_state_channels_client:purchase(Purchase, self());
        {reject, Rejection} ->
            lager:info("sc_handler client got rejection: ~p", [Rejection]),
            blockchain_state_channels_client:reject(Rejection, self());
        {response, Resp} ->
            lager:debug("sc_handler client got response: ~p", [Resp]),
            blockchain_state_channels_client:response(Resp)
    end,
    {noreply, State};
handle_data(server, Data, State=#state{pending_packet_offers=PendingOffers, pending_offer_limit=PendingOfferLimit}) ->
    PendingOfferCount = maps:size(PendingOffers),
    case blockchain_state_channel_message_v1:decode(Data) of
        {offer, Offer} when PendingOfferCount < PendingOfferLimit ->
            handle_offer(Offer, State);
        {offer, Offer} ->
            %% queue the offer
            {noreply, State#state{offer_queue=State#state.offer_queue ++ [Offer]}};
        {packet, Packet} ->
            PacketHash = blockchain_helium_packet_v1:packet_hash(blockchain_state_channel_packet_v1:packet(Packet)),
            case maps:get(PacketHash, PendingOffers, undefined) of
                undefined ->
                    lager:info("sc_handler server got packet: ~p", [Packet]),
                    blockchain_state_channels_server:packet(Packet, State#state.handler_mod, self()),
                    {noreply, State};
                PendingOffer ->
                    case blockchain_state_channel_packet_v1:validate(Packet, PendingOffer) of
                        {error, packet_offer_mismatch} ->
                            %% might as well try it, it's free
                            blockchain_state_channels_server:packet(Packet, State#state.handler_mod, self()),
                            lager:warning("packet failed to validate ~p against offer ~p", [Packet, PendingOffer]),
                            {stop, normal};
                        {error, Reason} ->
                            lager:warning("packet failed to validate ~p reason ~p", [Packet, Reason]),
                            {stop, normal};
                        true ->
                            lager:info("sc_handler server got packet: ~p", [Packet]),
                            blockchain_state_channels_server:packet(Packet, State#state.handler_mod, self()),
                            handle_next_offer(State#state{pending_packet_offers=maps:remove(PacketHash, PendingOffers)})
                    end
            end
    end.

handle_info(client, {send_offer, Offer}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Offer),
    {noreply, State, Data};
handle_info(client, {send_packet, Packet}, State) ->
    lager:debug("sc_handler client sending packet: ~p", [Packet]),
    Data = blockchain_state_channel_message_v1:encode(Packet),
    {noreply, State, Data};
handle_info(server, {send_banner, Banner}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Banner),
    {noreply, State, Data};
handle_info(server, {send_rejection, Rejection}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Rejection),
    {noreply, State, Data};
handle_info(server, {send_purchase, SignedPurchaseSC, Hotspot, PacketHash, Region}, State) ->
    %% NOTE: We're constructing the purchase with the hotspot obtained from offer here
    PurchaseMsg = blockchain_state_channel_purchase_v1:new(SignedPurchaseSC, Hotspot, PacketHash, Region),
    Data = blockchain_state_channel_message_v1:encode(PurchaseMsg),
    {noreply, State, Data};
handle_info(server, {send_response, Resp}, State) ->
    lager:debug("sc_handler server sending resp: ~p", [Resp]),
    Data = blockchain_state_channel_message_v1:encode(Resp),
    {noreply, State, Data};
handle_info(_Type, _Msg, State) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, State}.

handle_next_offer(State=#state{offer_queue=[]}) ->
    {noreply, State};
handle_next_offer(State=#state{offer_queue=[NextOffer|Offers], pending_packet_offers=PendingOffers, pending_offer_limit=Limit}) ->
    case maps:size(PendingOffers) < Limit of
        true ->
            handle_offer(NextOffer, State#state{offer_queue=Offers});
        false ->
            {noreply, State}
    end.

handle_offer(Offer, State) ->
    lager:info("sc_handler server got offer: ~p", [Offer]),
    case blockchain_state_channels_server:offer(Offer, State#state.ledger, State#state.handler_mod, self()) of
        ok ->
            %% offer is pending, just block the stream waiting for the purchase or rejection
            receive
                {send_purchase, SignedPurchaseSC, Hotspot, PacketHash, Region} ->
                    PacketHash = blockchain_state_channel_offer_v1:packet_hash(Offer),
                    %% NOTE: We're constructing the purchase with the hotspot obtained from offer here
                    PurchaseMsg = blockchain_state_channel_purchase_v1:new(SignedPurchaseSC, Hotspot, PacketHash, Region),
                    Data = blockchain_state_channel_message_v1:encode(PurchaseMsg),
                    {noreply, State#state{pending_packet_offers=maps:put(PacketHash, Offer, State#state.pending_packet_offers)}, Data};
                {send_rejection, Rejection} ->
                    Data = blockchain_state_channel_message_v1:encode(Rejection),
                    {noreply, State, Data}
            end;
        reject ->
            %% we were able to reject out of hand
            Rejection = blockchain_state_channel_rejection_v1:new(),
            Data = blockchain_state_channel_message_v1:encode(Rejection),
            {noreply, State, Data}
    end.
