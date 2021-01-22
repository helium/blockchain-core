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
    close/1
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

close(HandlerPid)->
    _ = libp2p_framed_stream:close(HandlerPid).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _) ->
    {ok, blockchain_state_channel_common:new_handler_state()};
init(server, _Conn, [_Path, Blockchain]) ->
    Ledger = blockchain:ledger(Blockchain),
    HandlerMod = application:get_env(blockchain, sc_packet_handler, undefined),
    OfferLimit = application:get_env(blockchain, sc_pending_offer_limit, 5),
    HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod, OfferLimit, true),
    case blockchain:config(?sc_version, Ledger) of
        {ok, N} when N > 1 ->
            case blockchain_state_channels_server:active_scs() of
                [] ->
                    SCBanner = blockchain_state_channel_banner_v1:new(),
                    lager:info("sc_handler, empty banner: ~p", [SCBanner]),
                    HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod, OfferLimit, true),
                    {ok, HandlerState,
                     blockchain_state_channel_message_v1:encode(SCBanner)};
                ActiveSCs ->
                    [ActiveSC|_] = ActiveSCs,
                    SCBanner = blockchain_state_channel_banner_v1:new(ActiveSC),
                    SCID = blockchain_state_channel_v1:id(ActiveSC),
                    lager:info("sending banner for sc ~p", [blockchain_utils:addr2name(SCID)]),
                    HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod, OfferLimit, true),
                    EncodedSCBanner =
                        e2qc:cache(
                            blockchain_state_channel_handler,
                            SCID,
                            fun() -> blockchain_state_channel_message_v1:encode(SCBanner) end),
                    {ok, HandlerState, EncodedSCBanner}
            end;
        _ ->
            HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod, OfferLimit, true),
            {ok, HandlerState}
    end.

handle_data(client, Data, HandlerState) ->
    %% get ledger if we don't yet have one
    Ledger = case blockchain_state_channel_common:ledger(HandlerState) of
                 undefined ->
                     case blockchain_worker:blockchain() of
                         undefined ->
                             undefined;
                         Chain ->
                             blockchain:ledger(Chain)
                     end;
                 L -> L
             end,
    case blockchain_state_channel_message_v1:decode(Data) of
        {banner, Banner} ->
            case blockchain_state_channel_banner_v1:sc(Banner) of
                undefined ->
                    %% empty banner, ignore
                    ok;
                BannerSC ->
                    lager:info("sc_handler client got banner, sc_id: ~p",
                               [blockchain_state_channel_v1:id(BannerSC)]),
                    %% either we don't have a ledger or we do and the SC is valid
                    case Ledger == undefined orelse blockchain_state_channel_common:is_active_sc(BannerSC, Ledger) == ok of
                        true ->
                            blockchain_state_channels_client:banner(Banner, self());
                        false ->
                            ok
                    end
            end;
        {purchase, Purchase} ->
            PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
            lager:info("sc_handler client got purchase, sc_id: ~p",
                       [blockchain_state_channel_v1:id(PurchaseSC)]),
            %% either we don't have a ledger or we do and the SC is valid
            case Ledger == undefined orelse blockchain_state_channel_common:is_active_sc(PurchaseSC, Ledger) == ok of
                true ->
                    blockchain_state_channels_client:purchase(Purchase, self());
                false ->
                    ok
            end;
        {reject, Rejection} ->
            lager:info("sc_handler client got rejection: ~p", [Rejection]),
            blockchain_state_channels_client:reject(Rejection, self());
        {response, Resp} ->
            lager:debug("sc_handler client got response: ~p", [Resp]),
            blockchain_state_channels_client:response(Resp)
    end,
    NewHandlerState = blockchain_state_channel_common:ledger(Ledger, HandlerState),
    {noreply, NewHandlerState};
handle_data(server, Data, HandlerState) ->
    PendingOffers = blockchain_state_channel_common:pending_packet_offers(HandlerState),
    PendingOfferLimit = blockchain_state_channel_common:pending_offer_limit(HandlerState),
    Time = erlang:system_time(millisecond),
    PendingOfferCount = maps:size(PendingOffers),
    case blockchain_state_channel_message_v1:decode(Data) of
        {offer, Offer} when PendingOfferCount < PendingOfferLimit ->
            blockchain_state_channel_common:handle_offer(Offer, Time, HandlerState);
        {offer, Offer} ->
            %% queue the offer
            CurOfferQueue = blockchain_state_channel_common:offer_queue(HandlerState),
            NewHandlerState = blockchain_state_channel_common:offer_queue(CurOfferQueue ++ [{Offer, Time}], HandlerState),
            {noreply, NewHandlerState};
        {packet, Packet} ->
            PacketHash = blockchain_helium_packet_v1:packet_hash(blockchain_state_channel_packet_v1:packet(Packet)),
            case maps:get(PacketHash, PendingOffers, undefined) of
                undefined ->
                    lager:info("sc_handler server got packet: ~p", [Packet]),
                    HandlerMod = blockchain_state_channel_common:handler_mod(HandlerState),
                    blockchain_state_channels_server:packet(Packet, Time, HandlerMod, self()),
                    {noreply, HandlerState};
                {PendingOffer, PendingOfferTime} ->
                    case blockchain_state_channel_packet_v1:validate(Packet, PendingOffer) of
                        {error, packet_offer_mismatch} ->
                            %% might as well try it, it's free
                            HandlerMod = blockchain_state_channel_common:handler_mod(HandlerState),
                            blockchain_state_channels_server:packet(Packet, Time, HandlerMod, self()),
                            lager:warning("packet failed to validate ~p against offer ~p", [Packet, PendingOffer]),
                            {stop, normal};
                        {error, Reason} ->
                            lager:warning("packet failed to validate ~p reason ~p", [Packet, Reason]),
                            {stop, normal};
                        true ->
                            lager:info("sc_handler server got packet: ~p", [Packet]),
                            HandlerMod = blockchain_state_channel_common:handler_mod(HandlerState),
                            blockchain_state_channels_server:packet(Packet, PendingOfferTime, HandlerMod, self()),
                            NewHandlerState = blockchain_state_channel_common:pending_packet_offers(maps:remove(PacketHash, PendingOffers), HandlerState),
                            blockchain_state_channel_common:handle_next_offer(NewHandlerState)
                    end
            end
    end.

handle_info(client, {send_offer, Offer}, HandlerState) ->
    Data = blockchain_state_channel_message_v1:encode(Offer),
    {noreply, HandlerState, Data};
handle_info(client, {send_packet, Packet}, HandlerState) ->
    lager:debug("sc_handler client sending packet: ~p", [Packet]),
    Data = blockchain_state_channel_message_v1:encode(Packet),
    {noreply, HandlerState, Data};
handle_info(server, {send_banner, Banner}, HandlerState) ->
    Data = blockchain_state_channel_message_v1:encode(Banner),
    {noreply, HandlerState, Data};
handle_info(server, {send_rejection, Rejection}, HandlerState) ->
    Data = blockchain_state_channel_message_v1:encode(Rejection),
    {noreply, HandlerState, Data};
handle_info(server, {send_purchase, SignedPurchaseSC, Hotspot, PacketHash, Region}, HandlerState) ->
    %% NOTE: We're constructing the purchase with the hotspot obtained from offer here
    PurchaseMsg = blockchain_state_channel_purchase_v1:new(SignedPurchaseSC, Hotspot, PacketHash, Region),
    Data = blockchain_state_channel_message_v1:encode(PurchaseMsg),
    {noreply, HandlerState, Data};
handle_info(server, {send_response, Resp}, HandlerState) ->
    lager:debug("sc_handler server sending resp: ~p", [Resp]),
    Data = blockchain_state_channel_message_v1:encode(Resp),
    {noreply, HandlerState, Data};
handle_info(_Type, _Msg, HandlerState) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, HandlerState}.


