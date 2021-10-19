%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_handler).

-behavior(libp2p_framed_stream).

% TODO

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

dial(SwarmTID, Peer, Opts) ->
    libp2p_swarm:dial_framed_stream(SwarmTID,
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
            ActiveSCs =
                e2qc:cache(
                    ?MODULE,
                    active_list,
                    10,
                    fun() -> maps:to_list(blockchain_state_channels_server:get_actives()) end
                ),
            case ActiveSCs of
                [] ->
                    SCBanner = blockchain_state_channel_banner_v1:new(),
                    lager:debug("sc_handler, empty banner: ~p", [SCBanner]),
                    HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod, OfferLimit, true),
                    {ok, HandlerState,
                     blockchain_state_channel_message_v1:encode(SCBanner)};
                ActiveSCs ->
                    [{SCID, {ActiveSC, _, _}}|_] = ActiveSCs,
                    SCBanner = blockchain_state_channel_banner_v1:new(ActiveSC),
                    HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod, OfferLimit, true),
                    EncodedSCBanner =
                        e2qc:cache(
                            ?MODULE,
                            SCID,
                            fun() -> blockchain_state_channel_message_v1:encode(SCBanner) end
                        ),
                    {ok, HandlerState, EncodedSCBanner}
            end;
        _ ->
            HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod, OfferLimit, true),
            {ok, HandlerState}
    end.

-spec handle_data(
        Kind :: libp2p_framed_stream:kind(),
        Data :: any(),
        HandlerState :: any()
) -> libp2p_framed_stream:handle_data_result().
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
                    lager:debug("sc_handler client got banner, sc_id: ~p",
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
            lager:debug("sc_handler client got purchase, sc_id: ~p",
                       [blockchain_state_channel_v1:id(PurchaseSC)]),
            %% either we don't have a ledger or we do and the SC is valid
            case Ledger == undefined orelse blockchain_state_channel_common:is_active_sc(PurchaseSC, Ledger) == ok of
                true ->
                    blockchain_state_channels_client:purchase(Purchase, self());
                false ->
                    ok
            end;
        {reject, Rejection} ->
            lager:debug("sc_handler client got rejection: ~p", [Rejection]),
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
            case blockchain_state_channel_common:handle_offer(Offer, Time, HandlerState) of
                {ok, State} -> {noreply, State};
                {ok, State, Msg} -> {noreply, State, Msg}
            end;
        {offer, Offer} ->
            %% queue the offer
            CurOfferQueue = blockchain_state_channel_common:offer_queue(HandlerState),
            NewHandlerState = blockchain_state_channel_common:offer_queue(CurOfferQueue ++ [{Offer, Time}], HandlerState),
            {noreply, NewHandlerState};
        {packet, Packet} ->
            Ledger = blockchain_state_channel_common:ledger(HandlerState),
            PacketHash = blockchain_helium_packet_v1:packet_hash(blockchain_state_channel_packet_v1:packet(Packet)),
            case maps:get(PacketHash, PendingOffers, undefined) of
                undefined ->
                    lager:info("sc_handler server got packet: ~p", [Packet]),
                    HandlerMod = blockchain_state_channel_common:handler_mod(HandlerState),
                    blockchain_state_channels_server:handle_packet(Packet, Time, HandlerMod, Ledger, self()),
                    {noreply, HandlerState};
                {PendingOffer, PendingOfferTime} ->
                    case blockchain_state_channel_packet_v1:validate(Packet, PendingOffer) of
                        {error, packet_offer_mismatch} ->
                            %% might as well try it, it's free
                            HandlerMod = blockchain_state_channel_common:handler_mod(HandlerState),
                            blockchain_state_channels_server:handle_packet(Packet, Time, HandlerMod, Ledger, self()),
                            lager:warning("packet failed to validate ~p against offer ~p", [Packet, PendingOffer]),
                            {stop, normal};
                        {error, Reason} ->
                            lager:warning("packet failed to validate ~p reason ~p", [Packet, Reason]),
                            {stop, normal};
                        true ->
                            lager:info("sc_handler server got packet: ~p", [Packet]),
                            HandlerMod = blockchain_state_channel_common:handler_mod(HandlerState),
                            blockchain_state_channels_server:handle_packet(Packet, PendingOfferTime, HandlerMod, Ledger, self()),
                            NewHandlerState = blockchain_state_channel_common:pending_packet_offers(maps:remove(PacketHash, PendingOffers), HandlerState),
                            case blockchain_state_channel_common:handle_next_offer(NewHandlerState) of
                                {ok, State} -> {noreply, State};
                                {ok, State, Msg} -> {noreply, State, Msg}
                            end
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
handle_info(server, {send_purchase, PurchaseSC, Hotspot, PacketHash, Region, OwnerSigFun}, HandlerState) ->
    %% NOTE: We're constructing the purchase with the hotspot obtained from offer here
    SignedPurchaseSC = blockchain_state_channel_v1:sign(PurchaseSC, OwnerSigFun),
    PurchaseMsg = blockchain_state_channel_purchase_v1:new(SignedPurchaseSC, Hotspot, PacketHash, Region),
    Data = blockchain_state_channel_message_v1:encode(PurchaseMsg),
    {noreply, HandlerState, Data};
handle_info(server, {send_purchase_diff, Summary, PacketHash, Region, Owner, OwnerSigFun}, HandlerState) ->
    DiffMsg = blockchain_state_channel_purchase_diff_v1:new(Summary, PacketHash, Region, Owner),
    SignedDiffMsg = blockchain_state_channel_purchase_diff_v1:sign(DiffMsg, OwnerSigFun),
    Data = blockchain_state_channel_message_v1:encode(SignedDiffMsg),
    {noreply, HandlerState, Data};
handle_info(server, {send_response, Resp}, HandlerState) ->
    lager:debug("sc_handler server sending resp: ~p", [Resp]),
    Data = blockchain_state_channel_message_v1:encode(Resp),
    {noreply, HandlerState, Data};
handle_info(_Type, _Msg, HandlerState) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, HandlerState}.


