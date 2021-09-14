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
    pending_offers = #{} :: #{binary() => {blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}},
    handler_mod :: atom()
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

-spec send_purchase(Pid :: pid(),
                    NewPurchaseSC :: blockchain_state_channel_v1:state_channel(),
                    Hotspot :: libp2p_crypto:pubkey_bin(),
                    PacketHash :: binary(),
                    Region :: atom()) -> ok.
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
    State = #state{ledger=Ledger, handler_mod=HandlerMod},
    case blockchain:config(?sc_version, Ledger) of
        {ok, N} when N > 1 ->
            ActiveSCs = maps:to_list(blockchain_state_channels_server:get_actives()),
            case ActiveSCs of
                [] ->
                    SCBanner = blockchain_state_channel_banner_v1:new(),
                    lager:info("sending empty banner", []),
                    {ok, State, blockchain_state_channel_message_v1:encode(SCBanner)};
                ActiveSCs ->
                    [{SCID, ActiveSC}|_] = ActiveSCs,
                    SCBanner = blockchain_state_channel_banner_v1:new(ActiveSC),
                    lager:info("sending banner for sc ~p", [blockchain_utils:addr2name(SCID)]),
                    EncodedSCBanner =
                        e2qc:cache(
                            ?MODULE,
                            SCID,
                            fun() ->
                                blockchain_state_channel_message_v1:encode(SCBanner)
                            end
                        ),
                    {ok, State, EncodedSCBanner}
            end;
        _ ->
            {ok, State#state{handler_mod=HandlerMod}}
    end.

handle_data(client, Data, State) ->
    %% get ledger if we don't yet have one
    Ledger =
        case State#state.ledger of
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
                    case Ledger == undefined orelse is_active_sc(BannerSC, Ledger) == ok of
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
            case Ledger == undefined orelse is_active_sc(PurchaseSC, Ledger) == ok of
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
    {noreply, State#state{ledger=Ledger}};
handle_data(server, Data, State=#state{pending_offers=PendingOffers}) ->
    Time = erlang:system_time(millisecond),
    case blockchain_state_channel_message_v1:decode(Data) of
        {offer, Offer} ->
            handle_offer(Offer, Time, State);
        {packet, Packet} ->
            PacketHash = blockchain_helium_packet_v1:packet_hash(blockchain_state_channel_packet_v1:packet(Packet)),
            case maps:get(PacketHash, PendingOffers, undefined) of
                undefined ->
                    lager:info("sc_handler server got packet: ~p", [Packet]),
                    blockchain_state_channels_server:handle_packet(Packet, Time, State#state.handler_mod, self()),
                    {noreply, State};
                {PendingOffer, PendingOfferTime} ->
                    case blockchain_state_channel_packet_v1:validate(Packet, PendingOffer) of
                        {error, packet_offer_mismatch} ->
                            %% might as well try it, it's free
                            blockchain_state_channels_server:handle_packet(Packet, Time, State#state.handler_mod, self()),
                            lager:warning("packet failed to validate ~p against offer ~p", [Packet, PendingOffer]),
                            {stop, normal};
                        {error, Reason} ->
                            lager:warning("packet failed to validate ~p reason ~p", [Packet, Reason]),
                            {stop, normal};
                        true ->
                            lager:info("sc_handler server got packet: ~p", [Packet]),
                            blockchain_state_channels_server:handle_packet(Packet, PendingOfferTime, State#state.handler_mod, self()),
                            {noreply, State}
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

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_offer(Offer, Time, State) ->
    lager:info("sc_handler server got offer: ~p", [Offer]),
    case blockchain_state_channels_server:handle_offer(Offer, State#state.handler_mod, self()) of
        ok ->
            PacketHash = blockchain_state_channel_offer_v1:packet_hash(Offer),
            {noreply, State#state{pending_offers=maps:put(PacketHash, {Offer, Time}, State#state.pending_offers)}};
        reject ->
            %% we were able to reject out of hand
            Rejection = blockchain_state_channel_rejection_v1:new(),
            Data = blockchain_state_channel_message_v1:encode(Rejection),
            {noreply, State, Data}
    end.

-spec is_active_sc(SC :: blockchain_state_channel_v1:state_channel(),
                   Ledger :: blockchain_ledger_v1:ledger()) -> ok | {error, inactive_sc}.
is_active_sc(SC, Ledger) ->
    SCOwner = blockchain_state_channel_v1:owner(SC),
    SCID = blockchain_state_channel_v1:id(SC),
    case blockchain_ledger_v1:find_state_channel(SCID, SCOwner, Ledger) of
        {ok, _SC} -> ok;
        _ -> {error, inactive_sc}
    end.
