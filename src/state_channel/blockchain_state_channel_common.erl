
-module(blockchain_state_channel_common).
-author("andrewmckenzie").

-export([
    new_handler_state/0,
    new_handler_state/7,
    ledger/1, ledger/2,
    pending_packet_offers/1, pending_packet_offers/2,
    offer_queue/1, offer_queue/2,
    handler_mod/1, handler_mod/2,
    pending_offer_limit/1, pending_offer_limit/2,
    chain/1, chain/2,
    encode_pb/1, encode_pb/2
]).

%% API
-export([
    send_purchase/6,
    send_response/2,
    send_banner/2,
    send_rejection/2,
    handle_client_msg/2,
    handle_server_msg/2,
    handle_offer/3,
    handle_next_offer/1,

    send_offer/2,
    send_packet/2,

    is_active_sc/2
]).

-record(handler_state, {
          chain :: undefined | blockchain:blockchain(),
          ledger :: undefined | blockchain_ledger_v1:ledger(),
          pending_packet_offers = #{} :: #{binary() => {blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}},
          offer_queue = [] :: [{blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}],
          handler_mod = undefined :: atom(),
          pending_offer_limit = undefined :: undefined | pos_integer(),
          encode_pb = true :: boolean()
         }).


-type handler_state() :: #handler_state{}.
-export_type([handler_state/0]).

-spec new_handler_state() -> handler_state().
new_handler_state()->
    #handler_state{}.
-spec new_handler_state(Chain :: blockchain:blockchain(),
                        Ledger ::  undefined | blockchain_ledger_v1:ledger(),
                        PendingPacketOffers :: #{binary() => {blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}},
                        OfferQueue :: [{blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}],
                        HandlerMod :: atom(),
                        PendingOfferLimit :: undefined | pos_integer(),
                        EncodePB :: boolean()
    ) -> handler_state().
new_handler_state(Chain, Ledger, PendingPacketOffers, OfferQueue, HandlerMod, PendingOfferLimit, EncodePB)->
    #handler_state{
        chain = Chain,
        ledger = Ledger,
        pending_packet_offers = PendingPacketOffers,
        offer_queue = OfferQueue,
        handler_mod = HandlerMod,
        pending_offer_limit = PendingOfferLimit,
        encode_pb = EncodePB
    }.


%%
%% State getters
%%
-spec chain(handler_state()) -> undefined | blockchain:blockchain().
chain(#handler_state{chain=V}) ->
    V.

-spec ledger(handler_state()) -> undefined | blockchain_ledger_v1:ledger().
ledger(#handler_state{ledger=V}) ->
    V.

-spec pending_packet_offers(handler_state()) -> #{binary() => {blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}}.
pending_packet_offers(#handler_state{pending_packet_offers=V}) ->
    V.

-spec offer_queue(handler_state()) -> [{blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}].
offer_queue(#handler_state{offer_queue=V}) ->
    V.

-spec handler_mod(handler_state()) -> atom().
handler_mod(#handler_state{handler_mod=V}) ->
    V.

-spec pending_offer_limit(handler_state()) -> undefined | pos_integer().
pending_offer_limit(#handler_state{pending_offer_limit=V}) ->
    V.

-spec encode_pb(handler_state()) -> boolean().
encode_pb(#handler_state{encode_pb=V}) ->
    V.

%%
%% State setters
%%
-spec chain(blockchain:blockchain(), handler_state()) -> handler_state().
chain(NewV, HandlerState) ->
    HandlerState#handler_state{chain=NewV}.

-spec ledger(blockchain_ledger_v1:ledger(), handler_state()) -> handler_state().
ledger(NewV, HandlerState) ->
    HandlerState#handler_state{ledger=NewV}.

-spec pending_packet_offers(#{binary() => {blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}}, handler_state()) -> handler_state().
pending_packet_offers(NewV, HandlerState) ->
    HandlerState#handler_state{pending_packet_offers=NewV}.

-spec offer_queue([{blockchain_state_channel_packet_offer_v1:offer(), pos_integer()}], handler_state()) -> handler_state().
offer_queue(NewV, HandlerState) ->
    HandlerState#handler_state{offer_queue=NewV}.

-spec handler_mod(atom(), handler_state()) -> handler_state().
handler_mod(NewV, HandlerState) ->
    HandlerState#handler_state{handler_mod=NewV}.

-spec pending_offer_limit(pos_integer(), handler_state()) -> handler_state().
pending_offer_limit(NewV, HandlerState) ->
    HandlerState#handler_state{pending_offer_limit=NewV}.

-spec encode_pb(boolean(), handler_state()) -> handler_state().
encode_pb(NewV, HandlerState) ->
    HandlerState#handler_state{encode_pb=NewV}.


%%
%% client API exports
%%
-spec send_packet(pid(), blockchain_state_channel_packet_v1:packet()) -> ok.
send_packet(Pid, Packet) ->
    lager:info("sending packet: ~p, pid: ~p", [Packet, Pid]),
    Pid ! {send_packet, Packet},
    ok.

-spec send_offer(pid(), blockchain_state_channel_packet_offer_v1:offer()) -> ok.
send_offer(Pid, Offer) ->
    lager:info("sending offer: ~p, pid: ~p", [Offer, Pid]),
    Pid ! {send_offer, Offer},
    ok.

%%
%% Server API exports
%%
-spec send_purchase(Pid :: pid(),
                    NewPurchaseSC :: blockchain_state_channel_v1:state_channel(),
                    Hotspot :: libp2p_crypto:pubkey_bin(),
                    PacketHash :: binary(),
                    Region :: atom(),
                    OwnerSigFun :: function()) -> ok.
send_purchase(Pid, NewPurchaseSC, Hotspot, PacketHash, Region, OwnerSigFun) ->
    lager:debug("sending purchase: ~p, pid: ~p", [NewPurchaseSC, Pid]),
    Pid ! {send_purchase, NewPurchaseSC, Hotspot, PacketHash, Region, OwnerSigFun},
    ok.

-spec send_banner(pid(), blockchain_state_channel_banner_v1:banner()) -> ok.
send_banner(Pid, Banner) ->
    lager:debug("sending banner: ~p, pid: ~p", [Banner, Pid]),
    Pid ! {send_banner, Banner},
    ok.

-spec send_rejection(pid(), blockchain_state_channel_rejection_v1:rejection()) -> ok.
send_rejection(Pid, Rejection) ->
    lager:debug("sending rejection: ~p, pid: ~p", [Rejection, Pid]),
    Pid ! {send_rejection, Rejection},
    ok.

-spec send_response(pid(), blockchain_state_channel_response_v1:response()) -> ok.
send_response(Pid, Resp) ->
    lager:debug("sending response: ~p, pid: ~p", [Resp, Pid]),
    Pid ! {send_response, Resp},
    ok.


handle_server_msg(Msg, HandlerState = #handler_state{
                                    ledger = Ledger,
                                    pending_packet_offers = PendingOffers,
                                    pending_offer_limit = PendingOfferLimit})->
    Time = erlang:system_time(millisecond),
    PendingOfferCount = maps:size(PendingOffers),
    case Msg of
        {offer, Offer} when PendingOfferCount < PendingOfferLimit ->
            handle_offer(Offer, Time, HandlerState);
        {offer, Offer} ->
            %% queue the offer
            {ok, HandlerState#handler_state{offer_queue=HandlerState#handler_state.offer_queue ++ [{Offer, Time}]}};
        {packet, Packet} ->
            PacketHash = blockchain_helium_packet_v1:packet_hash(blockchain_state_channel_packet_v1:packet(Packet)),
            case maps:get(PacketHash, PendingOffers, undefined) of
                undefined ->
                    lager:debug("sc_handler server got packet: ~p", [Packet]),
                    blockchain_state_channels_server:handle_packet(Packet, Time, HandlerState#handler_state.handler_mod, HandlerState#handler_state.ledger, self()),
                    {ok, HandlerState};
                {PendingOffer, PendingOfferTime} ->
                    case blockchain_state_channel_packet_v1:validate(Packet, PendingOffer) of
                        {error, packet_offer_mismatch} ->
                            %% might as well try it, it's free
                            blockchain_state_channels_server:handle_packet(Packet, Time, HandlerState#handler_state.handler_mod, HandlerState#handler_state.ledger, self()),
                            lager:warning("packet failed to validate ~p against offer ~p", [Packet, PendingOffer]),
                            stop;
                        {error, Reason} ->
                            lager:warning("packet failed to validate ~p reason ~p", [Packet, Reason]),
                            stop;
                        true ->
                            lager:debug("sc_handler server got packet: ~p", [Packet]),
                            blockchain_state_channels_server:handle_packet(Packet, PendingOfferTime, HandlerState#handler_state.handler_mod, HandlerState#handler_state.ledger, self()),
                            handle_next_offer(HandlerState#handler_state{pending_packet_offers=maps:remove(PacketHash, PendingOffers)})
                    end
            end;
        {banner, Banner} ->
            case blockchain_state_channel_banner_v1:sc(Banner) of
                undefined ->
                    %% empty banner, ignore
                    {ok, HandlerState};
                BannerSC ->
                    lager:debug("sc_handler client got banner, sc_id: ~p",
                               [blockchain_state_channel_v1:id(BannerSC)]),
                    %% either we don't have a ledger or we do and the SC is valid
                    case Ledger == undefined orelse is_active_sc(BannerSC, Ledger) == ok of
                        true ->
                            blockchain_state_channels_client:banner(Banner, self());
                        false ->
                            {ok, HandlerState}
                    end
            end;
        {purchase, Purchase} ->
            PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
            lager:debug("sc_handler client got purchase, sc_id: ~p",
                       [blockchain_state_channel_v1:id(PurchaseSC)]),
            %% either we don't have a ledger or we do and the SC is valid
            case Ledger == undefined orelse is_active_sc(PurchaseSC, Ledger) == ok of
                true ->
                    blockchain_state_channels_client:purchase(Purchase, self());
                false ->
                    {ok, HandlerState}
            end;
        {reject, Rejection} ->
            lager:debug("sc_handler client got rejection: ~p", [Rejection]),
            blockchain_state_channels_client:reject(Rejection, self());
        {response, Resp} ->
            lager:debug("sc_handler client got response: ~p", [Resp]),
            blockchain_state_channels_client:response(Resp)

    end.

handle_client_msg(Msg, HandlerState) ->
    %% get ledger if we don't yet have one
    Ledger = case HandlerState#handler_state.ledger of
                 undefined ->
                     case blockchain_worker:blockchain() of
                         undefined ->
                             undefined;
                         Chain ->
                             blockchain:ledger(Chain)
                     end;
                 L -> L
             end,
    case Msg of
        {banner, Banner} ->
            case blockchain_state_channel_banner_v1:sc(Banner) of
                undefined ->
                    %% empty banner, ignore
                    HandlerState;
                BannerSC ->
                    lager:debug("sc_handler client got banner, sc_id: ~p",
                               [blockchain_state_channel_v1:id(BannerSC)]),
                    %% either we don't have a ledger or we do and the SC is valid
                    case Ledger == undefined orelse is_active_sc(BannerSC, Ledger) == ok of
                        true ->
                            blockchain_state_channels_client:banner(Banner, self());
                        false ->
                            HandlerState
                    end
            end;
        {purchase, Purchase} ->
            PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
            lager:debug("sc_handler client got purchase, sc_id: ~p",
                       [blockchain_state_channel_v1:id(PurchaseSC)]),
            %% either we don't have a ledger or we do and the SC is valid
            case Ledger == undefined orelse is_active_sc(PurchaseSC, Ledger) == ok of
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
    HandlerState#handler_state{ledger=Ledger}.


-spec handle_next_offer(State) -> {ok, State} | {ok, State, Msg :: any()} when
    State :: #handler_state{}.
handle_next_offer(HandlerState=#handler_state{offer_queue=[]}) ->
    {ok, HandlerState};
handle_next_offer(HandlerState=#handler_state{offer_queue=[{NextOffer, OfferTime}|Offers], pending_packet_offers=PendingOffers, pending_offer_limit=Limit}) ->
    case maps:size(PendingOffers) < Limit of
        true ->
            handle_offer(NextOffer, OfferTime, HandlerState#handler_state{offer_queue=Offers});
        false ->
            {ok, HandlerState}
    end.

-spec handle_offer(
        Offer :: blockchain_state_channel_packet_offer_v1:offer(),
        Time :: pos_integer(),
        HandlerState :: #handler_state{}
) -> {ok, State :: #handler_state{}, Msg :: any()} | {ok, State :: #handler_state{}}.
handle_offer(Offer, Time, HandlerState) ->
    lager:debug("sc_handler server got offer: ~p", [Offer]),
    MaybeEncodeMsg = HandlerState#handler_state.encode_pb,
    case blockchain_state_channels_server:handle_offer(Offer, HandlerState#handler_state.handler_mod, HandlerState#handler_state.ledger, self()) of
        ok ->
            %% offer is pending, just block the stream waiting for the purchase or rejection
            receive
                {send_purchase, PurchaseSC, Hotspot, PacketHash, Region, OwnerSigFun} ->
                    % TODO: This is async and does not always return the right PacketHash for this offer
                    PacketHash = blockchain_state_channel_offer_v1:packet_hash(Offer),
                    %% NOTE: We're constructing the purchase with the hotspot obtained from offer here
                    SignedPurchaseSC = blockchain_state_channel_v1:sign(PurchaseSC, OwnerSigFun),
                    PurchaseMsg = blockchain_state_channel_purchase_v1:new(SignedPurchaseSC, Hotspot, PacketHash, Region),
                    Msg = maybe_encode_msg(MaybeEncodeMsg, PurchaseMsg),
                    {ok, HandlerState#handler_state{pending_packet_offers=maps:put(PacketHash, {Offer, Time}, HandlerState#handler_state.pending_packet_offers)}, Msg};
                {send_rejection, Rejection} ->
                    Msg = maybe_encode_msg(MaybeEncodeMsg, Rejection),
                    {ok, HandlerState, Msg}
            after timer:seconds(15) ->
                    lager:error("sc handle_offer timeout for offer: ~p", [Offer]),
                    {ok, HandlerState}
            end;
        reject ->
            %% we were able to reject out of hand
            PacketHash = blockchain_state_channel_offer_v1:packet_hash(Offer),
            Rejection = blockchain_state_channel_rejection_v1:new(PacketHash),
            Msg = maybe_encode_msg(MaybeEncodeMsg, Rejection),
            {ok, HandlerState, Msg}
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

maybe_encode_msg(true, Msg)->
    blockchain_state_channel_message_v1:encode(Msg);
maybe_encode_msg(false, Msg)->
    blockchain_state_channel_message_v1:wrap_msg(Msg).
