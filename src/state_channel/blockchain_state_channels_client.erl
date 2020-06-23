%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_client).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([start_link/1,
         packet/4,
         purchase/2,
         banner/2,
         state/0,
         response/1]).

%% ------------------------------------------------------------------
%% gen_server exports
%% ------------------------------------------------------------------
-export([init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("blockchain.hrl").

-define(SERVER, ?MODULE).

-record(state,{
          db :: rocksdb:db_handle(),
          bcf :: rocksdb:cf_handle(), %% banners cf
          pcf :: rocksdb:cf_handle(), %% purchases cf
          swarm :: pid(),
          streams = #{} :: streams(),
          packets = [] :: [blockchain_helium_packet_v1:packet()],
          waiting = #{} :: waiting()
         }).

-type state() :: #state{}.
-type streams() :: #{non_neg_integer() | string() => pid()}.
-type waiting_packet() :: {Packet :: blockchain_helium_packet_v1:packet(), Region :: atom()}.
-type waiting_key() :: non_neg_integer() | string().
-type waiting() :: #{waiting_key() => [waiting_packet()]}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec response(blockchain_state_channel_response_v1:response()) -> any().
response(Resp) ->
    erlang:spawn(fun() ->
        case application:get_env(blockchain, sc_client_handler, undefined) of
            undefined ->
                ok;
            Mod when is_atom(Mod) ->
                Mod:handle_response(Resp)
        end
    end).

-spec packet(Packet :: blockchain_helium_packet_v1:packet(),
             DefaultRouters :: [string()],
             Region :: atom(),
             Chain :: blockchain:blockchain()) -> ok.
packet(Packet, DefaultRouters, Region, Chain) ->
    case find_routing(Packet, Chain) of
        {error, _Reason} ->
            lager:error("failed to find router for packet with routing information ~p:~p, trying default routers",
                        [blockchain_helium_packet_v1:routing_info(Packet), _Reason]),
            gen_server:cast(?SERVER, {handle_packet, Packet, DefaultRouters, Region, Chain});
        {ok, Routes} ->
            gen_server:cast(?SERVER, {handle_packet, Packet, Routes, Region, Chain})
    end.

-spec state() -> state().
state() ->
    gen_server:call(?SERVER, state).

-spec purchase(Purchase :: blockchain_state_channel_purchase_v1:purchase(),
               HandlerPid :: pid()) -> ok.
purchase(Purchase, HandlerPid) ->
    gen_server:cast(?SERVER, {purchase, Purchase, HandlerPid}).

-spec banner(Banner :: blockchain_state_channel_banner_v1:banner(),
             HandlerPid :: pid()) -> ok.
banner(Banner, HandlerPid) ->
    gen_server:cast(?SERVER, {banner, Banner, HandlerPid}).

%% ------------------------------------------------------------------
%% init, terminate and code_change
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = blockchain_state_channels_db_owner:db(),
    BCF = blockchain_state_channels_db_owner:sc_client_banners_cf(),
    PCF = blockchain_state_channels_db_owner:sc_client_purchases_cf(),
    State = #state{db=DB, bcf=BCF, pcf=PCF, swarm=Swarm},
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% gen_server message handling
%% ------------------------------------------------------------------

handle_cast({banner, Banner, HandlerPid}, State) ->
    NewState = handle_banner(Banner, HandlerPid, State),
    {noreply, NewState};
handle_cast({purchase, Purchase, HandlerPid}, State) ->
    NewState = handle_purchase(Purchase, HandlerPid, State),
    {noreply, NewState};
handle_cast({handle_packet, Packet, RoutesOrAddresses, Region, Chain}, #state{swarm=Swarm}=State0) ->
    lager:info("handle_packet ~p to ~p", [Packet, RoutesOrAddresses]),
    State1 = lists:foldl(
               fun(RouteOrAddress, StateAcc) ->
                       StreamKey = case erlang:is_list(RouteOrAddress) of %% NOTE: This is actually a string check
                                       true ->
                                           {address, RouteOrAddress};
                                       false ->
                                           {oui, blockchain_ledger_routing_v1:oui(RouteOrAddress)}
                                   end,

                       case StreamKey of
                           {address, Address} ->
                               case find_stream(Address, StateAcc) of
                                   undefined ->
                                       lager:debug("stream undef dialing first"),
                                       ok = dial_and_send_packet(Swarm, RouteOrAddress, Packet, Region, Chain),
                                       add_stream(Address, dialing, StateAcc);
                                   dialing ->
                                       lager:debug("stream is still dialing queueing packet"),
                                       add_packet_to_waiting(Address, {Packet, Region}, StateAcc);
                                   Stream ->
                                       lager:debug("got stream sending offer"),
                                       ok = send_offer(Swarm, Stream, Packet, Region),
                                       StateAcc
                               end;
                           {oui, OUI} ->
                               case find_stream(OUI, StateAcc) of
                                   undefined ->
                                       lager:debug("stream undef dialing first"),
                                       ok = dial_and_send_packet(Swarm, RouteOrAddress, Packet, Region, Chain),
                                       add_stream(OUI, dialing, StateAcc);
                                   dialing ->
                                       lager:debug("stream is still dialing queueing packet"),
                                       add_packet_to_waiting(OUI, {Packet, Region}, StateAcc);
                                   Stream ->
                                       lager:debug("got stream sending offer"),
                                       ok = send_packet_or_offer(Swarm, Stream, Packet, Region, OUI, Chain),
                                       StateAcc
                               end
                       end
               end,
               State0,
               RoutesOrAddresses
              ),
    {noreply, enqueue_packet(Packet, State1)};
handle_cast(_Msg, State) ->
    lager:debug("unhandled receive: ~p", [_Msg]),
    {noreply, State}.

handle_call(state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info({dial_fail, AddressOrOUI, _Reason}, State0) ->
    Packets = get_waiting_packet(AddressOrOUI, State0),
    lager:error("failed to dial ~p: ~p dropping ~p packets", [AddressOrOUI, _Reason, erlang:length(Packets)+1]),
    State1 = remove_packet_from_waiting(AddressOrOUI, delete_stream(AddressOrOUI, State0)),
    {noreply, State1};
handle_info({dial_success, OUI, Stream, Chain}, #state{swarm=Swarm}=State0) when is_integer(OUI) ->
    Packets = get_waiting_packet(OUI, State0),
    lager:debug("dial_success sending ~p packets or offer depending on OUI", [erlang:length(Packets)]),
    lists:foreach(
        fun({Packet, Region}) ->
                ok = send_packet_or_offer(Swarm, Stream, Packet, Region, OUI, Chain)
        end,
        Packets
    ),
    erlang:monitor(process, Stream),
    State1 = add_stream(OUI, Stream, remove_packet_from_waiting(OUI, State0)),
    {noreply, State1};
handle_info({dial_success, Address, Stream, _Chain}, #state{swarm=Swarm}=State0) ->
    Packets = get_waiting_packet(Address, State0),
    lager:debug("dial_success sending ~p packet offers", [erlang:length(Packets)]),
    lists:foreach(
        fun({Packet, Region}) ->
            ok = send_offer(Swarm, Stream, Packet, Region)
        end,
        Packets
    ),
    erlang:monitor(process, Stream),
    State1 = add_stream(Address, Stream, remove_packet_from_waiting(Address, State0)),
    {noreply, State1};
handle_info({'DOWN', _Ref, process, Pid, _}, #state{streams=Streams}=State) ->
    FilteredStreams = maps:filter(fun(_Name, Stream) ->
                                          Stream /= Pid
                                  end, Streams),
    {noreply, State#state{streams=FilteredStreams}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec find_stream(AddressOrOUI :: string() | non_neg_integer(),
                  State :: state()) -> undefined | dialing |pid().
find_stream(AddressOrOUI, #state{streams=Streams}) ->
    maps:get(AddressOrOUI, Streams, undefined).

-spec add_stream(AddressOrOUI :: non_neg_integer() | string(), Stream :: pid() | dialing, State :: state()) -> state().
add_stream(AddressOrOUI, Stream, #state{streams=Streams}=State) ->
    State#state{streams=maps:put(AddressOrOUI, Stream, Streams)}.

-spec delete_stream(AddressOrOUI :: non_neg_integer() | string(), State :: state()) -> state().
delete_stream(AddressOrOUI, #state{streams=Streams}=State) ->
    State#state{streams=maps:remove(AddressOrOUI, Streams)}.

-spec get_waiting_packet(AddressOrOUI :: waiting_key(), State :: state()) -> [waiting_packet()].
get_waiting_packet(AddressOrOUI, #state{waiting=Waiting}) ->
    maps:get(AddressOrOUI, Waiting, []).

-spec add_packet_to_waiting(AddressOrOUI :: waiting_key(),
                            WaitingPacket :: waiting_packet(),
                            State :: state()) -> state().
add_packet_to_waiting(AddressOrOUI, {Packet, Region}, #state{waiting=Waiting}=State) ->
    Q = get_waiting_packet(AddressOrOUI, State),
    State#state{waiting=maps:put(AddressOrOUI, Q ++ [{Packet, Region}], Waiting)}.

-spec remove_packet_from_waiting(AddressOrOUI :: waiting_key(), State :: state()) -> state().
remove_packet_from_waiting(AddressOrOUI, #state{waiting=Waiting}=State) ->
    State#state{waiting=maps:remove(AddressOrOUI, Waiting)}.

-spec enqueue_packet(Packet :: blockchain_helium_packet_v1:packet(),
                     State :: state()) -> state().
enqueue_packet(Packet, #state{packets=Packets}=State) ->
    State#state{packets=[Packet | Packets]}.

-spec deque_packet(State :: state()) -> {undefined | blockchain_helium_packet_v1:packet(), state()}.
deque_packet(#state{packets=Packets}=State) when length(Packets) > 0 ->
    %% Remove from tail
    [ToPop | Rest] = lists:reverse(Packets),
    {ToPop, State#state{packets=lists:reverse(Rest)}};
deque_packet(State) ->
    {undefined, State}.

-spec find_routing(Packet :: blockchain_helium_packet_v1:packet(),
                   blockchain:blockchain() | undefined) ->{ok, [blockchain_ledger_routing_v1:routing()]} | {error, any()}.
find_routing(_Packet, undefined) ->
    {error, chain_undefined};
find_routing(Packet, Chain) ->
    %% transitional shim for ignoring on-chain OUIs
    case application:get_env(blockchain, use_oui_routers, true) of
        true ->
            Ledger = blockchain:ledger(Chain),
            blockchain_ledger_v1:find_routing_for_packet(Packet, Ledger);
        false ->
            {error, oui_routing_disabled}
    end.

-spec dial_and_send_packet(Swarm :: pid(),
                           Address :: string() | blockchain_ledger_routing_v1:routing(),
                           Packet :: blockchain_helium_packet_v1:packet(),
                           Region :: atom(),
                           Chain :: blockchain:blockchain()) -> ok.
dial_and_send_packet(Swarm, Address, Packet, Region, Chain) when is_list(Address) ->
    Self = self(),
    erlang:spawn(fun() ->
        lager:debug("dialing address ~p for packet ~p", [Address, Packet]),
        case blockchain_state_channel_handler:dial(Swarm, Address, []) of
            {error, _Reason} ->
                Self ! {dial_fail, Address, _Reason};
            {ok, Stream} ->
                unlink(Stream),
                ok = send_offer(Swarm, Stream, Packet, Region),
                Self ! {dial_success, Address, Stream, Chain}
        end
    end),
    ok;
dial_and_send_packet(Swarm, Route, Packet, Region, Chain) ->
    Self = self(),
    erlang:spawn(fun() ->
        lager:debug("dialing addresses ~p for packet ~p", [blockchain_ledger_routing_v1:addresses(Route), Packet]),
        Dialed = lists:foldl(
            fun(_PubkeyBin, {dialed, _}=Acc) ->
                Acc;
            (PubkeyBin, not_dialed) ->
                Address = libp2p_crypto:pubkey_bin_to_p2p(PubkeyBin),
                case blockchain_state_channel_handler:dial(Swarm, Address, []) of
                    {error, _Reason} ->
                        lager:error("failed to dial ~p:~p", [Address, _Reason]),
                        not_dialed;
                    {ok, Stream} ->
                        unlink(Stream),
                        {dialed, Stream}
                end
            end,
            not_dialed,
            blockchain_ledger_routing_v1:addresses(Route)
        ),
        OUI = blockchain_ledger_routing_v1:oui(Route),
        case Dialed of
            not_dialed ->
                Self ! {dial_fail, OUI, failed};
            {dialed, Stream} ->
                %% Check if hotspot and router oui match for this packet,
                %% if so, send packet directly otherwise send offer
                case is_hotspot_in_router_oui(Swarm, OUI, Chain) of
                    false ->
                        lager:debug("sending offer, hotspot not in router OUI: ~p", [OUI]),
                        ok = send_offer(Swarm, Stream, Packet, Region),
                        Self ! {dial_success, OUI, Stream, Chain};
                    true ->
                        lager:debug("sending packet, hotspot and router OUI match"),
                        ok = send_packet(Swarm, Stream, Packet, Region),
                        Self ! {dial_success, OUI, Stream, Chain}
                end
        end
    end),
    ok.

-spec send_packet(Swarm :: pid(),
                  Stream :: pid(),
                  Packet :: blockchain_helium_packet_v1:packet(),
                  Region :: atom()  ) -> ok.
send_packet(Swarm, Stream, Packet, Region) ->
    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubkeyBin, Region),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_handler:send_packet(Stream, PacketMsg1).

-spec send_offer(Swarm :: pid(),
                 Stream :: pid(),
                 Packet :: blockchain_helium_packet_v1:packet(),
                 Region :: atom()  ) -> ok.
send_offer(Swarm, Stream, Packet, Region) ->
    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    OfferMsg0 = blockchain_state_channel_offer_v1:from_packet(Packet, PubkeyBin, Region),
    OfferMsg1 = blockchain_state_channel_offer_v1:sign(OfferMsg0, SigFun),
    lager:info("OfferMsg1: ~p", [OfferMsg1]),
    blockchain_state_channel_handler:send_offer(Stream, OfferMsg1).

-spec handle_purchase(Purchase :: blockchain_state_channel_purchase_v1:purchase(),
                      Stream :: pid(),
                      State :: state()) -> state().
handle_purchase(Purchase, Stream, #state{db=DB, pcf=PCF, swarm=Swarm}=State) ->
    case validate_purchase(Purchase, State) of
        ok ->
            case deque_packet(State) of
                {undefined, _} ->
                    State;
                {Packet, NewState} ->
                    Region = blockchain_state_channel_purchase_v1:region(Purchase),
                    lager:debug("successful purchase validation, sending packet: ~p",
                               [blockchain_helium_packet_v1:packet_hash(Packet)]),
                    ok = send_packet(Swarm, Stream, Packet, Region),
                    PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
                    PurchaseSCID = blockchain_state_channel_v1:id(PurchaseSC),
                    ok = store_state_channel(DB, PCF, PurchaseSCID, PurchaseSC),
                    NewState
            end;
        {error, Reason} ->
            lager:error("validate_purchase failed, reason: ~p", [Reason]),
            %% conflict
            State
    end.

-spec handle_banner(Banner :: blockchain_state_channel_banner_v1:banner(),
                    Stream :: pid(),
                    State :: state()) -> state().
handle_banner(Banner, _Stream, #state{db=DB, bcf=BCF}=State) ->
    case blockchain_state_channel_banner_v1:sc(Banner) of
        undefined ->
            %% We likely got an empty banner
            State;
        BannerSC ->
            BannerSCID = blockchain_state_channel_v1:id(BannerSC),
            case get_state_channel(DB, BCF, BannerSCID) of
                {error, _} ->
                    %% We have no information about this state channel
                    %% Store it
                    ok = store_state_channel(DB, BCF, BannerSCID, BannerSC),
                    State;
                {ok, OurSC} ->
                    %% We already know about this state channel
                    %% Validate this banner
                    case validate_banner(BannerSC, OurSC) of
                        ok ->
                            lager:debug("successful banner validation, inserting BannerSC, id: ~p", [BannerSCID]),
                            ok = store_state_channel(DB, BCF, BannerSCID, BannerSC),
                            State;
                        {error, Reason} ->
                            lager:error("invalid banner, reason: ~p", [Reason]),
                            State
                    end
            end
    end.

-spec validate_banner(BannerSC :: blockchain_state_channel_v1:state_channel(),
                      OurSC :: blockchain_state_channel_v1:state_channel()) -> ok | {error, any()}.
validate_banner(BannerSC, OurSC) ->
    %% We check the following conditions here:
    %% - The nonce in BannerSC is higher or equal than OurSC (not strictly monotonically increasing)
    %% - All balances in BannerSC are higher or equal than OurSC balances
    BannerSCNonce = blockchain_state_channel_v1:nonce(BannerSC),
    OurSCNonce = blockchain_state_channel_v1:nonce(OurSC),
    case BannerSCNonce >= OurSCNonce of
        true ->
            compare_banner_summaries(BannerSC, OurSC);
        false ->
            {error, {invalid_banner_nonce, BannerSCNonce, OurSCNonce}}
    end.

-spec compare_banner_summaries(BannerSC :: blockchain_state_channel_v1:state_channel(),
                               OurSC :: blockchain_state_channel_v1:state_channel()) -> ok | {error, any()}.
compare_banner_summaries(BannerSC, OurSC) ->
    %% Every single hotspot in the banner summaries must have higher balances
    %% than our sc summaries

    OurSCSummaries = blockchain_state_channel_v1:summaries(OurSC),

    Res = lists:all(fun(OurSCSummary) ->
                            ClientPubkeyBin = blockchain_state_channel_summary_v1:client_pubkeybin(OurSCSummary),
                            case blockchain_state_channel_v1:get_summary(ClientPubkeyBin, BannerSC) of
                                {error, not_found} ->
                                    %% We have the summary but BannerSC does not, not allowed
                                    false;
                                {ok, BannerSCSummary} ->
                                    BannerSCNumPackets = blockchain_state_channel_summary_v1:num_packets(BannerSCSummary),
                                    BannerSCNumDCs = blockchain_state_channel_summary_v1:num_dcs(BannerSCSummary),
                                    OurSCNumPackets = blockchain_state_channel_summary_v1:num_packets(OurSCSummary),
                                    OurSCNumDCs = blockchain_state_channel_summary_v1:num_dcs(OurSCSummary),
                                    (BannerSCNumPackets >= OurSCNumPackets) andalso (BannerSCNumDCs >= OurSCNumDCs)
                            end
                    end, OurSCSummaries),

    case Res of
        false ->
            {error, {invalid_banner_summaries, BannerSC, OurSC}};
        true ->
            ok
    end.

-spec validate_purchase(Purchase :: blockchain_state_channel_purchase_v1:purchase(),
                        State :: blockchain_state_channels_client:state()) -> ok | {error, any()}.
validate_purchase(Purchase, #state{db=DB, pcf=PCF}) ->
    PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
    PurchaseSCID = blockchain_state_channel_v1:id(PurchaseSC),

    case blockchain_state_channel_v1:validate(PurchaseSC) of
        ok ->
            case get_state_channel(DB, PCF, PurchaseSCID) of
                {error, _} ->
                    %% We don't have any information about this state channel yet
                    %% Presumably our first packet
                    %% Check that the purchase sc nonce is set to 1
                    %% And the purchase sc summary only has 1 packet in it
                    check_first_purchase(Purchase);
                {ok, OurSC} ->
                    compare_purchase_summary(Purchase, OurSC)
            end;
        {error, _Reason}=E ->
            lager:error("purchase sc validation failed, ~p", [_Reason]),
            {error, {invalid_purchase_sc, E}}
    end.

-spec check_first_purchase(Purchase :: blockchain_state_channel_purchase_v1:purchase()) -> ok | {error, any()}.
check_first_purchase(Purchase) ->
    PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
    PurchaseHotspot = blockchain_state_channel_purchase_v1:hotspot(Purchase),
    PurchaseSCNonce = blockchain_state_channel_v1:nonce(PurchaseSC),
    case PurchaseSCNonce of
        1 ->
            case blockchain_state_channel_v1:get_summary(PurchaseHotspot, PurchaseSC) of
                {error, not_found}=E ->
                    %% how did you end here?
                    E;
                {ok, PurchaseSCSummary} ->
                    PurchaseNumPackets = blockchain_state_channel_summary_v1:num_packets(PurchaseSCSummary),
                    case PurchaseNumPackets == 1 of
                        true -> ok;
                        false ->
                            {error, first_purchase_packet_count_mismatch}
                    end
            end;
        _ ->
            %% This is first purchase without nonce set to 1?
            {error, first_purchase_nonce_mismatch}
    end.

-spec compare_purchase_summary(Purchase :: blockchain_state_channel_purchase_v1:purchase(),
                               OurSC :: blockchain_state_channel_v1:state_channel()) -> ok | {error, any()}.
compare_purchase_summary(Purchase, OurSC) ->
    OurSCNonce = blockchain_state_channel_v1:nonce(OurSC),
    PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
    PurchaseHotspot = blockchain_state_channel_purchase_v1:hotspot(Purchase),
    PurchaseSCNonce = blockchain_state_channel_v1:nonce(PurchaseSC),

    case PurchaseSCNonce == OurSCNonce + 1 of
        false ->
            {error, {invalid_purchase_nonce, {PurchaseSCNonce, OurSCNonce}}};
        true ->
            case blockchain_state_channel_v1:get_summary(PurchaseHotspot, OurSC) of
                {error, not_found}=E1 ->
                    %% we don't know about this, presumably first purchase?
                    %% check purchase summary
                    case blockchain_state_channel_v1:get_summary(PurchaseHotspot, PurchaseSC) of
                        {error, not_found}=E2 ->
                            %% how did you end here?
                            {error, {no_summaries, E1, E2}};
                        {ok, _PurchaseSCSummary} ->
                            %% XXX: What should we be checking here?
                            lager:debug("valid purchase"),
                            ok
                    end;
                {ok, OurSCSummary} ->
                    case blockchain_state_channel_v1:get_summary(PurchaseHotspot, PurchaseSC) of
                        {error, not_found}=E3 ->
                            %% how did you get here?
                            {error, {no_purchase_summary, E3}};
                        {ok, PurchaseSCSummary} ->
                            PurchaseNumPackets = blockchain_state_channel_summary_v1:num_packets(PurchaseSCSummary),
                            OurNumPackets = blockchain_state_channel_summary_v1:num_packets(OurSCSummary),
                            lager:debug("checking packet count, purchase: ~p, ours: ~p", [PurchaseNumPackets, OurNumPackets]),
                            case PurchaseNumPackets == OurNumPackets + 1 of
                                false ->
                                    {error, {invalid_purchase_packet_count, PurchaseNumPackets, OurNumPackets}};
                                true ->
                                    ok
                            end
                    end
            end
    end.

-spec is_hotspot_in_router_oui(Swarm :: pid(),
                               OUI :: pos_integer(),
                               Chain :: blockchain:blockchain()) -> boolean().
is_hotspot_in_router_oui(Swarm, OUI, Chain) ->
    {PubkeyBin, _SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_gateway_info(PubkeyBin, Ledger) of
        {error, _} ->
            false;
        {ok, Gw} ->
            case blockchain_ledger_gateway_v2:oui(Gw) of
                undefined ->
                    false;
                OUI ->
                    true
            end
    end.


-spec send_packet_or_offer(Swarm :: pid(),
                           Stream :: pid(),
                           Packet :: blockchain_helium_packet_v1:packet(),
                           Region :: atom(),
                           OUI :: pos_integer(),
                           Chain :: blockchain:blockchain()) -> ok.
send_packet_or_offer(Swarm, Stream, Packet, Region, OUI, Chain) ->
    case is_hotspot_in_router_oui(Swarm, OUI, Chain) of
        false ->
            send_offer(Swarm, Stream, Packet, Region);
        true ->
            send_packet(Swarm, Stream, Packet, Region)
    end.

%% ------------------------------------------------------------------
%% DB interaction
%% ------------------------------------------------------------------

-spec get_state_channel(DB :: rocksdb:db_handle(),
                        CF :: rocksdb:cf_handle(),
                        SCID :: blockchain_state_channel_v1:id()) ->
    {ok, blockchain_state_channel_v1:state_channel()} | {error, any()}.
get_state_channel(DB, CF, SCID) ->
    case rocksdb:get(DB, CF, SCID, [{sync, true}]) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {error, not_found};
        Error ->
            lager:error("error: ~p", [Error]),
            Error
    end.

-spec store_state_channel(DB :: rocksdb:db_handle(),
                          CF :: rocksdb:cf_handle(),
                          SCID :: blockchain_state_channel_v1:id(),
                          SC :: blockchain_state_channel_v1:state_channel()) ->
    ok | {error, any()}.
store_state_channel(DB, CF, SCID, SC) ->
    %% NOTE: This overwrites any state channel in any column family
    ToInsert = erlang:term_to_binary(SC),
    rocksdb:put(DB, CF, SCID, ToInsert, [{sync, true}]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).
-endif.
