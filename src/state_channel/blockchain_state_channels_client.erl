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
-export([
         start_link/1,
         packet/3,
         state/0,
         response/1,
         purchase/2,
         banner/2
        ]).

%% ------------------------------------------------------------------
%% gen_server exports
%% ------------------------------------------------------------------
-export([
         init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3
        ]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("blockchain.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          db :: rocksdb:db_handle(),
          swarm :: pid(),
          state_channels = #{} :: state_channels(),
          streams = #{} :: streams(),
          packets = [] :: packets()
         }).

-type state() :: #state{}.
-type state_channels() :: #{binary() => blockchain_state_channel_v1:state_channel()}.
-type streams() :: #{non_neg_integer() => pid()}.
-type packets() :: [blockchain_helium_packet_v1:packet()].

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec response(blockchain_state_channel_response_v1:response()) -> any().
response(Resp) ->
    case application:get_env(blockchain, sc_client_handler, undefined) of
        undefined ->
            ok;
        Mod when is_atom(Mod) ->
            Mod:handle_response(Resp)
    end.

-spec purchase(Purchase :: blockchain_state_channel_purchase_v1:purchase(),
               HandlerPid :: pid()) -> ok.
purchase(Purchase, HandlerPid) ->
    gen_server:cast(?SERVER, {purchase, Purchase, HandlerPid}).

-spec banner(Banner :: blockchain_state_channel_banner_v1:banner(),
               HandlerPid :: pid()) -> ok.
banner(Banner, HandlerPid) ->
    gen_server:cast(?SERVER, {banner, Banner, HandlerPid}).

-spec packet(blockchain_helium_packet_v1:packet(), [string()], atom()) -> ok.
packet(Packet, DefaultRouters, Region) ->
    gen_server:cast(?SERVER, {packet, Packet, DefaultRouters, Region}).

-spec state() -> state().
state() ->
    gen_server:call(?SERVER, state).

%% ------------------------------------------------------------------
%% init, terminate and code_change
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = blockchain_state_channels_db_owner:db(),
    State = #state{db=DB, swarm=Swarm},
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
handle_cast({packet, Packet, DefaultRouters, Region}, State) ->
    NewState = handle_packet(Packet, DefaultRouters, Region, State),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    lager:debug("unhandled receive: ~p", [_Msg]),
    {noreply, State}.

handle_call(state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info({'DOWN', _Ref, process, Pid, _}, State=#state{streams=Streams}) ->
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
-spec handle_banner(Banner :: blockchain_state_channel_banner_v1:banner(),
                    Stream :: pid(),
                    State :: state()) -> state().
handle_banner(Banner, _Stream, State) ->
    case blockchain_state_channel_banner_v1:sc(Banner) of
        undefined ->
            %% We likely got an empty banner
            State;
        BannerSC ->
            BannerSCID = blockchain_state_channel_v1:id(BannerSC),
            case find_sc(BannerSCID, State) of
                undefined ->
                    %% We have no information about this state channel
                    %% Store it
                    add_sc(BannerSC, State);
                OurSC ->
                    %% We already know about this state channel
                    %% Validate this banner
                    case validate_banner(BannerSC, OurSC) of
                        ok ->
                            lager:info("successful banner validation, inserting BannerSC, id: ~p", [BannerSCID]),
                            add_sc(BannerSC, State);
                        {error, Reason} ->
                            lager:error("invalid banner, reason: ~p", [Reason]),
                            State
                    end
            end
    end.

-spec handle_purchase(Purchase :: blockchain_state_channel_purchase_v1:purchase(),
                      Stream :: pid(),
                      State :: state()) -> state().
handle_purchase(Purchase, Stream, #state{swarm=Swarm}=State) ->
    case validate_purchase(Purchase, State) of
        ok ->
            case deque_packet(State) of
                {undefined, _} ->
                    State;
                {Packet, NewState} ->
                    Region = blockchain_state_channel_purchase_v1:region(Purchase),
                    lager:info("successful purchase validation, sending packet: ~p",
                               [blockchain_helium_packet_v1:packet_hash(Packet)]),
                    ok = send_packet(Packet, Swarm, Stream, Region),
                    PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
                    add_sc(PurchaseSC, NewState)
            end;
        {error, Reason} ->
            lager:error("validate_purchase failed, reason: ~p", [Reason]),
            %% conflict
            State
    end.

-spec handle_packet(Packet :: blockchain_helium_packet_v1:packet(),
                    DefaultRouters :: [string()],
                    Region :: atom(),
                    State :: state()) -> state().
handle_packet(Packet, DefaultRouters, Region, State=#state{swarm=Swarm}) ->
    State0 = case find_routing(Packet) of
                 {error, _Reason} ->
                     lager:error("failed to find router for packet with routing information ~p:~p, trying default routers",
                                 [blockchain_helium_packet_v1:routing_info(Packet), _Reason]),
                     lists:foldl(fun(Router, StateAcc) ->
                                         case find_stream(Router, StateAcc) of
                                             undefined ->
                                                 case blockchain_state_channel_handler:dial(Swarm, Router, []) of
                                                     {error, _Reason2} ->
                                                         StateAcc;
                                                     {ok, NewStream} ->
                                                         unlink(NewStream),
                                                         erlang:monitor(process, NewStream),
                                                         ok = send_offer(Packet, Swarm, NewStream, Region),
                                                         add_stream(Router, NewStream, State)
                                                 end;
                                             Stream ->
                                                 ok = send_offer(Packet, Swarm, Stream, Region),
                                                 StateAcc
                                         end
                                 end, State, DefaultRouters);
                 {ok, Routes} ->
                     lists:foldl(fun(Route, StateAcc) ->
                                         send_to_route(Packet, Route, Region, StateAcc)
                                 end, State, Routes)
             end,

    %% Hold onto the packet
    enqueue_packet(Packet, State0).

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
                        State :: state()) -> ok | {error, any()}.
validate_purchase(Purchase, State) ->
    PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),
    PurchaseSCID = blockchain_state_channel_v1:id(PurchaseSC),

    case blockchain_state_channel_v1:validate(PurchaseSC) of
        ok ->
            case find_sc(PurchaseSCID, State) of
                undefined ->
                    %% We don't have any information about this state channel yet
                    %% Presumably our first packet
                    %% Check that the purchase sc nonce is set to 1
                    %% And the purchase sc summary only has 1 packet in it
                    check_first_purchase(Purchase);
                OurSC ->
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

-spec send_packet(Packet :: blockchain_helium_packet_v1:packet(),
                  Swarm :: pid(),
                  Stream :: pid(),
                  Region :: atom()  ) -> ok.
send_packet(Packet, Swarm, Stream, Region) ->
    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubkeyBin, Region),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_handler:send_packet(Stream, PacketMsg1).

-spec send_offer(Packet :: blockchain_helium_packet_v1:packet(),
                 Swarm :: pid(),
                 Stream :: pid(),
                 Region :: atom()  ) -> ok.
send_offer(Packet, Swarm, Stream, Region) ->
    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    OfferMsg0 = blockchain_state_channel_offer_v1:from_packet(Packet, PubkeyBin, Region),
    OfferMsg1 = blockchain_state_channel_offer_v1:sign(OfferMsg0, SigFun),
    lager:info("OfferMsg1: ~p", [OfferMsg1]),
    blockchain_state_channel_handler:send_offer(Stream, OfferMsg1).

-spec find_sc(SCID :: binary(), State :: state()) -> undefined | blockchain_state_channel_v1:state_channel().
find_sc(SCID, #state{state_channels=SCs}) ->
    maps:get(SCID, SCs, undefined).

-spec find_stream(OUIOrDefaultRouter :: non_neg_integer() | string(), State :: state()) -> undefined | pid().
find_stream(OUI, #state{streams=Streams}) ->
    maps:get(OUI, Streams, undefined).

-spec add_stream(OUIOrDefaultRouter :: non_neg_integer() | string(), Stream :: pid(), State :: state()) -> state().
add_stream(OUI, Stream, #state{streams=Streams}=State) ->
    State#state{streams=maps:put(OUI, Stream, Streams)}.

-spec add_sc(SC :: blockchain_state_channel_v1:state_channel(),
             State :: state()) -> state().
add_sc(SC, #state{state_channels=SCs}=State) ->
    SCID = blockchain_state_channel_v1:id(SC),
    State#state{state_channels=maps:put(SCID, SC, SCs)}.

-spec find_routing(Packet :: blockchain_helium_packet_v1:packet()) -> {ok, [blockchain_ledger_routing_v1:routing(), ...]} | {error, any()}.
find_routing(Packet) ->
    %% transitional shim for ignoring on-chain OUIs
    case application:get_env(blockchain, use_oui_routers, true) of
        true ->
            Chain = blockchain_worker:blockchain(),
            Ledger = blockchain:ledger(Chain),
            blockchain_ledger_v1:find_routing_for_packet(Packet, Ledger);
        false ->
            {error, oui_routing_disabled}
    end.

-spec send_to_route(blockchain_helium_packet_v1:packet(), blockchain_ledger_routing_v1:routing(), atom(), state()) -> state().
send_to_route(Packet, Route, Region, State=#state{swarm=Swarm}) ->
    OUI = blockchain_ledger_routing_v1:oui(Route),
    case find_stream(OUI, State) of
        undefined ->
            %% Do not have a stream open for this oui
            %% Create one and add to state
            {_, NewState} = lists:foldl(fun(_PubkeyBin, {done, StateAcc}) ->
                                                %% was already able to send to one of this OUI's routers
                                                {done, StateAcc};
                                           (PubkeyBin, {not_done, StateAcc}) ->
                                                StreamPeer = libp2p_crypto:pubkey_bin_to_p2p(PubkeyBin),
                                                case blockchain_state_channel_handler:dial(Swarm, StreamPeer, []) of
                                                    {error, _Reason} ->
                                                        lager:error("failed to dial ~p:~p", [StreamPeer, _Reason]),
                                                        {not_done, StateAcc};
                                                    {ok, NewStream} ->
                                                        unlink(NewStream),
                                                        erlang:monitor(process, NewStream),
                                                        ok = send_packet(Packet, Swarm, NewStream, Region),
                                                        {done, add_stream(OUI, NewStream, State)}
                                                end
                                        end, {not_done, State}, blockchain_ledger_routing_v1:addresses(Route)),
            NewState;
        Stream ->
            ok = send_packet(Packet, Swarm, Stream, Region),
            State
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).

%% TODO: Add some eunits here...

-endif.
