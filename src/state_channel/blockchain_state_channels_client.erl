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
          cf :: rocksdb:cf_handle(),
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
    CF = blockchain_state_channels_db_owner:sc_clients_cf(),
    State = #state{db=DB, cf=CF, swarm=Swarm},
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% gen_server message handling
%% ------------------------------------------------------------------

handle_cast({banner, Banner, HandlerPid}, State) ->
    ok = handle_banner(Banner, HandlerPid, State),
    {noreply, State};
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

-spec handle_banner(Banner :: blockchain_state_channel_banner_v1:banner(),
                    Stream :: pid(),
                    State :: state()) -> ok | {error, any()}.
handle_banner(Banner, Stream, State) ->
    case blockchain_state_channel_banner_v1:sc(Banner) of
        undefined ->
            ok;
        BannerSC ->
            case is_valid_sc(BannerSC, Stream, State) of
                {error, causal_conflict} ->
                    store_state_channel(BannerSC, State);
                _ ->
                    ok
            end
    end.

-spec handle_purchase(Purchase :: blockchain_state_channel_purchase_v1:purchase(),
                      Stream :: pid(),
                      State :: state()) -> state().
handle_purchase(Purchase, Stream, #state{swarm=Swarm}=State) ->
    PurchaseSC = blockchain_state_channel_purchase_v1:sc(Purchase),

    case is_valid_sc(PurchaseSC, Stream, State) of
        {error, causal_conflict} ->
            %% store conflicted sc
            ok = store_state_channel(PurchaseSC, State),
            State;
        ok ->
            case deque_packet(State) of
                {undefined, _} ->
                    State;
                {Packet, NewState} ->
                    Region = blockchain_state_channel_purchase_v1:region(Purchase),
                    lager:debug("successful purchase validation, sending packet: ~p",
                                [blockchain_helium_packet_v1:packet_hash(Packet)]),
                    ok = send_packet(Swarm, Stream, Packet, Region),
                    NewState
            end;
        _ ->
            State
    end.

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
%% State channel validation functions
%% ------------------------------------------------------------------
-spec is_valid_sc(SC :: blockchain_state_channel_v1:state_channel(),
                  Stream :: pid(),
                  State :: state()) -> ok | {error, any()}.
is_valid_sc(SC, Stream, State) ->
    case blockchain_state_channel_v1:validate(SC) of
        {error, Reason}=E ->
            ok = libp2p_framed_stream:close(Stream),
            lager:error("invalid banner sc signature, reason: ~p", [Reason]),
            E;
        ok ->
            case is_active_sc(SC) of
                false ->
                    lager:error("inactive banner sc"),
                    ok = libp2p_framed_stream:close(Stream),
                    ok;
                true ->
                    case is_causally_correct_sc(SC, State) of
                        true ->
                            overwrite_state_channel(SC, State);
                        false ->
                            ok = libp2p_framed_stream:close(Stream),
                            {error, causal_conflict}
                    end
            end
    end.

-spec is_active_sc(SC :: blockchain_state_channel_v1:state_channel()) -> boolean().
is_active_sc(SC) ->
    %% TODO: cache chain in state?
    Ledger = blockchain:ledger(blockchain_worker:blockchain()),
    SCOwner = blockchain_state_channel_v1:owner(SC),
    SCID = blockchain_state_channel_v1:id(SC),
    {ok, LedgerSCIDs} = blockchain_ledger_v1:find_sc_ids_by_owner(SCOwner, Ledger),
    lists:member(SCID, LedgerSCIDs).

-spec is_causally_correct_sc(SC :: blockchain_state_channel_v1:state_channel(),
                             State :: state()) -> boolean().
is_causally_correct_sc(SC, State) ->
    SCID = blockchain_state_channel_v1:id(SC),

    case get_state_channels(SCID, State) of
        {error, not_found} ->
            true;
        {error, _} ->
            %% rocks blew up
            false;
        {ok, [KnownSC]} ->
            %% Check if SC is causally correct
            caused == blockchain_state_channel_v1:causality(KnownSC, SC) orelse
            equal == blockchain_state_channel_v1:causality(KnownSC, SC);
        {ok, _KnownSCs} ->
            %% We have a conflict among incoming state channels
            false
    end.

%% ------------------------------------------------------------------
%% DB functions
%% ------------------------------------------------------------------
-spec get_state_channels(SCID :: blockchain_state_channel_v1:id(),
                         State :: state()) ->
    {ok, [blockchain_state_channel_v1:state_channel()]} | {error, any()}.
get_state_channels(SCID, #state{db=DB, cf=CF}) ->
    case rocksdb:get(DB, CF, SCID, [{sync, true}]) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {error, not_found};
        Error ->
            lager:error("error: ~p", [Error]),
            Error
    end.

-spec store_state_channel(SC :: blockchain_state_channel_v1:state_channel(),
                          State :: state()) -> ok | {error, any()}.
store_state_channel(SC, #state{db=DB, cf=CF}=State) ->
    SCID = blockchain_state_channel_v1:id(SC),
    case get_state_channels(SCID, State) of
        {ok, SCs} ->
            ToInsert = erlang:term_to_binary([SC | SCs]),
            rocksdb:put(DB, CF, SCID, ToInsert, [{sync, true}]);
        {error, not_found} ->
            ToInsert = erlang:term_to_binary([SC]),
            rocksdb:put(DB, CF, SCID, ToInsert, [{sync, true}]);
        {error, _}=E ->
            E
    end.

-spec overwrite_state_channel(SC :: blockchain_state_channel_v1:state_channel(),
                              State :: state()) -> ok | {error, any()}.
overwrite_state_channel(SC, #state{db=DB, cf=CF}) ->
    SCID = blockchain_state_channel_v1:id(SC),
    ToInsert = erlang:term_to_binary([SC]),
    rocksdb:put(DB, CF, SCID, ToInsert, [{sync, true}]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).
-endif.
