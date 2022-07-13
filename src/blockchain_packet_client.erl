%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Packet Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_packet_client).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    packet/3,
    send_response/2,
    response/1
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

-include("blockchain_rocks.hrl").
-include("blockchain_vars.hrl").

-include_lib("grpc/autogen/server/state_channel_pb.hrl").

-define(SERVER, ?MODULE).
-define(ROUTING_CACHE, sc_client_routing).
%% 6 hours in seconds
-define(ROUTING_CACHE_TIMEOUT, 60 * 60 * 6).

-record(state, {
    swarm :: pid(),
    swarm_tid :: ets:tab(),
    pubkey_bin :: libp2p_crypto:pubkey_bin(),
    sig_fun :: libp2p_crypto:sig_fun(),
    chain = undefined :: undefined | blockchain:blockchain(),
    streams = #{} :: streams(),
    packets = #{} :: #{pid() => queue:queue(blockchain_helium_packet_v1:packet())},
    waiting = #{} :: waiting(),
    %% TODO GC these
    sc_client_transport_handler :: atom(),
    routers = [] :: [netid_to_oui()]
}).

-type state() :: #state{}.
-type stream_key() :: non_neg_integer() | string().
-type stream_val() :: undefined | dialing | {unverified, pid()} | pid().
-type streams() :: #{stream_key() => stream_val()}.
-type waiting_packet() ::
    {
        Packet :: blockchain_helium_packet_v1:packet(),
        Region :: atom(),
        ReceivedTime :: non_neg_integer()
    }.
-type waiting_key() :: non_neg_integer() | string().
-type waiting() :: #{waiting_key() => [waiting_packet()]}.
-type netid_to_oui() :: {pos_integer(), pos_integer()}.

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

-spec send_response(pid(), blockchain_state_channel_response_v1:response()) -> ok.
send_response(HandlerPid, Response) ->
    blockchain_state_channel_common:send_response(HandlerPid, Response).

-spec packet(
    Packet :: blockchain_helium_packet_v1:packet(), DefaultRouters :: [string()], Region :: atom()
) -> ok.
packet(Packet, DefaultRouters, Region) ->
    gen_server:cast(
        ?SERVER,
        {packet, Packet, DefaultRouters, Region, erlang:system_time(millisecond)}
    ),
    ok.

%% ------------------------------------------------------------------
%% init, terminate and code_change
%% ------------------------------------------------------------------
init(Args) ->
    SCClientTransportHandler =
        application:get_env(
            blockchain,
            sc_client_transport_handler,
            blockchain_packet_handler
        ),
    lager:info("~p init with ~p handler: ~p", [?SERVER, Args, SCClientTransportHandler]),

    Swarm = maps:get(swarm, Args),
    SwarmTID = libp2p_swarm:tid(Swarm),

    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    erlang:send_after(500, self(), post_init),
    State =
        #state{
            swarm = Swarm,
            swarm_tid = SwarmTID,
            pubkey_bin = PubkeyBin,
            sig_fun = SigFun,
            sc_client_transport_handler = SCClientTransportHandler
        },
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% gen_server message handling
%% ------------------------------------------------------------------

handle_cast(
    {packet, Packet, Routers, Region, ReceivedTime},
    #state{chain = Chain} = State
) ->
    State2 = handle_packet_routing(Packet, Chain, Routers, Region, ReceivedTime, State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    lager:debug("unhandled receive: ~p", [_Msg]),
    {noreply, State}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(post_init, #state{chain = undefined} = State) ->
    case blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), post_init),
            {noreply, State};
        Chain ->
            %% Also scan incoming blocks for updates; see add_block event.
            State1 = chain_var_routers_by_netid_to_oui(Chain, State),
            {noreply, State1#state{chain = Chain}}
    end;
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    State1 = chain_var_routers_by_netid_to_oui(NC, State),
    {noreply, State1#state{chain = NC}};
handle_info({dial_fail, AddressOrOUI, _Reason}, State0) ->
    Packets = get_waiting_packet(AddressOrOUI, State0),
    lager:error(
        "failed to dial ~p: ~p dropping ~p packets",
        [AddressOrOUI, _Reason, erlang:length(Packets) + 1]
    ),
    State1 = remove_packet_from_waiting(AddressOrOUI, delete_stream(AddressOrOUI, State0)),
    {noreply, State1};
handle_info({dial_success, AddressOrOUI, Stream}, #state{chain = undefined} = State) ->
    %% We somehow lost the chain here, likely we were restarting and haven't gotten it yet
    %% There really isn't anything we can do about it, but we should probably keep this stream
    %% (if we don't already have it)
    %%
    %% NOTE: We don't keep the packets we were waiting on as we lost the chain, maybe we should?
    NewState =
        case find_stream(AddressOrOUI, State) of
            undefined ->
                erlang:monitor(process, Stream),
                add_stream(AddressOrOUI, Stream, State);
            _ ->
                State
        end,
    {noreply, NewState};
handle_info({dial_success, OUIOrAddress, Stream}, State0) ->
    erlang:monitor(process, Stream),
    State1 = add_stream(OUIOrAddress, Stream, State0),
    {noreply, maybe_send_packets(OUIOrAddress, Stream, State1)};
handle_info(
    {'DOWN', _Ref, process, Pid, _},
    #state{streams = Streams, packets = Packets} = State
) ->
    FilteredStreams =
        maps:filter(
            fun
                (_Name, {unverified, Stream}) ->
                    Stream /= Pid;
                (_Name, Stream) ->
                    Stream /= Pid
            end,
            Streams
        ),

    %% Keep the streams which don't have downed pid, given we're monitoring correctly
    FilteredPackets =
        maps:filter(fun(StreamPid, _PacketQueue) -> StreamPid /= Pid end, Packets),

    {noreply, State#state{streams = FilteredStreams, packets = FilteredPackets}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
-spec handle_packet(
    Packet :: blockchain_helium_packet_v1:packet(),
    RoutesOrAddresses :: [string()] | [blockchain_ledger_routing_v1:routing()],
    Region :: atom(),
    ReceivedTime :: non_neg_integer(),
    State :: state()
) ->
    state().
handle_packet(
    Packet,
    RoutesOrAddresses,
    Region,
    ReceivedTime,
    #state{swarm_tid = SwarmTID, sc_client_transport_handler = SCClientTransportHandler} =
        State0
) ->
    lager:info(
        "handle_packet ~p to ~p with handler ~p",
        [
            lager:pr(Packet, blockchain_helium_packet_v1),
            print_routes(RoutesOrAddresses),
            SCClientTransportHandler
        ]
    ),
    lists:foldl(
        fun(RouteOrAddress, StateAcc) ->
            StreamKey =
                case blockchain_ledger_routing_v1:is_routing(RouteOrAddress) of
                    false ->
                        {address, RouteOrAddress};
                    true ->
                        {oui, blockchain_ledger_routing_v1:oui(RouteOrAddress)}
                end,

            case StreamKey of
                {address, Address} ->
                    case find_stream(Address, StateAcc) of
                        undefined ->
                            lager:debug(
                                "stream undef dialing first, address: ~p",
                                [Address]
                            ),
                            ok = dial(SCClientTransportHandler, SwarmTID, RouteOrAddress),
                            add_packet_to_waiting(
                                Address,
                                {Packet, Region, ReceivedTime},
                                add_stream(Address, dialing, StateAcc)
                            );
                        dialing ->
                            lager:debug(
                                "stream is still dialing queueing packet, address: ~p",
                                [Address]
                            ),
                            add_packet_to_waiting(
                                Address, {Packet, Region, ReceivedTime}, StateAcc
                            );
                        {unverified, _Stream} ->
                            %% queue it until we get a banner
                            lager:debug(
                                "unverified stream, add_packet_to_waiting, address: ~p",
                                [Address]
                            ),
                            add_packet_to_waiting(
                                Address, {Packet, Region, ReceivedTime}, StateAcc
                            );
                        Stream ->
                            lager:debug(
                                "stream ~p, send_packet_when_v1, address: ~p",
                                [Stream, Address]
                            ),
                            send_packet_immediately(Stream, Packet, Region, ReceivedTime, StateAcc)
                    end;
                {oui, OUI} ->
                    case find_stream(OUI, StateAcc) of
                        undefined ->
                            lager:debug("stream undef dialing first, oui: ~p", [OUI]),
                            ok = dial(SCClientTransportHandler, SwarmTID, RouteOrAddress),
                            add_packet_to_waiting(
                                OUI,
                                {Packet, Region, ReceivedTime},
                                add_stream(OUI, dialing, StateAcc)
                            );
                        dialing ->
                            lager:debug(
                                "stream is still dialing queueing packet, oui: ~p",
                                [OUI]
                            ),
                            add_packet_to_waiting(OUI, {Packet, Region, ReceivedTime}, StateAcc);
                        {unverified, _Stream} ->
                            %% queue it until we get a banner
                            lager:debug(
                                "unverified stream, add_packet_to_waiting, oui: ~p",
                                [OUI]
                            ),
                            add_packet_to_waiting(OUI, {Packet, Region, ReceivedTime}, StateAcc);
                        Stream ->
                            lager:debug(
                                "got stream: ~p, send_packet_or_offer, oui: ~p",
                                [Stream, OUI]
                            ),
                            send_packet_immediately(Stream, Packet, Region, ReceivedTime, StateAcc)
                    end
            end
        end,
        State0,
        RoutesOrAddresses
    ).

-spec find_stream(AddressOrOUI :: stream_key(), State :: state()) -> stream_val().
find_stream(AddressOrOUI, #state{streams = Streams}) ->
    maps:get(AddressOrOUI, Streams, undefined).

-spec add_stream(
    AddressOrOUI :: non_neg_integer() | string(),
    Stream :: pid() | dialing,
    State :: state()
) ->
    state().
add_stream(AddressOrOUI, Stream, #state{streams = Streams} = State) ->
    State#state{streams = maps:put(AddressOrOUI, {unverified, Stream}, Streams)}.

-spec delete_stream(AddressOrOUI :: non_neg_integer() | string(), State :: state()) ->
    state().
delete_stream(AddressOrOUI, #state{streams = Streams} = State) ->
    State#state{streams = maps:remove(AddressOrOUI, Streams)}.

lookup_stream_id(Pid, State) ->
    Result =
        maps:filter(
            fun
                (_Name, {unverified, Stream}) ->
                    Stream == Pid;
                (_Name, Stream) ->
                    Stream == Pid
            end,
            State#state.streams
        ),
    case maps:size(Result) of
        0 ->
            undefined;
        1 ->
            %% more than one is an error
            hd(maps:keys(Result))
    end.

verify_stream(Stream, #state{streams = Streams} = State) ->
    AddressOrOUI = lookup_stream_id(Stream, State),
    State#state{streams = maps:update(AddressOrOUI, Stream, Streams)}.

-spec get_waiting_packet(AddressOrOUI :: waiting_key(), State :: state()) ->
    [waiting_packet()].
get_waiting_packet(AddressOrOUI, #state{waiting = Waiting}) ->
    maps:get(AddressOrOUI, Waiting, []).

-spec add_packet_to_waiting(
    AddressOrOUI :: waiting_key(),
    WaitingPacket :: waiting_packet(),
    State :: state()
) ->
    state().
add_packet_to_waiting(
    AddressOrOUI,
    {Packet, Region, ReceivedTime},
    #state{waiting = Waiting} = State
) ->
    Q0 = get_waiting_packet(AddressOrOUI, State),
    lager:debug("add_packet_to_waiting, AddressOrOUI: ~p", [AddressOrOUI]),
    %% We should only ever keep 9+1 packet (for each Router)
    Q1 = lists:sublist(Q0, 9),
    State#state{
        waiting =
            %%
            maps:put(AddressOrOUI, Q1 ++ [{Packet, Region, ReceivedTime}], Waiting)
    }.

-spec remove_packet_from_waiting(AddressOrOUI :: waiting_key(), State :: state()) ->
    state().
remove_packet_from_waiting(AddressOrOUI, #state{waiting = Waiting} = State) ->
    State#state{waiting = maps:remove(AddressOrOUI, Waiting)}.

-spec find_routing(
    Packet :: blockchain_helium_packet_v1:packet(),
    Chain :: blockchain:blockchain()
) ->
    {ok, [blockchain_ledger_routing_v1:routing()]} | {error, any()}.
find_routing(_Packet, undefined) ->
    {error, no_chain};
find_routing(Packet, Chain) ->
    %% transitional shim for ignoring on-chain OUIs
    case application:get_env(blockchain, use_oui_routers, true) of
        true ->
            RoutingInfo = blockchain_helium_packet_v1:routing_info(Packet),
            e2qc:cache(
                ?ROUTING_CACHE,
                RoutingInfo,
                ?ROUTING_CACHE_TIMEOUT,
                fun() ->
                    Ledger = blockchain:ledger(Chain),
                    case blockchain_ledger_v1:find_routing_for_packet(Packet, Ledger) of
                        {error, _} = Error ->
                            Error;
                        {ok, Routes} ->
                            {ok, Routes}
                    end
                end
            );
        false ->
            {error, oui_routing_disabled}
    end.

-spec dial(
    SCClientTransportHandler :: atom(),
    SwarmTID :: ets:tab(),
    Address :: string() | blockchain_ledger_routing_v1:routing()
) ->
    ok.
dial(SCClientTransportHandler, SwarmTID, Address) when is_list(Address) ->
    Self = self(),
    erlang:spawn(fun() ->
        {P, R} =
            erlang:spawn_monitor(fun() ->
                case
                    SCClientTransportHandler:dial(
                        SwarmTID,
                        Address,
                        []
                    )
                of
                    {error, _Reason} ->
                        Self ! {dial_fail, Address, _Reason};
                    {ok, Stream} ->
                        unlink(Stream),
                        Self ! {dial_success, Address, Stream}
                end
            end),
        receive
            {'DOWN', R, process, P, normal} ->
                ok;
            {'DOWN', R, process, P, _Reason} ->
                Self ! {dial_fail, Address, _Reason}
        after application:get_env(blockchain, sc_packet_dial_timeout, 30000) ->
            erlang:exit(P, kill),
            Self ! {dial_fail, Address, timeout}
        end
    end),
    ok;
dial(SCClientTransportHandler, SwarmTID, Route) ->
    Self = self(),
    erlang:spawn(fun() ->
        OUI = blockchain_ledger_routing_v1:oui(Route),
        {P, R} =
            erlang:spawn_monitor(fun() ->
                Dialed =
                    lists:foldl(
                        fun
                            (
                                _PubkeyBin,
                                {dialed, _} = Acc
                            ) ->
                                Acc;
                            (PubkeyBin, not_dialed) ->
                                Address =
                                    libp2p_crypto:pubkey_bin_to_p2p(PubkeyBin),
                                case
                                    SCClientTransportHandler:dial(
                                        SwarmTID,
                                        Address,
                                        []
                                    )
                                of
                                    {error, _Reason} ->
                                        lager:error(
                                            "failed to dial ~p:~p",
                                            [
                                                Address,
                                                _Reason
                                            ]
                                        ),
                                        not_dialed;
                                    {ok, Stream} ->
                                        unlink(Stream),
                                        {dialed, Stream}
                                end
                        end,
                        not_dialed,
                        blockchain_ledger_routing_v1:addresses(Route)
                    ),
                case Dialed of
                    not_dialed ->
                        Self ! {dial_fail, OUI, failed};
                    {dialed, Stream} ->
                        Self ! {dial_success, OUI, Stream}
                end
            end),
        receive
            {'DOWN', R, process, P, normal} ->
                ok;
            {'DOWN', R, process, P, _Reason} ->
                Self ! {dial_fail, OUI, failed}
        after application:get_env(blockchain, sc_packet_dial_timeout, 30000) ->
            erlang:exit(P, kill),
            Self ! {dial_fail, OUI, timeout}
        end
    end),
    ok.

-spec send_packet(
    PubkeyBin :: libp2p_crypto:pubkey_bin(),
    SigFun :: libp2p_crypto:sig_fun(),
    Stream :: pid(),
    Packet :: blockchain_helium_packet_v1:packet(),
    Region :: atom(),
    ReceivedTime :: non_neg_integer()
) ->
    ok.
send_packet(PubkeyBin, SigFun, Stream, Packet, Region, ReceivedTime) ->
    HoldTime = erlang:system_time(millisecond) - ReceivedTime,
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubkeyBin, Region, HoldTime),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_common:send_packet(Stream, PacketMsg1).

-spec send_packet_immediately(
    Stream :: pid(),
    Packet :: blockchain_helium_packet_v1:packet(),
    Region :: atom(),
    ReceivedTime :: non_neg_integer(),
    State :: #state{}
) ->
    #state{}.
send_packet_immediately(
    Stream,
    Packet,
    Region,
    ReceivedTime,
    #state{pubkey_bin = PubkeyBin, sig_fun = SigFun} = State
) ->
    ok = send_packet(PubkeyBin, SigFun, Stream, Packet, Region, ReceivedTime),
    State.

-spec maybe_send_packets(
    AddressOrOUI :: non_neg_integer() | string(),
    HandlerPid :: pid(),
    #state{}
) -> #state{}.
maybe_send_packets(AddressOrOUI, HandlerPid, State) ->
    Packets = get_waiting_packet(AddressOrOUI, State),
    State1 = lists:foldl(
        fun({Packet, Region, ReceivedTime}, StateAcc) ->
            send_packet_immediately(HandlerPid, Packet, Region, ReceivedTime, StateAcc)
        end,
        State,
        Packets
    ),
    verify_stream(HandlerPid, remove_packet_from_waiting(AddressOrOUI, State1)).

%% ------------------------------------------------------------------
%% DB functions
%% ------------------------------------------------------------------

-spec chain_var_routers_by_netid_to_oui(
    Chain :: undefined | blockchain:blockchain(),
    State :: state()
) ->
    State1 :: state().
chain_var_routers_by_netid_to_oui(undefined, State) ->
    Ledger = blockchain:ledger(),
    chain_var_ledger_routers_by_netid_to_oui(Ledger, State);
chain_var_routers_by_netid_to_oui(Chain, State) ->
    Ledger = blockchain:ledger(Chain),
    chain_var_ledger_routers_by_netid_to_oui(Ledger, State).

-spec chain_var_ledger_routers_by_netid_to_oui(
    Ledger :: blockchain:ledger(),
    State :: state()
) ->
    State1 :: state().
chain_var_ledger_routers_by_netid_to_oui(Ledger, State) ->
    Routers =
        case blockchain_ledger_v1:config(?routers_by_netid_to_oui, Ledger) of
            {ok, Bin} ->
                binary_to_term(Bin);
            _ ->
                []
        end,
    State#state{routers = Routers}.

-spec handle_packet_routing(
    Packet :: blockchain_helium_packet_v1:packet(),
    Chain :: blockchain:blockchain(),
    DefaultRouters :: [string()] | [blockchain_ledger_routing_v1:routing()],
    Region :: atom(),
    ReceivedTime :: non_neg_integer(),
    State :: state()
) ->
    state().
handle_packet_routing(Packet, Chain, DefaultRouters, Region, ReceivedTime, State) ->
    case find_routing(Packet, Chain) of
        {error, _Reason} ->
            lager:warning(
                "failed to find router for join packet with routing information ~p:~p, trying default routers",
                [blockchain_helium_packet_v1:routing_info(Packet), _Reason]
            ),
            handle_packet(Packet, DefaultRouters, Region, ReceivedTime, State);
        {ok, Routes} ->
            lager:debug("found routes ~p", [Routes]),
            handle_packet(Packet, Routes, Region, ReceivedTime, State)
    end.

print_routes(RoutesOrAddresses) ->
    lists:map(
        fun(RouteOrAddress) ->
            case blockchain_ledger_routing_v1:is_routing(RouteOrAddress) of
                true ->
                    "OUI " ++
                        integer_to_list(blockchain_ledger_routing_v1:oui(RouteOrAddress));
                false ->
                    RouteOrAddress
            end
        end,
        RoutesOrAddresses
    ).
