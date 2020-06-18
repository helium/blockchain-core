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
         packet/3,
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

-record(state, {db :: rocksdb:db_handle(),
                swarm :: pid(),
                streams = #{} :: streams(),
                waiting = #{}}).

-type state() :: #state{}.
-type streams() :: #{non_neg_integer() | string() => pid()}.

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

-spec packet(blockchain_helium_packet_v1:packet(), [string()], atom()) -> ok.
packet(Packet, DefaultRouters, Region) ->
    case find_routing(Packet, blockchain_worker:blockchain()) of
        {error, _Reason} ->
            lager:error("failed to find router for packet with routing information ~p:~p, trying default routers",
                        [blockchain_helium_packet_v1:routing_info(Packet), _Reason]),
            gen_server:cast(?SERVER, {default_routers, Packet, DefaultRouters, Region});
        {ok, Routes} ->
            gen_server:cast(?SERVER, {routes, Packet, Routes, Region})
    end.

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
handle_cast({default_routers, Packet, DefaultRouters, Region}, #state{swarm=Swarm}=State0) ->
    State1 = lists:foldl(
        fun(Address, StateAcc) ->
            case find_stream(Address, StateAcc) of
                undefined ->
                    ok = dial_address_and_send_packet(Swarm, Address, Packet, Region),
                    add_stream(Address, dialing, StateAcc);
                dialing ->
                    queue_packet(Address, {Packet, Region}, StateAcc);
                Stream ->
                    ok = send_packet(Swarm, Stream, Packet, Region),
                    StateAcc
            end
        end,
        State0,
        DefaultRouters
    ),
    {noreply, State1};
handle_cast({routes, Packet, Routes, Region}, #state{swarm=Swarm}=State0) ->
    State1 = lists:foldl(
        fun(Route, StateAcc) ->
            OUI = blockchain_ledger_routing_v1:oui(Route),
            case find_stream(OUI, StateAcc) of
                undefined ->
                    ok = dial_route_and_send_packet(Swarm, Route, Packet, Region),
                    add_stream(OUI, dialing, StateAcc);
                dialing ->
                    queue_packet(OUI, {Packet, Region}, StateAcc);
                Stream ->
                    ok = send_packet(Swarm, Stream, Packet, Region),
                    StateAcc
            end
        end,
        State0,
        Routes
    ),
    {noreply, State1};
handle_cast(_Msg, State) ->
    lager:debug("unhandled receive: ~p", [_Msg]),
    {noreply, State}.

handle_call(state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info({dial_fail, AddressOrOUI, _Reason}, State0) ->
    Packets = get_queued_packet(AddressOrOUI, State0),
    lager:error("failed to dial ~p: ~p dropping ~p packets", [AddressOrOUI, _Reason, erlang:length(Packets)+1]),
    State1 = delete_queued_packet(AddressOrOUI, delete_stream(AddressOrOUI, State0)),
    {noreply, State1};
handle_info({dial_success, AddressOrOUI, Stream}, #state{swarm=Swarm}=State0) ->
    Packets = get_queued_packet(AddressOrOUI, State0),
    lists:foreach(
        fun({Packet, Region}) ->
            ok = send_packet(Swarm, Stream, Packet, Region)
        end,
        Packets
    ),
    erlang:monitor(process, Stream),
    State1 = add_stream(AddressOrOUI, Stream, delete_queued_packet(AddressOrOUI, State0)),
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

-spec find_stream(AddressOrOUI :: string() | non_neg_integer(), State :: state()) -> undefined | dialing |pid().
find_stream(AddressOrOUI, #state{streams=Streams}) ->
    maps:get(AddressOrOUI, Streams, undefined).

-spec add_stream(AddressOrOUI :: non_neg_integer() | string(), Stream :: pid() | dialing, State :: state()) -> state().
add_stream(AddressOrOUI, Stream, #state{streams=Streams}=State) ->
    State#state{streams=maps:put(AddressOrOUI, Stream, Streams)}.

-spec delete_stream(AddressOrOUI :: non_neg_integer() | string(), State :: state()) -> state().
delete_stream(AddressOrOUI, #state{streams=Streams}=State) ->
    State#state{streams=maps:remove(AddressOrOUI, Streams)}.

-spec get_queued_packet(AddressOrOUI :: non_neg_integer() | string(), State :: state()) -> [{Packet :: blockchain_helium_packet_v1:packet(), Region :: atom()}].
get_queued_packet(AddressOrOUI, #state{waiting=Waiting}) ->
    maps:get(AddressOrOUI, Waiting, []).

-spec queue_packet(AddressOrOUI :: non_neg_integer() | string(), {Packet :: blockchain_helium_packet_v1:packet(), Region :: atom()}, State :: state()) -> state().
queue_packet(AddressOrOUI, {Packet, Region}, #state{waiting=Waiting}=State) ->
    Q = get_queued_packet(AddressOrOUI, State),
    State#state{waiting=maps:put(AddressOrOUI, Q ++ [{Packet, Region}], Waiting)}.

-spec delete_queued_packet(AddressOrOUI :: non_neg_integer() | string(), State :: state()) -> state().
delete_queued_packet(AddressOrOUI, #state{waiting=Waiting}=State) ->
    State#state{waiting=maps:remove(AddressOrOUI, Waiting)}.

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

-spec dial_address_and_send_packet(pid(), string(), blockchain_helium_packet_v1:packet(), atom()) -> ok.
dial_address_and_send_packet(Swarm, Address, Packet, Region) ->
    Self = self(),
    erlang:spawn(fun() ->
        case blockchain_state_channel_handler:dial(Swarm, Address, []) of
            {error, _Reason} ->
                Self ! {dial_fail, Address, _Reason};
            {ok, Stream} ->
                unlink(Stream),
                ok = send_packet(Swarm, Stream, Packet, Region),
                Self ! {dial_success, Address, Stream, Region}
        end
    end),
    ok.

-spec dial_route_and_send_packet(pid(), blockchain_ledger_routing_v1:routing(), blockchain_helium_packet_v1:packet(), atom()) -> ok.
dial_route_and_send_packet(Swarm, Route, Packet, Region) ->
    Self = self(),
    erlang:spawn(fun() ->
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
                ok = send_packet(Swarm, Stream, Packet, Region),
                Self ! {dial_success, OUI, Stream, Region}
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

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).
-endif.
