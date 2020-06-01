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


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("blockchain.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          db :: rocksdb:db_handle(),
          swarm :: pid(),
          state_channels = #{} :: state_channels(),
          streams = #{} :: streams()
         }).

-type state() :: #state{}.
-type state_channels() :: #{binary() => blockchain_state_channel_v1:state_channel()}.
-type streams() :: #{non_neg_integer() => pid()}.

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
-spec handle_packet(Packet :: blockchain_helium_packet_v1:packet(),
                    DefaultRouters :: [string()],
                    Region :: atom(),
                    State :: state()) -> state().
handle_packet(Packet, DefaultRouters, Region, State=#state{swarm=Swarm}) ->
    case find_routing(Packet) of
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
                                                ok = send_packet(Packet, Swarm, NewStream, Region),
                                                add_stream(Router, NewStream, State)
                                        end;
                                    Stream ->
                                        ok = send_packet(Packet, Swarm, Stream, Region),
                                        StateAcc
                                end
                        end, State, DefaultRouters);
        {ok, Routes} ->
            lists:foldl(fun(Route, StateAcc) ->
                                send_to_route(Packet, Route, Region, StateAcc)
                        end, State, Routes)
    end.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

-spec send_packet(Packet :: blockchain_helium_packet_v1:packet(),
                  Swarm :: pid(),
                  Stream :: pid(),
                  Region :: atom()  ) -> ok.
send_packet(Packet, Swarm, Stream, Region) ->
    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubkeyBin, Region),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_handler:send_packet(Stream, PacketMsg1).

-spec find_stream(OUIOrDefaultRouter :: non_neg_integer() | string(), State :: state()) -> undefined | pid().
find_stream(OUI, #state{streams=Streams}) ->
    maps:get(OUI, Streams, undefined).

-spec add_stream(OUIOrDefaultRouter :: non_neg_integer() | string(), Stream :: pid(), State :: state()) -> state().
add_stream(OUI, Stream, #state{streams=Streams}=State) ->
    State#state{streams=maps:put(OUI, Stream, Streams)}.

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
