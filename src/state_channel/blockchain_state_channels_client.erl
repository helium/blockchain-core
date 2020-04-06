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
         packet/1,
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

-spec packet(blockchain_helium_packet_v1:packet()) -> ok.
packet(Packet) ->
    gen_server:cast(?SERVER, {packet, Packet}).

-spec state() -> state().
state() ->
    gen_server:call(?SERVER, state).

%% ------------------------------------------------------------------
%% init, terminate and code_change
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = maps:get(db, Args),
    State = #state{db=DB, swarm=Swarm},
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% gen_server message handling
%% ------------------------------------------------------------------
handle_cast({packet, Packet}, State) ->
    NewState = handle_packet(Packet, State),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    lager:debug("unhandled receive: ~p", [_Msg]),
    {noreply, State}.

handle_call(state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
-spec handle_packet(Packet :: blockchain_helium_packet_v1:packet(),
                    State :: state()) -> state().
handle_packet(Packet, #state{swarm=Swarm}=State) ->
    OUI = blockchain_helium_packet_v1:oui(Packet),
    case find_routing(OUI) of
        {error, _Reason} ->
            lager:error("failed to find router for oui ~p:~p", [OUI, _Reason]),
            State;
        {ok, Peer} ->
            case find_stream(OUI, State) of
                undefined ->
                    %% Do not have a stream open for this oui
                    %% Create one and add to state
                    case blockchain_state_channel_handler:dial(Swarm, Peer, []) of
                        {error, _Reason} ->
                            lager:error("failed to dial ~p:~p", [Peer, _Reason]),
                            %% TODO: retry?
                            State;
                        {ok, NewStream} ->
                            ok = send_packet(Packet, Swarm, NewStream),
                            add_stream(OUI, NewStream, State)
                    end;
                Stream ->
                    %% Found an existing stream for this oui, use that to send packet
                    ok = send_packet(Packet, Swarm, Stream),
                    State
            end
    end.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

-spec send_packet(Packet :: blockchain_helium_packet_v1:packet(),
                  Swarm :: pid(),
                  Stream :: pid()) -> ok.
send_packet(Packet, Swarm, Stream) ->
    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubkeyBin),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_handler:send_packet(Stream, PacketMsg1).

-spec find_stream(OUI :: non_neg_integer(), State :: state()) -> undefined | pid().
find_stream(OUI, #state{streams=Streams}) ->
    maps:get(OUI, Streams, undefined).

-spec add_stream(OUI :: non_neg_integer(), Stream :: pid(), State :: state()) -> state().
add_stream(OUI, Stream, #state{streams=Streams}=State) ->
    State#state{streams=maps:put(OUI, Stream, Streams)}.

-spec find_routing(OUI :: non_neg_integer()) -> {ok, string()} | {error, any()}.
find_routing(OUI) ->
    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Routing} ->
            % TODO: Select an address
            [Address|_] = blockchain_ledger_routing_v1:addresses(Routing),
            {ok, erlang:binary_to_list(Address)}
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).

%% TODO: Add some eunits here...

-endif.
