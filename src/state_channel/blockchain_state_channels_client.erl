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
         credits/1,
         packet/1,
         packets/0,
         state_channel_update/1,
         state/0
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
          packets = [] :: packets()
         }).

-type state() :: #state{}.
-type state_channels() :: #{binary() => blockchain_state_channel_v1:state_channel()}.
-type packet_key() :: {DevAddr :: binary(), SeqNum :: pos_integer(), MIC :: binary()}.
-type packet_info() :: {Key :: packet_key(), Packet :: blockchain_helium_packet_v1:packet()}.
-type packets() :: [packet_info()].

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits(ID :: blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
credits(ID) ->
    gen_server:call(?SERVER, {credits, ID}, infinity).

-spec packet(packet_info()) -> ok.
packet(PacketInfo) ->
    gen_server:cast(?SERVER, {packet, PacketInfo}).

-spec packets() -> packets().
packets() ->
    gen_server:call(?SERVER, packets).

-spec state() -> state().
state() ->
    gen_server:call(?SERVER, state).

-spec state_channel_update(SCUpdate :: blockchain_state_channel_update_v1:state_channel_update()) -> ok.
state_channel_update(SCUpdate) ->
    gen_server:cast(?SERVER, {state_channel_update, SCUpdate}).

%% ------------------------------------------------------------------
%% init, terminate and code_change
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = maps:get(db, Args),
    State = #state{db=DB, swarm=Swarm},
    schedule_packet_handling(),
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% gen_server message handling
%% ------------------------------------------------------------------

handle_cast({state_channel_update, SCUpdate}, #state{db=DB, state_channels=SCs}=State) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    ID = blockchain_state_channel_v1:id(UpdatedSC),
    lager:debug("received state channel update for ~p", [ID]),
    NewState = case validate_state_channel_update(maps:get(ID, SCs, undefined), UpdatedSC) of
                   {error, _Reason} ->
                       lager:warning("state channel ~p is invalid ~p", [UpdatedSC, _Reason]),
                       State;
                   ok ->
                       ok = blockchain_state_channel_v1:save(DB, UpdatedSC),
                       State#state{state_channels=maps:put(ID, UpdatedSC, SCs)}
               end,
    {noreply, NewState};
handle_cast({packet, PacketInfo}, #state{packets=[], swarm=Swarm}=State) ->
    %% process this packet immediately
    NewState = case handle_packet(PacketInfo, Swarm) of
                   {error, _} ->
                       State#state{packets=[PacketInfo]};
                   {ok, _PacketKey} ->
                       %% Already done processing this packet
                       State
               end,
    {noreply, NewState};
handle_cast({packet, PacketInfo}, #state{packets=Packets}=State) ->
    %% got a packet while still processing packets
    %% add this packet at the end and continue packet handling
    schedule_packet_handling(),
    NewPackets = Packets ++ [PacketInfo],
    {noreply, State#state{packets=NewPackets}};
handle_cast(_Msg, State) ->
    lager:debug("unhandled receive: ~p", [_Msg]),
    schedule_packet_handling(),
    {noreply, State}.


handle_call({credits, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
                undefined ->
                    {error, not_found};
                SC ->
                    {ok, blockchain_state_channel_v1:credits(SC)}
            end,
    {reply, Reply, State};
handle_call(packets, _From, #state{packets=Packets}=State) ->
    {reply, Packets, State};
handle_call(state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(process_packet, #state{packets=[]}=State) ->
    %% Don't have any packets to process, reschedule
    lager:debug("got process_packet, no packets to process"),
    {noreply, State};
handle_info(process_packet, #state{swarm=Swarm, packets=[PacketInfo | Remaining]=Packets}=State) ->
    %% Processing the first packet in queue
    %% It has no pending request, try it
    lager:debug("got process_packet, processing, packets: ~p", [PacketInfo]),
    NewState = case handle_packet(PacketInfo, Swarm) of
                   {error, _} ->
                       %% Send this packet to the back of the queue,
                       %% XXX: Could also drop it probably?
                       State#state{packets=Remaining ++ [PacketInfo]};
                   {ok, PacketKey} ->
                       %% This packet got processed, remove it from state
                       State#state{packets=lists:keydelete(PacketKey, 1, Packets)}
               end,
    schedule_packet_handling(),
    {noreply, NewState}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

schedule_packet_handling() ->
    erlang:send_after(timer:seconds(1), self(), process_packet).

-spec validate_state_channel_update(OldStateChannel :: blockchain_state_channel_v1:state_channel() | undefined,
                                    NewStateChannel :: blockchain_state_channel_v1:state_channel()) -> ok | {error, any()}.
validate_state_channel_update(undefined, NewStateChannel) ->
    blockchain_state_channel_v1:validate(NewStateChannel);
validate_state_channel_update(OldStateChannel, NewStateChannel) ->
    case blockchain_state_channel_v1:validate(NewStateChannel) of
        {error, _}=Error ->
            Error;
        ok ->
            NewNonce = blockchain_state_channel_v1:nonce(NewStateChannel),
            OldNonce = blockchain_state_channel_v1:nonce(OldStateChannel),
            case NewNonce > OldNonce of
                false -> {error, {bad_nonce, NewNonce, OldNonce}};
                true -> ok
            end
    end.

-spec handle_packet(PacketInfo :: packet_info(), Swarm :: pid()) -> {ok, packet_key()} | {error, any()}.
handle_packet({PacketKey, Packet}, Swarm) ->
    OUI = blockchain_helium_packet_v1:oui(Packet),
    case find_routing(OUI) of
        {error, _Reason} ->
            lager:error("failed to find router for oui ~p:~p", [OUI, _Reason]),
            {error, _Reason};
        {ok, Peer} ->
            case blockchain_state_channel_handler:dial(Swarm, Peer, []) of
                {error, _Reason} ->
                    lager:error("failed to dial ~p:~p", [Peer, _Reason]),
                    {error, _Reason};
                {ok, Stream} ->
                    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubkeyBin),
                    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
                    ok = blockchain_state_channel_handler:send_packet(Stream, PacketMsg1),
                    {ok, PacketKey}
            end
    end.

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
