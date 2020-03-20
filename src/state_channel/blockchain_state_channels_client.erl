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
         packet_details/0,
         response/1,
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
          db :: rocksdb:db_handle() | undefined,
          swarm :: pid(),
          state_channels = #{} :: state_channels(),
          packet_details = [] :: packet_details()
         }).

-type state() :: #state{}.
-type pending() :: undefined | {pid(), blockchain_state_channel_request_v1:request(), reference()}.
-type state_channels() :: #{binary() => blockchain_state_channel_v1:state_channel()}.
-type packet_info() :: {Packet :: blockchain_helium_packet_v1:packet(),
                        DevAddr :: binary(),
                        SeqNum :: pos_integer(),
                        MIC :: binary()}.
-type packet_details() :: [{packet_info(), pending()}].

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

-spec packet_details() -> packet_details().
packet_details() ->
    gen_server:call(?SERVER, packet_details).

-spec state() -> state().
state() ->
    gen_server:call(?SERVER, state).

-spec response(Resp :: blockchain_state_channel_response_v1:reponse()) -> ok.
response(Resp) ->
    gen_server:cast(?SERVER, {response, Resp}).

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
handle_cast({packet, PacketInfo}, #state{packet_details=[], swarm=Swarm}=State) ->
    %% process this packet immediately
    NewState = case handle_packet(PacketInfo, Swarm) of
                   {error, _} ->
                       State#state{packet_details=[{PacketInfo, undefined}]};
                   {ok, Pending} ->
                       State#state{packet_details=[{PacketInfo, Pending}]}
               end,
    {noreply, NewState};
handle_cast({packet, PacketInfo}, #state{packet_details=PacketDetails}=State) ->
    %% got a packet while still processing packets
    %% add this packet at the end and continue packet handling
    schedule_packet_handling(),
    NewPacketDetails = PacketDetails ++ [{PacketInfo, undefined}],
    {noreply, State#state{packet_details=NewPacketDetails}};
handle_cast({response, Resp}, #state{packet_details=[]}=State) ->
    %% Got a response when there are no packets
    lager:debug("dropping response: ~p, no packets...", [Resp]),
    {noreply, State};
handle_cast({response, Resp}, #state{db=DB, swarm=Swarm}=State) when DB /= undefined andalso Swarm /= undefined ->
    %% Got a response when there are packets
    %% Check if any of the pending request match this resp hash
    lager:debug("got response: ~p", [Resp]),
    NewState = handle_response(Resp, State),
    {noreply, NewState};
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
handle_call(packet_details, _From, #state{packet_details=PacketDetails}=State) ->
    {reply, PacketDetails, State};
handle_call(state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(process_packet, #state{packet_details=[]}=State) ->
    %% Don't have any packets to process, reschedule
    lager:debug("got process_packet, no packets to process"),
    {noreply, State};
handle_info(process_packet, #state{swarm=Swarm,
                                   packet_details=[{PacketInfo, undefined} | Remaining]}=State) ->
    %% Processing the first packet in queue
    %% It has no pending request, try it
    lager:debug("got process_packet, processing, packet_details: ~p, pending: undefined", [PacketInfo]),
    NewState = case handle_packet(PacketInfo, Swarm) of
                   {error, _} ->
                       %% Send this packet to the back of the queue,
                       %% Could also drop it probably
                       State#state{packet_details=Remaining ++ [{PacketInfo, undefined}]};
                   {ok, Pending} ->
                       %% This packet got processed, wait for a response, whenever
                       %% that happens, put it at the back of the queue
                       State#state{packet_details=Remaining ++ [{PacketInfo, Pending}]}
               end,
    schedule_packet_handling(),
    {noreply, NewState};
handle_info(process_packet, #state{packet_details=[{PacketDetails, Pending} | Remaining]}=State) ->
    %% The packet at the head already has a pending request presumably waiting for
    %% a response message, keep cycling the queue
    lager:debug("got process_packet, packet: ~p, pending: ~p", [PacketDetails, Pending]),
    NewState = State#state{packet_details=Remaining ++ [{PacketDetails, Pending}]},
    schedule_packet_handling(),
    {noreply, NewState};
handle_info({req_timeout, PacketInfo}, #state{packet_details=PacketDetails}=State) ->
    %% Request timed out
    lager:debug("req_timeout for packet_info: ~p, enqueued packets: ~p, rescheduling...",
               [PacketInfo, PacketDetails]),
    %% If we already had this packet remove it first
    NewPacketDetails0 = lists:keydelete(PacketInfo, 1, PacketDetails),
    %% Move this packet to the end of the queue.
    NewState = State#state{packet_details=NewPacketDetails0 ++ [{PacketInfo, undefined}]},
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

-spec handle_packet(PacketInfo :: packet_info(),
                    Swarm :: pid()) -> {ok, pending()} | {error, any()}.
handle_packet({Packet, DevAddr, SeqNum, MIC}=PacketInfo, Swarm) ->
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
                    Payload = blockchain_helium_packet_v1:payload(Packet),
                    Amount = calculate_dc_amount(PubkeyBin, OUI, Payload),
                    Req0 = blockchain_state_channel_request_v1:new(PubkeyBin,
                                                                   Amount,
                                                                   erlang:byte_size(Payload),
                                                                   DevAddr,
                                                                   SeqNum,
                                                                   MIC),
                    Req = blockchain_state_channel_request_v1:sign(Req0, SigFun),
                    lager:info("handle_packet, Req: ~p", [Req]),
                    lager:debug("sending payment req ~p to ~p", [Req, Peer]),
                    ok = blockchain_state_channel_handler:send_request(Stream, Req),
                    TimeRef = erlang:send_after(timer:seconds(30), self(), {req_timeout, PacketInfo}),
                    {ok, {Stream, Req, TimeRef}}
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

-spec calculate_dc_amount(PubkeyBin :: libp2p_crypto:pubkey_to_bin(),
                          OUI :: non_neg_integer(),
                          Payload :: binary()) -> non_neg_integer().
calculate_dc_amount(PubkeyBin, OUI, Payload) ->
    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    Size = erlang:byte_size(Payload),
    Price = blockchain_state_channel_utils:calculate_dc_amount(Size),
    case blockchain_ledger_v1:find_gateway_info(PubkeyBin, Ledger) of
        {error, _Reason} ->
            lager:warning("failed to find gateway ~p: ~p", [PubkeyBin, _Reason]),
            Price;
        {ok, GWInfo} ->
            case blockchain_ledger_gateway_v2:oui(GWInfo) of
                OUI -> 0;
                _ -> Price
            end
    end.

-spec check_pending_request(SC :: blockchain_state_channel_v1:state_channel() | undefined,
                            SCUpdate :: blockchain_state_channel_update_v1:state_channel_update(),
                            Packet :: blockchain_helium_packet_v1:packet(),
                            Req :: blockchain_state_channel_request_v1:request(),
                            PubkeyBin :: libp2p_crypto:pubkey_bin()) -> ok | {error, any()}.
check_pending_request(SC, SCUpdate, Packet, Req, PubkeyBin) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    case {check_root_hash(Packet, SCUpdate, PubkeyBin), check_balance(Req, SC, UpdatedSC)} of
        {false, _} -> {error, bad_root_hash};
        {_, false} -> {error, wrong_balance};
        {true, true} -> ok
    end.

-spec check_root_hash(Packet :: blockchain_helium_packet_v1:packet(),
                      SCUpdate :: blockchain_state_channel_update_v1:state_channel_update(),
                      PubkeyBin :: libp2p_crypto:pubkey_bin()) -> boolean().
check_root_hash(Packet, SCUpdate, PubkeyBin) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    RootHash = blockchain_state_channel_v1:root_hash(UpdatedSC),
    Hash = blockchain_state_channel_update_v1:previous_hash(SCUpdate),
    PayloadSize = erlang:byte_size(blockchain_helium_packet_v1:payload(Packet)),
    % TODO
    Value = <<PubkeyBin/binary, PayloadSize>>,
    skewed:verify(skewed:hash_value(Value), [Hash], RootHash).

-spec check_balance(Req :: blockchain_state_channel_request_v1:request(),
                    SC :: blockchain_state_channel_v1:state_channel() | undefined,
                    UpdateSC :: blockchain_state_channel_v1:state_channel()) -> boolean().
check_balance(Req, SC, UpdateSC) ->
    ReqPayee = blockchain_state_channel_request_v1:payee(Req),
    ReqPayloadSize = blockchain_state_channel_request_v1:payload_size(Req),
    case blockchain_state_channel_v1:balance(ReqPayee, UpdateSC) of
        {error, _} ->
            false;
        {ok, 0} ->
            ReqPayloadSize == 0;
        {ok, NewBalance} ->
            OldBalance = case SC == undefined of
                             false ->
                                 case blockchain_state_channel_v1:balance(ReqPayee, SC) of
                                     {ok, B} -> B;
                                     _ -> 0
                                 end;
                             true -> 0
                         end,
            NewBalance-OldBalance >= ReqPayloadSize
    end.

-spec send_packet(Packet :: blockchain_helium_packet_v1:packet(),
                  Stream :: pid(),
                  PubkeyBin :: libp2p_crypto:pubkey_bin(),
                  SigFun :: function()) -> ok.
send_packet(Packet, Stream, PubkeyBin, SigFun) ->
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubkeyBin),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_handler:send_packet(Stream, PacketMsg1).

-spec handle_response(Resp :: blockchain_state_channel_response_v1:response(),
                      State :: state()) -> state().
handle_response(Resp, #state{packet_details=PacketDetails, state_channels=SCs, db=DB, swarm=Swarm}=State) ->
    Filtered = lists:filter(fun({_PacketInfo, undefined}) ->
                                    false;
                               ({_PacketInfo, {_Stream, Req, _TimeRef}}) ->
                                    blockchain_state_channel_response_v1:req_hash(Resp) ==
                                    blockchain_state_channel_request_v1:hash(Req)
                            end,
                            PacketDetails),

    case Filtered of
        [] ->
            State;
        [{PacketInfo, {Stream, Req, TimeRef}}] ->
            erlang:cancel_timer(TimeRef),
            case do_response(Resp, Req, PacketInfo, SCs, Stream, DB, Swarm) of
                {error, _} ->
                    State;
                {ok, NewStateChannels} ->
                    State#state{state_channels=NewStateChannels,
                                packet_details=lists:keydelete(PacketInfo, 1, PacketDetails)}
            end;
        _ ->
            State
    end.


-spec do_response(Resp :: blockchain_state_channel_response_v1:response(),
                  Req :: blockchain_state_channel_request_v1:request(),
                  PacketInfo :: packet_info(),
                  SCs :: state_channels(),
                  Stream :: pid(),
                  DB :: rocksdb:handle(),
                  Swarm :: pid()) -> {error, any()} | {ok, state_channels()}.
do_response(Resp, Req, {Packet, _, _, _}, SCs, Stream, DB, Swarm) ->
    case blockchain_state_channel_response_v1:accepted(Resp) of
        false ->
            lager:error("request ~p got rejected, next...", [Req]),
            {error, request_rejected};
        true ->
            SCUpdate = blockchain_state_channel_response_v1:state_channel_update(Resp),
            UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
            ID = blockchain_state_channel_v1:id(UpdatedSC),
            case validate_state_channel_update(maps:get(ID, SCs, undefined), UpdatedSC) of
                {error, _Reason}=E0 ->
                    lager:error("state channel ~p is invalid ~p droping req", [UpdatedSC, _Reason]),
                    E0;
                ok ->
                    ok = blockchain_state_channel_v1:save(DB, UpdatedSC),
                    SC = maps:get(ID, SCs, undefined),
                    {PubkeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                    case check_pending_request(SC, SCUpdate, Packet, Req, PubkeyBin) of
                        {error, _Reason}=E1 ->
                            lager:error("state channel update did not match pending req ~p, dropping req", [_Reason]),
                            E1;
                        ok ->
                            ok = send_packet(Packet, Stream, PubkeyBin, SigFun),
                            {ok, maps:put(ID, UpdatedSC, SCs)}
                    end
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).
-endif.
