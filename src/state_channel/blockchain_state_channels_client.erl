%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_client).

-behavior(gen_statem).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    credits/1,
    packet/1,
    response/1,
    state_channel_update/1
]).

%% ------------------------------------------------------------------
%% gen_statem Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    code_change/3,
    callback_mode/0,
    terminate/2
]).

%% ------------------------------------------------------------------
%% gen_statem callbacks Exports
%% ------------------------------------------------------------------
-export([
    processing/3,
    waiting/3
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_longfi_pb.hrl").

-define(SERVER, ?MODULE).
-define(STATE_CHANNELS, <<"blockchain_state_channels_client.STATE_CHANNELS">>).

-record(data, {
    db :: rocksdb:db_handle() | undefined,
    swarm :: pid(),
    state_channels = #{} :: #{binary() => blockchain_state_channel_v1:state_channel()},
    pending = undefined :: undefined | pending(),
    packets = [] :: [any()]
}).

-type packet() :: #helium_LongFiRxPacket_pb{}.
-type pending() :: {blockchain_state_channel_request_v1:request(), any(), pid()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_statem:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
credits(ID) ->
    gen_statem:call(?SERVER, {credits, ID}).

-spec packet(packet()) -> ok.
packet(Packet) ->
    gen_statem:cast(?SERVER, {packet, Packet}).

-spec response(blockchain_state_channel_response_v1:reponse()) -> ok.
response(Resp) ->
    gen_statem:cast(?SERVER, {response, Resp}).

-spec state_channel_update(blockchain_state_channel_update_v1:state_channel_update()) -> ok.
state_channel_update(SCUpdate) ->
    gen_statem:cast(?SERVER, {state_channel_update, SCUpdate}).

%% ------------------------------------------------------------------
%% gen_statem Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    {ok, DB} = blockchain_state_channel_db:get(),
    {ok, processing, #data{db=DB, swarm=Swarm}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

callback_mode() -> state_functions.

terminate(_Reason, _Data) ->
    ok.

%% ------------------------------------------------------------------
%% gen_statem callbacks
%% ------------------------------------------------------------------

processing(cast, {packet, Packet}, #data{packets=Packets}=Data) ->
    ok = trigger_processing(),
    {keep_state,  Data#data{packets=[Packet|Packets]}};
processing(info, process_packet, #data{packets=[]}=Data) ->
    lager:debug("nothing to process"),
    {keep_state,  Data};
processing(info, process_packet, #data{swarm=Swarm, packets=Packets}=Data) ->
    Packet = lists:last(Packets),
    lager:debug("processing", [Packet]),
    case process_packet(Packet, Swarm) of
        {error, _Reason} ->
            lager:warning("failed to process packet ~p ~p", [Packet, _Reason]),
            ok = trigger_processing(),
            {keep_state,  Data};
        {ok, Pending} ->
            {next_state, waiting, Data#data{pending=Pending, packets=lists:droplast(Packets)}}
    end;
processing(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

waiting(cast, {packet, Packet}, #data{packets=Packets}=Data) ->
    {keep_state,  Data#data{packets=[Packet|Packets]}};
waiting(cast, {response, Resp}, #data{db=DB, swarm=Swarm, state_channels=SCs, pending={Req, _, _}=Pending}=Data) ->
    lager:debug("received resp ~p", [Resp]),
    case blockchain_state_channel_response_v1:req_hash(Resp) == blockchain_state_channel_request_v1:hash(Req) of
        false ->
            lager:warning("got unknown response ~p", [Resp]),
            {keep_state,  Data};
        true ->
            case blockchain_state_channel_response_v1:accepted(Resp) of
                false ->
                    lager:info("request ~p got rejected, next...", [Req]),
                    ok = trigger_processing(),
                    {next_state, processing, Data#data{pending=undefined}};
                true ->
                    SCUpdate = blockchain_state_channel_response_v1:state_channel_update(Resp),
                    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
                    ID = blockchain_state_channel_v1:id(UpdatedSC),
                    case validate_state_channel_update(maps:get(ID, SCs, undefined), UpdatedSC) of
                        {error, _Reason} -> 
                            lager:warning("state channel ~p is invalid ~p droping req", [UpdatedSC, _Reason]),
                            ok = trigger_processing(),
                            {next_state, processing, Data#data{pending=undefined}};
                        ok ->
                            ok = blockchain_state_channel_v1:save(DB, UpdatedSC),
                            SC = maps:get(ID, SCs, undefined),
                            {PubKeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                            case check_pending_request(SC, SCUpdate, Pending, PubKeyBin) of
                                {error, _Reason} ->
                                    lager:warning("state channel update did not match pending req ~p, dropping req", [_Reason]),
                                    ok = trigger_processing(),
                                    {next_state, processing, Data#data{pending=undefined}};
                                ok ->
                                    ok = send_packet(Pending, PubKeyBin, SigFun),
                                    ok = trigger_processing(),
                                    {next_state, processing, Data#data{state_channels=maps:put(ID, UpdatedSC, SCs),
                                                                       pending=undefined}}
                            end
                    end
            end
    end;
waiting(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_event({call, From}, {credits, ID}, #data{state_channels=SCs}=Data) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel_v1:credits(SC)}
    end,
    {keep_state, Data, [{reply, From, Reply}]};
handle_event(cast, {state_channel_update, SCUpdate}, #data{db=DB, state_channels=SCs}=Data) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    ID = blockchain_state_channel_v1:id(UpdatedSC),
    lager:debug("received state channel update for ~p", [ID]),
    case validate_state_channel_update(maps:get(ID, SCs, undefined), UpdatedSC) of
        {error, _Reason} -> 
            lager:warning("state channel ~p is invalid ~p", [UpdatedSC, _Reason]),
            {keep_state, Data};
        ok ->
            ok = blockchain_state_channel_v1:save(DB, UpdatedSC),
            {keep_state, Data#data{state_channels=maps:put(ID, UpdatedSC, SCs)}}
    end;
handle_event(_EventType, _EventContent, Data) ->
    lager:warning("ignoring unknown event [~p] ~p", [_EventType, _EventContent]),
    {keep_state, Data}.

-spec trigger_processing() -> ok.
trigger_processing() ->
    self() ! process_packet,
    ok.

-spec validate_state_channel_update(blockchain_state_channel_v1:state_channel() | undefined, blockchain_state_channel_v1:state_channel()) -> ok | {error, any()}.
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

-spec process_packet(packet(), pid()) -> {ok, pending()} | {error, any()}.
process_packet(#helium_LongFiRxPacket_pb{oui=OUI, fingerprint=Fingerprint, payload=Payload}=Packet, Swarm) ->
    case find_routing(OUI) of
        {error, _Reason} ->
            lager:warning("failed to find router for oui ~p:~p", [OUI, _Reason]),
             {error, _Reason};
        {ok, Peer} ->
            case blockchain_state_channel_handler:dial(Swarm, Peer, []) of
                {error, _Reason} ->
                    lager:warning("failed to dial ~p:~p", [Peer, _Reason]),
                    {error, _Reason};
                {ok, Pid} ->
                    {PubKeyBin, _SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                    Amount = calculate_dc_amount(PubKeyBin, OUI, Payload),
                    Req = blockchain_state_channel_request_v1:new(PubKeyBin, Amount, erlang:byte_size(Payload), Fingerprint),
                    lager:info("sending payment req ~p to ~p", [Req, Peer]),
                    blockchain_state_channel_handler:send_request(Pid, Req),
                    {ok, {Req, Packet, Pid}}
            end
    end.

-spec find_routing(non_neg_integer()) -> {ok, string()} | {error, any()}.
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

-spec calculate_dc_amount(libp2p_crypto:pubkey_to_bin(), non_neg_integer(), binary()) -> non_neg_integer().
calculate_dc_amount(PubKeyBin, OUI, Payload) ->
    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    Size = erlang:byte_size(Payload),
    Price = blockchain_state_channel_utils:calculate_dc_amount(Size),
    case blockchain_ledger_v1:find_gateway_info(PubKeyBin, Ledger) of
        {error, _Reason} ->
            lager:error("failed to find gateway ~p: ~p", [PubKeyBin, _Reason]),
            Price;
        {ok, GWInfo} ->
            case blockchain_ledger_gateway_v2:oui(GWInfo) of
                OUI -> 0;
                _ -> Price
            end
    end.

-spec check_pending_request(blockchain_state_channel_v1:state_channel() | undefined,
                            blockchain_state_channel_update_v1:state_channel_update(), pending(), libp2p_crypto:pubkey_bin()) -> ok | {error, any()}.
check_pending_request(SC, SCUpdate, {Req, Packet, _Pid}, PubkeyBin) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    case {check_root_hash(Packet, SCUpdate, PubkeyBin), check_balance(Req, SC, UpdatedSC)} of
        {false, _} -> {error, bad_root_hash};
        {_, false} -> {error, wrong_balance};
        {true, true} -> ok
    end.

-spec check_root_hash(packet(), blockchain_state_channel_update_v1:state_channel_update(), libp2p_crypto:pubkey_bin()) -> boolean().
check_root_hash(#helium_LongFiRxPacket_pb{fingerprint=Fingerprint, payload=Payload}, SCUpdate, PubkeyBin) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    RootHash = blockchain_state_channel_v1:root_hash(UpdatedSC),
    Hash = blockchain_state_channel_update_v1:previous_hash(SCUpdate),
    PayloadSize = erlang:byte_size(Payload),
    Value = <<PubkeyBin/binary, PayloadSize, Fingerprint>>,
    skewed:verify(skewed:hash_value(Value), [Hash], RootHash).

-spec check_balance(blockchain_state_channel_request_v1:request(),
                    blockchain_state_channel_v1:state_channel() | undefined,
                    blockchain_state_channel_v1:state_channel()) -> boolean().
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

-spec send_packet(pending(), libp2p_crypto:pubkey_bin(), function()) -> ok.
send_packet({_Req, Packet, Pid}, PubKeyBin, SigFun) ->
    Bin = helium_longfi_pb:encode_msg(Packet),
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Bin, PubKeyBin),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_handler:send_packet(Pid, PacketMsg1).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).
-endif.
