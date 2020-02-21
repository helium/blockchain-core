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
         terminate/2,
         handle_common/3
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
-include_lib("helium_proto/include/packet_pb.hrl").

-define(SERVER, ?MODULE).
-define(STATE_CHANNELS, <<"blockchain_state_channels_client.STATE_CHANNELS">>).

-record(data, {
          db :: rocksdb:db_handle() | undefined,
          swarm :: pid(),
          state_channels = #{} :: state_channels(),
          pending = undefined :: undefined | pending(),
          packets = [] :: [packet()]
         }).

-type packet() :: #packet_pb{}.
-type pending() :: {packet(), pid(), blockchain_state_channel_request_v1:request()}.
-type state_channels() :: #{binary() => blockchain_state_channel_v1:state_channel()}.
-type data() :: #data{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_statem:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits(ID :: blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
credits(ID) ->
    gen_statem:call(?SERVER, {credits, ID}).

-spec packet(Packet :: packet()) -> ok.
packet(Packet) ->
    gen_statem:cast(?SERVER, {packet, Packet}).

-spec response(Resp :: blockchain_state_channel_response_v1:reponse()) -> ok.
response(Resp) ->
    gen_statem:cast(?SERVER, {response, Resp}).

-spec state_channel_update(SCUpdate :: blockchain_state_channel_update_v1:state_channel_update()) -> ok.
state_channel_update(SCUpdate) ->
    gen_statem:cast(?SERVER, {state_channel_update, SCUpdate}).

%% ------------------------------------------------------------------
%% Required callbacks for statem
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    {ok, DB} = blockchain_state_channel_db:get(),
    %% Start in waiting state
    Data = #data{db=DB, swarm=Swarm},
    {ok, waiting, Data}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

callback_mode() ->
    [state_functions, state_enter].

terminate(_Reason, _Data) ->
    ok.

%% ------------------------------------------------------------------
%% States
%% ------------------------------------------------------------------
waiting(enter, _OldState, #data{packets=[]}) ->
    %% There are no packets to process
    keep_state_and_data;
waiting(state_timeout, NextState, Data) ->
    %% Switch state because of timeout
    {next_state, NextState, Data};
waiting(enter, _OldState, #data{packets=Packets, swarm=Swarm}=Data) ->
    %% pop and process the first packet in Packets
    [Packet | Rest] = Packets,
    case process_packet(Packet, Swarm) of
        {error, _Reason} ->
            lager:warning("failed to process packet ~p ~p", [Packet, _Reason]),
            {keep_state, Data, [postpone]};
        {ok, Pending} ->
            NewData = Data#data{pending=Pending, packets=Rest},
            {next_state, processing, NewData, [{state_timeout, timer:seconds(30), waiting}]}
    end.

processing(enter, _OldState, #data{pending=undefined}=Data) ->
    %% Entered processing state with no request pending
    %% Go back to waiting state
    {next_state, waiting, Data};
processing(cast, {response, _Resp}, #data{pending=undefined}=Data) ->
    %% Got response when there is no request pending
    %% Cool story?
    {next_state, waiting, Data};
processing(state_timeout, NextState, Data) ->
    %% Switch state because of timeout
    {next_state, NextState, Data};
processing(cast, {response, Resp}, Data) ->
    case handle_processing(Resp, Data) of
        {error, _Reason} ->
            lager:warning("failed to handle response ~p", [Resp]),
            {next_state, waiting, Data#data{pending=undefined}};
        {ok, NewStateChannels} ->
            {next_state, waiting, Data#data{state_channels=NewStateChannels, pending=undefined}}
    end.


%% ------------------------------------------------------------------
%% Common events
%% ------------------------------------------------------------------
handle_common({call,From}, {credits, ID}, #data{state_channels=SCs}) ->
    Reply = case maps:get(ID, SCs, undefined) of
                undefined ->
                    {error, not_found};
                SC ->
                    {ok, blockchain_state_channel_v1:credits(SC)}
            end,
    {keep_state_and_data, [{reply, From, Reply}]};
handle_common(cast, {state_channel_update, SCUpdate}, #data{db=DB, state_channels=SCs}=Data) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    ID = blockchain_state_channel_v1:id(UpdatedSC),
    lager:debug("received state channel update for ~p", [ID]),
    case validate_state_channel_update(maps:get(ID, SCs, undefined), UpdatedSC) of
        {error, _Reason} ->
            lager:warning("state channel ~p is invalid ~p", [UpdatedSC, _Reason]),
            keep_state_and_data;
        ok ->
            ok = blockchain_state_channel_v1:save(DB, UpdatedSC),
            {keep_state, Data#data{state_channels=maps:put(ID, UpdatedSC, SCs)}}
    end;
handle_common(cast, {packet, Packet}, #data{packets=[], swarm=Swarm}=Data) ->
    %% We got a packet message while in either waiting/processing state
    %% Process this packet immediately as we have no other packets to process
    case process_packet(Packet, Swarm) of
        {error, _Reason} ->
            lager:warning("failed to process packet ~p ~p", [Packet, _Reason]),
            NewData = Data#data{packets=[Packet]},
            %% We'll process this packet later
            {keep_state, NewData, [postpone]};
        {ok, Pending} ->
            %% We managed to get a pending request, go to processing
            {next_state, processing, Data#data{pending=Pending}, [{state_timeout, timer:seconds(30), waiting}]}
    end;
handle_common(cast, {packet, Packet}, #data{packets=Packets}=Data) ->
    %% We still have packets to process, add this received packet
    %% at the end of Packets to get processed when possible
    NewData = Data#data{packets=Packets ++ [Packet]},
    {keep_state, NewData, [postpone]}.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

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

-spec process_packet(Packet :: packet(),
                     Swarm :: pid()) -> {ok, pending()} | {error, any()}.
process_packet(#packet_pb{oui=OUI, payload=Payload}=Packet, Swarm) ->
    case find_routing(OUI) of
        {error, _Reason} ->
            lager:warning("failed to find router for oui ~p:~p", [OUI, _Reason]),
            {error, _Reason};
        {ok, Peer} ->
            case blockchain_state_channel_handler:dial(Swarm, Peer, []) of
                {error, _Reason} ->
                    lager:warning("failed to dial ~p:~p", [Peer, _Reason]),
                    {error, _Reason};
                {ok, Stream} ->
                    {PubKeyBin, _SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                    Amount = calculate_dc_amount(PubKeyBin, OUI, Payload),
                    Req = blockchain_state_channel_request_v1:new(PubKeyBin, Amount, erlang:byte_size(Payload)),
                    lager:info("sending payment req ~p to ~p", [Req, Peer]),
                    ok = blockchain_state_channel_handler:send_request(Stream, Req),
                    {ok, {Packet, Stream, Req}}
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

-spec calculate_dc_amount(PubKeyBin :: libp2p_crypto:pubkey_to_bin(),
                          OUI :: non_neg_integer(),
                          Payload :: binary()) -> non_neg_integer().
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

-spec check_pending_request(SC :: blockchain_state_channel_v1:state_channel() | undefined,
                            SCUpdate :: blockchain_state_channel_update_v1:state_channel_update(),
                            Pending :: pending(),
                            PubkeyBin :: libp2p_crypto:pubkey_bin()) -> ok | {error, any()}.
check_pending_request(SC, SCUpdate, {Packet, _Stream, Req}, PubkeyBin) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    case {check_root_hash(Packet, SCUpdate, PubkeyBin), check_balance(Req, SC, UpdatedSC)} of
        {false, _} -> {error, bad_root_hash};
        {_, false} -> {error, wrong_balance};
        {true, true} -> ok
    end.

-spec check_root_hash(Packet :: packet(),
                      SCUpdate :: blockchain_state_channel_update_v1:state_channel_update(),
                      PubKeyBin :: libp2p_crypto:pubkey_bin()) -> boolean().
check_root_hash(#packet_pb{payload=Payload}, SCUpdate, PubkeyBin) ->
    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
    RootHash = blockchain_state_channel_v1:root_hash(UpdatedSC),
    Hash = blockchain_state_channel_update_v1:previous_hash(SCUpdate),
    PayloadSize = erlang:byte_size(Payload),
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

-spec send_packet(Pending :: pending(),
                  PubkeyBin :: libp2p_crypto:pubkey_bin(),
                  SigFun :: function()) -> ok.
send_packet({Packet, Stream, _Req}, PubKeyBin, SigFun) ->
    PacketMsg0 = blockchain_state_channel_packet_v1:new(Packet, PubKeyBin),
    PacketMsg1 = blockchain_state_channel_packet_v1:sign(PacketMsg0, SigFun),
    blockchain_state_channel_handler:send_packet(Stream, PacketMsg1).

-spec handle_processing(Resp :: blockchain_state_channel_response_v1:response(),
                        Data :: data()) -> {error, any()} | {ok, state_channels()}.
handle_processing(Resp,
                  #data{db=DB,
                        swarm=Swarm,
                        pending={_Packet, _Stream, Req}=Pending,
                        state_channels=SCs}) ->
    case blockchain_state_channel_response_v1:req_hash(Resp) == blockchain_state_channel_request_v1:hash(Req) of
        false ->
            {error, hash_mismatch};
        true ->
            case blockchain_state_channel_response_v1:accepted(Resp) of
                false ->
                    lager:info("request ~p got rejected, next...", [Req]),
                    {error, request_rejected};
                true ->
                    SCUpdate = blockchain_state_channel_response_v1:state_channel_update(Resp),
                    UpdatedSC = blockchain_state_channel_update_v1:state_channel(SCUpdate),
                    ID = blockchain_state_channel_v1:id(UpdatedSC),
                    case validate_state_channel_update(maps:get(ID, SCs, undefined), UpdatedSC) of
                        {error, _Reason}=E0 ->
                            lager:warning("state channel ~p is invalid ~p droping req", [UpdatedSC, _Reason]),
                            E0;
                        ok ->
                            ok = blockchain_state_channel_v1:save(DB, UpdatedSC),
                            SC = maps:get(ID, SCs, undefined),
                            {PubKeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                            case check_pending_request(SC, SCUpdate, Pending, PubKeyBin) of
                                {error, _Reason}=E1 ->
                                    lager:warning("state channel update did not match pending req ~p, dropping req", [_Reason]),
                                    E1;
                                ok ->
                                    ok = send_packet(Pending, PubKeyBin, SigFun),
                                    NewStateChannels = maps:put(ID, UpdatedSC, SCs),
                                    {ok, NewStateChannels}
                            end
                    end
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------

-ifdef(TEST).
-endif.
