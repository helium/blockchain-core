%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Worker ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_worker).

-behavior(gen_server).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start/1,
    handle_offer/3,
    handle_packet/3
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(FP_RATE, 0.99).
-define(MAX_PAYLOAD_SIZE, 255). % lorawan max payload size is 255 bytes

-record(state, {
    id :: blockchain_state_channel_v1:id(),
    state_channel :: blockchain_state_channel_v1:state_channel(),
    skewed :: skewed:skewed(),
    db :: rocksdb:db_handle(),
    owner :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()},
    chain :: blockchain:blockchain(),
    dc_payload_size ::pos_integer(),
    sc_version :: non_neg_integer(), %% defaulting to 0 instead of undefined
    max_actors_allowed = ?SC_MAX_ACTORS :: pos_integer(),
    prevent_overspend = true,
    bloom :: bloom_nif:bloom(),
    handlers = #{} :: handlers()
}).

-type state() :: #state{}.
-type handlers() :: #{libp2p_crypto:pubkey_bin() => {pid(), reference()}}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
-spec start(map()) -> {ok, pid()} | ignore | {error, any()}.
start(Args) ->
    gen_server:start(?SERVER, Args, []).

-spec handle_offer(
    Pid :: pid(),
    Offer :: blockchain_state_channel_offer_v1:offer(),
    HandlerPid :: pid()
) -> ok | reject.
handle_offer(Pid, Offer, HandlerPid) ->
    PayloadSize = blockchain_state_channel_offer_v1:payload_size(Offer),
    case PayloadSize =< ?MAX_PAYLOAD_SIZE of
        false ->
            lager:error("payload size (~p) exceeds maximum (~p). Sending rejection of offer ~p from ~p",
                        [PayloadSize, ?MAX_PAYLOAD_SIZE, Offer, HandlerPid]),
            reject;
        true ->
            gen_server:cast(Pid, {handle_offer, Offer, HandlerPid})
    end.

-spec handle_packet(
    Pid :: pid(),
    SCPacketPacket :: blockchain_state_channel_packet_v1:packet(),
    HandlerPid :: pid()
) -> ok.
handle_packet(Pid, SCPacket, HandlerPid) ->
    gen_server:cast(Pid, {handle_packet, SCPacket, HandlerPid}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    erlang:process_flag(trap_exit, true),
    lager:info("~p init with ~p", [?SERVER, Args]),
    SC = maps:get(state_channel, Args),
    Amount = blockchain_state_channel_v1:amount(SC),
    {ok, Bloom} = bloom:new_optimal(max(Amount, 1), ?FP_RATE),
    Chain = maps:get(chain, Args),
    Ledger = blockchain:ledger(Chain),
    DCPayloadSize =
        case blockchain_ledger_v1:config(?dc_payload_size, Ledger) of
            {ok, DCP} -> DCP;
            _ -> 0
        end,
    SCVer =
        case blockchain_ledger_v1:config(?sc_version, Ledger) of
            {ok, SCV} -> SCV;
            _ -> 0
        end,
    MaxActorsAllowed = blockchain_ledger_v1:get_sc_max_actors(Ledger),
    ID = blockchain_state_channel_v1:id(SC),
    ok = refresh_cache(SC),
    State = #state{
        id = ID,
        state_channel = SC,
        skewed = maps:get(skewed, Args),
        bloom = Bloom,
        db = maps:get(db, Args),
        owner = maps:get(owner, Args),
        chain = Chain,
        dc_payload_size = DCPayloadSize,
        sc_version = SCVer,
        max_actors_allowed = MaxActorsAllowed,
        prevent_overspend = application:get_env(blockchain, prevent_sc_overspend, true)
    },
    {ok, State}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({handle_offer, Offer, HandlerPid}, State0) ->
    offer(Offer, HandlerPid, State0);
handle_cast({handle_packet, SCPacket, HandlerPid}, State0) ->
    State1 = packet(SCPacket, HandlerPid, State0),
    {noreply, State1};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _}, #state{handlers=Handlers}=State) ->
    FilteredHandlers =
        maps:filter(
            fun(_Name, {Handler, _}) ->
                Handler /= Pid
            end,
            Handlers
        ),
    {noreply, State#state{handlers=FilteredHandlers}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    Deleted = blockchain_state_channels_cache:delete_pids(self()),
    lager:info("terminate: ~p, deleted ~p from cache", [Deleted]),
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec offer(
    Offer :: blockchain_state_channel_offer_v1:offer(),
    HandlerPid :: pid(),
    State0 :: state()
) -> {noreply, state()} | {stop, any(), state()}.
offer(
    Offer,
    HandlerPid,
    #state{
        state_channel = SC,
        skewed=Skewed,
        db=DB,
        owner={_Owner, OwnerSigFun},
        dc_payload_size=DCPayloadSize,
        max_actors_allowed=MaxActorsAllowed,
        prevent_overspend=PreventOverSpend
    }=State0
) ->
    HotspotID = blockchain_state_channel_offer_v1:hotspot(Offer),
    HotspotName = blockchain_utils:addr2name(HotspotID),
    lager:debug("handling offer from ~p", [HotspotName]),
    PayloadSize = blockchain_state_channel_offer_v1:payload_size(Offer),
    NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, State0#state.dc_payload_size),
    TotalDCs = blockchain_state_channel_v1:total_dcs(SC),
    DCAmount = blockchain_state_channel_v1:amount(SC),
    case (TotalDCs + NumDCs) > DCAmount andalso PreventOverSpend of
        true ->
            lager:warning(
                "dropping this packet because it will overspend DC ~p, (cost: ~p, total_dcs: ~p)",
                [DCAmount, NumDCs, TotalDCs]
            ),
            ok = send_offer_rejection(HandlerPid),
            %% TODO: turn this off
            {stop, normal, State0};
        false ->
            Routing = blockchain_state_channel_offer_v1:routing(Offer),
            lager:debug("routing: ~p, hotspot: ~p", [Routing, HotspotName]),
            case
                try_update_summary(
                    SC,
                    HotspotID,
                    PayloadSize,
                    DCPayloadSize,
                    MaxActorsAllowed
                )
            of
                {error, _Reason} ->
                    lager:warning(
                        "dropping this packet because: ~p ~p",
                        [_Reason, lager:pr(SC, blockchain_state_channel_v1)]
                    ),
                    ok = send_offer_rejection(HandlerPid),
                    {noreply, State0};
                {ok, PurchaseSC} ->
                    lager:debug("purchasing offer from ~p", [HotspotName]),
                    SignedPurchaseSC = blockchain_state_channel_v1:sign(PurchaseSC, OwnerSigFun),
                    PacketHash = blockchain_state_channel_offer_v1:packet_hash(Offer),
                    Region = blockchain_state_channel_offer_v1:region(Offer),
                    ok = blockchain_state_channel_handler:send_purchase(HandlerPid,
                                                                        SignedPurchaseSC,
                                                                        HotspotID,
                                                                        PacketHash,
                                                                        Region),
                    ok = blockchain_state_channel_v1:save(DB, SignedPurchaseSC, Skewed),
                    State1 = update_streams(HotspotID, HandlerPid, State0),
                    {noreply, State1}
            end
    end.

-spec packet(
    SCPacket :: blockchain_state_channel_packet_v1:packet(),
    HandlerPid :: pid(),
    State0 :: state()
) -> {noreply, state()} | {stop, any(), state()}.
packet(
    SCPacket,
    HandlerPid,
    #state{
        state_channel=SC0,
        skewed=Skewed0,
        db=DB,
        owner={_Owner, OwnerSigFun},
        dc_payload_size=DCPayloadSize,
        sc_version=SCVer,
        max_actors_allowed = MaxActorsAllowed,
        bloom=Bloom
    }=State0
) ->
    Packet = blockchain_state_channel_packet_v1:packet(SCPacket),
    Payload = blockchain_helium_packet_v1:payload(Packet),
    HotspotID = blockchain_state_channel_packet_v1:hotspot(SCPacket),
    case SCVer > 1 andalso bloom:check_and_set(Bloom, Payload) of
        true ->
            update_streams(HotspotID, HandlerPid, State0);
        false ->
            {SC1, Skewed1} = blockchain_state_channel_v1:add_payload(Payload, SC0, Skewed0),
            ExistingSCNonce = blockchain_state_channel_v1:nonce(SC1),
            SC2 = blockchain_state_channel_v1:nonce(ExistingSCNonce + 1, SC1),
            SC3 = case SCVer of
                2 ->
                    %% we don't update the state channel summary here
                    %% it happens in `send_purchase` for v2 SCs
                    SC2;
                _ ->
                    {SC, _} =
                        update_sc_summary(
                            HotspotID,
                            erlang:byte_size(Payload),
                            DCPayloadSize,
                            SC2,
                            MaxActorsAllowed
                        ),
                    SC
            end,
            SignedSC = blockchain_state_channel_v1:sign(SC3, OwnerSigFun),
            ok = blockchain_state_channel_v1:save(DB, SignedSC, Skewed1),
            State1 = State0#state{state_channel=SignedSC, skewed=Skewed1},
            update_streams(HotspotID, HandlerPid, State1)
    end.

-spec update_streams(
    HotspotID :: libp2p_crypto:pubkey_bin(),
    Handler :: pid(),
    State :: state()
) -> state().
update_streams(HotspotID, Handler, #state{handlers=Handlers0}=State) ->
    Handlers1 = 
        maps:update_with(
            HotspotID,
            fun({_OldHandler, OldRef}) ->
                %% we have an existing stream, demonitor it
                %% and monitor new one
                erlang:demonitor(OldRef),
                Ref = erlang:monitor(process, Handler),
                {Handler, Ref}
            end,
            %% value if not present
            {Handler, erlang:monitor(process, Handler)},
            Handlers0
        ),
   State#state{handlers=Handlers1}.

-spec send_offer_rejection(HandlerPid :: pid()) -> ok.
send_offer_rejection(HandlerPid) ->
    _ = erlang:spawn(
        fun() ->
            RejectionMsg = blockchain_state_channel_rejection_v1:new(),
            ok = blockchain_state_channel_handler:send_rejection(HandlerPid, RejectionMsg)
        end
    ),
    ok.

-spec try_update_summary(SC :: blockchain_state_channel_v1:state_channel(),
                         HotspotID :: libp2p_crypto:pubkey_bin(),
                         PayloadSize :: pos_integer(),
                         DCPayloadSize :: undefined | pos_integer(),
                         MaxActorsAllowed :: non_neg_integer()) ->
    {ok, blockchain_state_channel_v1:state_channel()} | {error, does_not_fit}.
try_update_summary(SC, HotspotID, PayloadSize, DCPayloadSize, MaxActorsAllowed) ->
    SCNonce = blockchain_state_channel_v1:nonce(SC),
    NewPurchaseSC0 = blockchain_state_channel_v1:nonce(SCNonce + 1, SC),
    case update_sc_summary(HotspotID, PayloadSize, DCPayloadSize, NewPurchaseSC0, MaxActorsAllowed) of
        {NewPurchaseSC1, true} -> {ok, NewPurchaseSC1};
        {_SC, false} -> {error, does_not_fit}
    end.

-spec update_sc_summary(HotspotID :: libp2p_crypto:pubkey_bin(),
                        PayloadSize :: pos_integer(),
                        DCPayloadSize :: undefined | pos_integer(),
                        SC :: blockchain_state_channel_v1:state_channel(),
                        MaxActorsAllowed :: non_neg_integer()) ->
    {blockchain_state_channel_v1:state_channel(), boolean()}.
update_sc_summary(HotspotID, PayloadSize, DCPayloadSize, SC, MaxActorsAllowed) ->
    case blockchain_state_channel_v1:get_summary(HotspotID, SC) of
        {error, not_found} ->
            NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, DCPayloadSize),
            NewSummary = blockchain_state_channel_summary_v1:new(HotspotID, 1, NumDCs),
            %% Add this to summaries
            {NewSC, DidFit} = blockchain_state_channel_v1:update_summary_for(HotspotID,
                                                                             NewSummary,
                                                                             SC,
                                                                             MaxActorsAllowed),
            {NewSC, DidFit};
        {ok, ExistingSummary} ->
            %% Update packet count for this client
            ExistingNumPackets = blockchain_state_channel_summary_v1:num_packets(ExistingSummary),
            %% Update DC count for this client
            NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, DCPayloadSize),
            ExistingNumDCs = blockchain_state_channel_summary_v1:num_dcs(ExistingSummary),
            NewSummary = blockchain_state_channel_summary_v1:update(ExistingNumDCs + NumDCs,
                                                                    ExistingNumPackets + 1,
                                                                    ExistingSummary),
            %% Update summaries
            {NewSC, DidFit} = blockchain_state_channel_v1:update_summary_for(HotspotID,
                                                                             NewSummary,
                                                                             SC,
                                                                             MaxActorsAllowed),
            {NewSC, DidFit}
    end.

-spec refresh_cache(SC :: blockchain_state_channel_v1:state_channel()) -> ok. 
refresh_cache(SC) ->
    Pid = self(),
    Summaries = blockchain_state_channel_v1:summaries(SC),
    lists:foreach(
        fun(Summary) ->
            HotspotID = blockchain_state_channel_summary_v1:client_pubkeybin(Summary),
            ok = blockchain_state_channels_cache:insert_hotspot(HotspotID, Pid)
        end,
        Summaries
    ).
