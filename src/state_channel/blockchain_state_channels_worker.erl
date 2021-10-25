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
    get/2,
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
-define(EXPIRED, expired).
-define(OVERSPENT, overspent).
-define(HOOK_CLOSE_SUBMIT, sc_hook_close_submit).

-record(state, {
    parent :: pid(),
    id :: blockchain_state_channel_v1:id(),
    state_channel :: blockchain_state_channel_v1:state_channel(),
    skewed :: skewed:skewed(),
    db :: rocksdb:db_handle(),
    owner :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()},
    chain :: blockchain:blockchain(),
    dc_payload_size ::pos_integer(),
    sc_version :: non_neg_integer(),
    max_actors_allowed = ?SC_MAX_ACTORS :: pos_integer(),
    prevent_overspend = true,
    bloom :: bloom_nif:bloom()
}).

-type state() :: #state{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
-spec start(map()) -> {ok, pid()} | ignore | {error, any()}.
start(Args) ->
    gen_server:start(?SERVER, Args, []).

-spec get(Pid :: pid(), Timeout :: non_neg_integer()) -> blockchain_state_channel_v1:state_channel().
get(Pid, Timeout) ->
    {SC, OwnerSigFun} = gen_server:call(Pid, get, Timeout),
    blockchain_state_channel_v1:sign(SC, OwnerSigFun).

-spec handle_offer(
    Pid :: pid(),
    Offer :: blockchain_state_channel_offer_v1:offer(),
    HandlerPid :: pid()
) -> ok | reject.
handle_offer(Pid, Offer, HandlerPid) ->
    lager:debug("got offer ~p from ~p (to ~p)", [Offer, HandlerPid, Pid]),
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
    lager:debug("got packet ~p from ~p (to ~p)", [SCPacket, HandlerPid, Pid]),
    gen_server:cast(Pid, {handle_packet, SCPacket, HandlerPid}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Parent = maps:get(parent, Args),
    _Ref = erlang:monitor(process, Parent),
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
    Owner = maps:get(owner, Args),
    {_, OwnerSigFun} = Owner,
    ok = blockchain_event:add_handler(self()),
    lager:info("started ~p", [blockchain_utils:addr2name(ID)]),
    State = #state{
        parent = Parent,
        id = ID,
        state_channel = blockchain_state_channel_v1:sign(SC, OwnerSigFun),
        skewed = maps:get(skewed, Args),
        bloom = Bloom,
        db = maps:get(db, Args),
        owner = Owner,
        chain = Chain,
        dc_payload_size = DCPayloadSize,
        sc_version = SCVer,
        max_actors_allowed = MaxActorsAllowed,
        prevent_overspend = application:get_env(blockchain, prevent_sc_overspend, true)
    },
    {ok, State}.

handle_call(get, _From, #state{state_channel=SC, owner={_Owner, OwnerSigFun}}=State) ->
    {reply, {SC, OwnerSigFun}, State};
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

handle_info({blockchain_event, {new_chain, Chain}}, State) ->
    {noreply, State#state{chain=Chain}};
handle_info(
    {blockchain_event, {add_block, _BlockHash, _Syncing, Ledger}},
    #state{id=ID, state_channel=SC, owner={Owner, OwnerSigFun}}=State
) ->
    Name = blockchain_utils:addr2name(ID),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
    lager:debug("got block ~p for ~p expires at ~p", [Height, Name, ExpireAt]),
    case ExpireAt =< Height of
        false ->
            {noreply, State};
        true ->
            ClosedSC = blockchain_state_channel_v1:state(closed, SC),
            SignedSC = blockchain_state_channel_v1:sign(ClosedSC, OwnerSigFun),
            Txn = blockchain_txn_state_channel_close_v1:new(SignedSC, Owner),
            SignedTxn = blockchain_txn_state_channel_close_v1:sign(Txn, OwnerSigFun),
            F = fun(Result) ->
                ok = handle_close_submit(Result, SignedTxn),
                case application:get_env(blockchain, ?HOOK_CLOSE_SUBMIT, undefined) of
                    undefined ->
                        ok;
                    SubmitHook ->
                        _ = SubmitHook:?HOOK_CLOSE_SUBMIT(Result, SignedTxn),
                        ok
                end
            end,
            ok = blockchain_txn_mgr:submit(SignedTxn, F),
            {stop, {shutdown, ?EXPIRED}, State#state{state_channel=SignedSC}}
    end;
handle_info(?OVERSPENT, State) ->
    lager:info("state channel overspent shuting down"),
    {stop, {shutdown, ?OVERSPENT}, State};
handle_info({'DOWN', _Ref, process, Parent, _}, #state{parent=Parent}=State) ->
    {stop, {shutdown, parent_down}, State};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, #state{id=ID, state_channel=SC, skewed=Skewed, db=DB, owner={_Owner, OwnerSigFun}}=_State) ->
    SignedSC = blockchain_state_channel_v1:sign(SC, OwnerSigFun),
    ok = blockchain_state_channels_server:update_state_channel(SignedSC),
    ok = blockchain_state_channel_v1:save(DB, SignedSC, Skewed),
    Deleted = blockchain_state_channels_cache:delete_pids(self()),
    lager:info("terminate ~p for : ~p, deleted ~p from cache", [blockchain_utils:addr2name(ID), Reason, Deleted]),
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
-spec handle_close_submit(any(), blockchain_txn_state_channel_close_v1:txn_state_channel_close()) -> ok.
handle_close_submit(ok, SignedTxn) ->
    lager:info("txn accepted, ~p", [blockchain_txn_state_channel_close_v1:print(SignedTxn)]);
handle_close_submit(Error, SignedTxn) ->
    lager:error("failed to submit txn ~p", [Error]),
    lager:error("~p", [SignedTxn]).

-spec offer(
    Offer :: blockchain_state_channel_offer_v1:offer(),
    HandlerPid :: pid(),
    State0 :: state()
) -> {noreply, state()} | {stop, any(), state()}.
offer(
    Offer,
    HandlerPid,
    #state{
        id = SCID,
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
    NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, DCPayloadSize),
    TotalDCs = blockchain_state_channel_v1:total_dcs(SC),
    DCAmount = blockchain_state_channel_v1:amount(SC),
    case (TotalDCs + NumDCs) > DCAmount andalso PreventOverSpend of
        true ->
            ok = send_offer_rejection(HandlerPid, Offer),
            lager:warning(
                "dropping this packet because it will overspend DC ~p, (cost: ~p, total_dcs: ~p)",
                [DCAmount, NumDCs, TotalDCs]
            ),
            %% This allow for packets (accepted offer) to come through
            _ = erlang:send_after(1000, self(), ?OVERSPENT),
            {noreply, State0};
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
                        "[~p] dropping this packet because: ~p",
                        [blockchain_state_channel_v1:id(SC), _Reason]
                    ),
                    ok = send_offer_rejection(HandlerPid, Offer),
                    {noreply, State0};
                {ok, PurchaseSC} ->
                    lager:debug("[~p] purchasing offer from ~p", [blockchain_state_channel_v1:id(PurchaseSC), HotspotName]),
                    PacketHash = blockchain_state_channel_offer_v1:packet_hash(Offer),
                    Region = blockchain_state_channel_offer_v1:region(Offer),
                    ok = blockchain_state_channel_common:send_purchase(
                        HandlerPid,
                        PurchaseSC,
                        HotspotID,
                        PacketHash,
                        Region,
                        OwnerSigFun
                    ),
                    ok = blockchain_state_channel_v1:save(DB, PurchaseSC, Skewed),
                    case (TotalDCs + NumDCs + 1) > DCAmount andalso PreventOverSpend of
                        false ->
                            ok;
                        true ->
                            lager:info("next packet will overspend"),
                            _ = erlang:send_after(1000, self(), ?OVERSPENT)
                    end,
                    {noreply, State0#state{state_channel=PurchaseSC}}
            end
    end.

-spec packet(
    SCPacket :: blockchain_state_channel_packet_v1:packet(),
    HandlerPid :: pid(),
    State0 :: state()
) -> state().
packet(
    SCPacket,
    _HandlerPid,
    #state{
        state_channel=SC0,
        skewed=Skewed0,
        db=DB,
        owner={_Owner, OwnerSigFun},
        bloom=Bloom
    }=State0
) ->
    Packet = blockchain_state_channel_packet_v1:packet(SCPacket),
    Payload = blockchain_helium_packet_v1:payload(Packet),
    case bloom:check_and_set(Bloom, Payload) of
        true ->
            lager:debug("skewed already updated with ~p", [Payload]),
            State0;
        false ->
            lager:debug("updating skewed with ~p", [Payload]),
            {SC1, Skewed1} = blockchain_state_channel_v1:add_payload(Payload, SC0, Skewed0),
            ok = blockchain_state_channel_v1:save(DB, SC1, Skewed1),
            State0#state{state_channel=SC1, skewed=Skewed1}
    end.


-spec send_offer_rejection(HandlerPid :: pid(), Offer :: blockchain_state_channel_offer_v1:offer()) -> ok.
send_offer_rejection(HandlerPid, Offer) ->
    HotspotID = blockchain_state_channel_offer_v1:hotspot(Offer),
    ok = blockchain_state_channels_cache:delete_hotspot(HotspotID),
    PacketHash = blockchain_state_channel_offer_v1:packet_hash(Offer),
    RejectionMsg = blockchain_state_channel_rejection_v1:new(PacketHash),
    ok = blockchain_state_channel_common:send_rejection(HandlerPid, RejectionMsg).

-spec try_update_summary(
    SC :: blockchain_state_channel_v1:state_channel(),
    HotspotID :: libp2p_crypto:pubkey_bin(),
    PayloadSize :: pos_integer(),
    DCPayloadSize :: undefined | pos_integer(),
    MaxActorsAllowed :: non_neg_integer()
) ->
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
            {NewSC, DidFit} = blockchain_state_channel_v1:update_summary_for(HotspotID,
                                                                             NewSummary,
                                                                             SC,
                                                                             MaxActorsAllowed),
            {NewSC, DidFit};
        {ok, ExistingSummary} ->
            ExistingNumPackets = blockchain_state_channel_summary_v1:num_packets(ExistingSummary),
            NumDCs = blockchain_utils:do_calculate_dc_amount(PayloadSize, DCPayloadSize),
            ExistingNumDCs = blockchain_state_channel_summary_v1:num_dcs(ExistingSummary),
            NewSummary = blockchain_state_channel_summary_v1:update(ExistingNumDCs + NumDCs,
                                                                    ExistingNumPackets + 1,
                                                                    ExistingSummary),
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
