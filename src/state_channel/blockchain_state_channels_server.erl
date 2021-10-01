%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_server).

-behavior(gen_server).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    get_all/0,
    get_actives/0,
    get_active_pid/1,
    get_actives_count/0,
    gc_state_channels/1,
    handle_offer/4,
    handle_packet/5,
    handle_worker_terminate/2
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
-define(SC_WORKER_GROUP, state_channel_workers_union).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    cf :: rocksdb:cf_handle() | undefined,
    chain = undefined :: blockchain:blockchain() | undefined,
    height :: non_neg_integer() | undefined,
    owner = undefined :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()} | undefined,
    state_channels = #{} :: state_channels(),
    actives = [] :: [{pid(), blockchain_state_channel_v1:id()}],
    sc_version = 0 :: non_neg_integer() %% defaulting to 0 instead of undefined
}).

-type state() :: #state{}.
-type state_channels() :: #{blockchain_state_channel_v1:id() => blockchain_state_channel_v1:state_channel()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec get_all() -> state_channels().
get_all() ->
    All = gen_server:call(?SERVER, get_all, infinity),
    Actives = get_actives(),
    maps:merge(All, Actives).

-spec get_actives() -> state_channels().
get_actives() ->
    maps:from_list(
        blockchain_utils:pmap(
            fun({Pid, ID}) ->
                    SC = blockchain_state_channels_worker:get(Pid),
                    {ID, SC}
            end,
            gen_server:call(?SERVER, get_actives, infinity)
        )
    ).

-spec get_active_pid(ID :: blockchain_state_channel_v1:id()) -> pid() | undefined.
get_active_pid(ID) ->
    gen_server:call(?SERVER, {get_active_pid, ID}, infinity).

-spec get_actives_count() -> non_neg_integer().
get_actives_count() ->
    erlang:length(get_actives_from_cache()).

-spec gc_state_channels([binary()]) -> ok.
gc_state_channels([]) -> ok;
gc_state_channels(SCIDs) ->
    gen_server:cast(?SERVER, {gc_state_channels, SCIDs}).

-spec handle_offer(
    Offer :: blockchain_state_channel_offer_v1:offer(),
    SCPacketHandler :: atom(),
    Ledger :: blockchain_ledger_v1:ledger(),
    HandlerPid :: pid()
) -> ok | reject.
handle_offer(Offer, SCPacketHandler, Ledger, HandlerPid) ->
    lager:debug("handle_offer ~p from ~p", [Offer, HandlerPid]),
    case blockchain_state_channel_offer_v1:validate(Offer) of
        {error, _Reason} ->
            lager:debug("offer failed to validate ~p ~p", [_Reason, Offer]),
            reject;
        true ->
            case SCPacketHandler:handle_offer(Offer, HandlerPid) of
                {error, _Why} ->
                    reject;
                ok ->
                    handle_offer(Offer, Ledger, HandlerPid)
            end
    end.

-spec handle_packet(
    SCPacket :: blockchain_state_channel_packet_v1:packet(),
    PacketTime :: pos_integer(),
    SCPacketHandler :: atom(),
    Ledger :: blockchain_ledger_v1:ledger(),
    HandlerPid :: pid()
) ->ok.
handle_packet(SCPacket, PacketTime, SCPacketHandler, Ledger, HandlerPid) ->
    lager:debug("handle_packet ~p from ~p (~pms)", [SCPacket, HandlerPid, PacketTime]),
    case SCPacketHandler:handle_packet(SCPacket, PacketTime, HandlerPid) of
        ok ->
            HotspotID = blockchain_state_channel_packet_v1:hotspot(SCPacket),
            HotspotName = blockchain_utils:addr2name(HotspotID),
            case blockchain_state_channels_cache:lookup_hotspot(HotspotID) of
                undefined ->
                    case select_best_active(HotspotID, Ledger) of
                        {ok, Pid} ->
                            lager:debug("found ~p for ~p without an offer", [Pid, HotspotName]),
                            ok = blockchain_state_channels_cache:insert_hotspot(HotspotID, Pid),
                            blockchain_state_channels_worker:handle_packet(
                              Pid,
                              SCPacket,
                              HandlerPid
                             );
                        {error, _Reason} ->
                            lager:debug("could not find any state channels for ~p", [HotspotName]),
                            ok
                    end;
                Pid ->
                    blockchain_state_channels_worker:handle_packet(
                        Pid,
                        SCPacket,
                        HandlerPid
                    )
            end;
        {error, _Why} ->
            lager:debug("handle_packet failed: ~p", [_Why]),
            ok
    end.

-spec handle_worker_terminate(SC :: blockchain_state_channel_v1:state_channel(), Reason :: any()) -> ok.
handle_worker_terminate(SC, Reason) ->
    gen_server:cast(?SERVER, {handle_worker_terminate, SC, Reason}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = maps:get(swarm, Args),
    DB = blockchain_state_channels_db_owner:db(),
    SCF = blockchain_state_channels_db_owner:sc_servers_cf(),
    ok = blockchain_event:add_handler(self()),
    {Owner, OwnerSigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    %% This is cleanup just in case of restart
    Actives = blockchain_state_channels_cache:lookup_actives(),
    _ = [blockchain_state_channels_worker:shutdown(Pid, cleanup) || Pid <- Actives],
    ok = blockchain_state_channels_cache:overwrite_actives([]),
    %%
    _ = erlang:send_after(500, self(), post_init),
    {ok, #state{db=DB, cf=SCF, owner={Owner, OwnerSigFun}}}.

handle_call(get_all, _From, #state{state_channels=SCs}=State) ->
    {reply, SCs, State};
handle_call(get_actives, _From, #state{actives=ActiveSCs}=State) ->
    {reply, ActiveSCs, State};
handle_call({get_active_pid, ID}, _From, #state{actives=ActiveSCs}=State) ->
    Reply = 
        case lists:keyfind(ID, 2, ActiveSCs) of
            false -> undefined;
            {Pid, ID} -> Pid
        end,
    {reply, Reply, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({gc_state_channels, SCIDs}, #state{state_channels=SCs}=State) ->
    %% let's make sure whatever IDs we are getting rid of here we also dump
    %% from pending writes... we don't want some ID that's been
    %% deleted from the DB to ressurrect like a zombie because it was
    %% a pending write.
    ok = blockchain_state_channels_db_owner:gc(SCIDs),
    lists:foreach(
        fun(ID) ->
            case get_worker_pid(ID, State) of
                undefined ->
                    ok;
                Pid ->
                    ok = blockchain_state_channels_worker:shutdown(Pid, gc)
            end
        end,
        SCIDs
    ),
    {noreply, State#state{state_channels=maps:without(SCIDs, SCs)}};
handle_cast(maybe_get_new_active, State0) ->
    State1 =
        case State0#state.actives of
            [] ->
                lager:info("no active state channels, getting new one"),
                get_new_active(State0);
            _Active ->
                lager:info("~p active state channels, ignoring", [length(_Active)]),
                State0
        end,
    {noreply, State1};
handle_cast(get_new_active, State0) ->
    lager:info("get a new active state channel"),
    State1 = get_new_active(State0),
    {noreply, State1};
handle_cast({handle_worker_terminate, SC, Reason}, #state{state_channels=SCs}=State) ->
    ID = blockchain_state_channel_v1:id(SC),
    lager:info("~p is terminated ~p", [blockchain_utils:addr2name(ID), Reason]),
    {noreply, State#state{state_channels=maps:put(ID, SC, SCs)}};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(post_init, #state{chain=undefined}=State0) ->
    case blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), post_init),
            {noreply, State0};
        Chain ->
            {ok, BlockHeight} = blockchain:height(Chain),
            Ledger = blockchain:ledger(Chain),
            SCVer =
                case blockchain_ledger_v1:config(?sc_version, Ledger) of
                    {ok, SCV} ->
                        SCV;
                    _ ->
                        0
                end,
            State1 = State0#state{
                chain=Chain,
                sc_version=SCVer
            },
            {SCsWithSkewed, ActiveSCIDs} = load_state_channels(State1),
            State2 = start_workers(SCsWithSkewed, ActiveSCIDs, State1),
            StateChannels = maps:map(fun(_, {SC,_}) -> SC end, SCsWithSkewed),
            {noreply, State2#state{height=BlockHeight, state_channels=StateChannels}}
    end;
handle_info({blockchain_event, {new_chain, Chain}}, State) ->
    {noreply, State#state{chain=Chain}};
handle_info({blockchain_event, {add_block, _BlockHash, _Syncing, _Ledger}}, #state{chain=undefined}=State) ->
    erlang:send_after(500, self(), post_init),
    {noreply, State};
handle_info({blockchain_event, {add_block, BlockHash, _Syncing, _Ledger}}, #state{chain=Chain}=State) ->
    Self = self(),
    erlang:spawn(fun() ->
        lager:debug("fetching block ~p (syncing=~p)", [BlockHash, _Syncing]),
        case get_state_channel_txns_from_block(Chain, BlockHash, State) of
            {_, undefined} ->
                lager:error("failed to get block ~p", [BlockHash]);
            {Txns, Block} ->
                Self ! {got_block, Block, BlockHash, Txns}
        end
    end),
    {noreply, State};
handle_info({got_block, Block, _BlockHash, []}, State0) ->
    Height = blockchain_block:height(Block),
    lager:debug("no transactions found in ~p", [Height]),
    State1 = check_state_channel_expiration(Block, State0),
    {noreply, State1#state{height=Height}};
handle_info({got_block, Block, BlockHash, Txns}, State0) ->
    Height = blockchain_block:height(Block),
    lager:info("found ~p in ~p", [Txns, Height]),
    State1 =
        lists:foldl(
            fun(Txn, State) ->
                    case blockchain_txn:type(Txn) of
                        blockchain_txn_state_channel_open_v1 ->
                            opened_state_channel(Txn, BlockHash, Block, State);
                        blockchain_txn_state_channel_close_v1 ->
                            closed_state_channel(Txn, State)
                    end
            end,
            State0,
            Txns
        ),
    State2 = check_state_channel_expiration(Block, State1),
    {noreply, State2#state{height=Height}};
handle_info( {'DOWN', _Ref, process, Pid, _Reason}, #state{actives=Actives}=State0) ->
    {Pid, ID} = lists:keyfind(Pid, 1, Actives),
    lager:info("state channel ~p @ ~p went down: ~p", [blockchain_utils:addr2name(ID), Pid, _Reason]),
    ok = blockchain_state_channels_cache:delete_actives(Pid),
    State1 = State0#state{
        actives=lists:keydelete(Pid, 1, Actives)
    },
    {noreply, State1};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec handle_offer(
    Offer :: blockchain_state_channel_offer_v1:offer(),
    Ledger :: blockchain_ledger_v1:ledger(),
    HandlerPid :: pid()
) -> ok | reject.
handle_offer(Offer, Ledger, HandlerPid) ->
    HotspotID = blockchain_state_channel_offer_v1:hotspot(Offer),
    HotspotName = blockchain_utils:addr2name(HotspotID),
    case blockchain_state_channels_cache:lookup_hotspot(HotspotID) of
        undefined ->
            lager:debug("could not finds hotspot in cache for ~p", [HotspotName]),
            case select_best_active(HotspotID, Ledger) of
                {ok, Pid} ->
                    lager:debug("found ~p for ~p", [Pid, HotspotName]),
                    ok = blockchain_state_channels_cache:insert_hotspot(HotspotID, Pid),
                    blockchain_state_channels_worker:handle_offer(
                        Pid,
                        Offer,
                        HandlerPid
                    );
                {error, _Reason} ->
                    lager:debug("count not find any state channel for ~p", [HotspotName]),
                    ok = get_new_active(),
                    %% TODO: maybe we should not reject here?
                    reject
            end;
        Pid ->
            lager:debug("found ~p for ~p", [Pid, HotspotName]),
            blockchain_state_channels_worker:handle_offer(
                Pid,
                Offer,
                HandlerPid
            )
    end.

-spec select_best_active(
    HotspotID :: libp2p_crypto:pubkey_bin(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> {ok, pid()} | {error, not_found}.
select_best_active(HotspotID, Ledger) ->
    Actives = get_actives_from_cache(),
    Fun = fun(Pid, HID, Max) ->
        %% Only blocking call so far but done in parallel 
        SC = blockchain_state_channels_worker:get(Pid),
        case blockchain_state_channel_v1:can_fit(HID, SC, Max) of
            false -> false;
            {true, 1} ->
                ok = get_new_active(),
                {true, Pid};
            {true, _Spots} ->
                {true, Pid};
            %% we don't care about found ones because if process was alive it should have been in ets cache
            found -> {true, Pid}
        end
    end,
    MaxActorsAllowed = blockchain_ledger_v1:get_sc_max_actors(Ledger),
    Todos = [[Pid, HotspotID, MaxActorsAllowed] || Pid <- Actives],
    case blockchain_utils:pfind(Fun, Todos) of
        false -> {error, not_found};
        {true, Pid} -> {ok, Pid}
    end.

-spec get_actives_from_cache() -> list(pid()).
get_actives_from_cache() ->
    {Actives, Dead} =
        lists:partition(
            fun erlang:is_process_alive/1,
            blockchain_state_channels_cache:lookup_actives()
        ),
    case Dead of
        [] ->
            ok;
        _ ->
            lager:warning("we have some dead SC in cache ~p", [Dead]),
            ok = blockchain_state_channels_cache:overwrite_actives(Actives)
    end,
    Actives.

-spec maybe_get_new_active() -> ok.
maybe_get_new_active() ->
    gen_server:cast(?SERVER, maybe_get_new_active).


-spec get_new_active() -> ok.
get_new_active() ->
    gen_server:cast(?SERVER, get_new_active).

-spec get_new_active(State :: state()) -> state().
get_new_active(
    #state{
        db=DB,
        height=BlockHeight,
        state_channels=SCs,
        actives=Actives,
        sc_version=SCVersion
    }=State0
) ->
    case maps:to_list(maps:without([ID || {_, ID} <- Actives], SCs)) of
        [] ->
            lager:warning("don't have any state channel left unused"),
            State0;
        PassiveSCs ->
            %% We want to pick the next active state channel which has a higher block expiration
            %% but lower nonce
            Headroom =
                case application:get_env(blockchain, sc_headroom, 11) of
                    {ok, X} -> X;
                    X -> X
                end,
            FilterFun =
                fun({ID, SC}) ->
                    case SCVersion of
                        2 ->
                            case blockchain_state_channel_v1:fetch(DB, ID) of
                                {ok, _} ->
                                    ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
                                    ExpireAt > BlockHeight andalso
                                    blockchain_state_channel_v1:state(SC) == open andalso
                                    blockchain_state_channel_v1:amount(SC) > (blockchain_state_channel_v1:total_dcs(SC) + Headroom);
                                _Error ->
                                    lager:error("failed to fetch ~p", [blockchain_utils:addr2name(ID), _Error]),
                                    false
                            end;
                        _ ->
                            %% We are not on sc_version=2, just set this to true to include any state channel
                            true
                    end
                end,
            SCSortFun1 =
                fun({_ID1, SC1}, {_ID2, SC2}) ->
                    blockchain_state_channel_v1:expire_at_block(SC1) =< blockchain_state_channel_v1:expire_at_block(SC2)
                end,
            SCSortFun2 =
                fun({_ID1, SC1}, {_ID2, SC2}) ->
                    blockchain_state_channel_v1:nonce(SC1) >= blockchain_state_channel_v1:nonce(SC2)
                end,
            case lists:sort(SCSortFun2, lists:sort(SCSortFun1, lists:filter(FilterFun, PassiveSCs))) of
                [] ->
                    lager:warning("don't have any qualifying state channel left unused"),
                    State0;
                Filtered ->
                    [{ID, SC}|_] = Filtered,
                    {ok, {_, Skewed}} = blockchain_state_channel_v1:fetch(DB, ID),
                    Pid = start_worker(SC, Skewed, State0),
                    lager:info("~p is now active", [blockchain_utils:addr2name(ID)]),
                    State0#state{actives=[{Pid, ID}|Actives]}
            end
    end.

-spec opened_state_channel(
    Txn :: blockchain_txn_state_channel_open_v1:txn_state_channel_open(),
    BlockHash :: blockchain_block:hash(),
    Block :: blockchain_block:block(),
    State :: state()
) -> state().
opened_state_channel(
    Txn,
    BlockHash,
    Block,
    #state{
        db=DB,
        owner={Owner, OwnerSigFun},
        state_channels=SCs,
        actives=Actives
    }=State0
) ->
    ID = blockchain_txn_state_channel_open_v1:id(Txn),
    Amt = blockchain_txn_state_channel_open_v1:amount(Txn),
    ExpireWithin = blockchain_txn_state_channel_open_v1:expire_within(Txn),
    BlockHeight = blockchain_block:height(Block),
    ExpireAt = BlockHeight + ExpireWithin,
    {SC, Skewed} =
        blockchain_state_channel_v1:new(
            ID,
            Owner,
            Amt,
            BlockHash,
            ExpireAt
        ),
    SignedSC = blockchain_state_channel_v1:sign(SC, OwnerSigFun),
    ok = blockchain_state_channel_v1:save(DB, SignedSC, Skewed),
    State1 = State0#state{state_channels=maps:put(ID, SignedSC, SCs)},
    SCName = blockchain_utils:addr2name(blockchain_state_channel_v1:id(SignedSC)),
    lager:info("opened state channel ~p (with ~p DC) will expire at block ~p", [SCName, Amt, ExpireAt]),
    case Actives of
        [] ->
            lager:info("no active state channel setting ~p as active", [SCName]),
            Pid = start_worker(SC, Skewed, State1),
            State1#state{actives=[{Pid, ID}|Actives]};
        _ ->
            lager:debug("already got some active state channels"),
            State1
    end.

-spec closed_state_channel(
    Txn :: blockchain_txn_state_channel_close_v1:txn_state_channel_close(),
    State :: state()
) -> state().
closed_state_channel(Txn, #state{state_channels=SCs}=State) ->
    ClosedSC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
    ClosedID = blockchain_state_channel_v1:id(ClosedSC),
    State#state{
        state_channels=maps:remove(ClosedID, SCs)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Close expired state channels
%% @end
%%--------------------------------------------------------------------
-spec check_state_channel_expiration(
    Block :: blockchain_block:block(),
    State :: state()
) -> state().
check_state_channel_expiration(
    Block,
    #state{
        owner={Owner, OwnerSigFun},
        state_channels=SCs0
    }=State
) ->
    BlockHeight = blockchain_block:height(Block),
    SCs1 = maps:map(
        fun(ID, SC) ->
            ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
            case ExpireAt =< BlockHeight andalso blockchain_state_channel_v1:state(SC) == open of
                false ->
                    SC;
                true ->
                    lager:info("closing ~p expired", [blockchain_utils:addr2name(ID)]),
                    LatestSC =
                        case get_worker_pid(ID, State) of
                            undefined ->
                                SC;
                            Pid ->
                                blockchain_state_channels_worker:get(Pid)
                        end,
                    SC0 = blockchain_state_channel_v1:state(closed, LatestSC),
                    SC1 = blockchain_state_channel_v1:sign(SC0, OwnerSigFun),
                    ok = expire_state_channel(SC1, Owner, OwnerSigFun, State),
                    SC1
            end
        end,
        SCs0
    ),
    State#state{state_channels=SCs1}.

%%--------------------------------------------------------------------
%% @doc
%% Close state channel
%% @end
%%--------------------------------------------------------------------
-spec expire_state_channel(
    SC :: blockchain_state_channel_v1:state_channel(),
    Owner :: libp2p_crypto:pubkey_bin(),
    OwnerSigFun :: function(),
    State :: state()
) -> ok.
expire_state_channel(SC, Owner, OwnerSigFun, State) ->
    SignedSC = blockchain_state_channel_v1:sign(SC, OwnerSigFun),
    Txn = blockchain_txn_state_channel_close_v1:new(SignedSC, Owner),
    SignedTxn = blockchain_txn_state_channel_close_v1:sign(Txn, OwnerSigFun),
    ok = blockchain_worker:submit_txn(SignedTxn),
    ID = blockchain_state_channel_v1:id(SC),
    Name = blockchain_utils:addr2name(ID),
    lager:info(
        "submit close state channel txn for ~p: ~p",
        [Name, SignedTxn]
    ),
    case get_worker_pid(ID, State) of
        undefined ->
            lager:warning("failed to find pid for ~p", [Name]);
        Pid ->
            ok = blockchain_state_channels_worker:shutdown(Pid, expired),
            ok = maybe_get_new_active()
    end,
    ok.

-spec get_worker_pid(
    ID :: blockchain_state_channel_v1:id(),
    State :: state()
) -> pid() | undefined.
get_worker_pid(ID, #state{actives=Actives}) ->
    case lists:keyfind(ID, 2, Actives) of
        {Pid, ID} ->
            case erlang:is_process_alive(Pid) of
                false -> undefined;
                true -> Pid
            end;
        _ ->
            undefined
    end.

-spec start_workers(
    SCsWithSkewed :: #{blockchain_state_channel_v1:id() => {blockchain_state_channel_v1:state_channel(), skewed:skewed()}},
    ActiveSCIDs :: [blockchain_state_channel_v1:id()],
    State :: state()
) -> state().
start_workers(_SCsWithSkewed, [], State) ->
    get_new_active(State);
start_workers(SCsWithSkewed, ActiveSCIDs, State0) ->
    lists:foldl(
        fun(ID, #state{actives=Actives}=State) ->
            {SC, Skewed} = maps:get(ID, SCsWithSkewed),
            Pid = start_worker(SC, Skewed, State),
            State#state{actives=[{Pid, ID}|Actives]}
        end,
        State0,
        ActiveSCIDs
    ).

-spec start_worker(
    SC :: blockchain_state_channel_v1:state_channel(),
    Skewed :: skewed:skewed(),
    State :: state()
) -> pid().
start_worker(SC, Skewed, #state{db=DB, chain=Chain, owner=Owner}) ->
    Args = #{
        state_channel => SC,
        chain => Chain,
        skewed => Skewed,
        db => DB,
        owner => Owner
    },
    {ok, Pid} = blockchain_state_channels_worker:start(Args),
    _Ref = erlang:monitor(process, Pid),
    ok = blockchain_state_channels_cache:insert_actives(Pid),
    Pid.

%%--------------------------------------------------------------------
%% @doc
%% Get Block and open/close transactions
%% @end
%%--------------------------------------------------------------------
-spec get_state_channel_txns_from_block(
    Chain :: blockchain:blockchain(),
    BlockHash :: blockchain_block:hash(),
    State :: state()
) ->
    {[
        blockchain_txn_state_channel_open_v1:txn_state_channel_open() |
        blockchain_txn_state_channel_close_v1:txn_state_channel_close()
    ], undefined | blockchain_block:block()}.
get_state_channel_txns_from_block(Chain, BlockHash, #state{owner={Owner, _}}) ->
    case blockchain:get_block(BlockHash, Chain) of
        {error, _Reason} ->
            lager:error("failed to get block:~p ~p", [BlockHash, _Reason]),
            {[], undefined};
        {ok, Block} ->
            {lists:filter(
                fun(Txn) ->
                    case blockchain_txn:type(Txn) of
                        blockchain_txn_state_channel_open_v1 ->
                            blockchain_txn_state_channel_open_v1:owner(Txn) == Owner;
                        blockchain_txn_state_channel_close_v1 ->
                            SC = blockchain_txn_state_channel_close_v1:state_channel(Txn),
                            blockchain_state_channel_v1:owner(SC) == Owner;
                        _ ->
                            false
                    end
                end,
                blockchain_block:transactions(Block)
            ), Block}
    end.

-spec load_state_channels(State0 :: state()) ->
    {#{blockchain_state_channel_v1:id() => {blockchain_state_channel_v1:state_channel(), skewed:skewed()}},
     [blockchain_state_channel_v1:id()]}.
load_state_channels(#state{db=DB, chain=Chain}=State0) ->
    LedgerSCs = get_state_channels_from_ledger(State0),
    LedgerSCKeys = maps:keys(LedgerSCs),
    lager:info("state channels rehydrated from ledger: ~p", [[blockchain_utils:addr2name(ID)|| ID <- LedgerSCKeys]]),
   
    DBSCs = 
        lists:foldl(
            fun(ID, Acc) ->
                    case blockchain_state_channel_v1:fetch(DB, ID) of
                        {error, _Reason} ->
                            lager:warning("could not get state channel ~p: ~p",
                                          [blockchain_utils:addr2name(ID), _Reason]),
                            Acc;
                        {ok, {SC, Skewed}} ->
                            lager:info("updating state from scdb ID: ~p ~p",
                                        [blockchain_utils:addr2name(ID), SC]),
                            maps:put(ID, {SC, Skewed}, Acc)
                    end
            end,
            #{},
            LedgerSCKeys
        ),
    
    DBSCKeys = maps:keys(DBSCs),
    lager:info("fetched state channels from database writes: ~p", [[blockchain_utils:addr2name(ID)|| ID <- DBSCKeys]]),

    %% These don't exist in the db but we have them in the ledger, to avoid any conflict we will ignore them
    NotInDBKeys = LedgerSCKeys -- DBSCKeys,
    lager:warning("not in db sc ids: ~p", [[blockchain_utils:addr2name(I) || I <- NotInDBKeys]]),

    %% Merge DBSCs with LedgerSCs with only matching IDs and remove not in DB state channels 
    SCs = maps:without(NotInDBKeys, maps:merge(LedgerSCs, maps:with(LedgerSCKeys, DBSCs))),
    lager:info("scs after merge: ~p", [[blockchain_utils:addr2name(ID)|| ID <- maps:keys(SCs)]]),

    %% These don't exist in the ledger but we have them in the sc db, presumably these have been closed
    ClosedSCIDs = DBSCKeys -- LedgerSCKeys,
    lager:warning("presumably closed sc ids: ~p", [[blockchain_utils:addr2name(I) || I <- ClosedSCIDs]]),

    {ok, BlockHeight} = blockchain:height(Chain),
    Headroom =
        case application:get_env(blockchain, sc_headroom, 11) of
            {ok, X} -> X;
            X -> X
        end,
    ActiveSCIDs =
        maps:fold(
            fun(ID, {SC, _}, Acc) ->
                ExpireAt = blockchain_state_channel_v1:expire_at_block(SC),
                case
                    ExpireAt > BlockHeight andalso
                    blockchain_state_channel_v1:state(SC) == open andalso
                    blockchain_state_channel_v1:amount(SC) >
                        (blockchain_state_channel_v1:total_dcs(SC) + Headroom) andalso
                    erlang:length(blockchain_state_channel_v1:summaries(SC)) > 0
                of
                    true ->
                        [ID|Acc];
                    false ->
                        Acc
                end
            end,
            [],
            SCs
        ),
    SortedActiveSCIDs =
        lists:sort(
            fun(IDA, IDB) ->
                {SCA, _} = maps:get(IDA, SCs),
                {SCB, _} = maps:get(IDB, SCs),
                erlang:length(blockchain_state_channel_v1:summaries(SCA)) >
                    erlang:length(blockchain_state_channel_v1:summaries(SCB))
            end,
            ActiveSCIDs
        ),
    {SCs, SortedActiveSCIDs}.

-spec get_state_channels_from_ledger(State :: state()) ->
    #{blockchain_state_channel_v1:id() => {blockchain_state_channel_v1:state_channel(), skewed:skewed()}}.
get_state_channels_from_ledger(#state{chain=Chain, owner={Owner, OwnerSigFun}}) ->
    Ledger = blockchain:ledger(Chain),
    {ok, LedgerSCs} = blockchain_ledger_v1:find_scs_by_owner(Owner, Ledger),
    {ok, Head} = blockchain:head_block(Chain),
    maps:map(
        fun(ID, LedgerStateChannel) ->
            SCMod = blockchain_ledger_v1:get_sc_mod(LedgerStateChannel, Ledger),
            Owner = SCMod:owner(LedgerStateChannel),
            ExpireAt = SCMod:expire_at_block(LedgerStateChannel),
            Amount =
                case SCMod of
                    blockchain_ledger_state_channel_v2 ->
                        SCMod:original(LedgerStateChannel);
                    _ ->
                        0
                end,
            SC0 = blockchain_state_channel_v1:new(ID, Owner, Amount),
            Nonce = SCMod:nonce(LedgerStateChannel),
            Filter =
                fun(T) ->
                    blockchain_txn:type(T) == blockchain_txn_state_channel_open_v1 andalso
                    blockchain_txn_state_channel_open_v1:id(T) == ID andalso
                    blockchain_txn_state_channel_open_v1:nonce(T) == Nonce
                end,
            BlockHash =
                blockchain:fold_chain(
                    fun(Block, undefined) ->
                        case blockchain_utils:find_txn(Block, Filter) of
                            [_T] ->
                                blockchain_block:hash_block(Block);
                            _ ->
                                undefined
                        end;
                    (_, _Hash) ->
                        return
                    end,
                    undefined,
                    Head,
                    Chain
            ),
            SC1 = blockchain_state_channel_v1:expire_at_block(ExpireAt, SC0),
            SignedSC = blockchain_state_channel_v1:sign(SC1, OwnerSigFun),
            Skewed = skewed:new(BlockHash),
            {SignedSC, Skewed}
        end,
        LedgerSCs
    ).
