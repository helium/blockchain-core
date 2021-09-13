%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_server).

-behavior(gen_server).

-include("blockchain_vars.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    get_all/0,
    get_actives/0,
    handle_offer/3,
    handle_packet/4
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

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    cf :: rocksdb:cf_handle() | undefined,
    chain = undefined :: blockchain:blockchain() | undefined,
    owner = undefined :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()} | undefined,
    state_channels = #{} :: state_channels(),
    actives = #{} :: #{pid() => blockchain_state_channel_v1:id()},
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
    gen_server:call(?SERVER, get_all, infinity).

-spec get_actives() -> [blockchain_state_channel_v1:id()].
get_actives() ->
    gen_server:call(?SERVER, get_actives, infinity).

-spec handle_offer(
    Offer :: blockchain_state_channel_offer_v1:offer(),
    SCPacketHandler :: atom(),
    HandlerPid :: pid()
) -> ok | reject.
handle_offer(Offer, SCPacketHandler, HandlerPid) ->
    case blockchain_state_channel_offer_v1:validate(Offer) of
        {error, _Reason} ->
            lager:debug("offer failed to validate ~p ~p", [_Reason, Offer]),
            reject;
        true ->
            case SCPacketHandler:handle_offer(Offer, HandlerPid) of
                {error, _Why} ->
                    reject;
                ok ->
                    HotspotID = blockchain_state_channel_offer_v1:hotspot(Offer),
                    case blockchain_state_channels_cache:lookup_hotspot(HotspotID) of
                        undefined ->
                            ok; %% TODO: select new
                        Pid ->
                            blockchain_state_channels_worker:handle_offer(
                                Pid,
                                Offer,
                                HandlerPid
                            )
                    end
            end
    end.

-spec handle_packet(blockchain_state_channel_packet_v1:packet(), pos_integer(), atom(), pid()) -> ok.
handle_packet(SCPacket, PacketTime, SCPacketHandler, HandlerPid) ->
    case SCPacketHandler:handle_packet(SCPacket, PacketTime, HandlerPid) of
        ok ->
            HotspotID = blockchain_state_channel_packet_v1:hotspot(SCPacket),
            case blockchain_state_channels_cache:lookup_hotspot(HotspotID) of
                undefined ->
                    ok; %% TODO: maybe select new?
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
    erlang:send_after(500, self(), post_init),
    {ok, #state{db=DB, cf=SCF, owner={Owner, OwnerSigFun}}}.

handle_call(get_all, _From, #state{state_channels=SCs}=State) ->
    {reply, SCs, State};
handle_call(get_actives, _From, #state{state_channels=SCs, actives=ActiveSCs}=State) ->
    {reply, maps:with(maps:values(ActiveSCs), SCs), State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(post_init, #state{chain=undefined}=State0) ->
    case blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), post_init),
            {noreply, State0};
        Chain ->
            Ledger = blockchain:ledger(Chain),
            SCVer =
                case blockchain_ledger_v1:config(?sc_version, Ledger) of
                    {ok, SCV} ->
                        SCV;
                    _ ->
                        0
                end,
            State1 = State0#state{chain=Chain, sc_version=SCVer},
            {SCsWithSkewed, ActiveSCIDs} = load_state_channels(State1),
            State2 = start_workers(SCsWithSkewed, ActiveSCIDs, State1),
            {noreply, State2}
    end;
handle_info({blockchain_event, {new_chain, Chain}}, State) ->
    {noreply, State#state{chain=Chain}};
handle_info({blockchain_event, {add_block, _BlockHash, _Syncing, _Ledger}}, #state{chain=undefined}=State) ->
    erlang:send_after(500, self(), post_init),
    {noreply, State};
handle_info({blockchain_event, {add_block, BlockHash, _Syncing, _Ledger}}, #state{chain=Chain}=State0) ->
    case get_state_channel_txns_from_block(Chain, BlockHash, State0) of
        {_, undefined} ->
            lager:error("failed to get block ~p", [BlockHash]),
            {noreply, State0};
        {[], _} ->
            lager:debug("no transactions found in ~p", [BlockHash]),
            {noreply, State0};
        {Txns, Block} ->
            State1 =
                lists:foldl(
                    fun(Txn, State) ->
                            case blockchain_txn:type(Txn) of
                                blockchain_txn_state_channel_open_v1 ->
                                    open_state_channel(Txn, BlockHash, Block, State);
                                blockchain_txn_state_channel_close_v1 ->
                                    State % TODO: shutdown worker
                            end
                    end,
                    State0,
                    Txns
                ),
                %% TODO: check expiration
            {noreply, State1}
    end;
handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{state_channels=SCs, actives=Actives}=State) ->
    %% TODO: we need to do more here
    ID = maps:get(Pid, Actives),
    lager:info("~p went @ ~p down: ~p", [blockchain_utils:addr2name(ID), Pid, _Reason]),
    {noreply, State#state{state_channels=maps:remove(ID, SCs), actives=maps:remove(Pid, Actives)}};
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

-spec open_state_channel(
    Txn :: blockchain_txn_state_channel_open_v1:txn_state_channel_open(),
    BlockHash :: blockchain_block:hash(),
    Block :: blockchain_block:block(),
    State :: state()
) -> state().
open_state_channel(
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
    {SC, Skewed} =
        blockchain_state_channel_v1:new(
            ID,
            Owner,
            Amt,
            BlockHash,
            (BlockHeight + ExpireWithin)
        ),
    SignedSC = blockchain_state_channel_v1:sign(SC, OwnerSigFun),
    ok = blockchain_state_channel_v1:save(DB, SignedSC, Skewed),
    State1 = State0#state{state_channels=maps:put(ID, SignedSC, SCs)},
    case Actives of
        [] ->
            lager:info("no active state channel setting ~p as active",
                        [blockchain_utils:addr2name(blockchain_state_channel_v1:id(SignedSC))]),
            Pid = start_worker(SC, Skewed, State1),
            State1#state{actives=maps:put(Pid, ID, Actives)};
        _ ->
            lager:debug("already got some active state channels"),
            State1
    end.

-spec start_workers(
    SCsWithSkewed :: #{blockchain_state_channel_v1:id() => {blockchain_state_channel_v1:state_channel(), skewed:skewed()}},
    ActiveSCIDs :: [blockchain_state_channel_v1:id()],
    State :: state()
) -> state().
start_workers(SCsWithSkewed, ActiveSCIDs, State0) ->
    lists:foldl(
        fun(ID, #state{actives=Actives}=State) ->
            {SC, Skewed} = maps:get(ID, SCsWithSkewed),
            Pid = start_worker(SC, Skewed, State),
            State#state{actives=maps:put(Pid, ID, Actives)}
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
                        _ -> false
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
    lager:info("state channels rehydrated from ledger: ~p", [LedgerSCs]),
    LedgerSCKeys = maps:keys(LedgerSCs),
    DBSCs = lists:foldl(
              fun(ID, Acc) ->
                      case blockchain_state_channel_v1:fetch(DB, ID) of
                          {error, _Reason} ->
                              % TODO: Maybe cleanup not_found state channels from list
                              lager:warning("could not get state channel ~p: ~p",
                                            [blockchain_utils:addr2name(ID), _Reason]),
                              Acc;
                          {ok, {SC, Skewed}} ->
                              lager:info("updating state from scdb ID: ~p ~p",
                                         [blockchain_utils:addr2name(ID), SC]),
                              maps:put(ID, {SC, Skewed}, Acc)
                      end
              end,
              #{}, LedgerSCKeys),
    lager:info("fetched state channels from database writes: ~p", [DBSCs]),
    %% Merge DBSCs with LedgerSCs with only matching IDs
    SCs = maps:merge(LedgerSCs, maps:with(LedgerSCKeys, DBSCs)),
    lager:info("scs after merge: ~p", [SCs]),

    %% These don't exist in the ledger but we have them in the sc db,
    %% presumably these have been closed
    ClosedSCIDs = maps:keys(maps:without(LedgerSCKeys, DBSCs)),
    lager:info("presumably closed sc ids: ~p", [[blockchain_utils:addr2name(I) || I <- ClosedSCIDs]]),

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

-spec get_state_channels_from_ledger(State :: state()) -> state_channels().
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
                    blockchain_ledger_state_channel_v2 -> SCMod:original(LedgerStateChannel);
                    _ -> 0
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