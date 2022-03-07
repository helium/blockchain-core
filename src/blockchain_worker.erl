% %%-------------------------------------------------------------------
%% @doc
%% == Blockchain Core Worker ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_worker).

-behavior(gen_server).

-include("blockchain_vars.hrl").
-include_lib("kernel/include/file.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    blockchain/0, blockchain/1,
    num_consensus_members/0,
    consensus_addrs/0,
    integrate_genesis_block/1,
    integrate_genesis_block_synchronously/1,
    submit_txn/1, submit_txn/2,
    peer_height/3,
    notify/1,
    mismatch/0,
    signed_metadata_fun/0,

    new_ledger/1,

    load/2,

    maybe_sync/0,
    target_sync/3,
    sync/0,
    cancel_sync/0,
    pause_sync/0,
    sync_paused/0,

    set_resyncing/3,
    resync_done/0,
    is_resyncing/0,

    set_absorbing/3,
    absorb_done/0,
    is_absorbing/0,

    install_snapshot/4,
    install_snapshot_from_file/1,
    install_aux_snapshot/1,
    reset_ledger_to_snap/2,
    async_reset/1,

    grab_snapshot/2,

    add_commit_hook/3, add_commit_hook/4,
    remove_commit_hook/1,

    monitor_rocksdb_gc/1
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

-include("blockchain.hrl").

-define(SERVER, ?MODULE).
-define(READ_SIZE, 16 * 1024). % read 16 kb chunks
-define(WEEK_OLD_SECONDS, 7*24*60*60). %% a week's worth of seconds
-define(MAX_ATTEMPTS, 3).

-ifdef(TEST).
-define(SYNC_TIME, 1000).
-else.
-define(SYNC_TIME, 75000).
-endif.

-type snap_hash() :: binary().

-record(snapshot_info, {
          hash = <<>> :: snap_hash(),
          height = 0 :: integer(),
          etag = undefined :: undefined | string(),
          file_hash = undefined :: undefined | binary(),
          file_size = undefined :: undefined | pos_integer(),
          is_compressed = false :: boolean(),
          last_success = 0 :: non_neg_integer(), %% posix time of last successful download
          download_attempts = 0 :: non_neg_integer()
         }).

-type snapshot_info() :: #snapshot_info{}.

-record(state,
        {
         blockchain :: undefined | {no_genesis, blockchain:blockchain()} | blockchain:blockchain(),
         swarm_tid :: undefined | ets:tab(),
         sync_timer = make_ref() :: reference(),
         sync_ref = make_ref() :: reference(),
         sync_pid :: undefined | pid(),
         sync_paused = false :: boolean(),
         snapshot_timer = undefined :: undefined | reference(),
         snapshot_info :: undefined | snapshot_info(),
         gossip_ref = make_ref() :: reference(),
         absorb_info :: undefined | {pid(), reference()},
         absorb_retries = 3 :: pos_integer(),
         resync_info :: undefined | {pid(), reference()},
         resync_retries = 3 :: pos_integer(),
         rocksdb_gc_mref :: undefined | reference(),
         mode = normal :: snapshot | normal | reset
        }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, [{hibernate_after, 5000}]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec blockchain() -> blockchain:blockchain()  | undefined.
blockchain() ->
    gen_server:call(?SERVER, blockchain, infinity).

-spec blockchain(blockchain:blockchain()) -> ok.
blockchain(Chain) ->
    gen_server:call(?SERVER, {blockchain, Chain}, infinity).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec num_consensus_members() -> integer().
num_consensus_members() ->
    gen_server:call(?SERVER, num_consensus_members, infinity).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_addrs() -> {ok, [libp2p_crypto:pubkey_bin()]} | {error, any()}.
consensus_addrs() ->
    gen_server:call(?SERVER, consensus_addrs, infinity).

sync() ->
    gen_server:call(?SERVER, sync, infinity).

cancel_sync() ->
    gen_server:call(?SERVER, cancel_sync, infinity).

pause_sync() ->
    gen_server:call(?SERVER, pause_sync, infinity).

maybe_sync() ->
    gen_server:cast(?SERVER, maybe_sync).

target_sync(Target, Heights, GossipedHash) ->
    gen_server:cast(?SERVER, {target_sync, Target, Heights, GossipedHash}).

sync_paused() ->
    try
        gen_server:call(?SERVER, sync_paused, 100)  % intentionally very low
    catch _:_ ->
            true  % it's fine to occasionally get this wrong
    end.

new_ledger(Dir) ->
    gen_server:call(?SERVER, {new_ledger, Dir}, infinity).

load(BaseDir, GenDir) ->
    gen_server:cast(?SERVER, {load, BaseDir, GenDir}).

-spec set_absorbing(blockchain_block:block(), blockchain:blockchain(), boolean()) -> ok.
set_absorbing(Block, Blockchain, Syncing) ->
    gen_server:cast(?SERVER, {set_absorbing, Block, Blockchain, Syncing}).

-spec reset_ledger_to_snap(binary(), pos_integer()) -> ok.
reset_ledger_to_snap(Hash, Height) ->
    gen_server:call(?SERVER, {reset_ledger_to_snap, Hash, Height}, infinity).

install_snapshot(Height, Hash, Snapshot, BinSnap) ->
    gen_server:call(?SERVER, {install_snapshot, Height, Hash, Snapshot, BinSnap}, infinity).

install_snapshot_from_file(Filename) ->
    gen_server:call(?SERVER, {install_snapshot_from_file, Filename}, infinity).

install_aux_snapshot(Snapshot) ->
    gen_server:call(?SERVER, {install_aux_snapshot, Snapshot}, infinity).

absorb_done() ->
    gen_server:call(?SERVER, absorb_done, infinity).

is_absorbing() ->
    gen_server:call(?SERVER, is_absorbing, infinity).

-spec set_resyncing(pos_integer(), pos_integer(), blockchain:blockchain()) -> ok.
set_resyncing(ChainHeight, LedgerHeight, Blockchain) ->
    gen_server:cast(?SERVER, {set_resyncing, ChainHeight, LedgerHeight, Blockchain}).

resync_done() ->
    gen_server:call(?SERVER, resync_done, infinity).

is_resyncing() ->
    gen_server:call(?SERVER, is_resyncing, infinity).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec integrate_genesis_block(blockchain_block:block()) -> ok.
integrate_genesis_block(Block) ->
    gen_server:cast(?SERVER, {integrate_genesis_block, Block}).

-spec integrate_genesis_block_synchronously(blockchain_block:block()) ->
    ok | {error, block_not_genesis}. % TODO Other errors?
integrate_genesis_block_synchronously(Block) ->
    Msg = {integrate_genesis_block_synchronously, Block},
    gen_server:call(?SERVER, Msg, infinity).

%%--------------------------------------------------------------------
%% @doc
%% submit_txn/1 is deprecated.  use blockchain_txn_mgr:submit_txn/2 instead
%% @end
%%--------------------------------------------------------------------
-spec submit_txn(blockchain_txn:txn()) -> ok.
submit_txn(Txn) ->
    gen_server:cast(?SERVER, {submit_txn, Txn}).

%%--------------------------------------------------------------------
%% @doc
%% submit_txn/2 is deprecated.  use blockchain_txn_mgr:submit_txn/2 instead
%% @end
%%--------------------------------------------------------------------
-spec submit_txn(blockchain_txn:txn(), fun()) -> ok.
submit_txn(Txn, Callback) ->
    gen_server:cast(?SERVER, {submit_txn, Txn, Callback}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec peer_height(integer(), blockchain_block:hash(), libp2p_crypto:pubkey_bin()) -> ok.
peer_height(Height, Head, Sender) ->
    gen_server:cast(?SERVER, {peer_height, Height, Head, Sender}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec notify(any()) -> ok.
notify(Msg) ->
    ok = gen_event:sync_notify(?EVT_MGR, Msg).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec mismatch() -> ok.
mismatch() ->
    gen_server:cast(?SERVER, mismatch).

-spec async_reset(pos_integer()) -> ok.
async_reset(Height) ->
    gen_server:cast(?SERVER, {async_reset, Height}).

add_commit_hook(CF, HookIncFun, HookEndFun) ->
    gen_server:call(?SERVER, {add_commit_hook, CF, HookIncFun, HookEndFun}).

add_commit_hook(CF, HookIncFun, HookEndFun, Pred) ->
    gen_server:call(?SERVER, {add_commit_hook, CF, HookIncFun, HookEndFun, Pred}).

remove_commit_hook(RefOrAtom) ->
    gen_server:call(?SERVER, {remove_commit_hook, RefOrAtom}).

monitor_rocksdb_gc(Pid) ->
    gen_server:cast(?SERVER, {monitor_rocksdb_gc, Pid}).

signed_metadata_fun() ->
    %% cache the chain handle in the peerbook processes' dictionary
    Chain = case get(peerbook_md_fun_blockchain) of
                undefined ->
                    C = blockchain_worker:blockchain(),
                    put(peerbook_md_fun_blockchain, C),
                    C;
                C ->
                    C
            end,
    case Chain of
        undefined ->
            %% don't have a chain, no metadata to add
            #{};
        _ ->
            %% check if the rocksdb handle has died
            try
                HeightMD =
                    case blockchain:sync_height(Chain) of
                        {ok, Height} ->
                            #{<<"height">> => Height};
                        {error, _} ->
                            #{}
                    end,
                Ledger = blockchain:ledger(Chain),
                IsFollowing = application:get_env(blockchain, follow_mode, false),
                FPMD = case IsFollowing == false andalso blockchain:sync_height(Chain) == blockchain_ledger_v1:current_height(Ledger) of
                           true ->
                               Ht0 = maps:get(<<"height">>, HeightMD, 1),
                               Ht = max(1, Ht0 - (Ht0 rem 40)),
                               {ok, LedgerAt} = blockchain:ledger_at(Ht, Chain),
                               Res = case blockchain_ledger_v1:fingerprint(LedgerAt) of
                                   {ok, Fingerprint} ->
                                       maps:merge(HeightMD, Fingerprint);
                                   _ ->
                                       HeightMD
                               end,
                               blockchain_ledger_v1:delete_context(LedgerAt),
                               Res;
                           false ->
                               %% if the chain height and the ledger height diverge we can't meaningfully
                               %% report fingerprint hashes, so skip it here
                               %% TODO once we figure out the peer metadata gossip limit bug, we should
                               %% put both heights in the signed metadata which would allow us to report
                               %% fingerprints all the time
                               HeightMD
                       end,
                FPMD#{<<"last_block_add_time">> => blockchain:last_block_add_time(Chain)}
            catch
                _:_ ->
                    %% probably have an expired blockchain handle
                    %% don't retry here, to avoid looping, but delete our cached handle for next time
                    put(peerbook_md_fun_blockchain, undefined),
                    #{}
            end
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    SwarmTID = blockchain_swarm:tid(),
    %% allows the default interface to be to overridden, for example tests work better running with just 127.0.0.1 rather than running on all interfaces
    ListenInterface = application:get_env(blockchain, listen_interface, "0.0.0.0"),
    %% Get list of listening addresses. If deprecated 'ports' or 'port' variable used,
    %% assume listening IP of 0.0.0.0. otherwise use listen_addresses parameter.
    ListenAddrs = case application:get_env(blockchain, ports, undefined) of
                      undefined ->
                          case application:get_env(blockchain, port, undefined) of
                              undefined ->
                                  case application:get_env(blockchain, listen_addresses, undefined) of
                                      undefined -> ["/ip4/" ++ ListenInterface ++ "/tcp/0"];
                                      AddrList when is_list(erlang:hd(AddrList)) -> AddrList;
                                      AddrList when is_list(AddrList) -> [ AddrList ]
                                  end;
                              Port ->
                                  lager:warning("Using deprecated port configuration parameter. Switch to {listen_addresses, ~p}", [["/ip4/0.0.0.0/tcp/" ++ integer_to_list(Port)]]),
                                  ["/ip4/" ++ ListenInterface ++ "/tcp/" ++ integer_to_list(Port)]
                          end;
                      PortList when is_list(PortList) ->
                          lager:warning("Using deprecated ports configuration parameter. Swtich to {listen_addresses, ~p}",
                                        ["/ip4/" ++ ListenInterface ++ "/tcp/" ++ integer_to_list(Port) || Port <- PortList]),
                          ["/ip4/" ++ ListenInterface ++ "/tcp/" ++ integer_to_list(Port) || Port <- PortList]
                  end,
    {Blockchain, Ref} =
        case application:get_env(blockchain, autoload, true) of
            false ->
                %% some applications might not want the chain to load up and do work until they're ready
                {undefined, make_ref()};
            true ->
                BaseDir = proplists:get_value(base_dir, Args, "data"),
                GenDir = proplists:get_value(update_dir, Args, undefined),
                load_chain(SwarmTID, BaseDir, GenDir)
        end,

    true = lists:all(fun(E) -> E == ok end,
                     [ libp2p_swarm:listen(SwarmTID, Addr) || Addr <- ListenAddrs ]),
    NewState = #state{swarm_tid = SwarmTID, blockchain = Blockchain,
                gossip_ref = Ref},
    {Mode, Info} = get_sync_mode(NewState),
    SnapshotTimerRef = schedule_snapshot_timer(),
    {ok, NewState#state{snapshot_timer=SnapshotTimerRef, mode=Mode, snapshot_info=Info}}.

handle_call({integrate_genesis_block_synchronously, GenesisBlock}, _From, #state{}=S0) ->
    {Result, S1} = integrate_genesis_block_(GenesisBlock, S0),
    {reply, Result, S1};
handle_call(Msg, _From, #state{blockchain={no_genesis, _}}=State) ->
    lager:debug("Called when blockchain={no_genesis_}. Returning undefined. Msg: ~p", [Msg]),
    {reply, undefined, State};
handle_call(Msg, _From, #state{blockchain=undefined}=State) ->
    lager:debug("Called when blockchain=undefined. Returning undefined. Msg: ~p", [Msg]),
    {reply, undefined, State};
handle_call(num_consensus_members, _From, #state{blockchain = Chain} = State) ->
    {ok, N} = blockchain:config(?num_consensus_members, blockchain:ledger(Chain)),
    {reply, N, State};
handle_call(consensus_addrs, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)), State};
handle_call(blockchain, _From, #state{blockchain=Chain}=State) ->
    {reply, Chain, State};
handle_call({blockchain, NewChain}, _From, #state{swarm_tid = SwarmTID} = State) ->
    notify({new_chain, NewChain}),
    remove_handlers(SwarmTID),
    {ok, GossipRef} = add_handlers(SwarmTID, NewChain),
    {reply, ok, State#state{blockchain = NewChain, gossip_ref = GossipRef}};
handle_call({new_ledger, Dir}, _From, #state{blockchain=Chain}=State) ->
    %% We do this here so the same process that normally owns the ledger
    %% will own it when we do a reset ledger or whatever. Otherwise the
    %% snapshot cache ETS table can be owned by an ephemeral process.
    Ledger1 = blockchain_ledger_v1:new(Dir,
                                       blockchain:db_handle(Chain),
                                       blockchain:blocks_cf(Chain),
                                       blockchain:heights_cf(Chain),
                                       blockchain:info_cf(Chain)),
    {reply, {ok, Ledger1}, State};

handle_call({install_snapshot_from_file, Filename}, From, State) ->
    lager:info("attempting to load snapshot from file ~p", [Filename]),
    %% do this so the blockchain worker is the owner of the raw file handle
    {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize({file, Filename}),
    Hash = blockchain_ledger_snapshot_v1:hash(Snapshot),
    Height = blockchain_ledger_snapshot_v1:height(Snapshot),
    handle_call({install_snapshot, Height, Hash, Snapshot, {file, Filename}}, From, State);
handle_call({install_snapshot, Height, Hash, Snapshot, BinSnap}, _From,
            #state{blockchain = Chain, mode = Mode, swarm_tid = SwarmTID} = State) ->
    lager:info("installing snapshot ~p", [Hash]),
    %% I don't think that we want to auto-repair right now, do default
    %% this to disabled.  nothing currently will ever set the mode to
    %% reset, so we should never go into the `true` clause here.
    Halt = application:get_env(blockchain, halt_on_reset, true),
    case Mode == reset andalso Halt of
        false ->
            ok = blockchain_lock:acquire(),
            OldLedger = blockchain:ledger(Chain),
            blockchain_ledger_v1:clean(OldLedger),
            %% TODO proper error checking and recovery/retry
            NewLedger = blockchain_ledger_snapshot_v1:import(Chain, Height, Hash, Snapshot, BinSnap),
            Chain1 = blockchain:ledger(NewLedger, Chain),
            ok = blockchain:mark_upgrades(?BC_UPGRADE_NAMES, NewLedger),
            try
                %% There is a hole in the snapshot history where this will be
                %% true, but later it will have come from the snap.

                %% fail into the catch if it's an older record
                true = blockchain_ledger_snapshot_v1:is_v6(Snapshot),
                %% fail into the catch if it's missing
                H3dex = blockchain_ledger_snapshot_v1:get_h3dex(Snapshot),

                case length(H3dex) > 0 of
                    true -> ok;
                    false -> throw(bootstrap) % fail into the catch it's an empty default value
                end
            catch _:_ ->
                    NewLedger1 = blockchain_ledger_v1:new_context(NewLedger),
                    blockchain:bootstrap_h3dex(NewLedger1),
                    blockchain_ledger_v1:commit_context(NewLedger1)
            end,
            remove_handlers(SwarmTID),
            NewChain = blockchain:delete_temp_blocks(Chain1),
            notify({new_chain, NewChain}),
            {ok, GossipRef} = add_handlers(SwarmTID, NewChain),
            {ok, LedgerHeight} = blockchain_ledger_v1:current_height(NewLedger),
            {ok, ChainHeight} = blockchain:height(NewChain),
            case LedgerHeight >= ChainHeight of
                true -> ok;
                false ->
                    %% we likely retain some old blocks, and we should absorb them
                    set_resyncing(ChainHeight, LedgerHeight, NewChain)
            end,
            blockchain_lock:release(),
            {reply, ok, maybe_sync(State#state{mode = normal, sync_paused = false,
                                               blockchain = NewChain, gossip_ref = GossipRef})};
        true ->
            %% if we don't want to auto-clean the ledger, stop
            {stop, shutdown, State}
        end;

handle_call({install_aux_snapshot, Snapshot}, _From,
            #state{blockchain = Chain, swarm_tid = SwarmTID} = State) ->
    ok = blockchain_lock:acquire(),
    OldLedger = blockchain:ledger(Chain),
    blockchain_ledger_v1:clean_aux(OldLedger),
    NewLedger = blockchain_aux_ledger_v1:new(OldLedger),
    blockchain_ledger_snapshot_v1:load_into_ledger(Snapshot, NewLedger, aux),
    blockchain_ledger_snapshot_v1:load_blocks(blockchain_ledger_v1:mode(aux, NewLedger), Chain, Snapshot),
    NewChain = blockchain:ledger(NewLedger, Chain),
    remove_handlers(SwarmTID),
    {ok, GossipRef} = add_handlers(SwarmTID, NewChain),
    notify({new_chain, NewChain}),
    blockchain_lock:release(),
    {reply, ok, maybe_sync(State#state{mode = normal, sync_paused = false,
                                       blockchain = NewChain, gossip_ref = GossipRef})};

handle_call(sync, _From, State) ->
    %% if sync is paused, unpause it
    {reply, ok, maybe_sync(State#state{sync_paused = false})};
handle_call(cancel_sync, _From, State) ->
    {reply, ok, cancel_sync(State, true)};
handle_call(pause_sync, _From, State) ->
    {reply, ok, pause_sync(State)};
handle_call(sync_paused, _From, State) ->
    {reply, State#state.sync_paused, State};

handle_call(absorb_done, _From, #state{absorb_info = {_Pid, Ref}} = State) ->
    _ = erlang:demonitor(Ref, [flush]),
    {reply, ok, maybe_sync(State#state{absorb_info = undefined, sync_paused = false})};
handle_call(is_absorbing, _From, State) ->
    {reply, State#state.absorb_info /= undefined, State};

handle_call(resync_done, _From, #state{resync_info = {_Pid, Ref}} = State) ->
    _ = erlang:demonitor(Ref, [flush]),
    {reply, ok, maybe_sync(State#state{resync_info = undefined, sync_paused = false})};
handle_call(is_resyncing, _From, State) ->
    {reply, State#state.resync_info /= undefined, State};

handle_call({reset_ledger_to_snap, Hash, Height}, _From, State) ->
    {reply, ok, reset_ledger_to_snap(Hash, Height, State)};

handle_call({add_commit_hook, CF, HookIncFun, HookEndFun} , _From, #state{blockchain = Chain} = State) ->
    Ledger = blockchain:ledger(Chain),
    {Ref, Ledger1} = blockchain_ledger_v1:add_commit_hook(CF, HookIncFun, HookEndFun, Ledger),
    Chain1 = blockchain:ledger(Ledger1, Chain),
    {reply, Ref, State#state{blockchain = Chain1}};
handle_call({add_commit_hook, CF, HookIncFun, HookEndFun, Pred} , _From, #state{blockchain = Chain} = State) ->
    Ledger = blockchain:ledger(Chain),
    {Ref, Ledger1} = blockchain_ledger_v1:add_commit_hook(CF, HookIncFun, HookEndFun, Pred, Ledger),
    Chain1 = blockchain:ledger(Ledger1, Chain),
    {reply, Ref, State#state{blockchain = Chain1}};
handle_call({remove_commit_hook, RefOrCF} , _From, #state{blockchain = Chain} = State) ->
    Ledger = blockchain:ledger(Chain),
    Ledger1 = blockchain_ledger_v1:remove_commit_hook(RefOrCF, Ledger),
    Chain1 = blockchain:ledger(Ledger1, Chain),
    {reply, ok, State#state{blockchain = Chain1}};

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({load, BaseDir, GenDir}, #state{blockchain=undefined}=State) ->
    {Blockchain, Ref} = load_chain(State#state.swarm_tid, BaseDir, GenDir),
    {Mode, Info} = get_sync_mode(State#state{blockchain=Blockchain, gossip_ref=Ref}),
    NewState = State#state{blockchain = Blockchain, gossip_ref = Ref, mode=Mode, snapshot_info=Info},
    notify({new_chain, Blockchain}),
    {noreply, NewState};
handle_cast({integrate_genesis_block, GenesisBlock}, #state{}=S0) ->
    {_, S1} = integrate_genesis_block_(GenesisBlock, S0),
    {noreply, S1};
handle_cast(_, #state{blockchain=undefined}=State) ->
    {noreply, State};
handle_cast(_, #state{blockchain={no_genesis, _}}=State) ->
    {noreply, State};
handle_cast({set_absorbing, Block, Blockchain, Syncing}, State=#state{absorb_info=undefined, resync_info=undefined}) ->
    Info = spawn_monitor(
             fun() ->
                     blockchain:absorb_temp_blocks_fun(Block, Blockchain, Syncing)
             end),
    %% just don't sync, it's a waste of bandwidth
    {noreply, State#state{absorb_info = Info, sync_paused = true, absorb_retries = 3}};
handle_cast({set_absorbing, _Block, _Blockchain, _Syncing}, State) ->
    {noreply, State};
handle_cast({set_resyncing, BlockHeight, LedgerHeight, Blockchain}, State=#state{absorb_info=undefined, resync_info=undefined}) ->
    Info = spawn_monitor(
             fun() ->
                     blockchain:resync_fun(BlockHeight, LedgerHeight, Blockchain)
             end),
    %% just don't sync, it's a waste of bandwidth
    {noreply, State#state{resync_info = Info, sync_paused = true, resync_retries = 3}};
handle_cast({set_resyncing, _Block, _Blockchain, _Syncing}, State) ->
    {noreply, State};

handle_cast(maybe_sync, State) ->
    {noreply, maybe_sync(State)};
handle_cast({target_sync, Target, Heights, GossipedHash}, State) ->
    {noreply, target_sync(Target, Heights, GossipedHash, State)};
handle_cast({submit_txn, Txn}, State) ->
    ok = send_txn(Txn),
    {noreply, State};
handle_cast({submit_txn, Txn, Callback}, State) ->
    ok = send_txn(Txn, Callback),
    {noreply, State};
handle_cast({peer_height, Height, Head, _Sender}, #state{blockchain=Chain}=State) ->
    lager:info("got peer height message with blockchain ~p", [lager:pr(Chain, blockchain)]),
    NewState =
        case blockchain:head_block_info(Chain) of
            {error, _Reason} ->
                lager:error("could not get head info ~p", [_Reason]),
                State;
            {ok, #block_info_v2{hash = LocalHead, height = LocalHeight}} ->
                case LocalHeight < Height orelse (LocalHeight == Height andalso Head /= LocalHead) of
                    false ->
                        ok;
                    true ->
                        start_sync(State)
                end
        end,
    {noreply, NewState};
handle_cast({async_reset, _Height}, State) ->
    lager:info("got async_reset at height ~p, ignoring", [_Height]),
    {noreply, State};
handle_cast({monitor_rocksdb_gc, Pid}, State) ->
    MRef = monitor(process, Pid),
    {noreply, State#state{rocksdb_gc_mref = MRef}};

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(snapshot_timer_tick, State) ->
    Tref = schedule_snapshot_timer(),
    {Mode, Info} = get_sync_mode(State),
    {noreply, State#state{snapshot_timer = Tref, mode=Mode, snapshot_info=Info}};
handle_info(maybe_sync, State) ->
    {noreply, maybe_sync(State)};

handle_info({'DOWN', SyncRef, process, _SyncPid, normal},
            #state{sync_ref = SyncRef, mode = snapshot, snapshot_info = SnapInfo} = State) ->
    %% snapshot process completed normally;
    %%
    %% Schedule a sync "as usual" in "normal" mode
    lager:info("snapshot process completed normally; switching to normal sync mode."),
    {noreply, schedule_sync(State#state{mode=normal,
                       sync_pid = undefined,
                       snapshot_info = SnapInfo#snapshot_info{
                                         download_attempts = 0,
                                         last_success = erlang:system_time(seconds) }})};

%% snapshot attempt failed, so we will retry up to MAX ATTEMPTS
handle_info({'DOWN', SyncRef, process, _SyncPid, Reason},
            #state{sync_ref = SyncRef, mode = snapshot,
                   snapshot_info = #snapshot_info{ download_attempts = Attempts } = SnapInfo} = State) when Attempts < ?MAX_ATTEMPTS ->
    lager:warning("Snapshot attempt ~p exited with: ~p; retrying", [Attempts, Reason]),
    {noreply, snapshot_sync(State#state{ sync_pid = undefined,
                                         snapshot_info = SnapInfo#snapshot_info{download_attempts = Attempts + 1}})};

%% we hit MAX ATTEMPTS, so give up... we will retry again in snapshot sync interval hours
handle_info({'DOWN', SyncRef, process, _SyncPid, Reason},
            #state{sync_ref = SyncRef, mode = snapshot,
                   snapshot_info = #snapshot_info{ download_attempts = Attempts } = SnapInfo} = State) when Attempts >= ?MAX_ATTEMPTS ->
    lager:warning("Snapshot attempt ~p exited with: ~p; will retry again in a while...", [Attempts, Reason]),
    {noreply, schedule_sync(State#state{mode = normal, sync_pid = undefined, snapshot_info=SnapInfo#snapshot_info{ download_attempts = 0 }})};

%% "normal" sync mode handling...
handle_info({'DOWN', SyncRef, process, _SyncPid, Reason},
            #state{sync_ref = SyncRef, mode = normal, blockchain = Chain} = State0) ->
    State = State0#state{sync_pid = undefined},
    %% TODO: this sometimes we're gonna have a failed snapshot sync
    %% and we need to handle that here somehow.

    %% we're done with our sync.  determine if we're very far behind,
    %% and should resync immediately, or if we're relatively close to
    %% the present and can afford to retry later.
    case blockchain:head_block_info(Chain) of
        {ok, #block_info_v2{time = Time}} ->
            Now = erlang:system_time(seconds),
            case Now - Time of
                N when N < 0 ->
                    %% if blocktimes are in the future, we're confused about
                    %% the time, proceed as if we're synced.
                    {noreply, schedule_sync(State)};
                N when N < 30 * 60 andalso Reason == normal ->
                    %% relatively recent
                    {noreply, schedule_sync(State)};
                _ ->
                    case Reason of dial -> ok; _ -> lager:info("block sync down: ~p", [Reason]) end,
                    %% we're deep in the past here, or the last one errored out, so just start the next sync
                    {noreply, start_sync(State)}
            end;
        {error, not_found} ->
            lager:warning("cannot get head block"),
            {noreply, start_sync(State)}
    end;
handle_info({'DOWN', GossipRef, process, _GossipPid, _Reason},
            #state{gossip_ref = GossipRef, blockchain = Blockchain,
                   swarm_tid = SwarmTID} = State) ->
    Gossip = libp2p_swarm:gossip_group(SwarmTID),
    libp2p_group_gossip:add_handler(Gossip, ?GOSSIP_PROTOCOL_V1,
                                    {blockchain_gossip_handler, [SwarmTID, Blockchain]}),
    NewGossipRef = erlang:monitor(process, Gossip),
    {noreply, State#state{gossip_ref = NewGossipRef}};
handle_info({'DOWN', AbsorbRef, process, AbsorbPid, Reason},
            #state{absorb_info = {AbsorbPid, AbsorbRef}, absorb_retries = Retries} = State) ->
    case Reason of
        normal ->
            lager:info("Absorb process completed successfully"),
            {noreply, schedule_sync(State#state{sync_paused=false, absorb_info=undefined})};
        shutdown ->
            {noreply, State#state{absorb_info=undefined}};
        Reason when Retries > 0 ->
            lager:warning("Absorb process exited with reason ~p, retrying ~p more times", [Reason, Retries]),
            blockchain:init_assumed_valid(State#state.blockchain, get_assumed_valid_height_and_hash()),
            {noreply, State#state{absorb_info=undefined, absorb_retries = Retries - 1}};
        Reason ->
            lager:warning("Absorb process exited with reason ~p, stopping", [Reason]),
            %% ran out of retries
            {stop, Reason, State}
    end;
handle_info({'DOWN', ResyncRef, process, ResyncPid, Reason},
            #state{resync_info = {ResyncPid, ResyncRef}, resync_retries = Retries} = State) ->
    case Reason of
        normal ->
            lager:info("Resync process completed successfully"),
            %% check if we have any pending assume valids to take care of
            blockchain:init_assumed_valid(State#state.blockchain, get_assumed_valid_height_and_hash()),
            {noreply, schedule_sync(State#state{sync_paused=false, resync_info=undefined})};
        shutdown ->
            {noreply, State#state{resync_info=undefined}};
        Reason when Retries > 0 ->
            lager:warning("Resync process exited with reason ~p, retrying ~p more times", [Reason, Retries]),
            {noreply, State#state{resync_info=undefined, resync_retries = Retries - 1}};
        Reason ->
            lager:warning("Resync process exited with reason ~p, stopping", [Reason]),
            %% ran out of retries
            {stop, Reason, State}
    end;
handle_info({'DOWN', RocksGCRef, process, RocksGCPid, Reason},
            #state{rocksdb_gc_mref = RocksGCRef} = State) ->
    case Reason of
        normal ->
            lager:info("rocksdb_gc process completed normally");
        shutdown ->
            ok;
        Reason ->
            lager:error("rocksdb_gc pid ~p crashed because ~p", [RocksGCPid, Reason])
    end,
    {noreply, State#state{rocksdb_gc_mref = undefined}};

handle_info({blockchain_event, {new_chain, NC}}, State) ->
    {noreply, State#state{blockchain = NC}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{blockchain=undefined}) ->
    lager:debug("blockchain terminate ~p", [_Reason]),
    ok;
terminate(_Reason, #state{blockchain={no_genesis, Chain}}) ->
    lager:debug("blockchain terminate ~p", [_Reason]),
    catch blockchain:close(Chain),
    ok;
terminate(_Reason, #state{blockchain=Chain}) ->
    lager:debug("blockchain terminate ~p", [_Reason]),
    catch blockchain:close(Chain),
    ok.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

integrate_genesis_block_(
    GenesisBlock,
    #state{
        blockchain = {no_genesis, Chain},
        swarm_tid  = SwarmTid
    }=S0
) ->
    case blockchain_block:is_genesis(GenesisBlock) of
        false ->
            lager:warning("~p is not a genesis block", [GenesisBlock]),
            {{error, block_not_genesis}, S0};
        true ->
            ok = blockchain:integrate_genesis(GenesisBlock, Chain),
            [ConsensusAddrs] =
                [
                    blockchain_txn_consensus_group_v1:members(T)
                ||
                    T <- blockchain_block:transactions(GenesisBlock),
                    blockchain_txn:type(T) == blockchain_txn_consensus_group_v1
                ],
            lager:info(
                "blockchain started with ~p, consensus ~p",
                [lager:pr(Chain, blockchain), ConsensusAddrs]
            ),
            {ok, GenesisHash} = blockchain:genesis_hash(Chain),
            ok = notify({integrate_genesis_block, GenesisHash}),
            {ok, GossipRef} = add_handlers(SwarmTid, Chain),
            ok = blockchain_txn_mgr:set_chain(Chain),
            true = libp2p_swarm:network_id(SwarmTid, GenesisHash),
            self() ! maybe_sync,
            S1 =
                S0#state{
                    blockchain = Chain,
                    gossip_ref = GossipRef
                },
            {Mode, SyncInfo} = get_sync_mode(S1),
            {ok, S1#state{mode=Mode, snapshot_info=SyncInfo}}
    end;
integrate_genesis_block_(_, #state{}=State0) ->
    {{error, not_in_no_genesis_state}, State0}.

maybe_sync(#state{mode = normal} = State) ->
    maybe_sync_blocks(State);
maybe_sync(#state{mode = snapshot, blockchain = Chain,
                  sync_pid = Pid, snapshot_info = #snapshot_info{height = Height}} = State) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:current_height(Ledger) of
        %% still waiting for the background process to download and
        %% install the ledger snapshot
        {ok, _N} when Pid /= undefined ->
            reset_sync_timer(State);
        {ok, CurrHeight} ->
            case Height of
                %% when the current height is *lower* than than the
                %% snap height, start the sync
                Height when CurrHeight < Height - 1 ->
                    snapshot_sync(State);
                _ ->
                    reset_sync_timer(State)
            end;
        {error, Error} ->
            lager:info("couldn't get current height: ~p", [Error]),
            reset_sync_timer(State)
    end.

target_sync(_Target, _Heights, _GossipedHash, #state{sync_paused = true} = State) ->
    State;
target_sync(_Target, _Heights, _GossipedHash, #state{sync_pid = Pid} = State) when Pid /= undefined ->
    State;
target_sync(Target0, Heights, GossipedHash, #state{blockchain = Chain, swarm_tid = SwarmTID} = State) ->
    Target = libp2p_crypto:pubkey_bin_to_p2p(Target0),
    {Pid, Ref} = start_block_sync(SwarmTID, Chain, Target, Heights, GossipedHash),
    lager:info("targeted block sync starting with Pid: ~p, Ref: ~p, Peer: ~p",
               [Pid, Ref, Target]),
    State#state{sync_pid = Pid, sync_ref = Ref}.

maybe_sync_blocks(#state{sync_paused = true} = State) ->
    State;
maybe_sync_blocks(#state{sync_pid = Pid} = State) when Pid /= undefined ->
    State;
maybe_sync_blocks(#state{blockchain = Chain} = State) ->
    %% last block add time is relative to the system clock so as long as the local
    %% clock mostly increments this will eventually be true on a stuck node
    SyncCooldownTime = application:get_env(blockchain, sync_cooldown_time, 60),
    SkewedSyncCooldownTime = application:get_env(blockchain, skewed_sync_cooldown_time, 300),
    {ok, #block_info_v2{time = Time, height = Height}} = blockchain:head_block_info(Chain),
    case erlang:system_time(seconds) - Time of
        %% negative time means we're skewed, so rely on last add time
        %% until ntp fixes us.
        T when T < 0 ->
            case erlang:system_time(seconds) - blockchain:last_block_add_time(Chain) of
                X when X > SkewedSyncCooldownTime orelse Height == 1  ->
                    start_sync(State);
                _ ->
                    %% no need to sync now, check again later
                    schedule_sync(State)
            end;
        T when T > SyncCooldownTime orelse Height == 1 ->
            start_sync(State);
        _ ->
            %% no need to sync now, check again later
            schedule_sync(State)
    end.

snapshot_sync(#state{sync_pid = Pid} = State) when Pid /= undefined ->
    State;
snapshot_sync(State) ->
    {Pid, Ref, SnapInfo} = start_snapshot_sync(State),
    lager:info("snapshot_sync starting ~p ~p", [Pid, Ref]),
    State#state{sync_pid = Pid, sync_ref = Ref, mode = snapshot, snapshot_info=SnapInfo}.

reset_ledger_to_snap(Hash, Height, State) ->
    lager:info("clearing the ledger now"),
    State1 = pause_sync(State),
    SnapInfo = #snapshot_info{hash=Hash, height=Height},
    snapshot_sync(State1#state{snapshot_info=SnapInfo}).

start_sync(#state{blockchain = Chain, swarm_tid = SwarmTID} = State) ->
    case get_random_peer(SwarmTID) of
        no_peers ->
            %% try again later when there's peers
            schedule_sync(State);
        RandomPeer ->
            {Pid, Ref} = start_block_sync(SwarmTID, Chain, RandomPeer, [], <<>>),
            lager:info("new block sync starting with Pid: ~p, Ref: ~p, Peer: ~p",
                       [Pid, Ref, RandomPeer]),
            State#state{sync_pid = Pid, sync_ref = Ref}
    end.

-spec get_random_peer(SwarmTID :: ets:tab()) -> no_peers | string().
get_random_peer(SwarmTID) ->
    Peerbook = libp2p_swarm:peerbook(SwarmTID),
    %% limit peers to random connections with public addresses
    F = fun(Peer) ->
                case application:get_env(blockchain, testing, false) of
                    false ->
                        lists:any(fun libp2p_transport_tcp:is_public/1,
                                  libp2p_peer:listen_addrs(Peer));
                    true ->
                        true
                end
        end,
    case libp2p_peerbook:random(Peerbook, [], F, 100) of
        false -> no_peers;
        {Addr, _Peer} ->
            "/p2p/" ++ libp2p_crypto:bin_to_b58(Addr)
    end.

reset_sync_timer(State)  ->
    lager:info("try again in ~p", [?SYNC_TIME]),
    erlang:cancel_timer(State#state.sync_timer),
    Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
    State#state{sync_timer=Ref}.

cancel_sync(#state{sync_pid = undefined} = State, _Restart) ->
    State;
cancel_sync(#state{sync_pid = Pid, sync_ref = Ref} = State, Restart) ->
    case Restart of
        false ->
            erlang:demonitor(Ref, [flush]);
        _ -> ok
    end,
    Pid ! cancel,
    State#state{sync_pid = undefined, sync_ref = make_ref()}.

pause_sync(State) ->
    State1 = cancel_sync(State, false),
    State1#state{sync_paused = true}.

-spec add_handlers(ets:tab(), blockchain:blockchain()) -> {ok, reference()}.
add_handlers(SwarmTID, Blockchain) ->
    GossipPid = libp2p_swarm:gossip_group(SwarmTID),
    Ref = erlang:monitor(process, GossipPid),
    %% add the gossip handler
    ok = libp2p_group_gossip:add_handler(GossipPid, ?GOSSIP_PROTOCOL_V1,
                                         {blockchain_gossip_handler, [SwarmTID, Blockchain]}),

    %% add the sync handlers, sync handlers support multiple versions so we need to add for each
    SyncAddFun = fun(ProtocolVersion) ->
                        ok = libp2p_swarm:add_stream_handler(SwarmTID, ProtocolVersion,
                                {libp2p_framed_stream, server, [blockchain_sync_handler, ?SERVER, [ProtocolVersion, Blockchain]]}) end,
    lists:foreach(SyncAddFun, ?SUPPORTED_SYNC_PROTOCOLS),
    %% add the FF handlers, FF handlers support multiple versions so we need to add for each
    FFAddFun = fun(ProtocolVersion) ->
                        ok = libp2p_swarm:add_stream_handler(SwarmTID, ProtocolVersion,
                                {libp2p_framed_stream, server, [blockchain_fastforward_handler, ?SERVER, [ProtocolVersion, Blockchain]]}) end,
    lists:foreach(FFAddFun, ?SUPPORTED_FASTFORWARD_PROTOCOLS),
    case application:get_env(blockchain, follow_mode, false) of
        false ->
            ok = libp2p_swarm:add_stream_handler(
                   SwarmTID,
                   ?SNAPSHOT_PROTOCOL,
                   {libp2p_framed_stream, server, [blockchain_snapshot_handler, ?SERVER, Blockchain]}
                  );
        true ->
            %% don't serve snapshots from follower nodes for now because libp2p doesn't support
            %% streaming data off disk, and follower nodes tend to have less RAM
            ok
    end,
    ok = libp2p_swarm:add_stream_handler(
        SwarmTID,
        ?STATE_CHANNEL_PROTOCOL_V1,
        {libp2p_framed_stream, server, [blockchain_state_channel_handler, Blockchain]}
    ),

    {ok, Ref}.

-spec remove_handlers(ets:tab()) -> ok.
remove_handlers(SwarmTID) ->
    %% remove the gossip handler
    catch libp2p_group_gossip:remove_handler(libp2p_swarm:gossip_group(SwarmTID), ?GOSSIP_PROTOCOL_V1),

    %% remove the sync handlers
    SyncRemoveFun = fun(ProtocolVersion) ->
                        libp2p_swarm:remove_stream_handler(SwarmTID, ProtocolVersion) end,
    lists:foreach(SyncRemoveFun, ?SUPPORTED_SYNC_PROTOCOLS),
    %% remove the FF handlers
    FFRemoveFun = fun(ProtocolVersion) ->
                        libp2p_swarm:remove_stream_handler(SwarmTID, ProtocolVersion) end,
    lists:foreach(FFRemoveFun, ?SUPPORTED_FASTFORWARD_PROTOCOLS),
    libp2p_swarm:remove_stream_handler(SwarmTID, ?SNAPSHOT_PROTOCOL),
    libp2p_swarm:remove_stream_handler(SwarmTID, ?STATE_CHANNEL_PROTOCOL_V1).

-spec start_block_sync(
        SwarmTID :: ets:tab(),
        Chain :: blockchain:blockchain(),
        Peer :: libp2p_crypto:pubkey_bin(),
        Heights :: [pos_integer()],
        GossipedHash :: binary()
) -> {pid(), reference()} | ok.
start_block_sync(SwarmTID, Chain, Peer, Heights, GossipedHash) ->
    DialFun =
        fun() ->
                case blockchain_sync_handler:dial(SwarmTID, Chain, Peer, Heights, GossipedHash) of
                    {ok, Stream} ->
                        {ok, HeadHash} = blockchain:sync_hash(Chain),
                        Stream ! {hash, HeadHash},
                        Ref1 = erlang:monitor(process, Stream),
                        %% we have issues with rate control in some situations, so sleep for a bit
                        %% before checking
                        CheckDelay = application:get_env(blockchain, sync_check_delay_ms, 10000),
                        timer:sleep(CheckDelay),
                        receive
                            cancel ->
                                libp2p_framed_stream:close(Stream);
                            {'DOWN', Ref1, process, Stream, normal} ->
                                %% we're done, nothing to do here.
                                ok;
                            {'DOWN', Ref1, process, Stream, Reason} ->
                                lager:info("block sync failed with error ~p", [Reason]),
                                exit({down, Reason})
                        after timer:minutes(application:get_env(blockchain, sync_timeout_mins, 5)) ->
                                libp2p_framed_stream:close(Stream),
                                lager:info("block sync timed out"),
                                exit(timeout)
                        end;
                    {error, _Reason} ->
                        lager:debug("dialing sync stream failed: ~p",[_Reason]),
                        exit(dial)
                end
        end,
    spawn_monitor(fun() -> DialFun() end).

grab_snapshot(Height, Hash) ->
    Chain = blockchain_worker:blockchain(),
    SwarmTID = blockchain_swarm:tid(),

    case get_random_peer(SwarmTID) of
        no_peers -> {error, no_peers};
        Peer ->
            case libp2p_swarm:dial_framed_stream(SwarmTID,
                                                 Peer,
                                                 ?SNAPSHOT_PROTOCOL,
                                                 blockchain_snapshot_handler,
                                                 [Hash, Height, Chain, self()])
            of
                {ok, Stream} ->
                    Ref1 = erlang:monitor(process, Stream),
                    receive
                        {ok, Snapshot} ->
                            {ok, Snapshot};
                        {error, not_found} ->
                            {error, not_found};
                        cancel ->
                            lager:info("snapshot sync cancelled"),
                            _ = libp2p_framed_stream:close(Stream),
                            {error, snap_sync_cancel};
                        {'DOWN', Ref1, process, Stream, normal} ->
                            {error, down};
                        {'DOWN', Ref1, process, Stream, Reason} ->
                            lager:info("snapshot sync failed with error ~p", [Reason]),
                            {error, down, Reason}
                    after timer:minutes(1) ->
                            {error, timeout}
                    end;
                _ ->
                    ok
            end
    end.

start_snapshot_sync(#state{blockchain=Chain, sync_paused=SyncPaused, snapshot_info=SnapInfo}) ->

    BaseUrl = application:get_env(blockchain, snap_source_base_url, undefined),
    try
        NewSnapInfo = fetch_and_parse_latest_snapshot(SnapInfo),

        {Pid, Ref} = spawn_monitor(fun() ->
                              try
                                  HonorQS = application:get_env(blockchain, honor_quick_sync, true),
                                  case {HonorQS, SyncPaused} of
                                      {true, false} ->
                                          maybe_sleep(SnapInfo), %% if we're retrying we want to space out download attempts here
                                          {ok, Filename} = attempt_fetch_snap_source_snapshot(BaseUrl, NewSnapInfo),
                                          lager:info("Loading snap from ~p", [Filename]),
                                          %% if the file doesn't deserialize correctly, it will
                                          %% get deleted, so we can redownload it on some other
                                          %% attempt
                                          ok = attempt_load_snapshot_from_disk(Filename,
                                                                               NewSnapInfo#snapshot_info.hash,
                                                                               Chain);
                                      _ ->
                                          %% don't do anything
                                          ok
                                  end
                              catch
                                  _Type:Error:St ->
                                      lager:error("snapshot download or loading failed because ~p: ~p", [Error, St]),
                                      exit(Error)
                              end
                      end),
        {Pid, Ref, NewSnapInfo}
    catch
        _T:Error:St ->
            lager:error("couldn't sync snapshot because ~p: ~p", [Error, St]),
            {Pid0, Ref0} = spawn_monitor(fun() -> exit(Error) end),
            {Pid0, Ref0, SnapInfo}
    end.

maybe_sleep(#snapshot_info{ download_attempts = 0 }) -> ok;
maybe_sleep(#snapshot_info{ download_attempts = N }) ->
    Secs = rand:uniform(10000) + (N*1000),
    timer:sleep(Secs),
    ok.

fetch_and_parse_latest_snapshot(SnapInfo0) ->
    URL = application:get_env(blockchain, snap_source_base_url, undefined),

    SnapInfo = case is_record(SnapInfo0, snapshot_info) of
                   true -> SnapInfo0;
                   false ->
                       case get_blessed_snapshot_height_and_hash() of
                           {undefined, _} -> #snapshot_info{};
                           {_, undefined} -> #snapshot_info{};
                           {BlessedHash, BlessedHeight} ->
                               #snapshot_info{hash=BlessedHash, height=BlessedHeight}
                       end
               end,

    case application:get_env(blockchain, fetch_latest_from_snap_source, true) of
        true ->
            try
                get_latest_snap_data(URL, SnapInfo)
            catch
                _Type:Error:St ->
                    lager:error("Couldn't fetch latest-snap.json because ~p: ~p", [Error, St]),
                    SnapInfo
            end;
        false ->
            SnapInfo
    end.

-spec get_latest_snap_data( URL :: string(), snapshot_info() ) -> no_return().
get_latest_snap_data(URL, SnapInfo) ->
    ReqHeaders0 = [{"user-agent", "snapjson-3"}],
    Etag = case SnapInfo of
               #snapshot_info{etag=undefined} -> undefined;
               #snapshot_info{etag=Etag0} -> Etag0;
               _ -> undefined
           end,
    ReqHeaders = case Etag of
                  undefined -> ReqHeaders0;
                  _ -> [ {<<"if-none-match">>, list_to_binary("\"" ++ Etag ++ "\"")} | ReqHeaders0 ]
               end,
    %% shorter timeouts here because we're hitting S3 usually...
    Options = [{recv_timeout, 30000}, % milliseconds, 30 sec overall request timeout
               {connect_timeout, 10000}, % milliseconds, 10 second connection timeout
               with_body],
    case hackney:request(get, URL ++ "/latest-snap.json", ReqHeaders, <<>>, Options) of
        {ok, 200, RespHeaders, Body} ->
            Data = #{<<"height">> := Height,
              <<"hash">> := B64Hash} = jsx:decode(Body, [{return_maps, true}]),
            Hash = base64url:decode(B64Hash),
            MaybeFileHash = case maps:get(<<"file_hash">>, Data, undefined) of
                                undefined -> undefined;
                                B64FileHash -> base64url:decode(B64FileHash)
                            end,
            MaybeFileSize = case maps:get(<<"file_size">>, Data, undefined) of
                                undefined -> undefined;
                                Sz -> Sz
                            end,
            MaybeCompressedSize = case maps:get(<<"compressed_size">>, Data, undefined) of
                                      undefined -> undefined;
                                      CSz -> CSz
                                  end,
            MaybeCompressedHash = case maps:get(<<"compressed_hash">>, Data, undefined) of
                                      undefined -> undefined;
                                      B64CHash -> base64url:decode(B64CHash)
                                  end,
            NewEtag = get_etag(RespHeaders),
            lager:debug("new latest-json data: previous etag: ~p; height: ~p, internal snapshot hash: ~p, file hash: ~p, file size: ~p, compressed size: ~p, compressed hash: ~p, new etag: ~p",
                        [Etag, Height, B64Hash, MaybeFileHash, MaybeFileSize,
                         MaybeCompressedSize, MaybeCompressedHash, NewEtag]),
            {IsCompressed, FHash, FSz} = case MaybeCompressedHash of
                               undefined -> {false, MaybeFileHash, MaybeFileSize};
                               _ -> {true, MaybeCompressedHash, MaybeCompressedSize}
                           end,
            SnapInfo#snapshot_info{height=Height, hash=Hash,
                                   file_hash=FHash, file_size=FSz,
                                   is_compressed=IsCompressed, etag=NewEtag};
        {ok, 304, _RespHeaders, _Body} ->
            lager:debug("Got 304 from ~p; latest-snap.json has not been modified", [URL]),
            SnapInfo;
        {ok, 403, _RespHeaders, _Body} -> throw({error, url_forbidden});
        {ok, 404, _RespHeaders, _Body} -> throw({error, url_not_found});
        {ok, Status, _RespHeaders, Body} -> throw({error, {Status, Body}});
        Other -> throw(Other)
    end.

get_etag(Headers) ->
    case lists:keyfind("etag", 1, Headers) of
        {"etag", Etag} -> string:trim(Etag, both, "\"");
        _ -> undefined
    end.

build_filename(Height) ->
    HeightStr = integer_to_list(Height),
    "snap-" ++ HeightStr.

build_url(BaseUrl, Filename) ->
    BaseUrl ++ "/" ++ Filename.

set_filename(BaseFilename, true) ->
    BaseFilename ++ ".gz";
set_filename(BaseFilename, false) ->
    BaseFilename.

attempt_fetch_snap_source_snapshot(BaseUrl, #snapshot_info{height = Height, file_hash = Hash,
                                                           file_size = Size, is_compressed = IsCompressed}) ->
    %% httpc and ssl applications are started in the top level blockchain supervisor
    Filename = set_filename(build_filename(Height), IsCompressed),
    HashFilename = Filename ++ ".hash",
    BaseDir = application:get_env(blockchain, base_dir, "data"),
    Filepath = filename:join([BaseDir, "snap", Filename]),
    ok = filelib:ensure_dir(Filepath),

    HashStateFile = filename:join([BaseDir, "snap", HashFilename]),

    %% clean_dir will remove files older than 1 week (help prevent
    %% filling up the SSD card with old files/snapshots)
    ok = clean_dir(filename:dirname(Filepath)),

    %% if the snapshot file exists
    %%  - if .hash file doesn't exist,
    %%    + hash snapshot and compare it
    %%    + write hash file
    %%  - if .hash file exists
    %%    + read it
    %%    + compare file size on disk
    %%    + compare stored hash to given hash
    case filelib:is_regular(Filepath) of
        true ->
            case same_stored_hash(HashStateFile, Hash) of
                true ->
                    lager:info("Already have snapshot file for height ~p with hash ~p", [Height, Hash]),
                    {ok, Filepath};
                false ->
                    case {has_same_file_size(Filepath, Size),
                          is_file_hash_valid(Filepath, Hash)} of
                        {true, true} ->
                            lager:info("Already have snapshot file for height ~p with hash ~p", [Height, Hash]),
                            ok = file:write_file(HashStateFile, Hash),
                            {ok, Filepath};
                        {smaller, _} ->
                            %% see if this is a a resumable download
                            _ = do_snap_source_download(build_url(BaseUrl, Filename), Filepath);
                        _ ->
                            %% file is bigger than it should be, or the hash is wrong, scrap it
                            safe_delete(Filename),
                            _ = do_snap_source_download(build_url(BaseUrl, Filename), Filepath)
                    end
            end;
        false ->
            _ = do_snap_source_download(build_url(BaseUrl, Filename), Filepath)
    end.

same_stored_hash(File, Hash) ->
    case file:read_file(File) of
        {ok, Hash} -> true;
        _ -> false
    end.

has_same_file_size(_Filepath, undefined) -> false;
has_same_file_size(Filepath, Size) ->
    case file:read_file_info(Filepath, [raw, {time, posix}]) of
        {ok, #file_info{ size = Size }} -> true;
        {ok, #file_info{ size = FSize }} when FSize < Size  -> smaller;
        {ok, #file_info{ size = FSize }} when FSize > Size  -> larger
    end.

is_file_hash_valid(_Filepath, undefined) -> false;
is_file_hash_valid(Filepath, FileHash) ->
    case blockchain_utils:streaming_file_hash(Filepath) of
        {ok, FileHash} -> true;
        {ok, OtherHash} ->
            lager:warning("Computed hash ~p does not match expected hash of ~p", [OtherHash, FileHash]),
            false;
        {error, Error} ->
            lager:error("While computing streaming hash of ~p, got error ~p", [Filepath, Error]),
            false
    end.

safe_delete(File) ->
    case file:delete(File) of
        ok -> ok;
        {error, enoent} -> ok;
        Other -> Other
    end.

-spec do_snap_source_download(URL :: string(), Filepath :: file:filename_all()) -> no_return().
do_snap_source_download(Url, Filepath) ->
    ScratchFile = Filepath ++ ".scratch",
    ok = filelib:ensure_dir(ScratchFile),

    Headers = [
               {"user-agent", "snapdownload-3"}
              ],
    Options = [
                   {recv_timeout, 900000}, % milliseconds, 900 sec overall request timeout
                   {connect_timeout, 60000}, % milliseconds, 60 second connection timeout,
                   async
                  ],
    lager:info("Attempting snapshot download from ~p, writing to scratch file ~p",
               [Url, ScratchFile]),

    %% check if we have a partial download
    Start = case file:read_file_info(ScratchFile) of
                {ok, #file_info{size=Size}} ->
                    lager:info("resuming snapshot download at byte ~p", [Size]),
                    Size;
                _ ->
                    0
            end,
    {ok, FD} = file:open(ScratchFile, [raw, write, append]),
    ReceiveSnapshotLoop = fun Loop(Ref) ->
                                  receive
                                      {hackney_response, Ref, {status, 200, _}} ->
                                          Loop(Ref);
                                      {hackney_response, Ref, {status, 206, _}} ->
                                          Loop(Ref);
                                      {hackney_response, Ref, {status, 403, _}} ->
                                          throw({error, url_forbidden});
                                      {hackney_response, Ref, {status, 403, _}} ->
                                          throw({error, url_not_found});
                                      {hackney_response, Ref, {status, Status, Response}} ->
                                          throw({error, {Status, Response}});
                                      {hackney_response, Ref, {headers, _Headers}} ->
                                          Loop(Ref);
                                      {hackney_response, Ref, done} ->
                                          file:close(FD),
                                          lager:info("snap written to scratch file ~p", [ScratchFile]),
                                          %% prof assures me rename is atomic :)
                                          ok = file:rename(ScratchFile, Filepath),
                                          {ok, Filepath};
                                      {hackney_response, Ref, Bin} ->
                                          file:write(FD, Bin),
                                          Loop(Ref)
                                  after 900000 ->
                                            %% hackney doesn't seem to do anything on a timeout in async mode
                                            throw(timeout)
                                  end
                          end,
    case hackney:request(get, Url, Headers ++ [{<<"range">>, list_to_binary("bytes=" ++ integer_to_list(Start) ++ "-")}], <<>>, Options) of
        {ok, ClientRef} ->
            ReceiveSnapshotLoop(ClientRef);
        Other -> throw(Other)
    end.

attempt_load_snapshot_from_disk(Filename, Hash, Chain) ->
    lager:debug("attempting to deserialize snapshot in ~p and validate hash ~p", [Filename, Hash]),
    try blockchain_ledger_snapshot_v1:deserialize(Hash, {file, Filename}) of
        {error, _} = Err ->
            lager:warning("While deserializing ~p, got ~p. Deleting ~p",
                        [Filename, Err, Filename]),
            ok = file:delete(Filename),
            Err;
        {ok, Snap} ->
            SnapHeight = blockchain_ledger_snapshot_v1:height(Snap),
            lager:debug("attempting to store snapshot in rocks"),
            ok = blockchain:add_bin_snapshot({file, Filename}, SnapHeight, Hash, Chain),
            lager:info("Stored snap ~p - attempting install", [SnapHeight]),
            blockchain_worker:install_snapshot_from_file(Filename)
    catch
        What:Why:Stack ->
            lager:error("While deserializing ~p, got ~p:~p:~p. Deleting ~p",
                        [Filename, What, Why, Stack, Filename]),
            ok = file:delete(Filename),
            {error, {What, Why, Stack}}
    end.

send_txn(Txn) ->
    ok = blockchain_txn_mgr:submit(Txn,
                                   (fun(Res) ->
                                            case Res of
                                                ok ->
                                                    lager:info("successfully submit txn: ~s", [blockchain_txn:print(Txn)]);
                                                {error, rejected = Reason} ->
                                                    lager:error("failed to submit txn: ~s error: ~p", [blockchain_txn:print(Txn), Reason]);
                                                {error, {invalid, _InvalidReason} = Reason} ->
                                                    lager:error("failed to submit txn: ~s error: ~p", [blockchain_txn:print(Txn), Reason])
                                            end
                                    end)).

send_txn(Txn, Callback) ->
    ok = blockchain_txn_mgr:submit(Txn, Callback).

get_assumed_valid_height_and_hash() ->
    {application:get_env(blockchain, assumed_valid_block_hash, undefined),
     application:get_env(blockchain, assumed_valid_block_height, undefined)}.

get_blessed_snapshot_height_and_hash() ->
    case blockchain_utils:get_boolean_os_env_var("LOAD_SNAPSHOT", true) of
        true ->
            {application:get_env(blockchain, blessed_snapshot_block_hash, undefined),
             application:get_env(blockchain, blessed_snapshot_block_height, undefined)};
        false ->
            lager:debug("LOAD_SNAPSHOT is false; returning undefined height and hash"),
            {undefined, undefined}
    end.

-spec get_quick_sync_height_and_hash(assumed_valid | blessed_snapshot) -> undefined | {blockchain_block:hash(),pos_integer()}.
get_quick_sync_height_and_hash(Mode) ->

    HashHeight =
        case Mode of
            assumed_valid ->
                get_assumed_valid_height_and_hash();
            blessed_snapshot ->
                #snapshot_info{hash=BlessedHash, height=BlessedHeight} = fetch_and_parse_latest_snapshot(undefined),
                {BlessedHash, BlessedHeight}
        end,

    case HashHeight of
        {undefined, _} ->
            undefined;
        {_, undefined} ->
            undefined;
        BlockHashAndHeight ->
            case application:get_env(blockchain, honor_quick_sync, false) of
                true ->
                    lager:info("quick syncing with ~p: ~p", [Mode, BlockHashAndHeight]),
                    BlockHashAndHeight;
                _ ->
                    undefined
            end
    end.

load_chain(SwarmTID, BaseDir, GenDir) ->
    {QuickSyncMode, QuickSyncData} = case application:get_env(blockchain, honor_quick_sync, false) of
        true ->
            Mode = application:get_env(blockchain, quick_sync_mode, assumed_valid),
            {Mode,
            get_quick_sync_height_and_hash(Mode)};
        false ->
            {undefined, undefined}
    end,
    case blockchain:new(BaseDir, GenDir, QuickSyncMode, QuickSyncData) of
        {no_genesis, _Chain}=R ->
            %% mark all upgrades done
            {R, make_ref()};
        {ok, Chain} ->
            %% blockchain:new will take care of any repairs needed, possibly asynchronously
            %%
            %% do ledger upgrade
            {ok, GossipRef} = add_handlers(SwarmTID, Chain),
            self() ! maybe_sync,
            {ok, GenesisHash} = blockchain:genesis_hash(Chain),
            ok = blockchain_txn_mgr:set_chain(Chain),
            true = libp2p_swarm:network_id(SwarmTID, GenesisHash),
            {Chain, GossipRef}
    end.

schedule_sync(State) ->
    Ref = case State#state.sync_paused of
              true ->
                  make_ref();
              false ->
                  case erlang:read_timer(State#state.sync_timer) of
                      false ->
                          %% timer is expired or invalid
                          erlang:send_after(?SYNC_TIME, self(), maybe_sync);
                      _Time ->
                          %% existing timer still has some time left
                          State#state.sync_timer
                  end
          end,
    State#state{sync_timer=Ref}.

schedule_snapshot_timer() ->
    Interval = application:get_env(blockchain, snapshot_polling_interval_hours, 4),
    Millis = Interval * 3600 * 1000,
    erlang:send_after(Millis, self(), snapshot_timer_tick).

get_sync_mode(State) ->
    case application:get_env(blockchain, honor_quick_sync, false)
            andalso blockchain_utils:get_boolean_os_env_var("LOAD_SNAPSHOT", true) of
        true ->
            case application:get_env(blockchain, quick_sync_mode, assumed_valid) of
                assumed_valid -> {normal, undefined};
                blessed_snapshot ->
                    %% reconsider whether the latest-snap.json data is newer
                    %% than whatever our current ledger height is
                    #snapshot_info{height=Height} = SnapInfo = fetch_and_parse_latest_snapshot(State#state.snapshot_info),
                    Autoload = application:get_env(blockchain, autoload, true),
                    case State#state.blockchain of
                        undefined when Autoload == false ->
                            {normal, undefined};
                        undefined ->
                            {snapshot, SnapInfo};
                        {no_genesis, _} ->
                            {snapshot, SnapInfo};
                        _Chain ->
                            {ok, CurrHeight} = blockchain:height(State#state.blockchain),
                            case CurrHeight >= Height - 1 of
                                %% already loaded the snapshot
                                true -> {normal, undefined};
                                false -> {snapshot, SnapInfo}
                            end
                    end
            end;
        false ->
            %% full sync only ever syncs blocks, so just sync blocks
            {normal, undefined}
    end.

get_files_to_delete(Dir) ->
    case file:list_dir(Dir) of
        {error, enoent} -> [];
        {error, _} = Err -> throw(Err);
        {ok, Fs} -> [ filename:join(Dir, F) || F <- Fs ]
    end.

clean_dir(Dir) ->
    WeekOld = erlang:system_time(seconds) - ?WEEK_OLD_SECONDS,
    FilterFun = fun(F) ->
                        case file:read_file_info(F, [raw, {time, posix}]) of
                            {error, _} -> false;
                            {ok, FI} ->
                                FI#file_info.type == regular
                                andalso FI#file_info.mtime =< WeekOld
                        end
                end,

    do_clean_dir(get_files_to_delete(Dir), FilterFun).

do_clean_dir(Files, FilterFun) ->
    lists:foreach(fun(F) -> file:delete(F) end,
                  lists:filter(FilterFun, Files)).

