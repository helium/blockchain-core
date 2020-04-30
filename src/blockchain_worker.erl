
%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Core Worker ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_worker).

-behavior(gen_server).

-include("blockchain_vars.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    blockchain/0, blockchain/1,
    num_consensus_members/0,
    consensus_addrs/0,
    integrate_genesis_block/1,
    submit_txn/1, submit_txn/2,
    peer_height/3,
    notify/1,
    mismatch/0,
    signed_metadata_fun/0,

    new_ledger/1,

    maybe_sync/0,
    sync/0,
    cancel_sync/0,
    pause_sync/0,
    sync_paused/0,

    set_absorbing/3,
    absorb_done/0,
    is_absorbing/0
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
-define(SYNC_TIME, 75000).

-record(state,
        {
         blockchain :: {no_genesis, blockchain:blockchain()} | blockchain:blockchain(),
         swarm :: undefined | pid(),
         swarm_tid :: undefined | ets:tab(),
         sync_timer = make_ref() :: reference(),
         sync_ref = make_ref() :: reference(),
         sync_pid :: undefined | pid(),
         sync_paused = false :: boolean(),
         gossip_ref = make_ref() :: reference(),
         absorb_info :: undefined | {pid(), reference()},
         absorb_retry = 3 :: pos_integer()
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

sync_paused() ->
    try
        gen_server:call(?SERVER, sync_paused, 100)  % intentionally very low
    catch _:_ ->
            true  % it's fine to occasionally get this wrong
    end.

new_ledger(Dir) ->
    gen_server:call(?SERVER, {new_ledger, Dir}, infinity).

-spec set_absorbing(blockchain_block:block(), blockchain:blockchain(), boolean()) -> ok.
set_absorbing(Block, Blockchain, Syncing) ->
    gen_server:cast(?SERVER, {set_absorbing, Block, Blockchain, Syncing}).

absorb_done() ->
    gen_server:call(?SERVER, absorb_done, infinity).

is_absorbing() ->
    gen_server:call(?SERVER, is_absorbing, infinity).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec integrate_genesis_block(blockchain_block:block()) -> ok.
integrate_genesis_block(Block) ->
    gen_server:cast(?SERVER, {integrate_genesis_block, Block}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec submit_txn(blockchain_txn:txn()) -> ok.
submit_txn(Txn) ->
    gen_server:cast(?SERVER, {submit_txn, Txn}).

%%--------------------------------------------------------------------
%% @doc
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
                FPMD = case blockchain:sync_height(Chain) == blockchain_ledger_v1:current_height(Ledger) of
                           true ->
                               Ht0 = maps:get(<<"height">>, HeightMD, 1),
                               Ht = max(1, Ht0 - (Ht0 rem 40)),
                               {ok, LedgerAt} = blockchain:ledger_at(Ht, Chain),
                               case blockchain_ledger_v1:fingerprint(LedgerAt) of
                                   {ok, Fingerprint} ->
                                       maps:merge(HeightMD, Fingerprint);
                                   _ ->
                                       HeightMD
                               end;
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
    erlang:process_flag(trap_exit, true),
    ok = blockchain_event:add_handler(self()),
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = blockchain_swarm:swarm(),
    SwarmTID = libp2p_swarm:tid(Swarm),
    Ports = case application:get_env(blockchain, ports, undefined) of
                undefined ->
                    %% fallback to the single 'port' app env var
                    [proplists:get_value(port, Args, 0)];
                PortList when is_list(PortList) ->
                    PortList
            end,
    BaseDir = proplists:get_value(base_dir, Args, "data"),
    GenDir = proplists:get_value(update_dir, Args, undefined),
    AssumedValidBlockHashAndHeight = case {application:get_env(blockchain, assumed_valid_block_hash, undefined),
                                  application:get_env(blockchain, assumed_valid_block_height, undefined)} of
                                {undefined, _} ->
                                    undefined;
                                {_, undefined} ->
                                    undefined;
                                BlockHashAndHeight ->
                                    case application:get_env(blockchain, honor_assumed_valid, false) of
                                        true ->
                                            BlockHashAndHeight;
                                        _ ->
                                            undefined
                                    end
                            end,
    {Blockchain, Ref} =
        case blockchain:new(BaseDir, GenDir, AssumedValidBlockHashAndHeight) of
            {no_genesis, _Chain}=R ->
                %% mark all upgrades done
                {R, make_ref()};
            {ok, Chain} ->
                %% blockchain:new will take care of any repairs needed, possibly asynchronously
                {ok, GossipRef} = add_handlers(Swarm, Chain),
                self() ! maybe_sync,
                {ok, GenesisHash} = blockchain:genesis_hash(Chain),
                ok = blockchain_txn_mgr:set_chain(Chain),
                true = libp2p_swarm:network_id(Swarm, GenesisHash),
                {Chain, GossipRef}
        end,
    true = lists:all(fun(E) -> E == ok end,
                     [ libp2p_swarm:listen(Swarm, "/ip4/0.0.0.0/tcp/" ++ integer_to_list(Port)) || Port <- Ports ]),
    {ok, #state{swarm = Swarm, swarm_tid = SwarmTID, blockchain = Blockchain, gossip_ref = Ref}}.

handle_call(_, _From, #state{blockchain={no_genesis, _}}=State) ->
    {reply, undefined, State};
handle_call(num_consensus_members, _From, #state{blockchain = Chain} = State) ->
    {ok, N} = blockchain:config(?num_consensus_members, blockchain:ledger(Chain)),
    {reply, N, State};
handle_call(consensus_addrs, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)), State};
handle_call(blockchain, _From, #state{blockchain=Chain}=State) ->
    {reply, Chain, State};
handle_call({blockchain, NewChain}, _From, #state{swarm = Swarm} = State) ->
    notify({new_chain, NewChain}),
    remove_handlers(Swarm),
    {ok, GossipRef} = add_handlers(Swarm, NewChain),
    {reply, ok, State#state{blockchain = NewChain, gossip_ref = GossipRef}};
handle_call({new_ledger, Dir}, _From, State) ->
    %% We do this here so the same process that normally owns the ledger
    %% will own it when we do a reset ledger or whatever. Otherwise the
    %% snapshot cache ETS table can be owned by an ephemeral process.
    Ledger1 = blockchain_ledger_v1:new(Dir),
    {reply, {ok, Ledger1}, State};

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
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({set_absorbing, Block, Blockchain, Syncing}, State=#state{absorb_info=undefined}) ->
    Info = spawn_monitor(
             fun() ->
                     blockchain:absorb_temp_blocks_fun(Block, Blockchain, Syncing)
             end),
    %% just don't sync, it's a waste of bandwidth
    {noreply, State#state{absorb_info = Info, sync_paused = true}};
handle_cast({set_absorbing, _Block, _Blockchain, _Syncing}, State) ->
    {noreply, State};
handle_cast(maybe_sync, State) ->
    {noreply, maybe_sync(State)};
handle_cast({integrate_genesis_block, GenesisBlock}, #state{blockchain={no_genesis, Blockchain},
                                                            swarm=Swarm}=State) ->
    case blockchain_block:is_genesis(GenesisBlock) of
        false ->
            lager:warning("~p is not a genesis block", [GenesisBlock]),
            {noreply, State};
        true ->
            ok = blockchain:integrate_genesis(GenesisBlock, Blockchain),
            [ConsensusAddrs] = [blockchain_txn_consensus_group_v1:members(T)
                                || T <- blockchain_block:transactions(GenesisBlock),
                                   blockchain_txn:type(T) == blockchain_txn_consensus_group_v1],
            lager:info("blockchain started with ~p, consensus ~p", [lager:pr(Blockchain, blockchain), ConsensusAddrs]),
            {ok, GenesisHash} = blockchain:genesis_hash(Blockchain),
            ok = notify({integrate_genesis_block, GenesisHash}),
            {ok, GossipRef} = add_handlers(Swarm, Blockchain),
            ok = blockchain_txn_mgr:set_chain(Blockchain),
            true = libp2p_swarm:network_id(Swarm, GenesisHash),
            self() ! maybe_sync,
            {noreply, State#state{blockchain=Blockchain, gossip_ref = GossipRef}}
    end;
handle_cast(_, #state{blockchain={no_genesis, _}}=State) ->
    {noreply, State};
handle_cast({submit_txn, Txn}, State) ->
    ok = send_txn(Txn),
    {noreply, State};
handle_cast({submit_txn, Txn, Callback}, State) ->
    ok = send_txn(Txn, Callback),
    {noreply, State};
handle_cast({peer_height, Height, Head, Sender}, #state{blockchain=Chain, swarm=Swarm}=State) ->
    lager:info("got peer height message with blockchain ~p", [lager:pr(Chain, blockchain)]),
    case {blockchain:head_hash(Chain), blockchain:head_block(Chain)} of
        {{error, _Reason}, _} ->
            lager:error("could not get head hash ~p", [_Reason]);
        {_, {error, _Reason}} ->
            lager:error("could not get head block ~p", [_Reason]);
        {{ok, LocalHead}, {ok, LocalHeadBlock}} ->
            LocalHeight = blockchain_block:height(LocalHeadBlock),
            case LocalHeight < Height orelse (LocalHeight == Height andalso Head /= LocalHead) of
                false ->
                    ok;
                true ->
                    case libp2p_swarm:dial_framed_stream(Swarm,
                                                         libp2p_crypto:pubkey_bin_to_p2p(Sender),
                                                         ?SYNC_PROTOCOL,
                                                         blockchain_sync_handler,
                                                         [Chain]) of
                        {ok, Stream} ->
                            Stream ! {hash, LocalHead};
                        _ ->
                            lager:warning("Failed to dial sync service on: ~p", [Sender])
                    end
            end
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(maybe_sync, State) ->
    {noreply, maybe_sync(State)};
handle_info({blockchain_event, {add_block, _Hash, _Sync, _Ledger}}, State) ->
    %% nothing to do here, block re-gossip is handled by the gossip handler
    {noreply, State};
handle_info({'DOWN', SyncRef, process, _SyncPid, _Reason},
            #state{sync_ref = SyncRef, blockchain = Chain} = State0) ->
    State = State0#state{sync_pid = undefined},
    %% we're done with our sync.  determine if we're very far behind,
    %% and should resync immediately, or if we're relatively close to
    %% the present and can afford to retry later.
    {ok, Block} = blockchain:head_block(Chain),
    Now = erlang:system_time(seconds),
    Time = blockchain_block:time(Block),
    case Now - Time of
        N when N < 0 ->
            %% if blocktimes are in the future, we're confused about
            %% the time, proceed as if we're synced.
            Ref = case State#state.sync_paused of
                      true ->
                          make_ref();
                      false ->
                          erlang:send_after(?SYNC_TIME, self(), maybe_sync)
                  end,
            {noreply, State#state{sync_timer = Ref}};
        N when N < 60 * 60 ->
            %% relatively recent
            Ref = case State#state.sync_paused of
                      true ->
                          make_ref();
                      false ->
                          erlang:send_after(?SYNC_TIME, self(), maybe_sync)
                  end,
            {noreply, State#state{sync_timer = Ref}};
        _ ->
            %% we're deep in the past here, so just start the next sync
            {noreply, start_sync(State)}
    end;
handle_info({'DOWN', GossipRef, process, _GossipPid, _Reason},
            #state{gossip_ref = GossipRef, blockchain = Blockchain,
                   swarm = Swarm} = State) ->
    Gossip = libp2p_swarm:gossip_group(Swarm),
    libp2p_group_gossip:add_handler(Gossip, ?GOSSIP_PROTOCOL,
                                    {blockchain_gossip_handler, [Swarm, Blockchain]}),
    NewGossipRef = erlang:monitor(process, Gossip),
    {noreply, State#state{gossip_ref = NewGossipRef}};
handle_info({'DOWN', AbsorbRef, process, AbsorbPid, Reason},
            #state{absorb_info = {AbsorbPid, AbsorbRef}} = State0) ->
    case Reason of
        normal ->
            lager:info("Absorb process completed successfully"),
            {noreply, State0#state{sync_paused=false, absorb_info=undefined}};
        shutdown ->
            {noreply, State0#state{absorb_info=undefined}};
        Reason ->
            lager:warning("Absorb process exited with reason ~p", [Reason]),
            AssumedValidBlockHashAndHeight = case {application:get_env(blockchain, assumed_valid_block_hash, undefined),
                                                   application:get_env(blockchain, assumed_valid_block_height, undefined)} of
                                                 {undefined, _} ->
                                                     undefined;
                                                 {_, undefined} ->
                                                     undefined;
                                                 BlockHashAndHeight ->
                                                     case application:get_env(blockchain, honor_assumed_valid, false) of
                                                         true ->
                                                             BlockHashAndHeight;
                                                         _ ->
                                                             undefined
                                                     end
                                             end,
            blockchain:init_assumed_valid(State0#state.blockchain, AssumedValidBlockHashAndHeight),
            {noreply, State0#state{absorb_info=undefined}}
    end;
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    {noreply, State#state{blockchain = NC}};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{blockchain={no_genesis, Chain}}) ->
    ok = blockchain:close(Chain),
    ok;
terminate(_Reason, #state{blockchain=Chain}) ->
    ok = blockchain:close(Chain),
    ok.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

maybe_sync(#state{sync_paused = true} = State) ->
    State;
maybe_sync(#state{sync_pid = Pid} = State) when Pid /= undefined ->
    State;
maybe_sync(#state{blockchain = Chain} = State) ->
    erlang:cancel_timer(State#state.sync_timer),
    %% last block add time is relative to the system clock so as long as the local
    %% clock mostly increments this will eventually be true on a stuck node
    SyncCooldownTime = application:get_env(blockchain, sync_cooldown_time, 60),
    SkewedSyncCooldownTime = application:get_env(blockchain, skewed_sync_cooldown_time, 300),
    {ok, HeadBlock} = blockchain:head_block(Chain),
    Height = blockchain_block:height(HeadBlock),
    case erlang:system_time(seconds) - blockchain_block:time(HeadBlock) of
        %% negative time means we're skewed, so rely on last add time
        %% until ntp fixes us.
        T when T < 0 ->
            case erlang:system_time(seconds) - blockchain:last_block_add_time(Chain) of
                X when X > SkewedSyncCooldownTime orelse Height == 1  ->
                    start_sync(State);
                _ ->
                    %% no need to sync now, check again later
                    Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
                    State#state{sync_timer=Ref}
            end;
        T when T > SyncCooldownTime orelse Height == 1 ->
            start_sync(State);
        _ ->
            %% no need to sync now, check again later
            Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
            State#state{sync_timer=Ref}
    end.

start_sync(#state{blockchain = Chain, swarm = Swarm, swarm_tid = SwarmTID} = State) ->
    %% figure out who we're connected to
    {Peers0, _} = lists:unzip(libp2p_config:lookup_sessions(SwarmTID)),
    %% Get the p2p addresses of our peers, so we will connect on existing sessions
    Peers = lists:filter(fun(E) ->
                                 case libp2p_transport_p2p:p2p_addr(E) of
                                     {ok, _} -> true;
                                     _       -> false
                                 end
                         end, Peers0),
    case Peers of
        [] ->
            %% try again later when there's peers
            Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
            State#state{sync_timer=Ref};
        Peers ->
            RandomPeer = lists:nth(rand:uniform(length(Peers)), Peers),
            {Pid, Ref} = sync(Swarm, Chain, RandomPeer),
            lager:info("unknown starting ~p ~p", [Pid, Ref]),
            State#state{sync_pid = Pid, sync_ref = Ref}
    end.

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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_handlers(pid(), blockchain:blockchain()) -> {ok, reference()}.
add_handlers(Swarm, Blockchain) ->
    Gossip = libp2p_swarm:gossip_group(Swarm),
    libp2p_group_gossip:add_handler(Gossip, ?GOSSIP_PROTOCOL,
                                    {blockchain_gossip_handler, [Swarm, Blockchain]}),
    Ref = erlang:monitor(process, Gossip),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?SYNC_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_sync_handler, ?SERVER, Blockchain]}
    ),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?FASTFORWARD_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_fastforward_handler, ?SERVER, Blockchain]}
    ),
    {ok, Ref}.

-spec remove_handlers(pid()) -> ok.
remove_handlers(Swarm) ->
    libp2p_group_gossip:remove_handler(libp2p_swarm:gossip_group(Swarm), ?GOSSIP_PROTOCOL),
    libp2p_swarm:remove_stream_handler(Swarm, ?SYNC_PROTOCOL),
    libp2p_swarm:remove_stream_handler(Swarm, ?FASTFORWARD_PROTOCOL).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
sync(Swarm, Chain, Peer) ->
    spawn_monitor(fun() ->
        case libp2p_swarm:dial_framed_stream(Swarm,
                                             Peer,
                                             ?SYNC_PROTOCOL,
                                             blockchain_sync_handler,
                                             [Chain])
        of
            {ok, Stream} ->
                {ok, HeadHash} = blockchain:sync_hash(Chain),
                Stream ! {hash, HeadHash},

                Ref1 = erlang:monitor(process, Stream),
                receive
                    cancel ->
                        libp2p_framed_stream:close(Stream);
                    {'DOWN', Ref1, process, Stream, _} ->
                        %% we're done, nothing to do here.
                        ok
                after timer:minutes(application:get_env(blockchain, sync_timeout_mins, 10)) ->
                        libp2p_framed_stream:close(Stream),
                        ok
                end;
            _ ->
                ok
        end
    end).

send_txn(Txn) ->
    ok = blockchain_txn_mgr:submit(Txn,
                                   (fun(Res) ->
                                            case Res of
                                                ok ->
                                                    lager:info("successfully submit txn: ~s", [blockchain_txn:print(Txn)]);
                                                {error, Reason} ->
                                                    lager:error("failed to submit txn: ~s error: ~p", [blockchain_txn:print(Txn), Reason])
                                            end
                                    end)).

send_txn(Txn, Callback) ->
    ok = blockchain_txn_mgr:submit(Txn, Callback).
