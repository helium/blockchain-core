
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
    blockchain/0,
    num_consensus_members/0,
    consensus_addrs/0,
    integrate_genesis_block/1,
    synced_blocks/0,
    spend/3, spend/4,
    payment_txn/5, payment_txn/6,
    submit_txn/1, submit_txn/2,
    create_htlc_txn/6,
    redeem_htlc_txn/3,
    peer_height/3,
    notify/1,
    mismatch/0,
    signed_metadata_fun/0
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
-define(SYNC_TIME, 60000).

-record(state, {
    blockchain :: {no_genesis, blockchain:blockchain()} | blockchain:blockchain(),
    swarm :: undefined | pid(),
    sync_timer = make_ref() :: reference()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec blockchain() -> blockchain:blockchain()  | undefined.
blockchain() ->
    gen_server:call(?SERVER, blockchain, infinity).

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
-spec synced_blocks() -> ok.
synced_blocks() ->
    gen_server:cast(?SERVER, synced_blocks).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec spend(libp2p_crypto:pubkey_bin(), pos_integer(), non_neg_integer()) -> ok.
spend(Recipient, Amount, Fee) ->
    gen_server:cast(?SERVER, {spend, Recipient, Amount, Fee}).

-spec spend(libp2p_crypto:pubkey_bin(), pos_integer(), non_neg_integer(), pos_integer()) -> ok.
spend(Recipient, Amount, Fee, Nonce) ->
    gen_server:cast(?SERVER, {spend, Recipient, Amount, Fee, Nonce}).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payment_txn(libp2p_crypto:sig_fun(), libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), integer(), non_neg_integer()) -> ok.
payment_txn(SigFun, PubkeyBin, Recipient, Amount, Fee) ->
    gen_server:cast(?SERVER, {payment_txn, SigFun, PubkeyBin, Recipient, Amount, Fee}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payment_txn(libp2p_crypto:sig_fun(), libp2p_crypto:address(), libp2p_crypto:address(), integer(), non_neg_integer(), non_neg_integer()) -> ok.
payment_txn(SigFun, PubkeyBin, Recipient, Amount, Fee, Nonce) ->
    %% Support user specified nonce
    gen_server:cast(?SERVER, {payment_txn, SigFun, PubkeyBin, Recipient, Amount, Fee, Nonce}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_htlc_txn(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
create_htlc_txn(Payee, PubkeyBin, Hashlock, Timelock, Amount, Fee) ->
    gen_server:cast(?SERVER, {create_htlc_txn, Payee, PubkeyBin, Hashlock, Timelock, Amount, Fee}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec redeem_htlc_txn(libp2p_crypto:pubkey_bin(), binary(), non_neg_integer()) -> ok.
redeem_htlc_txn(PubkeyBin, Preimage, Fee) ->
    gen_server:cast(?SERVER, {redeem_htlc_txn, PubkeyBin, Preimage, Fee}).

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
    ok = gen_event:notify(?EVT_MGR, Msg).

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
            try blockchain:sync_height(Chain) of
                {ok, Height} ->
                    #{<<"height">> => Height};
                {error, _} ->
                    #{}
            catch
                _:_ ->
                    %% probably have an expired blockchain handle
                    %% don't retry here, to avoid looping, but delete our cached handle for next time
                    put(peerbook_md_fun_blockchain, undefined)
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
    Ports = case application:get_env(blockchain, ports, undefined) of
                undefined ->
                    %% fallback to the single 'port' app env var
                    [proplists:get_value(port, Args, 0)];
                PortList when is_list(PortList) ->
                    PortList
            end,
    BaseDir = proplists:get_value(base_dir, Args, "data"),
    GenDir = proplists:get_value(update_dir, Args, undefined),
    AssumedValidBlockHash = case application:get_env(blockchain, assumed_valid_block_hash, undefined) of
                                undefined ->
                                    undefined;
                                BlockHash ->
                                    case application:get_env(blockchain, honor_assumed_valid, false) of
                                        true ->
                                            BlockHash;
                                        _ ->
                                            undefined
                                    end
                            end,
    Blockchain =
        case blockchain:new(BaseDir, GenDir, AssumedValidBlockHash) of
            {no_genesis, _Chain}=R ->
                R;
            {ok, Chain} ->
                {ok, N} = blockchain:config(?num_consensus_members, blockchain:ledger(Chain)),
                ok = add_handlers(Swarm, N, Chain),
                self() ! maybe_sync,
                {ok, GenesisHash} = blockchain:genesis_hash(Chain),
                ok = blockchain_txn_mgr:set_chain(Chain),
                true = libp2p_swarm:network_id(Swarm, GenesisHash),
                Chain
        end,
    true = lists:all(fun(E) -> E == ok end,
                     [ libp2p_swarm:listen(Swarm, "/ip4/0.0.0.0/tcp/" ++ integer_to_list(Port)) || Port <- Ports ]),
    {ok, #state{swarm=Swarm, blockchain=Blockchain}}.

handle_call(_, _From, #state{blockchain={no_genesis, _}}=State) ->
    {reply, undefined, State};
handle_call(num_consensus_members, _From, #state{blockchain = Chain} = State) ->
    {ok, N} = blockchain:config(?num_consensus_members, blockchain:ledger(Chain)),
    {reply, N, State};
handle_call(consensus_addrs, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)), State};
handle_call(blockchain, _From, #state{blockchain=Chain}=State) ->
    {reply, Chain, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({integrate_genesis_block, GenesisBlock}, #state{blockchain={no_genesis, Blockchain}
                                                            ,swarm=Swarm}=State) ->
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
            ok = notify({integrate_genesis_block, blockchain:genesis_hash(Blockchain)}),
            ok = add_handlers(Swarm, length(ConsensusAddrs), Blockchain),
            {ok, GenesisHash} = blockchain:genesis_hash(Blockchain),
            ok = blockchain_txn_mgr:set_chain(Blockchain),
            true = libp2p_swarm:network_id(Swarm, GenesisHash),
            self() ! maybe_sync,
            {noreply, State#state{blockchain=Blockchain}}
    end;
handle_cast(_, #state{blockchain={no_genesis, _}}=State) ->
    {noreply, State};
handle_cast(synced_blocks, State) ->
    lager:info("got synced_blocks msg"),
        erlang:cancel_timer(State#state.sync_timer),
    %% schedule another sync to see if there's more waiting
    Ref = erlang:send_after(5000, self(), maybe_sync),
    {noreply, State#state{sync_timer=Ref}};
handle_cast({spend, Recipient, Amount, Fee}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    PubkeyBin = libp2p_swarm:pubkey_bin(Swarm),
    case blockchain_ledger_v1:find_entry(PubkeyBin, Ledger) of
        {error, _Reason} ->
            lager:error("could not get entry ~p", [_Reason]);
        {ok, Entry} ->
            Nonce = blockchain_ledger_entry_v1:nonce(Entry),
            PaymentTxn = blockchain_txn_payment_v1:new(PubkeyBin, Recipient, Amount, Fee, Nonce + 1),
            {ok, _PubKey, SigFun, _ECDHFun} = libp2p_swarm:keys(Swarm),
            SignedPaymentTxn = blockchain_txn_payment_v1:sign(PaymentTxn, SigFun),
            ok = send_txn(SignedPaymentTxn)
    end,
    {noreply, State};
handle_cast({spend, Recipient, Amount, Fee, Nonce}, #state{swarm=Swarm}=State) ->
    PubkeyBin = libp2p_swarm:pubkey_bin(Swarm),
    PaymentTxn = blockchain_txn_payment_v1:new(PubkeyBin, Recipient, Amount, Fee, Nonce),
    {ok, _PubKey, SigFun, _ECDHFun} = libp2p_swarm:keys(Swarm),
    SignedPaymentTxn = blockchain_txn_payment_v1:sign(PaymentTxn, SigFun),
    ok = send_txn(SignedPaymentTxn),
    {noreply, State};
handle_cast({payment_txn, SigFun, PubkeyBin, Recipient, Amount, Fee}, #state{blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(PubkeyBin, Ledger) of
        {error, _Reason} ->
            lager:error("could not get entry ~p", [_Reason]);
        {ok, Entry} ->
            Nonce = blockchain_ledger_entry_v1:nonce(Entry),
            PaymentTxn = blockchain_txn_payment_v1:new(PubkeyBin, Recipient, Amount, Fee, Nonce + 1),
            SignedPaymentTxn = blockchain_txn_payment_v1:sign(PaymentTxn, SigFun),
            ok = send_txn(SignedPaymentTxn)
    end,
    {noreply, State};
handle_cast({payment_txn, SigFun, PubkeyBin, Recipient, Amount, Fee, Nonce}, #state{blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(PubkeyBin, Ledger) of
        {error, _Reason} ->
            lager:error("could not get entry ~p", [_Reason]);
        {ok, _Entry} ->
            PaymentTxn = blockchain_txn_payment_v1:new(PubkeyBin, Recipient, Amount, Fee, Nonce),
            SignedPaymentTxn = blockchain_txn_payment_v1:sign(PaymentTxn, SigFun),
            ok = send_txn(SignedPaymentTxn)
    end,
    {noreply, State};
handle_cast({create_htlc_txn, Payee, PubkeyBin, Hashlock, Timelock, Amount, Fee}, #state{swarm=Swarm}=State) ->
    Payer = libp2p_swarm:pubkey_bin(Swarm),
    CreateTxn = blockchain_txn_create_htlc_v1:new(Payer, Payee, PubkeyBin, Hashlock, Timelock, Amount, Fee),
    {ok, _PubKey, SigFun, _ECDHFun} = libp2p_swarm:keys(Swarm),
    SignedCreateHTLCTxn = blockchain_txn_create_htlc_v1:sign(CreateTxn, SigFun),
    ok = send_txn(SignedCreateHTLCTxn),
    {noreply, State};
handle_cast({redeem_htlc_txn, PubkeyBin, Preimage, Fee}, #state{swarm=Swarm}=State) ->
    Payee = libp2p_swarm:pubkey_bin(Swarm),
    RedeemTxn = blockchain_txn_redeem_htlc_v1:new(Payee, PubkeyBin, Preimage, Fee),
    {ok, _PubKey, SigFun, _ECDHFun} = libp2p_swarm:keys(Swarm),
    SignedRedeemHTLCTxn = blockchain_txn_redeem_htlc_v1:sign(RedeemTxn, SigFun),
    ok = send_txn(SignedRedeemHTLCTxn),
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
                    {ok, N} = blockchain:config(?num_consensus_members, blockchain:ledger(Chain)),
                    case libp2p_swarm:dial_framed_stream(Swarm,
                                                         libp2p_crypto:pubkey_bin_to_p2p(Sender),
                                                         ?SYNC_PROTOCOL,
                                                         blockchain_sync_handler,
                                                         [N, Chain]) of
                        {ok, Stream} ->
                            Stream ! {hash, LocalHead};
                        _ ->
                            lager:warning("Failed to dial sync service on: ~p", [Sender])
                    end
            end
    end,
    {noreply, State};
handle_cast(mismatch, #state{blockchain=Chain}=State) ->
    lager:warning("got mismatch message"),
    blockchain_lock:acquire(),
    Ledger = blockchain:ledger(Chain),
    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
    {ok, ChainHeight} = blockchain:height(Chain),
    lists:foreach(
        fun(H) ->
            {ok, Block} = blockchain:get_block(H, Chain),
            ok = blockchain:delete_block(Block, Chain)
        end,
        lists:reverse(lists:seq(LedgerHeight+1, ChainHeight))
    ),
    blockchain_lock:release(),
    {noreply, State};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(maybe_sync, #state{blockchain=Chain, swarm=Swarm}=State) ->
    erlang:cancel_timer(State#state.sync_timer),
    case blockchain:head_block(Chain) of
        {error, _Reason} ->
            lager:error("could not get head block ~p", [_Reason]),
            Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
            {noreply, State#state{sync_timer=Ref}};
        {ok, Head} ->
            BlockTime = blockchain_block:time(Head),
            {ok, N} = blockchain:config(?num_consensus_members, blockchain:ledger(Chain)),
            case erlang:system_time(seconds) - BlockTime of
                X when X > 300 ->
                    %% figure out our gossip peers
                    Peers = libp2p_group_gossip:connected_addrs(libp2p_swarm:gossip_group(Swarm), all),
                    case Peers of
                        [] ->
                            %% try again later when there's peers
                            Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
                            {noreply, State#state{sync_timer=Ref}};
                        [Peer] ->
                            sync(Swarm, N, Chain, Peer),
                            {noreply, State};
                        Peers ->
                            RandomPeer = lists:nth(rand:uniform(length(Peers)), Peers),
                            sync(Swarm, N, Chain, RandomPeer),
                            {noreply, State}
                    end;
                _ ->
                    %% no need to sync now, check again later
                    Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
                    {noreply, State#state{sync_timer=Ref}}
            end
    end;
handle_info({update_timer, Ref}, State) ->
    {noreply, State#state{sync_timer=Ref}};
handle_info({blockchain_event, {add_block, Hash, Sync, _Ledger}}, #state{swarm=Swarm, blockchain=Chain} = State) ->
    %% we added a new block to the chain, send it to all our peers
    case blockchain:get_block(Hash, Chain) of
        {ok, Block} when Sync == false ->
            libp2p_group_gossip:send(
              libp2p_swarm:gossip_group(Swarm),
              ?GOSSIP_PROTOCOL,
              blockchain_gossip_handler:gossip_data(Swarm, Block)
             );
        {ok, _} ->
            %% don't gossip blocks we're syncing
            ok;
        {error, Reason} ->
            lager:warning("unable to find new block ~p in blockchain: ~p", [Hash, Reason])
    end,
    {noreply, State};
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_handlers(pid(), pos_integer(), blockchain:blockchain()) -> ok.
add_handlers(Swarm, N, Blockchain) ->
    libp2p_group_gossip:add_handler(libp2p_swarm:gossip_group(Swarm), ?GOSSIP_PROTOCOL,
                                    {blockchain_gossip_handler, [Swarm, N, Blockchain]}),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?SYNC_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_sync_handler, ?SERVER, N, Blockchain]}
    ),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?FASTFORWARD_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_fastforward_handler, ?SERVER, N, Blockchain]}
    ).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
sync(Swarm, N, Chain, Peer) ->
    Parent = self(),
    spawn(fun() ->
        Ref = erlang:send_after(?SYNC_TIME, Parent, maybe_sync),
        case libp2p_swarm:dial_framed_stream(Swarm,
                                             Peer,
                                             ?SYNC_PROTOCOL,
                                             blockchain_sync_handler,
                                             [N, Chain])
        of
            {ok, Stream} ->
                {ok, HeadHash} = blockchain:sync_hash(Chain),
                Stream ! {hash, HeadHash},
                %% this timer will likely get cancelled when the sync response comes in
                %% but if that never happens, we don't forget to sync
                Parent ! {update_timer, Ref};
            _ ->
                erlang:cancel_timer(Ref),
                %% schedule a sync retry with another peer
                Ref2 = erlang:send_after(5000, Parent, maybe_sync),
                Parent ! {update_timer, Ref2}
        end
    end).

send_txn(Txn) ->
    ok = blockchain_txn_mgr:submit(Txn,
                                   (fun(Res) ->
                                            case Res of
                                                ok ->
                                                    lager:info("successfully submit txn: ~p", [Txn]);
                                                {error, Reason} ->
                                                    lager:error("failed to submit txn: ~p error: ~p", [Txn, Reason])
                                            end
                                    end)).

send_txn(Txn, Callback) ->
    ok = blockchain_txn_mgr:submit(Txn, Callback).
