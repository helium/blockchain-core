%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Core Worker ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_worker).

-behavior(gen_server).

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
    notify/1
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
    n :: integer(),
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

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    erlang:process_flag(trap_exit, true),
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = blockchain_swarm:swarm(),
    Port = erlang:integer_to_list(proplists:get_value(port, Args, 0)),
    N = proplists:get_value(num_consensus_members, Args, 0),
    BaseDir = proplists:get_value(base_dir, Args, "data"),
    GenDir = proplists:get_value(update_dir, Args, undefined),
    Blockchain =
        case blockchain:new(BaseDir, GenDir) of
            {no_genesis, _Chain}=R ->
                R;
            {ok, Chain} ->
                ok = add_handlers(Swarm, N, Chain),
                self() ! maybe_sync,
                {ok, GenesisHash} = blockchain:genesis_hash(Chain),
                ok = blockchain_swarm:network_id(GenesisHash),
                Chain
        end,
    ok = libp2p_swarm:listen(Swarm, "/ip4/0.0.0.0/tcp/" ++ Port),
    {ok, #state{swarm=Swarm, n=N, blockchain=Blockchain}}.

%% NOTE: num_consensus_members and consensus_addrs can be called before there is a blockchain
handle_call(num_consensus_members, _From, #state{n=N}=State) ->
    {reply, N, State};
handle_call(_, _From, #state{blockchain={no_genesis, _}}=State) ->
    {reply, undefined, State};
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
            ok = add_handlers(Swarm, State#state.n, Blockchain),
            {ok, GenesisHash} = blockchain:genesis_hash(Blockchain),
            ok = blockchain_swarm:network_id(GenesisHash),
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
            {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
            SignedPaymentTxn = blockchain_txn_payment_v1:sign(PaymentTxn, SigFun),
            ok = send_txn(SignedPaymentTxn, Chain)
    end,
    {noreply, State};
handle_cast({spend, Recipient, Amount, Fee, Nonce}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    PubkeyBin = libp2p_swarm:pubkey_bin(Swarm),
    PaymentTxn = blockchain_txn_payment_v1:new(PubkeyBin, Recipient, Amount, Fee, Nonce),
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedPaymentTxn = blockchain_txn_payment_v1:sign(PaymentTxn, SigFun),
    ok = send_txn(SignedPaymentTxn, Chain),
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
            ok = send_txn(SignedPaymentTxn, Chain)
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
            ok = send_txn(SignedPaymentTxn, Chain)
    end,
    {noreply, State};
handle_cast({create_htlc_txn, Payee, PubkeyBin, Hashlock, Timelock, Amount, Fee}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    Payer = libp2p_swarm:pubkey_bin(Swarm),
    CreateTxn = blockchain_txn_create_htlc_v1:new(Payer, Payee, PubkeyBin, Hashlock, Timelock, Amount, Fee),
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedCreateHTLCTxn = blockchain_txn_create_htlc_v1:sign(CreateTxn, SigFun),
    ok = send_txn(SignedCreateHTLCTxn, Chain),
    {noreply, State};
handle_cast({redeem_htlc_txn, PubkeyBin, Preimage, Fee}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    Payee = libp2p_swarm:pubkey_bin(Swarm),
    RedeemTxn = blockchain_txn_redeem_htlc_v1:new(Payee, PubkeyBin, Preimage, Fee),
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedRedeemHTLCTxn = blockchain_txn_redeem_htlc_v1:sign(RedeemTxn, SigFun),
    ok = send_txn(SignedRedeemHTLCTxn, Chain),
    {noreply, State};
handle_cast({submit_txn, Txn}, #state{blockchain=Chain}=State) ->
    ok = send_txn(Txn, Chain),
    {noreply, State};
handle_cast({submit_txn, Txn, Callback}, #state{blockchain=Chain}=State) ->
    ok = send_txn(Txn, Chain, Callback),
    {noreply, State};
handle_cast({peer_height, Height, Head, Sender}, #state{n=N, blockchain=Chain, swarm=Swarm}=State) ->
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
                                                         [N, Chain]) of
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

handle_info(maybe_sync, #state{blockchain=Chain, swarm=Swarm}=State) ->
    erlang:cancel_timer(State#state.sync_timer),
    case blockchain:head_block(Chain) of
        {error, _Reason} ->
            lager:error("could not get head block ~p", [_Reason]),
            Ref = erlang:send_after(?SYNC_TIME, self(), maybe_sync),
            {noreply, State#state{sync_timer=Ref}};
        {ok, Head} ->
            BlockTime = blockchain_block:time(Head),
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
                            sync(Swarm, State#state.n, Chain, Peer),
                            {noreply, State};
                        Peers ->
                            RandomPeer = lists:nth(rand:uniform(length(Peers)), Peers),
                            sync(Swarm, State#state.n, Chain, RandomPeer),
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
                {ok, HeadHash} = blockchain:head_hash(Chain),
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

send_txn(Txn, Chain) ->
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    ok = blockchain_txn_manager:submit(Txn,
                                       ConsensusMembers,
                                       (fun(Res) ->
                                                case Res of
                                                    ok ->
                                                        lager:info("successfully submit txn: ~p", [Txn]);
                                                    {error, Reason} ->
                                                        lager:error("failed to submit txn: ~p error: ~p", [Txn, Reason])
                                                end
                                        end)).

send_txn(Txn, Chain, Callback) ->
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(blockchain:ledger(Chain)),
    ok = blockchain_txn_manager:submit(Txn, ConsensusMembers, Callback).
