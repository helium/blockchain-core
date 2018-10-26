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
    start_link/1
    ,height/0
    ,blockchain/0
    ,head_hash/0, head_block/0
    ,genesis_hash/0, genesis_block/0
    ,get_block/1
    ,ledger/0
    ,num_consensus_members/0
    ,consensus_addrs/0
    ,integrate_genesis_block/1
    ,add_block/2
    ,sync_blocks/1
    ,spend/3
    ,payment_txn/4
    ,submit_txn/2
    ,create_htlc_txn/4
    ,redeem_htlc_txn/2
    ,add_gateway_request/1
    ,add_gateway_txn/1
    ,assert_location_request/2
    ,assert_location_txn/1
    ,peer_height/3
    ,notify/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1
    ,handle_call/3
    ,handle_cast/2
    ,handle_info/2
    ,terminate/2
    ,code_change/3
]).

-include("blockchain.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    blockchain :: {undefined, file:filename_all()} | blockchain:blockchain()
    ,swarm :: undefined | pid()
    ,n :: integer()
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
-spec height() -> integer()  | undefined.
height() ->
    gen_server:call(?SERVER, height).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec blockchain() -> blockchain:blockchain()  | undefined.
blockchain() ->
    gen_server:call(?SERVER, blockchain).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_hash() -> blockchain_block:hash()  | undefined.
head_hash() ->
    gen_server:call(?SERVER, head_hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_block() -> blockchain_block:block()  | undefined.
head_block() ->
    gen_server:call(?SERVER, head_block).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_hash() -> blockchain_block:hash()  | undefined.
genesis_hash() ->
    gen_server:call(?SERVER, genesis_hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_block() -> blockchain_block:block()  | undefined.
genesis_block() ->
    gen_server:call(?SERVER, genesis_block).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_block(Hash) ->
    gen_server:call(?SERVER, {get_block, Hash}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec ledger() -> blockchain_ledger:ledger() | undefined.
ledger() ->
    gen_server:call(?SERVER, ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec num_consensus_members() -> integer().
num_consensus_members() ->
    gen_server:call(?SERVER, num_consensus_members).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_addrs() -> [libp2p_crypto:address()].
consensus_addrs() ->
    gen_server:call(?SERVER, consensus_addrs).

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
-spec add_block(blockchain_block:block(), libp2p_crypto:address()) -> ok.
add_block(Block, Sender) ->
    gen_server:cast(?SERVER, {add_block, Block, Sender}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sync_blocks([blockchain_block:block()]) -> ok.
sync_blocks(Blocks) ->
    gen_server:cast(?SERVER, {sync_blocks, Blocks}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec spend(libp2p_crypto:address(), pos_integer(), non_neg_integer()) -> ok.
spend(Recipient, Amount, Fee) ->
    gen_server:cast(?SERVER, {spend, Recipient, Amount, Fee}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payment_txn(libp2p_crypto:private_key(), libp2p_crypto:address(), libp2p_crypto:address(), integer()) -> ok.
payment_txn(PrivKey, Address, Recipient, Amount) ->
    gen_server:cast(?SERVER, {payment_txn, PrivKey, Address, Recipient, Amount}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_htlc_txn(libp2p_crypto:address(), non_neg_integer(), binary(), non_neg_integer()) -> ok.
create_htlc_txn(Address, Amount, Hashlock, Timelock) ->
    gen_server:cast(?SERVER, {create_htlc_txn, Address, Amount, Hashlock, Timelock}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec redeem_htlc_txn(libp2p_crypto:address(), binary()) -> ok.
redeem_htlc_txn(Address, Preimage) ->
    gen_server:cast(?SERVER, {redeem_htlc_txn, Address, Preimage}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec submit_txn(atom(), blockchain_transactions:transaction()) -> ok.
submit_txn(Type, Txn) ->
    gen_server:cast(?SERVER, {submit_txn, Type, Txn}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_request(libp2p_crypto:address()) -> ok.
add_gateway_request(OwnerAddress) ->
    gen_server:call(?SERVER, {add_gateway_request, OwnerAddress}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_txn(blockchain_txn_add_gateway:txn_add_gateway()) -> ok.
add_gateway_txn(AddGatewayRequest) ->
    gen_server:cast(?SERVER, {add_gateway_txn, AddGatewayRequest}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
%% TODO: better spec for location
-spec assert_location_request(libp2p_crypto:address(), integer()) -> {error, gateway_not_found} |
                                                                     {error, invalid_owner} |
                                                                     blockchain_txn_assert_location:txn_assert_location().
assert_location_request(OwnerAddress, Location) ->
    gen_server:call(?SERVER, {assert_location_request, OwnerAddress, Location}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec assert_location_txn(blockchain_txn_assert_location:txn_assert_location()) -> ok.
assert_location_txn(AssertLocRequest) ->
    gen_server:cast(?SERVER, {assert_location_txn, AssertLocRequest}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec peer_height(integer(), blockchain_block:hash(), libp2p_crypto:address()) -> ok.
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
    Dir = proplists:get_value(base_dir, Args, "data"),

    Blockchain =
        case blockchain:load(Dir) of
            undefined -> {undefined, Dir};
            Chain ->
                ok = add_handlers(Swarm, Chain),
                Chain
        end,
    ok = libp2p_swarm:listen(Swarm, "/ip4/0.0.0.0/tcp/" ++ Port),
    {ok, #state{swarm=Swarm, n=N, blockchain=Blockchain}}.

%% NOTE: num_consensus_members and consensus_addrs can be called before there is a blockchain
handle_call(num_consensus_members, _From, #state{n=N}=State) ->
    {reply, N, State};
handle_call(consensus_addrs, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain_ledger:consensus_members(blockchain:ledger(Chain)), State};
handle_call(_, _From, #state{blockchain={undefined, _}}=State) ->
    {reply, undefined, State};
handle_call(height, _From, #state{blockchain=Chain}=State) ->
    CurrentBlock = blockchain:head_block(Chain),
    Height = blockchain_block:height(CurrentBlock),
    {reply, Height, State};
handle_call(blockchain, _From, #state{blockchain=Chain}=State) ->
    {reply, Chain, State};
handle_call(head_hash, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:head_hash(Chain), State};
handle_call(head_block, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:head_block(Chain), State};
handle_call(genesis_hash, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:genesis_hash(Chain), State};
handle_call(genesis_block, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:genesis_block(Chain), State};
handle_call({get_block, Hash}, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:get_block(Hash, Chain), State};
handle_call(ledger, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:ledger(Chain), State};
handle_call({add_gateway_request, OwnerAddress}, _From, State=#state{swarm=Swarm}) ->
    Address = libp2p_swarm:address(Swarm),
    AddGwTxn = blockchain_txn_add_gateway:new(OwnerAddress, Address),
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedAddGwTxn = blockchain_txn_add_gateway:sign_request(AddGwTxn, SigFun),
    {reply, SignedAddGwTxn, State};
handle_call({assert_location_request, Owner, Location}, _From, State=#state{swarm=Swarm, blockchain=Chain}) ->
    Address = libp2p_swarm:address(Swarm),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger:find_gateway_info(Address, Ledger) of
        undefined ->
            lager:info("gateway not found in ledger."),
            {reply, {error, gateway_not_found}, State};
        GwInfo ->
            Nonce = blockchain_ledger:assert_location_nonce(GwInfo),
            %% check that the correct owner has been specified
            case Owner =:= blockchain_ledger:gateway_owner(GwInfo) of
                true ->
                    AssertLocationRequestTxn = blockchain_txn_assert_location:new(Address, Owner, Location, Nonce+1),
                    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
                    SignedAssertLocRequestTxn = blockchain_txn_assert_location:sign_request(AssertLocationRequestTxn, SigFun),
                    lager:info(
                      "assert_location_request, Address: ~p, Location: ~p, LedgerNonce: ~p, Txn: ~p"
                      ,[Address, Location, Nonce, SignedAssertLocRequestTxn]
                     ),
                    {reply, SignedAssertLocRequestTxn, State};
                false ->
                    {reply, {error, invalid_owner}, State}
            end
    end;
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({integrate_genesis_block, GenesisBlock}, #state{blockchain={undefined, Dir}
                                                            ,swarm=Swarm}=State) ->
    case blockchain_block:is_genesis(GenesisBlock) of
        false ->
            lager:warning("~p is not a genesis block", [GenesisBlock]),
            {noreply, State};
        true ->
            Blockchain = blockchain:new(GenesisBlock, Dir),
            [ConsensusAddrs] = [blockchain_txn_gen_consensus_group:members(T)
                                || T <- blockchain_block:transactions(GenesisBlock)
                                ,blockchain_txn_gen_consensus_group:is(T)],
            lager:info("blockchain started with ~p, consensus ~p", [lager:pr(Blockchain, blockchain), ConsensusAddrs]),
            ok = blockchain:save(Blockchain),
            ok = notify({integrate_genesis_block, blockchain:genesis_hash(Blockchain)}),
            ok = add_handlers(Swarm, Blockchain),
            {noreply, State#state{blockchain=Blockchain}}
    end;
handle_cast(_, #state{blockchain={undefined, _}}=State) ->
    {noreply, State};
handle_cast({add_block, Block, Sender}, #state{blockchain=Chain, swarm=Swarm
                                                ,n=N}=State) ->
    lager:info("Sender: ~p, MyAddress: ~p", [Sender, blockchain_swarm:address()]),
    Head = blockchain:head_hash(Chain),
    Hash = blockchain_block:hash_block(Block),
    F = ((N-1) div 3),
    case blockchain_block:prev_hash(Block) == Head of
        true ->
            lager:info("prev hash matches the gossiped block"),
            Ledger = blockchain:ledger(Chain),
            case blockchain_block:verify_signature(Block
                                                   ,blockchain_ledger:consensus_members(Ledger)
                                                   ,blockchain_block:signature(Block)
                                                   ,N-F)
            of
                {true, _} ->
                    NewChain = blockchain:add_block(Block, Chain),
                    lager:info("sending the gossipped block to other workers"),
                    Address = libp2p_swarm:address(Swarm),
                    libp2p_group_gossip:send(libp2p_swarm:gossip_group(Swarm), ?GOSSIP_PROTOCOL, term_to_binary({block, Address, Block})),
                    ok = notify({add_block, Hash, true}),
                    {noreply, State#state{blockchain=NewChain}};
                false ->
                    lager:warning("signature on block ~p is invalid", [Block]),
                    {noreply, State}
            end;
        false when Hash == Head ->
            lager:info("already have this block"),
            {noreply, State};
        false ->
            lager:warning("gossipped block doesn't fit with our chain"),
            P2PAddress = libp2p_crypto:address_to_p2p(Sender),
            lager:info("syncing with the sender ~p", [P2PAddress]),
            case libp2p_swarm:dial_framed_stream(Swarm,
                                                 P2PAddress,
                                                 ?SYNC_PROTOCOL,
                                                 blockchain_sync_handler,
                                                 [self()]) of
                {ok, Stream} ->
                    Stream ! {hash, blockchain:head_hash(Chain)};
                _Error ->
                    lager:warning("Failed to dial sync service on: ~p ~p", [P2PAddress, _Error])
            end,
            {noreply, State}
    end;
handle_cast({sync_blocks, Blocks}, #state{n=N}=State0) when is_list(Blocks) ->
    lager:info("got sync_blocks msg ~p", [Blocks]),
    F = ((N-1) div 3),
    % TODO: Too much nesting
    State1 =
        lists:foldl(
            fun(Block, #state{blockchain=Chain}=State) ->
                Head = blockchain:head_hash(Chain),
                case blockchain_block:prev_hash(Block) == Head of
                    true ->
                        lager:info("prev hash matches the gossiped block"),
                        Ledger = blockchain:ledger(Chain),
                        case blockchain_block:verify_signature(Block
                                                               ,blockchain_ledger:consensus_members(Ledger)
                                                               ,blockchain_block:signature(Block)
                                                               ,N-F)
                        of
                            {true, _} ->
                                NewChain = blockchain:add_block(Block, Chain),
                                ok = notify({add_block, blockchain_block:hash_block(Block), false}),
                                State#state{blockchain=NewChain};
                            false ->
                                State
                        end;
                    false ->
                        State
                end
            end
            ,State0
            ,Blocks
        ),
    {noreply, State1};
handle_cast({spend, Recipient, Amount, Fee}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    Address = libp2p_swarm:address(Swarm),
    Entry = blockchain_ledger:find_entry(Address, blockchain_ledger:entries(Ledger)),
    Nonce = blockchain_ledger:payment_nonce(Entry),
    PaymentTxn = blockchain_txn_payment:new(Address, Recipient, Amount, Fee, Nonce + 1),
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedPaymentTxn = blockchain_txn_payment:sign(PaymentTxn, SigFun),
    ok = send_txn(payment_txn, SignedPaymentTxn, State),
    {noreply, State};
handle_cast({payment_txn, PrivKey, Address, Recipient, Amount, Fee}, #state{blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    Entry = blockchain_ledger:find_entry(Address, blockchain_ledger:entries(Ledger)),
    Nonce = blockchain_ledger:payment_nonce(Entry),
    PaymentTxn = blockchain_txn_payment:new(Address, Recipient, Amount, Fee, Nonce + 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedPaymentTxn = blockchain_txn_payment:sign(PaymentTxn, SigFun),
    ok = send_txn(payment_txn, SignedPaymentTxn, State),
    {noreply, State};
handle_cast({create_htlc_txn, Address, Amount, Hashlock, Timelock}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    Payer = libp2p_swarm:address(Swarm),
    Entry = blockchain_ledger:find_entry(Payer, blockchain_ledger:entries(Ledger)),
    Nonce = blockchain_ledger:payment_nonce(Entry),
    CreateTxn = blockchain_txn_create_htlc:new(Payer, Address, Hashlock, Timelock, Amount, Nonce),
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedCreateTxn = blockchain_txn_create_htlc:sign(CreateTxn, SigFun),
    ok = send_txn(create_htlc_txn, SignedCreateTxn, State),
    {noreply, State};
handle_cast({create_htlc_txn, Address, Preimage}, #state{swarm=Swarm}=State) ->
    Payee = libp2p_swarm:address(Swarm),
    RedeemTxn = blockchain_txn_redeem_htlc:new(Payee, Address, Preimage),
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedRedeemTxn = blockchain_txn_redeem_htlc:sign(RedeemTxn, SigFun),
    ok = send_txn(redeem_htlc_txn, SignedRedeemTxn, State),
    {noreply, State};
handle_cast({submit_txn, Type, Txn}, State) ->
    ok = send_txn(Type, Txn, State),
    {noreply, State};
handle_cast({add_gateway_txn, AddGwTxn}, #state{swarm=Swarm}=State) ->
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedAddGwTxn = blockchain_txn_add_gateway:sign(AddGwTxn, SigFun),
    ok = send_txn(add_gateway_txn, SignedAddGwTxn, State),
    {noreply, State};
handle_cast({assert_location_txn, AssertLocTxn}, #state{swarm=Swarm}=State) ->
    {ok, _PubKey, SigFun} = libp2p_swarm:keys(Swarm),
    SignedAssertLocTxn = blockchain_txn_assert_location:sign(AssertLocTxn, SigFun),
    ok = send_txn(assert_location_txn, SignedAssertLocTxn, State),
    {noreply, State};
handle_cast({peer_height, Height, Head, Sender}, #state{blockchain=Chain, swarm=Swarm}=State) ->
    lager:info("got peer height message with blockchain ~p", [lager:pr(Chain, blockchain)]),
    LocalHead = blockchain:head_hash(Chain),
    LocalHeight = blockchain_block:height(blockchain:head_block(Chain)),
    case LocalHeight < Height orelse (LocalHeight == Height andalso Head /= LocalHead) of
        true ->
            case libp2p_swarm:dial_framed_stream(Swarm,
                                                 libp2p_crypto:address_to_p2p(Sender),
                                                 ?SYNC_PROTOCOL,
                                                 blockchain_sync_handler,
                                                 [self()]) of
                {ok, Stream} ->
                    Stream ! {hash, blockchain:head_hash(Chain)};
                _ -> lager:notice("Failed to dial sync service on: ~p", [Sender])
            end;
        false -> ok
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_handlers(pid(), blockchain:blockchain()) -> ok.
add_handlers(Swarm, Chain) ->
    Address = libp2p_swarm:address(Swarm),
    libp2p_group_gossip:add_handler(libp2p_swarm:gossip_group(Swarm), ?GOSSIP_PROTOCOL, {blockchain_gossip_handler, [Address]}),

    ok = libp2p_swarm:add_stream_handler(
        Swarm
        ,?SYNC_PROTOCOL
        ,{libp2p_framed_stream, server, [blockchain_sync_handler, ?SERVER, blockchain:dir(Chain)]}
    ),

    ok = libp2p_swarm:add_stream_handler(
        Swarm
        ,?GW_REGISTRATION_PROTOCOL
        ,{libp2p_framed_stream, server, [blockchain_gw_registration_handler, ?SERVER]}
    ),

    ok = libp2p_swarm:add_stream_handler(
        Swarm
        ,?LOC_ASSERTION_PROTOCOL
        ,{libp2p_framed_stream, server, [blockchain_loc_assertion_handler, ?SERVER]}
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
send_txn(Type, Txn, #state{swarm=Swarm, blockchain=Chain}) ->
    do_send(
        Swarm
        ,blockchain_ledger:consensus_members(blockchain:ledger(Chain))
        ,erlang:term_to_binary({Type, Txn})
        ,?TX_PROTOCOL
        ,blockchain_txn_handler
        ,[self()]
        ,false
    ).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
do_send(_Swarm, [], _DataToSend, _Protocol, _Module, _Args, _Retry) ->
    ok;
do_send(Swarm, [Address|Tail]=Addresses, DataToSend, Protocol, Module, Args, Retry) ->
    P2PAddress = libp2p_crypto:address_to_p2p(Address),
    case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, Protocol, Module, Args) of
        {ok, Stream} ->
            lager:info("dialed peer ~p via ~p~n", [Address, Protocol]),
            libp2p_framed_stream:send(Stream, DataToSend),
            libp2p_framed_stream:close(Stream),
            ok;
        Other when Retry == false ->
            lager:notice("failed to dial ~p service on ~p : ~p", [Protocol, Address, Other]),
            do_send(Swarm, Tail, DataToSend, Protocol, Module, Args, Retry);
        Other ->
            lager:notice("Failed to dial ~p service on ~p : ~p", [Protocol, Address, Other]),
            do_send(Swarm, Addresses, DataToSend, Protocol, Module, Args, Retry)
    end.
