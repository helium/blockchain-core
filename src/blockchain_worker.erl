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
    ,blocks/0
    ,blocks/2
    ,ledger/0
    ,num_consensus_members/0
    ,consensus_addrs/0, consensus_addrs/1
    ,integrate_genesis_block/1
    ,add_block/2
    ,sync_blocks/1
    ,spend/2
    ,payment_txn/4
    ,submit_txn/2
    ,add_gateway_request/1
    ,add_gateway_txn/1
    ,assert_location_txn/1
    ,peer_height/3
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
    ,consensus_addrs = [] :: [libp2p_crypto:address()]
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
height() ->
    gen_server:call(?SERVER, height).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
blockchain() ->
    gen_server:call(?SERVER, blockchain).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
head_hash() ->
    gen_server:call(?SERVER, head_hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
head_block() ->
    gen_server:call(?SERVER, head_block).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
genesis_hash() ->
    gen_server:call(?SERVER, genesis_hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
genesis_block() ->
    gen_server:call(?SERVER, genesis_block).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
blocks() ->
    gen_server:call(?SERVER, blocks).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
blocks(Height, Hash) ->
    gen_server:call(?SERVER, {blocks, Height, Hash}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
ledger() ->
    gen_server:call(?SERVER, ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
num_consensus_members() ->
    gen_server:call(?SERVER, num_consensus_members).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
consensus_addrs() ->
    gen_server:call(?SERVER, consensus_addrs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
consensus_addrs(Addresses) ->
    gen_server:cast(?SERVER, {consensus_addrs, Addresses}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
integrate_genesis_block(Block) ->
    gen_server:cast(?SERVER, {integrate_genesis_block, Block}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
add_block(Block, Sender) ->
    gen_server:cast(?SERVER, {add_block, Block, Sender}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
sync_blocks(Blocks) ->
    gen_server:cast(?SERVER, {sync_blocks, Blocks}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
spend(Recipient, Amount) ->
    gen_server:cast(?SERVER, {spend, Recipient, Amount}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
payment_txn(PrivKey, Address, Recipient, Amount) ->
    gen_server:cast(?SERVER, {payment_txn, PrivKey, Address, Recipient, Amount}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
submit_txn(Type, Txn) ->
    gen_server:cast(?SERVER, {submit_txn, Type, Txn}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
add_gateway_request(OwnerAddress) ->
    gen_server:call(?SERVER, {add_gateway_request, OwnerAddress}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
add_gateway_txn(AddGatewayRequest) ->
    gen_server:cast(?SERVER, {add_gateway_txn, AddGatewayRequest}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
assert_location_txn(Location) ->
    gen_server:cast(?SERVER, {assert_location_txn, Location}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
peer_height(Height, Head, Session) ->
    gen_server:cast(?SERVER, {peer_height, Height, Head, Session}).

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
            Else -> Else
        end,

    ok = libp2p_swarm:add_stream_handler(
        Swarm
        ,?GOSSIP_PROTOCOL
        ,{libp2p_framed_stream, server, [blockchain_gossip_handler, ?SERVER]}
    ),
    ok = libp2p_swarm:add_stream_handler(
        Swarm
        ,?SYNC_PROTOCOL
        ,{libp2p_framed_stream, server, [blockchain_sync_handler, ?SERVER]}
    ),
    ok = libp2p_swarm:listen(Swarm, "/ip4/0.0.0.0/tcp/" ++ Port),

    {ok, #state{swarm=Swarm, n=N, blockchain=Blockchain}}.

%% NOTE: num_consensus_members and consensus_addrs can be called before there is a blockchain
handle_call(num_consensus_members, _From, #state{n=N}=State) ->
    {reply, N, State};
handle_call(consensus_addrs, _From, #state{consensus_addrs=Addresses}=State) ->
    {reply, Addresses, State};
handle_call(_, _From, #state{blockchain={undefined, _}}}=State) ->
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
handle_call(blocks, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:blocks(Chain), State};
handle_call(ledger, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:ledger(Chain), State};
handle_call({add_gateway_request, OwnerAddress}, _From, State=#state{swarm=Swarm}) ->
    Address = libp2p_swarm:address(Swarm),
    AddGwTxn = blockchain_transaction:new_add_gateway_txn(OwnerAddress, Address),
    SignedAddGwTxn = blockchain_transaction:sign_add_gateway_request(AddGwTxn, Swarm),
    {reply, SignedAddGwTxn, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({integrate_genesis_block, GenesisBlock}, #state{blockchain={undefined, Dir}}=State) ->
    case blockchain_block:is_genesis(GenesisBlock) of
        false ->
            lager:warning("~p is not a genesis block", [GenesisBlock]),
            {noreply, State};
        true ->
            Transactions = blockchain_block:transactions(GenesisBlock),
            {ok, Ledger} = blockchain_transaction:absorb_transactions(Transactions, #{}),
            Blockchain = blockchain:new(GenesisBlock, Ledger, Dir),
            [ConsensusAddrs] = [blockchain_transaction:genesis_consensus_group_members(T)
                                || T <- blockchain_block:transactions(GenesisBlock)
                                ,blockchain_transaction:is_genesis_consensus_group_txn(T)],
            lager:info("blockchain started with ~p, consensus ~p", [lager:pr(Blockchain, blockchain), ConsensusAddrs]),
            ok = blockchain:save(Blockchain),
            {noreply, State#state{blockchain=Blockchain, consensus_addrs=ConsensusAddrs}}
    end;
handle_cast({consensus_addrs, Addresses}, State) ->
    {noreply, State#state{consensus_addrs=Addresses}};
handle_cast(_, #state{blockchain={undefined, _}}=State) ->
    {noreply, State};
handle_cast({add_block, Block, Session}, #state{blockchain=Chain, swarm=Swarm
                                                ,n=N}=State) ->
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
                    SwarmAgent = libp2p_swarm:group_agent(Swarm),
                    lager:info("sending the gossipped block to other workers"),
                    libp2p_group:send(SwarmAgent, erlang:term_to_binary({block, Block})),
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
            lager:info("syncing with the sender ~p", [Session]),
            Height = blockchain_block:height(blockchain:head_block(Chain)),
            Protocol = ?SYNC_PROTOCOL ++ "/" ++ erlang:integer_to_list(Height)
                       ++ "/" ++ blockchain_util:hexdump(Head),
            case libp2p_session:dial_framed_stream(Protocol, Session, blockchain_sync_handler, [self()]) of
                {ok, _Stream} -> ok;
                _ -> lager:notice("Failed to dial sync service on ~p", [Session])
            end,
            {noreply, State}
    end;
handle_cast({sync_blocks, {sync, Blocks}}, #state{n=N}=State0) when is_list(Blocks) ->
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
handle_cast({spend, Recipient, Amount}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    Address = libp2p_swarm:address(Swarm),
    Entry = blockchain_ledger:find_entry(Address, Ledger),
    Nonce = blockchain_ledger:payment_nonce(Entry),
    PaymentTxn = blockchain_transaction:new_payment_txn(Address
                                                        ,Recipient
                                                        ,Amount
                                                        ,Nonce + 1),
    SignedPaymentTxn = blockchain_transaction:sign_payment_txn(PaymentTxn, Swarm),
    ok = send_txn(payment_txn, SignedPaymentTxn, State),
    {noreply, State};
handle_cast({payment_txn, PrivKey, Address, Recipient, Amount}, #state{blockchain=Chain}=State) ->
    Ledger = blockchain:ledger(Chain),
    Entry = blockchain_ledger:find_entry(Address, Ledger),
    Nonce = blockchain_ledger:payment_nonce(Entry),
    PaymentTxn = blockchain_transaction:new_payment_txn(Address
                                                        ,Recipient
                                                        ,Amount
                                                        ,Nonce + 1),
    SignedPaymentTxn = blockchain_transaction:sign_payment_txn(PaymentTxn, PrivKey),
    ok = send_txn(payment_txn, SignedPaymentTxn, State),
    {noreply, State};
handle_cast({submit_txn, Type, Txn}, State) ->
    ok = send_txn(Type, Txn, State),
    {noreply, State};
handle_cast({add_gateway_txn, AddGwTxn}, #state{swarm=Swarm}=State) ->
    SignedAddGwTxn = blockchain_transaction:sign_add_gateway_txn(AddGwTxn, Swarm),
    ok = send_txn(add_gateway_txn, SignedAddGwTxn, State),
    {noreply, State};
handle_cast({assert_location_txn, Location}, #state{swarm=Swarm, blockchain=Chain}=State) ->
    Address = libp2p_swarm:address(Swarm),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger:find_gateway_info(Address, Ledger) of
        undefined ->
            lager:info("gateway not found in ledger.");
        GwInfo ->
            Nonce = blockchain_ledger:assert_location_nonce(GwInfo),
            AssertLocationTxn = blockchain_transaction:new_assert_location_txn(Address, Location, Nonce+1),
            SignedAssertLocationTxn = blockchain_transaction:sign_assert_location_txn(AssertLocationTxn, Swarm),
            lager:info("assert_location_txn, Address: ~p, Location: ~p, LedgerNonce: ~p, Txn: ~p", [Address, Location, Nonce, SignedAssertLocationTxn]),
            ok = send_txn(assert_location_txn, SignedAssertLocationTxn, State)
    end,
    {noreply, State};
handle_cast({peer_height, Height, Head, Session}, #state{blockchain=Chain}=State) ->
    lager:info("got peer height message with blockchain ~p", [lager:pr(Chain, blockchain)]),
    LocalHead = blockchain:head_hash(Chain),
    LocalHeight = blockchain_block:height(blockchain:head_block(Chain)),
    case LocalHeight < Height orelse (LocalHeight == Height andalso Head /= LocalHead) of
        true ->
            Protocol = ?SYNC_PROTOCOL ++ "/" ++ erlang:integer_to_list(LocalHeight) ++ "/" ++ blockchain_util:hexdump(LocalHead),
            case libp2p_session:dial_framed_stream(Protocol, Session, blockchain_sync_handler, [self()]) of
                {ok, _Stream} ->
                    ok;
                _ ->
                    lager:notice("Failed to dial sync service on ~p", [Session])
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
send_txn(Type, Txn, #state{swarm=Swarm, consensus_addrs=Addresses}) ->
    do_send(
        Swarm
        ,Addresses
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
