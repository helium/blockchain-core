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
    ,head/0
    ,genesis_hash/0
    ,blocks/0
    ,blocks/2
    ,ledger/0
    ,consensus_addrs/0
    ,integrate_genesis_block/1
    ,add_block/2
    ,sync_blocks/1
    ,spend/2
    ,payment_txn/4
    ,add_gateway_txn/1, add_gateway_txn/2
    ,assert_location_txn/1, assert_location_txn/3
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
    blockchain :: undefined | blockchain:blockchain()
    ,swarm :: undefined | pid()
    ,consensus_addrs = [] :: [libp2p_crypto:address()]
    ,n :: integer()
    ,dir :: string()
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
head() ->
    gen_server:call(?SERVER, head).

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
consensus_addrs() ->
    gen_server:call(?MODULE, consensus_addrs).

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
add_gateway_txn(Address) ->
    gen_server:cast(?SERVER, {add_gateway_txn, Address}).

add_gateway_txn(PrivKey, Address) ->
    gen_server:cast(?SERVER, {add_gateway_txn, PrivKey, Address}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
assert_location_txn(Location) ->
    gen_server:cast(?SERVER, {assert_location_txn, Location}).

assert_location_txn(PrivKey, Address, Location) ->
    gen_server:cast(?SERVER, {assert_location_txn, PrivKey, Address, Location}).

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
    Blockchain = blockchain:load(Dir),
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
    {ok, #state{swarm=Swarm, n=N, dir=Dir, blockchain=Blockchain}}.

handle_call(_, _From, #state{blockchain=undefined}=State) ->
    {reply, undefined, State};
handle_call(height, _From, #state{blockchain=Chain}=State) ->
    CurrentBlock = blockchain:current_block(Chain),
    Height = blockchain_block:height(CurrentBlock),
    {reply, Height, State};
handle_call(blockchain, _From, #state{blockchain=Chain}=State) ->
    {reply, Chain, State};
handle_call(head, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:head(Chain), State};
handle_call(genesis_hash, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:genesis_hash(Chain), State};
handle_call(blocks, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:blocks(Chain), State};
% handle_call({blocks, _Height, Hash}, _From, #state{blockchain=Chain}=State) ->
%     FirstBlock = maps:get(Hash, Chain#blockchain.blocks, maps:get(Chain#blockchain.genesis_hash, Chain#blockchain.blocks)),
%     Blocks = build_chain(FirstBlock, maps:values(Chain#blockchain.blocks), []),
%     {reply, {ok, Blocks}, State};
handle_call(ledger, _From, #state{blockchain=Chain}=State) ->
    {reply, blockchain:ledger(Chain), State};
handle_call(consensus_addrs, _From, #state{consensus_addrs=Addresses}=State) ->
    {reply, Addresses, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({integrate_genesis_block, GenesisBlock}, #state{blockchain=undefined, dir=Dir}=State) ->
    case blockchain_block:is_genesis(GenesisBlock) of
        false ->
            lager:warning("~p is not a genesis block", [GenesisBlock]),
            {noreply, State};
        true ->
            GenesisHash = blockchain_block:hash_block(GenesisBlock),
            Transactions = blockchain_block:transactions(GenesisBlock),
            {ok, Ledger} = blockchain_transaction:absorb_transactions(Transactions, #{}),
            Blockchain = blockchain:new(GenesisHash, GenesisBlock, Ledger),
            [ConsensusAddrs] = [blockchain_transaction:genesis_consensus_group_members(T)
                              || T <- blockchain_block:transactions(GenesisBlock)
                              ,blockchain_transaction:is_genesis_consensus_group_txn(T)],
            lager:info("blockchain started with ~p, consensus ~p", [lager:pr(Blockchain, blockchain), ConsensusAddrs]),
            ok = blockchain:save(Blockchain, Dir),
            {noreply, State#state{blockchain=Blockchain, consensus_addrs=ConsensusAddrs}}
    end;
handle_cast(_, #state{blockchain=undefined}=State) ->
    {noreply, State};
handle_cast({add_block, Block, _Session}, #state{blockchain=Chain, swarm=Swarm
                                                 ,n=N, dir=Dir}=State) ->
    Head = blockchain:head(Chain),
    Hash = blockchain_block:hash_block(Block),
    F = ((N-1) div 3),
    case blockchain_block:prev_hash(Block) == Head of
        true ->
            lager:info("prev hash matches the gossiped block"),
            BinBlock = erlang:term_to_binary(blockchain_block:remove_signature(Block)),
            Ledger = blockchain:ledger(Chain),
            case blockchain_block:verify_signature(BinBlock
                                                   ,blockchain_ledger:consensus_members(Ledger)
                                                   ,blockchain_block:signature(Block)
                                                   ,N-F)
            of
                {true, _} ->
                    NewChain = blockchain:add_block(Chain, Block),
                    ok = blockchain:save(NewChain, Dir),
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
            % TODO: Sync here
            {noreply, State}
    end;
handle_cast({sync_blocks, {sync, Blocks}}, #state{n=N, dir=Dir}=State0) when is_list(Blocks) ->
    lager:info("got sync_blocks msg ~p", [Blocks]),
    F = ((N-1) div 3),
    % TODO: Too much nesting
    State1 =
        lists:foldl(
            fun(Block, #state{blockchain=Chain}=State) ->
                Head = blockchain:head(Chain),
                case blockchain_block:prev_hash(Block) == Head of
                    true ->
                        lager:info("prev hash matches the gossiped block"),
                        BinBlock = erlang:term_to_binary(blockchain_block:remove_signature(Block)),
                        Ledger = blockchain:ledger(Chain),
                        case blockchain_block:verify_signature(BinBlock
                                                               ,blockchain_ledger:consensus_members(Ledger)
                                                               ,blockchain_block:signature(Block)
                                                               ,N-F)
                        of
                            {true, _} ->
                                NewChain = blockchain:add_block(Chain, Block),
                                ok = blockchain:save(NewChain, Dir),
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
    Nonce = blockchain_ledger:nonce(Entry),
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
    Nonce = blockchain_ledger:nonce(Entry),
    PaymentTxn = blockchain_transaction:new_payment_txn(Address
                                                        ,Recipient
                                                        ,Amount
                                                        ,Nonce + 1),
    SignedPaymentTxn = blockchain_transaction:sign_payment_txn(PaymentTxn, PrivKey),
    ok = send_txn(payment_txn, SignedPaymentTxn, State),
    {noreply, State};
handle_cast({add_gateway_txn, Address}, #state{swarm=Swarm}=State) ->
    AddGwTxn = blockchain_transaction:new_add_gateway_txn(Address),
    SignedAddGwTxn = blockchain_transaction:sign_add_gateway_txn(AddGwTxn, Swarm),
    ok = send_txn(add_gateway_txn, SignedAddGwTxn, State),
    {noreply, State};
handle_cast({add_gateway_txn, PrivKey, Address}, State) ->
    AddGwTxn = blockchain_transaction:new_add_gateway_txn(Address),
    SignedAddGwTxn = blockchain_transaction:sign_add_gateway_txn(AddGwTxn, PrivKey),
    ok = send_txn(add_gateway_txn, SignedAddGwTxn, State),
    {noreply, State};
handle_cast({assert_location_txn, Location}, #state{swarm=Swarm}=State) ->
    Address = libp2p_swarm:address(Swarm),
    AssertLocationTxn = blockchain_transaction:new_assert_location_txn(Address, Location),
    SignedAssertLocationTxn = blockchain_transaction:sign_assert_location_txn(AssertLocationTxn, Swarm),
    ok = send_txn(assert_location_txn, SignedAssertLocationTxn, State),
    {noreply, State};
handle_cast({assert_location_txn, PrivKey, Address, Location}, State) ->
    AssertLocationTxn = blockchain_transaction:new_assert_location_txn(Address, Location),
    SignedAssertLocationTxn = blockchain_transaction:sign_assert_location_txn(AssertLocationTxn, PrivKey),
    ok = send_txn(assert_location_txn, SignedAssertLocationTxn, State),
    {noreply, State};
handle_cast({peer_height, Height, Head, Session}, #state{blockchain=Chain}=State) ->
    lager:info("got peer height message with blockchain ~p", [lager:pr(Chain, blockchain)]),
    LocalHead = blockchain:head(Chain),
    LocalHeight = blockchain_block:height(blockchain:get_block(Chain, LocalHead)),
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
