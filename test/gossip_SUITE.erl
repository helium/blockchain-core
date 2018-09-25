-module(gossip_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain.hrl").

-export([
    all/0
]).

-export([
    basic/1
]).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%%   Running tests for this suite
%% @end
%%--------------------------------------------------------------------
all() ->
    [basic].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic(_Config) ->
    BaseDir = "data/gossip_SUITE/basic",
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
    Entries = [blockchain_ledger:find_entry(Addr, Ledger) || {Addr, _} <- ConsensusMembers],
    _ = [{?assertEqual(Balance, blockchain_ledger:balance(Entry))
          ,?assertEqual(0, blockchain_ledger:payment_nonce(Entry))}
         || Entry <- Entries],

    % Test a payment transaction, add a block and check balances
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:address(),
    Tx = blockchain_txn_payment:new(Payer, Recipient, 2500, 1),
    SignedTx = blockchain_txn_payment:sign(Tx, PayerPrivKey),
    Block = test_utils:create_block(ConsensusMembers, [SignedTx]),

    % ok = blockchain_worker:add_block(Block, self()),
    {ok, Swarm} = libp2p_swarm:start(gossip_SUITE, []),
    [ListenAddr|_] = libp2p_swarm:listen_addrs(blockchain_swarm:swarm()),
    {ok, Stream} = libp2p_swarm:dial_framed_stream(
        Swarm
        ,ListenAddr
        ,?GOSSIP_PROTOCOL
        ,blockchain_gossip_handler
        ,[]
    ),

    _ = blockchain_gossip_handler:send(Stream, erlang:term_to_binary({block, Block})),

    ok = test_utils:wait_until(fun() -> 2 =:= blockchain_worker:height() end),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain_worker:head_hash()),
    ?assertEqual(Block, blockchain_worker:head_block()),
    
    true = erlang:exit(Sup, normal),
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
