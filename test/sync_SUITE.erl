-module(sync_SUITE).

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
    BaseDir = "data/sync_SUITE/basic",
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
    Entries = [blockchain_ledger:find_entry(Addr, Ledger) || {Addr, _} <- ConsensusMembers],
    _ = [{?assertEqual(Balance, blockchain_ledger:balance(Entry))
          ,?assertEqual(0, blockchain_ledger:payment_nonce(Entry))}
         || Entry <- Entries],

    % Create 10 empty blocks
    Blocks = create_blocks(10, ConsensusMembers),

    % {ok, Swarm} = libp2p_swarm:start(sync_SUITE, []),
    % [ListenAddr|_] = libp2p_swarm:listen_addrs(blockchain_swarm:swarm()),
    % {ok, Stream} = libp2p_swarm:dial_framed_stream(
    %     Swarm
    %     ,ListenAddr
    %     ,?SYNC_PROTOCOL
    %     ,blockchain_sync_handler
    %     ,[]
    % ),

    ok = blockchain_worker:sync_blocks(Blocks),

    ok = test_utils:wait_until(fun() -> 11 =:= blockchain_worker:height() end),
    LastBlock = lists:last(Blocks),
    ?assertEqual(blockchain_block:hash_block(LastBlock), blockchain_worker:head_hash()),
    ?assertEqual(LastBlock, blockchain_worker:head_block()),

    true = erlang:exit(Sup, normal),
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
create_blocks(N, ConsensusMembers) ->
    create_blocks(N, ConsensusMembers, []).

create_blocks(0, _, Blocks) ->
    lists:reverse(Blocks);
create_blocks(N, ConsensusMembers, []=Blocks) ->
    PrevHash = blockchain_worker:head_hash(),
    Height = blockchain_worker:height() + 1,
    Block0 = blockchain_block:new(PrevHash, Height, [], <<>>, #{}),
    BinBlock = erlang:term_to_binary(blockchain_block:remove_signature(Block0)),
    Signatures = signatures(ConsensusMembers, BinBlock),
    Block1 = blockchain_block:sign_block(erlang:term_to_binary(Signatures), Block0),
    create_blocks(N-1, ConsensusMembers, [Block1|Blocks]);
create_blocks(N, ConsensusMembers, [LastBlock|_]=Blocks) ->
    PrevHash = blockchain_block:hash_block(LastBlock),
    Height = blockchain_block:height(LastBlock) + 1,
    Block0 = blockchain_block:new(PrevHash, Height, [], <<>>, #{}),
    BinBlock = erlang:term_to_binary(blockchain_block:remove_signature(Block0)),
    Signatures = signatures(ConsensusMembers, BinBlock),
    Block1 = blockchain_block:sign_block(erlang:term_to_binary(Signatures), Block0),
    create_blocks(N-1, ConsensusMembers, [Block1|Blocks]).

signatures(ConsensusMembers, BinBlock) ->
    lists:foldl(
        fun({A, {_, _, F}}, Acc) ->
            Sig = F(BinBlock),
            [{A, Sig}|Acc]
        end
        ,[]
        ,ConsensusMembers
    ).
