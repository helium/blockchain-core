-module(plausible_block_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain.hrl").

-export([
    all/0, init_per_testcase/2, end_per_testcase/2
]).

-export([
    basic/1,
    definitely_invalid/1,
    ultimately_invalid/1
        ]).

all() ->
    [basic, definitely_invalid, ultimately_invalid].

init_per_testcase(TestCase, Config) ->
    catch gen_server:stop(blockchain_sup),
    blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config).



end_per_testcase(_, _Config) ->
    catch gen_server:stop(blockchain_sup),
    ok.

basic(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined),

    plausible = blockchain:can_add_block(LastBlock, Chain),
    %% check we return the plausible message to the caller
    plausible = blockchain:add_block(LastBlock, Chain),
    %% don't return the plausible message more than once
    ok = blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 1}, blockchain:sync_height(Chain)),
    [LastBlock] = blockchain:get_plausible_blocks(Chain),
    %% add a block and check the plausible block remains
    ok = blockchain:add_block(hd(Blocks), Chain),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, 2}, blockchain:sync_height(Chain)),
    [LastBlock] = blockchain:get_plausible_blocks(Chain),
    %% add all the rest of the blocks (emulate a sync)
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    %% check the plausible block is now the head of the chain
    ?assertEqual({ok, 101}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(LastBlock)}),
    %% make sure the plausible blocks got removed
    [] = blockchain:get_plausible_blocks(Chain),
    %% make a new block
    {ok, FinalBlock} = test_utils:create_block(ConsensusMembers, []),
    blockchain:add_block(FinalBlock, Chain0),
    ok = blockchain:add_block(FinalBlock, Chain),
    ?assertEqual({ok, 102}, blockchain:height(Chain)),
    ?assertEqual({ok, 102}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(FinalBlock)}),
    [] = blockchain:get_plausible_blocks(Chain),
    %% try adding the previously plausible block again, it should not work
    ok = blockchain:add_block(LastBlock, Chain),
    [] = blockchain:get_plausible_blocks(Chain),
    ok.

definitely_invalid(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    %% tear down the chain
    gen_server:stop(blockchain_sup),
    os:cmd("rm -rf" ++ proplists:get_value(base_dir, _Opts)),

    %% boot an entirely disjoint chain
    {ok, _Sup1, {PrivKey1, PubKey1}, _Opts1} = test_utils:init(BaseDir++"extra"),
    {ok, _GenesisMembers1, _ConsensusMembers1, _} = test_utils:init_chain(Balance, {PrivKey1, PubKey1}),
    Chain1 = blockchain_worker:blockchain(),

    % Add some blocks
    Blocks1 = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain1),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks1),

    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined),

    {error, disjoint_chain} = blockchain:can_add_block(LastBlock, Chain),
    {error, disjoint_chain} = blockchain:add_block(hd(Blocks1), Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 1}, blockchain:sync_height(Chain)),
    [] = blockchain:get_plausible_blocks(Chain),
    %% add a block and check the plausible block remains
    ok = blockchain:add_block(hd(Blocks), Chain),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, 2}, blockchain:sync_height(Chain)),
    [] = blockchain:get_plausible_blocks(Chain),
    %% add all the rest of the blocks (emulate a sync)
    ok = blockchain:add_blocks(Blocks, Chain),
    %% check the plausible block is not the head of the chain
    ?assertEqual({ok, 101}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(lists:last(Blocks))}),
    %% make sure the plausible block is not in the chain at all
    {error, not_found} = blockchain:get_block(blockchain_block:hash_block(LastBlock), Chain),
    %% make sure the plausible blocks got removed
    [] = blockchain:get_plausible_blocks(Chain),
    ok.

ultimately_invalid(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    %% tear down the chain
    gen_server:stop(blockchain_sup),
    os:cmd("rm -rf" ++ proplists:get_value(base_dir, _Opts)),

    %% boot an entirely disjoint chain
    {ok, _Sup1, {PrivKey, PubKey}, _Opts1} = test_utils:init(BaseDir++"extra", {PrivKey, PubKey}),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, GenesisMembers, #{}),
    Chain1 = blockchain_worker:blockchain(),

    % Add some blocks
    Blocks1 = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain1),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks1),

    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined),

    plausible = blockchain:can_add_block(LastBlock, Chain),
    %% check we return the plausible message to the caller
    plausible = blockchain:add_block(LastBlock, Chain),
    %% don't return the plausible message more than once
    ok = blockchain:add_block(LastBlock, Chain),
    %% try to add a block that should be invalid right away
    {error, disjoint_chain} = blockchain:add_block(hd(Blocks1), Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 1}, blockchain:sync_height(Chain)),
    [LastBlock] = blockchain:get_plausible_blocks(Chain),
    %% add a block and check the plausible block remains
    ok = blockchain:add_block(hd(Blocks), Chain),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, 2}, blockchain:sync_height(Chain)),
    [LastBlock] = blockchain:get_plausible_blocks(Chain),
    %% add all the rest of the blocks (emulate a sync)
    ok = blockchain:add_blocks(Blocks, Chain),
    %% check the plausible block is not the head of the chain
    ?assertEqual({ok, 101}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(lists:last(Blocks))}),
    %% make sure the plausible block is not in the chain at all
    {error, not_found} = blockchain:get_block(blockchain_block:hash_block(LastBlock), Chain),
    %% make sure the plausible blocks got removed
    [] = blockchain:get_plausible_blocks(Chain),
    ok.

