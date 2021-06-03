-module(plausible_block_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0, init_per_testcase/2, end_per_testcase/2
]).

-export([
    basic/1,
    definitely_invalid/1,
    ultimately_invalid/1,
    valid/1
]).

all() ->
    [basic, definitely_invalid, ultimately_invalid, valid].

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
    BlocksN = 80,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
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

    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined, undefined),

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
    ?assertEqual({ok, 81}, blockchain:height(Chain)),
    ?assertEqual({ok, 81}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(LastBlock)}),
    %% make sure the plausible blocks got removed
    [] = blockchain:get_plausible_blocks(Chain),
    %% make a new block
    {ok, FinalBlock} = test_utils:create_block(ConsensusMembers, []),
    blockchain:add_block(FinalBlock, Chain0),
    ok = blockchain:add_block(FinalBlock, Chain),
    ?assertEqual({ok, 82}, blockchain:height(Chain)),
    ?assertEqual({ok, 82}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(FinalBlock)}),
    [] = blockchain:get_plausible_blocks(Chain),
    %% try adding the previously plausible block again, it should not work
    exists = blockchain:add_block(LastBlock, Chain),
    [] = blockchain:get_plausible_blocks(Chain),
    ok.

definitely_invalid(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 80,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
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
    {ok, _GenesisMembers1, _GenesisBlock2, _ConsensusMembers1, _} = test_utils:init_chain(Balance, {PrivKey1, PubKey1}),
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

    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined, undefined),

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
    ?assertEqual({ok, 81}, blockchain:height(Chain)),
    ?assertEqual({ok, 81}, blockchain:sync_height(Chain)),
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
    BlocksN = 80,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
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
    {ok, _GenesisMembers, _GenesisBlock2, ConsensusMembers, _} = test_utils:init_chain(Balance, GenesisMembers, #{}),
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

    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined, undefined),

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
    ?assertEqual({ok, 81}, blockchain:height(Chain)),
    ?assertEqual({ok, 81}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(lists:last(Blocks))}),
    %% make sure the plausible block is not in the chain at all
    {error, not_found} = blockchain:get_block(blockchain_block:hash_block(LastBlock), Chain),
    %% make sure the plausible blocks got removed
    [] = blockchain:get_plausible_blocks(Chain),
    ok.

valid(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 80,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
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

    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined, undefined),

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
    ?assertEqual({ok, 81}, blockchain:height(Chain)),
    ?assertEqual({ok, 81}, blockchain:sync_height(Chain)),
    ?assertEqual(blockchain:head_hash(Chain), {ok, blockchain_block:hash_block(LastBlock)}),
    %% make sure the plausible blocks got removed
    [] = blockchain:get_plausible_blocks(Chain),

    %% NOTE:
    %% N = 7, F = 2 (testing)
    %% Even if we replace upto 3 ( F + 1) members, it should be
    %% considered a plausible block
    true = check_plausibility_after_replacing_k_members(1, ConsensusMembers, Chain),        %% keep 6
    true = check_plausibility_after_replacing_k_members(2, ConsensusMembers, Chain),        %% keep 5
    true = check_plausibility_after_replacing_k_members(3, ConsensusMembers, Chain),        %% keep 4
    true = check_plausibility_after_replacing_k_members(4, ConsensusMembers, Chain),        %% keep 3
    false = check_plausibility_after_replacing_k_members(5, ConsensusMembers, Chain),       %% keep 2
    false = check_plausibility_after_replacing_k_members(6, ConsensusMembers, Chain),       %% keep 1
    false = check_plausibility_after_replacing_k_members(7, ConsensusMembers, Chain),       %% keep 0

    ok.

replace_members(ConsensusMembers, ToReplace) ->
    N = length(ConsensusMembers),
    NewKeys = test_utils:generate_keys(ToReplace),
    lists:sublist(ConsensusMembers, (N - ToReplace)) ++ NewKeys.

check_plausibility_after_replacing_k_members(K, ConsensusMembers, Chain) ->
    N = length(ConsensusMembers),
    ct:pal("N: ~p", [N]),
    ct:pal("K: ~p", [K]),
    ?assertEqual(7, N),

    NewMembers = replace_members(ConsensusMembers, K),
    ?assertEqual(N, length(NewMembers)),

    CM = [libp2p_crypto:bin_to_b58(I) || {I, _} <- ConsensusMembers],
    CM2 = [libp2p_crypto:bin_to_b58(I) || {I, _} <- NewMembers],
    ct:pal("ConsensusMembers: ~p", [CM]),
    ct:pal("NewMembers: ~p", [CM2]),

    %% check that ConsensusMembers and NewMembers have (N - K) elements in common
    true = sets:size(sets:intersection(sets:from_list(ConsensusMembers), sets:from_list(NewMembers))) == (N - K),

    {ok, BlockByNewMembers} = test_utils:create_block(NewMembers, []),
    ct:pal("AnotherBlock: ~p", [BlockByNewMembers]),

    IsPlausible = blockchain:is_block_plausible(BlockByNewMembers, Chain),
    ct:pal("IsPlausible: ~p", [IsPlausible]),
    IsPlausible.
