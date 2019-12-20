-module(assume_valid_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain.hrl").

-export([
    all/0
]).

-export([
    basic/1,
    wrong_height/1,
    blockchain_restart/1,
    blockchain_almost_synced/1,
    blockchain_crash_while_absorbing/1,
    blockchain_crash_while_absorbing_and_assume_valid_moves/1,
    overlapping_streams/1
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
    [basic, wrong_height, blockchain_restart, blockchain_almost_synced, blockchain_crash_while_absorbing, blockchain_crash_while_absorbing_and_assume_valid_moves, overlapping_streams].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

basic(_Config) ->
    BaseDir = "data/assume_valid_SUITE/basic",
    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

    SimDir = "data/assume_valid_SUITE/basic_sim",
    {ok, Chain} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    %% this should fail without all the supporting blocks
    blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    ok = blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 101}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    ok.

wrong_height(_Config) ->
    BaseDir = "data/assume_valid_SUITE/wrong_height",
    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

    SimDir = "data/assume_valid_SUITE/wrong_height_sim",
    {ok, Chain} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock) + 1}),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    %% this should fail without all the supporting blocks
    blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    {error, _} = blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    ok.


blockchain_restart(_Config) ->
    BaseDir = "data/assume_valid_SUITE/blockchain_restart",
    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

    SimDir = "data/assume_valid_SUITE/blockchain_restart_sim",
    {ok, Chain} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    {ok, Chain1} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    ?assertEqual({ok, 1}, blockchain:height(Chain1)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain1)),
    ok = blockchain:add_block(LastBlock, Chain1),
    ?assertEqual({ok, 101}, blockchain:height(Chain1)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain1)),
    ok.

blockchain_almost_synced(_Config) ->
    BaseDir = "data/assume_valid_SUITE/blockchain_almost_synced",
    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

    SimDir = "data/assume_valid_SUITE/blockchain_almost_synced_sim",
    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    ?assertEqual({ok, 100}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    %% re-open with the assumed-valid hash supplied, like if we got an OTA
    {ok, Chain1} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    ?assertEqual({ok, 100}, blockchain:height(Chain1)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain1)),
    ok = blockchain:add_block(LastBlock, Chain1),
    ?assertEqual({ok, 101}, blockchain:height(Chain1)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain1)),
    ok.

blockchain_crash_while_absorbing(_Config) ->
    BaseDir = "data/assume_valid_SUITE/blockchain_crash_while_absorbing",
    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),
    ExplodeBlock = lists:nth(50, Blocks),

    SimDir = "data/assume_valid_SUITE/blockchain_crash_while_absorbing_sim",
    {ok, Chain} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

    meck:new(blockchain_txn, [passthrough]),
    meck:expect(blockchain_txn, unvalidated_absorb_and_commit,
                fun(B, C, BC, R) ->
                        case B == ExplodeBlock of
                            true ->
                                blockchain_lock:release(),
                                error(explode);
                            false ->
                                meck:passthrough([B, C, BC, R])
                        end
                end),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    blockchain:add_blocks(Blocks, Chain),
    ?assertEqual({ok, 50}, blockchain:height(Chain)),
    meck:unload(blockchain_txn),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    %% re-open with the assumed-valid hash supplied, like if we got an OTA
    {ok, Chain1} = blockchain:new(SimDir, Genesis, undefined), %blockchain_block:hash_block(LastBlock)),
    %% the sync height should be 50 because we don't have the assume valid hash
    ?assertEqual({ok, 50}, blockchain:sync_height(Chain1)),
    %% the actual height should be right before the explode block
    ?assertEqual({ok, 50}, blockchain:height(Chain1)),
    %% check the hashes
    ?assertEqual(blockchain:head_hash(Chain1), {ok, blockchain_block:prev_hash(ExplodeBlock)}),
    ?assertEqual(blockchain:sync_hash(Chain1), {ok, blockchain_block:prev_hash(ExplodeBlock)}),
    blockchain:close(Chain1),
    %% reopen the blockchain and provide the assume-valid hash
    %% since we already have all the blocks, it should immediately sync
    {ok, Chain2} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    %% the sync is done in a background process, so wait for it to complete
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 101} =:= blockchain:sync_height(Chain2) end),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 101} =:= blockchain:height(Chain2) end),
    ?assertEqual(blockchain:sync_hash(Chain2), {ok, blockchain_block:hash_block(LastBlock)}),
    ?assertEqual(blockchain:head_hash(Chain2), {ok, blockchain_block:hash_block(LastBlock)}),
    ok.


blockchain_crash_while_absorbing_and_assume_valid_moves(_Config) ->
    BaseDir = "data/assume_valid_SUITE/blockchain_crash_while_absorbing_and_assume_valid_moves",
    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),
    ExplodeBlock = lists:nth(50, Blocks),

    FinalLastBlock = test_utils:create_block(ConsensusMembers, []),
    blockchain:add_block(FinalLastBlock, Chain0),

    SimDir = "data/assume_valid_SUITE/blockchain_crash_while_absorbing_and_assume_valid_moves_sim",
    {ok, Chain} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

    meck:new(blockchain_txn, [passthrough]),
    meck:expect(blockchain_txn, unvalidated_absorb_and_commit,
                fun(B, C, BC, R) ->
                        case B == ExplodeBlock of
                            true ->
                                blockchain_lock:release(),
                                error(explode);
                            false ->
                                meck:passthrough([B, C, BC, R])
                        end
                end),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    blockchain:add_blocks(Blocks, Chain),
    ?assertEqual({ok, 50}, blockchain:height(Chain)),
    meck:unload(blockchain_txn),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    %% re-open with a newer assumed-valid hash supplied, like if we got an OTA
    {ok, Chain1} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(FinalLastBlock), blockchain_block:height(FinalLastBlock)}),
    %% the sync height should be 101 because we don't have the new assume valid hash
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain1)),
    %% the actual height should be right before the explode block
    ?assertEqual({ok, 50}, blockchain:height(Chain1)),
    %% check the hashes
    ?assertEqual(blockchain:head_hash(Chain1), {ok, blockchain_block:prev_hash(ExplodeBlock)}),
    ?assertEqual(blockchain:sync_hash(Chain1), {ok, blockchain_block:hash_block(LastBlock)}),
    %% add the new final block
    ok = blockchain:add_block(FinalLastBlock, Chain1),
    ?assertEqual(blockchain:sync_hash(Chain1), {ok, blockchain_block:hash_block(FinalLastBlock)}),
    ?assertEqual(blockchain:head_hash(Chain1), {ok, blockchain_block:hash_block(FinalLastBlock)}),
    ?assertEqual({ok, 102}, blockchain:height(Chain1)),
    ?assertEqual({ok, 102}, blockchain:sync_height(Chain1)),
    ok.

overlapping_streams(_Config) ->
    BaseDir = "data/assume_valid_SUITE/overlapping_streams",
    Balance = 5000,
    BlocksN = 100,
    {ok, _Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            Block = test_utils:create_block(ConsensusMembers, []),
            blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

    SimDir = "data/assume_valid_SUITE/overlapping_streams_sim",
    {ok, Chain1} = blockchain:new(SimDir, Genesis, undefined),%, blockchain_block:hash_block(LastBlock)),

    ok = blockchain:add_blocks(lists:sublist(Blocks, 15), Chain1),

    ?assertEqual({ok, 16}, blockchain:height(Chain1)),
    blockchain:close(Chain1),

    {ok, Chain} = blockchain:new(SimDir, Genesis, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    %% this should fail without all the supporting blocks
    blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 16}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(lists:sublist(Blocks, 16, length(Blocks)) -- [LastBlock], Chain),
    ?assertEqual({ok, 16}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),


    %% re-add some old blocks again
    ok = blockchain:add_blocks(lists:sublist(Blocks, 20), Chain),

    %% assert we only have one sync head
    ?assertEqual(1, length(lists:usort([ blockchain:sync_hash(Chain) || _ <- lists:seq(1, 100)]))),

    ok = blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 101}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    ok.
