-module(assume_valid_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain.hrl").

-export([
    all/0, init_per_testcase/2, end_per_testcase/2
]).

-export([
    basic/1,
    wrong_height/1,
    blockchain_restart/1,
    blockchain_almost_synced/1,
    blockchain_crash_while_absorbing/1,
    blockchain_crash_while_absorbing_and_assume_valid_moves/1,
    blockchain_crash_while_absorbing_then_resync/1,
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
    [basic, wrong_height, blockchain_restart, blockchain_almost_synced, blockchain_crash_while_absorbing, blockchain_crash_while_absorbing_and_assume_valid_moves, overlapping_streams, blockchain_crash_while_absorbing_then_resync].


%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config).

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, Config) ->
    catch gen_server:stop(blockchain_sup),
    application:unset_env(blockchain, assumed_valid_block_hash),
    application:unset_env(blockchain, assumed_valid_block_height),
    application:unset_env(blockchain, honor_quick_sync),
    persistent_term:erase(blockchain_core_assumed_valid_block_hash_and_height),
    test_utils:cleanup_tmp_dir(?config(base_dir, Config)),
    test_utils:cleanup_tmp_dir(?config(sim_dir, Config)),
    ok.


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

basic(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    ct:pal("ff suite base dir: ~p", [BaseDir]),
    ct:pal("ff suite base SIM dir: ~p", [SimDir]),

    Balance = 5000,
    BlocksN = 100,
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

    erlang:garbage_collect(),
    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    %% this should fail without all the supporting blocks
    blockchain:add_block(LastBlock, Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    ok = blockchain:add_block(LastBlock, Chain),
    ok = wait_until_absorbed(),
    ?assertEqual({ok, 101}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    ok.

wrong_height(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
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

    %% sanity check the old chain
    ?assertEqual({ok, BlocksN + 1}, blockchain:height(Chain0)),

    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock) + 1}),

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


blockchain_restart(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
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

    %% sanity check the old chain
    ?assertEqual({ok, BlocksN + 1}, blockchain:height(Chain0)),

    erlang:garbage_collect(),
    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    erlang:garbage_collect(),
    {ok, Chain1} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    ?assertEqual({ok, 1}, blockchain:height(Chain1)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain1)),
    ok = blockchain:add_block(LastBlock, Chain1),
    ok = wait_until_absorbed(),
    ?assertEqual({ok, 101}, blockchain:height(Chain1)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain1)),
    ok.

blockchain_almost_synced(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
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

    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid, undefined),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ok = blockchain:add_blocks(Blocks -- [LastBlock], Chain),
    ?assertEqual({ok, 100}, blockchain:height(Chain)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain)),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    %% re-open with the assumed-valid hash supplied, like if we got an OTA
    {ok, Chain1} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    ?assertEqual({ok, 100}, blockchain:height(Chain1)),
    ?assertEqual({ok, 100}, blockchain:sync_height(Chain1)),
    ok = blockchain:add_block(LastBlock, Chain1),
    ok = wait_until_absorbed(),
    ?assertEqual({ok, 101}, blockchain:height(Chain1)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain1)),
    ok.

blockchain_crash_while_absorbing(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
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
    ExplodeBlock = lists:nth(50, Blocks),

    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

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
    ok = wait_until_absorbed(),
    ?assertEqual({ok, 50}, blockchain:height(Chain)),
    meck:unload(blockchain_txn),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    %% re-open with the assumed-valid hash supplied, like if we got an OTA
    {ok, Chain1} = blockchain:new(SimDir, Genesis, assumed_valid, undefined), %blockchain_block:hash_block(LastBlock)),
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
    {ok, Chain2} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    %% the sync is done in a background process, so wait for it to complete
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 101} =:= blockchain:sync_height(Chain2) end),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 101} =:= blockchain:height(Chain2) end),
    ?assertEqual(blockchain:sync_hash(Chain2), {ok, blockchain_block:hash_block(LastBlock)}),
    ?assertEqual(blockchain:head_hash(Chain2), {ok, blockchain_block:hash_block(LastBlock)}),
    ok.


blockchain_crash_while_absorbing_and_assume_valid_moves(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
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
    ExplodeBlock = lists:nth(50, Blocks),

    {ok, FinalLastBlock} = test_utils:create_block(ConsensusMembers, []),
    blockchain:add_block(FinalLastBlock, Chain0),

    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

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
    ok = wait_until_absorbed(),
    ?assertEqual({ok, 50}, blockchain:height(Chain)),
    meck:unload(blockchain_txn),
    %% simulate the node stopping or crashing
    blockchain:close(Chain),
    %% re-open with a newer assumed-valid hash supplied, like if we got an OTA
    {ok, Chain1} = blockchain:new(SimDir, Genesis, assumed_valid,
                                  {blockchain_block:hash_block(FinalLastBlock), blockchain_block:height(FinalLastBlock)}),
    blockchain_worker:blockchain(Chain1),

    %% the sync height should be 101 because we don't have the new assume valid hash
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain1)),
    %% the actual height should be right before the explode block
    ?assertEqual({ok, 50}, blockchain:height(Chain1)),
    %% check the hashes
    ?assertEqual(blockchain:head_hash(Chain1), {ok, blockchain_block:prev_hash(ExplodeBlock)}),
    ?assertEqual(blockchain:sync_hash(Chain1), {ok, blockchain_block:hash_block(LastBlock)}),
    %% add the new final block
    ok = blockchain:add_block(FinalLastBlock, Chain1),
    ok = wait_until_absorbed(),
    ?assertEqual(blockchain:sync_hash(Chain1), {ok, blockchain_block:hash_block(FinalLastBlock)}),
    ?assertEqual(blockchain:head_hash(Chain1), {ok, blockchain_block:hash_block(FinalLastBlock)}),
    ?assertEqual({ok, 102}, blockchain:height(Chain1)),
    ?assertEqual({ok, 102}, blockchain:sync_height(Chain1)),
    ok.


blockchain_crash_while_absorbing_then_resync(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    sys:suspend(blockchain_txn_mgr),
    sys:suspend(blockchain_score_cache),
    {ok, _GenesisMembers, _, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
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
    ExplodeBlock = lists:nth(50, Blocks),

    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid,
                                 {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),

    meck:new(blockchain_txn, [passthrough]),
    MeckFun = fun(Height) ->
                      Block = lists:nth(Height, Blocks),
                      fun(B, C, BC, R) ->
                              case B == Block of
                                  true ->
                                      blockchain_lock:release(),
                                      error(explode);
                                  false ->
                                      meck:passthrough([B, C, BC, R])
                              end
                      end
              end,
    meck:expect(blockchain_txn, unvalidated_absorb_and_commit, MeckFun(50)),
    meck:expect(blockchain_txn, absorb_and_commit, MeckFun(50)),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    blockchain:add_blocks(Blocks, Chain),
    ok = wait_until_absorbed(),
    ?assertEqual({ok, 50}, blockchain:height(Chain)),
    S = sys:get_state(blockchain_worker),
    sys:suspend(blockchain_worker),
    ct:pal("S ~p", [S]),

    %% reset the ledger
    ok = blockchain_ledger_v1:clean(blockchain:ledger(Chain)),

    %% simulate the node stopping or crashing
    ?assertEqual({ok, 50}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    %% clear the persistent_term
    persistent_term:erase(blockchain_core_assumed_valid_block_hash_and_height),
    ?assertEqual({ok, 50}, blockchain:sync_height(Chain)),

    catch blockchain:close(Chain),
    erlang:garbage_collect(),
    meck:unload(blockchain_txn),
    catch gen_server:stop(blockchain_sup),
    ok = blockchain_ct_utils:wait_until(fun() -> erlang:is_process_alive(Sup) == false end),
    timer:sleep(1000),

    {ok, _Sup2, {PrivKey, PubKey}, _Opts2} = test_utils:init(SimDir, {PrivKey, PubKey}),

    Chain1 = blockchain_worker:blockchain(),
    %% the actual height should be right before the explode block
    ?assertEqual({ok, 50}, blockchain:height(Chain1)),
    %% the sync height should be 50 because we don't have the assume valid hash
    ?assertEqual({ok, 50}, blockchain:sync_height(Chain1)),
    %% check the hashes
    ?assertEqual(blockchain:head_hash(Chain1), {ok, blockchain_block:prev_hash(ExplodeBlock)}),
    ?assertEqual(blockchain:sync_hash(Chain1), {ok, blockchain_block:prev_hash(ExplodeBlock)}),
    blockchain:close(Chain1),
    %% reopen the blockchain and provide the assume-valid hash
    %% since we already have all the blocks, it should immediately sync
    application:set_env(blockchain, assumed_valid_block_hash, blockchain_block:hash_block(LastBlock)),
    application:set_env(blockchain, assumed_valid_block_height, blockchain_block:height(LastBlock)),
    application:set_env(blockchain, honor_quick_sync, true),
    erlang:garbage_collect(),
    {ok, Chain2} = blockchain:new(SimDir, Genesis, assumed_valid,
                                  {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
    blockchain_worker:blockchain(Chain2),
    %% the sync is done in a background process, so wait for it to complete
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 101} =:= blockchain:sync_height(Chain2) end),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 101} =:= blockchain:height(Chain2) end),
    ?assertEqual(blockchain:sync_hash(Chain2), {ok, blockchain_block:hash_block(LastBlock)}),
    ?assertEqual(blockchain:head_hash(Chain2), {ok, blockchain_block:hash_block(LastBlock)}),
    ok.


overlapping_streams(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),

    Balance = 5000,
    BlocksN = 100,
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

    {ok, Chain1} = blockchain:new(SimDir, Genesis, undefined, undefined),%, blockchain_block:hash_block(LastBlock)),

    ok = blockchain:add_blocks(lists:sublist(Blocks, 15), Chain1),

    ?assertEqual({ok, 16}, blockchain:height(Chain1)),
    blockchain:close(Chain1),

    erlang:garbage_collect(),
    {ok, Chain} = blockchain:new(SimDir, Genesis, assumed_valid, {blockchain_block:hash_block(LastBlock), blockchain_block:height(LastBlock)}),
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
    ok = wait_until_absorbed(),
    ?assertEqual({ok, 101}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:sync_height(Chain)),
    ok.

wait_until_absorbed() ->
    blockchain_ct_utils:wait_until(fun() -> false == blockchain_worker:is_absorbing() end).
