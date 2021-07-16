-module(blockchain_snapshot_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1,
    new_test/1,
    mem_limit_test/1
]).

-import(blockchain_utils, [normalize_float/1]).

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
    [
        basic_test,
        new_test,
        mem_limit_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(mem_limit_test, Config) ->
    {ok, _} = application:ensure_all_started(lager),

    Dir = ?config(priv_dir, Config),
    PrivDir = filename:join([Dir, "priv"]),
    NewDir = PrivDir ++ "/ledger/",
    ok = filelib:ensure_dir(NewDir),

    SnapFileName = "snap-913684",
    SnapFilePath = Dir ++ "/" ++ SnapFileName,
    os:cmd("cd " ++ Dir ++ " && wget -c https://snapshots.helium.wtf/mainnet/" ++ SnapFileName),

    [{filename, SnapFilePath} | Config];
init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain(Balance, {PrivKey, PubKey}, true, #{}),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    _ = lists:foreach(fun(Entry) ->
                              Balance = blockchain_ledger_entry_v1:balance(Entry),
                              0 = blockchain_ledger_entry_v1:nonce(Entry)
                      end, maps:values(Entries)),

    [
     {balance, Balance},
     {sup, Sup},
     {pubkey, PubKey},
     {privkey, PrivKey},
     {opts, Opts},
     {chain, Chain},
     {swarm, Swarm},
     {n, N},
     {consensus_members, ConsensusMembers},
     {genesis_members, GenesisMembers},
     Keys
     | Config0
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------
basic_test(Config) ->
    LedgerA = ledger(Config),
    case blockchain_ledger_v1:get_h3dex(LedgerA) of
        #{} ->
            LedgerBoot = blockchain_ledger_v1:new_context(LedgerA),
            blockchain:bootstrap_h3dex(LedgerBoot),
            blockchain_ledger_v1:commit_context(LedgerBoot);
        _ -> ok
    end,
    {ok, SnapshotA} = blockchain_ledger_snapshot_v1:snapshot(LedgerA, []),
    %% make a dir for the loaded snapshot
    Dir = ?config(priv_dir, Config),
    PrivDir = filename:join([Dir, "priv"]),
    NewDir = PrivDir ++ "/ledger2/",
    ok = filelib:ensure_dir(NewDir),

    ?assertMatch(
        [_|_],
        blockchain_ledger_snapshot_v1:deserialize_field(upgrades, maps:get(upgrades, SnapshotA, undefined)),
        "New snapshot (A) has \"upgrades\" field."
    ),
    SnapshotAIOList = blockchain_ledger_snapshot_v1:serialize(SnapshotA),
    SnapshotABin = iolist_to_binary(SnapshotAIOList),
    ct:pal("dir: ~p", [os:cmd("pwd")]),
    {ok, BinGen} = file:read_file("../../../../test/genesis"),
    GenesisBlock = blockchain_block:deserialize(BinGen),
    {ok, Chain} = blockchain:new(NewDir, GenesisBlock, blessed_snapshot, undefined),
    {ok, SnapshotB} = blockchain_ledger_snapshot_v1:deserialize(SnapshotABin),
    ?assertMatch(
        [_|_],
        blockchain_ledger_snapshot_v1:deserialize_field(upgrades, maps:get(upgrades, SnapshotB, undefined)),
        "Deserialized snapshot (B) has \"upgrades\" field."
    ),
    ?assertEqual(
        snap_hash_without_field(upgrades, SnapshotA),
        snap_hash_without_field(upgrades, SnapshotB),
        "Hashes A and B are equal without \"upgrades\" field."
    ),
    LedgerB =
        blockchain_ledger_snapshot_v1:import(
            Chain,
            snap_hash_without_field(upgrades, SnapshotA),
            SnapshotB
        ),
    {ok, SnapshotC} = blockchain_ledger_snapshot_v1:snapshot(LedgerB, []),
    ?assertMatch(
        [_|_],
        blockchain_ledger_snapshot_v1:deserialize_field(upgrades, maps:get(upgrades, SnapshotC, undefined)),
        "New snapshot (C) has \"upgrades\" field."
    ),
    ?assertEqual(
        snap_hash_without_field(upgrades, SnapshotB),
        snap_hash_without_field(upgrades, SnapshotC),
        "Hashes B and C are equal without \"upgrades\" field."
    ),

    DiffAB = blockchain_ledger_snapshot_v1:diff(SnapshotA, SnapshotB),
    ct:pal("DiffAB: ~p", [DiffAB]),
    ?assertEqual([], DiffAB),
    ?assertEqual(SnapshotA, SnapshotB),
    DiffBC = blockchain_ledger_snapshot_v1:diff(SnapshotB, SnapshotC),
    ct:pal("DiffBC: ~p", [DiffBC]),

    %% C has new elements in upgrades, otherwise B and C should be the same:
    ?assertEqual(
        maps:remove(upgrades, SnapshotB),
        maps:remove(upgrades, SnapshotC)
    ),

    ok = blockchain:add_snapshot(SnapshotC, Chain),
    HashC = blockchain_ledger_snapshot_v1:hash(SnapshotC),
    {ok, SnapshotDBin} = blockchain:get_snapshot(HashC, Chain),
    {ok, SnapshotD} = blockchain_ledger_snapshot_v1:deserialize(HashC, SnapshotDBin),
    ?assertEqual(SnapshotC, SnapshotD),
    HashD = blockchain_ledger_snapshot_v1:hash(SnapshotD),
    ?assertEqual(HashC, HashD),
    ok.

new_test(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% add 20 blocks
    ok = add_k_blocks(Config, 20),

    Ht = 21,

    {ok, Ht} = blockchain:height(Chain),
    {ok, Ht} = blockchain_ledger_v1:current_height(Ledger),

    {ok, Snap} = blockchain_ledger_v1:new_snapshot(Ledger),
    ct:pal("Snap: ~p", [Snap]),

    %% check that check points sub dirs is not empty
    %% call clean_checkpoints, this will invoke remove_checkpoints
    %% check that subdirs are empty after cleanup

    CheckpointDir = blockchain_ledger_v1:checkpoint_dir(Ledger, Ht),
    ct:pal("CheckpointDir: ~p", [CheckpointDir]),

    RecordDir = blockchain_ledger_v1:dir(Ledger),
    CPBase = blockchain_ledger_v1:checkpoint_base(RecordDir),

    CPs = filename:join([CPBase, "checkpoints"]),
    {ok, Subdirs} = file:list_dir(CPs),

    ct:pal("Subdirs: ~p", [Subdirs]),
    ?assertEqual(20, length(Subdirs)),

    ok = blockchain_ledger_v1:clean_checkpoints(Ledger),

    CPs1 = filename:join([CPBase, "checkpoints"]),
    {ok, Subdirs1} = file:list_dir(CPs1),

    ct:pal("Subdirs1: ~p", [Subdirs1]),
    ?assertEqual(0, length(Subdirs1)),

    ok.

mem_limit_test(Config) ->
    Filename = ?config(filename, Config),

    {ok, BinSnap} = file:read_file(Filename),

    {Pid, Ref} =
        spawn_monitor(
          fun() ->
                  %% on master, this takes MB = 160 to pass on my laptop reliably, but this version
                  %% seems fine with 10?  I'm not sure how this works.
                  MB = 10,
                  erlang:process_flag(max_heap_size, #{kill => true, size => (MB * 1024 * 1024) div 8}),
                  {ok, _Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap)
          end),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok;
        {'DOWN', Ref, process, Pid, Info} -> error(Info)
    end,

    ok.


%% utils
-spec snap_hash_without_field(atom(), map()) -> map().
snap_hash_without_field(Field, Snap) ->
    blockchain_ledger_snapshot_v1:hash(maps:remove(Field, Snap)).

ledger(Config) ->
    Dir = ?config(priv_dir, Config),
    PrivDir = filename:join([Dir, "priv"]),
    NewDir = PrivDir ++ "/ledger/",
    ok = filelib:ensure_dir(NewDir),

    SnapFileName = "snap-913684",
    SnapFilePath = filename:join(Dir, SnapFileName),
    SnapURI = "https://snapshots.helium.wtf/mainnet/" ++ SnapFileName,
    os:cmd("cd " ++ Dir ++ "  && wget -c " ++ SnapURI),

    {ok, BinSnap} = file:read_file(SnapFilePath),

    {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
    SHA = blockchain_ledger_snapshot_v1:hash(Snapshot),

    {ok, BinGen} = file:read_file("../../../../test/genesis"),
    GenesisBlock = blockchain_block:deserialize(BinGen),
    {ok, Chain} = blockchain:new(NewDir, GenesisBlock, blessed_snapshot, undefined),

    Ledger1 = blockchain_ledger_snapshot_v1:import(Chain, SHA, Snapshot),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger1),

    ct:pal("loaded ledger at height ~p", [Height]),
    Ledger1.


add_k_blocks(Config, K) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    lists:reverse(
      lists:foldl(
        fun(_, Acc) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:swarm()),
                [Block | Acc]
        end,
        [],
        lists:seq(1, K)
       )),
    ok.
