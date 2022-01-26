-module(blockchain_snapshot_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([
    all/0
]).

-export([
    basic_test/1,
    new_test/1,
    mem_limit_test/1
]).

-import(blockchain_utils, [normalize_float/1]).

all() ->
    [
        basic_test,
        new_test,
        mem_limit_test
    ].

%% ----------------------------------------------------------------------------
%% Test cases
%% ----------------------------------------------------------------------------

basic_test(Cfg0) ->
    % XXX Snap equality check eats 90+% of my 32GB of RAM on failures. Use diff instead. -- @xandkar

    %% FIXME 1160641 gets OOM-killed on blockchain_ledger_snapshot_v1:snapshot
    %SnapHeight = 1160641,
    SnapHeight = 913684,
    Cfg = chain_start_from_snap(SnapHeight, Cfg0),
    Chain = ?config(chain, Cfg),
    LedgerA = blockchain:ledger(Chain),
    case blockchain_ledger_v1:get_h3dex(LedgerA) of
        #{} ->
            LedgerBoot = blockchain_ledger_v1:new_context(LedgerA),
            blockchain:bootstrap_h3dex(LedgerBoot),
            blockchain_ledger_v1:commit_context(LedgerBoot);
        _ -> ok
    end,
    {ok, SnapshotA} = blockchain_ledger_snapshot_v1:snapshot(LedgerA, [], []),

    ?assertMatch(
        [_|_],
        blockchain_ledger_snapshot_v1:deserialize_field(upgrades, maps:get(upgrades, SnapshotA, undefined)),
        "New snapshot (A) has \"upgrades\" field."
    ),
    SnapshotAIOList = blockchain_ledger_snapshot_v1:serialize(SnapshotA),
    SnapshotABin = iolist_to_binary(SnapshotAIOList),
    {ok, SnapshotB} = blockchain_ledger_snapshot_v1:deserialize(SnapshotABin),
    ?assertMatch(
        [_|_],
        blockchain_ledger_snapshot_v1:deserialize_field(upgrades, maps:get(upgrades, SnapshotB, undefined)),
        "Deserialized snapshot (B) has \"upgrades\" field."
    ),
    ?assertEqual(
        snap_hash_without_fields([upgrades], SnapshotA),
        snap_hash_without_fields([upgrades], SnapshotB),
        "Hashes A and B are equal without \"upgrades\" field."
    ),

    Ledger0 = blockchain:ledger(Chain),
    {ok, Height0} = blockchain_ledger_v1:current_height(Ledger0),
    ct:pal("ledger height BEFORE snap load: ~p", [Height0]),
    LedgerB =
        blockchain_ledger_snapshot_v1:import(
            Chain,
            Height0,
            snap_hash_without_fields([upgrades], SnapshotA),
            SnapshotB,
            SnapshotABin
        ),
    {ok, Height1} = blockchain_ledger_v1:current_height(LedgerB),
    ct:pal("ledger height AFTER snap load: ~p", [Height1]),

    {ok, SnapshotC} = blockchain_ledger_snapshot_v1:snapshot(LedgerB, [], []),
    ?assertMatch(
        [_|_],
        blockchain_ledger_snapshot_v1:deserialize_field(upgrades, maps:get(upgrades, SnapshotC, undefined)),
        "New snapshot (C) has \"upgrades\" field."
    ),
    DiffBC = blockchain_ledger_snapshot_v1:diff(SnapshotB, SnapshotC),
    ct:pal("DiffBC: ~p", [DiffBC]),
    %% C has new elements in upgrades, otherwise B and C should be the same.
    %% However, diff ignores the upgrades field.
    ?assertEqual([], DiffBC),

    ?assertEqual(
        snap_hash_without_fields([upgrades], SnapshotB),
        snap_hash_without_fields([upgrades], SnapshotC),
        "Hashes B and C are equal without fields:  [upgrades]."
    ),

    DiffAB = blockchain_ledger_snapshot_v1:diff(SnapshotA, SnapshotB),
    ct:pal("DiffAB: ~p", [DiffAB]),
    ?assertEqual([], DiffAB),

    HashC = blockchain_ledger_snapshot_v1:hash(SnapshotC),
    {ok, Height2, HashC2} = blockchain:add_snapshot(SnapshotC, Chain),
    ?assertEqual(Height1, Height2),
    ?assertEqual(HashC, HashC2),
    {ok, SnapshotDBin} = blockchain:get_snapshot(HashC, Chain),
    {ok, SnapshotD} = blockchain_ledger_snapshot_v1:deserialize(HashC, SnapshotDBin),
    ?assertEqual([], blockchain_ledger_snapshot_v1:diff(SnapshotC, SnapshotD)),
    HashD = blockchain_ledger_snapshot_v1:hash(SnapshotD),
    ?assertEqual(HashC, HashD),
    ok.

new_test(Cfg0) ->
    Cfg = t_chain:start(Cfg0),
    Chain = ?config(chain, Cfg),
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Ledger = blockchain:ledger(Chain),

    N = 20,
    ?assertMatch(ok, t_chain:commit_n_empty_blocks(Chain, ConsensusMembers, N)),
    HeightExpected = N + 1,

    {ok, HeightChain} = blockchain:height(Chain),
    {ok, HeightLedger} = blockchain_ledger_v1:current_height(Ledger),
    ?assertEqual(HeightExpected, HeightChain),
    ?assertEqual(HeightExpected, HeightLedger),

    {ok, Snap} = blockchain_ledger_v1:new_snapshot(Ledger),
    ct:pal("Snap: ~p", [Snap]),

    %% check that check points sub dirs is not empty
    %% call clean_checkpoints, this will invoke remove_checkpoints
    %% check that subdirs are empty after cleanup

    CheckpointDir = blockchain_ledger_v1:checkpoint_dir(Ledger, HeightExpected),
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

mem_limit_test(Cfg) ->
    Filename = snap_download(1160641, Cfg),
    {ok, BinSnap} = file:read_file(Filename),
    {Pid, Ref} =
        spawn_monitor(
          fun() ->
                  %% on master, this takes MB = 160 to pass on my laptop reliably, but this version
                  %% seems fine with 10?  I'm not sure how this works.
                  %% -- @evanmcc
                  MB = 10,
                  erlang:process_flag(max_heap_size, #{kill => true, size => (MB * 1024 * 1024) div 8}),
                  {ok, _Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap)
          end),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok;
        {'DOWN', Ref, process, Pid, Info} -> error(Info)
    end,

    ok.

%% ----------------------------------------------------------------------------
%% Helpers
%% ----------------------------------------------------------------------------

-spec snap_hash_without_fields([atom()], map()) -> map().
snap_hash_without_fields(Fields, Snap) ->
    blockchain_ledger_snapshot_v1:hash(snap_without_fields(Fields, Snap)).

-spec snap_without_fields([atom()], map()) -> map().
snap_without_fields(Fields, Snap) ->
    lists:foldl(fun (F, S) -> maps:remove(F, S) end, Snap, Fields).

chain_start_from_snap(SnapHeight, Cfg) ->
    {ok, _} = application:ensure_all_started(lager),
    SnapFilePath = snap_download(SnapHeight, Cfg),
    {ok, BinSnap} = file:read_file(SnapFilePath),
    {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
    SHA = blockchain_ledger_snapshot_v1:hash(Snapshot),
    {ok, BinGen} = file:read_file("../../../../test/genesis"),
    GenesisBlock = blockchain_block:deserialize(BinGen),
    LedgerDir = filename:join([?config(priv_dir, Cfg), "priv", "ledger"]),
    ok = filelib:ensure_dir(filename:join(LedgerDir, "DUMMY_FILENAME_TO_ENSURE_PARENT_DIR")),
    {ok, Chain0} = blockchain:new(LedgerDir, GenesisBlock, blessed_snapshot, undefined),
    Ledger0 = blockchain:ledger(Chain0),
    {ok, Height0} = blockchain_ledger_v1:current_height(Ledger0),
    ct:pal("ledger height BEFORE snap load: ~p", [Height0]),
    Ledger1 =
        blockchain_ledger_snapshot_v1:import(
            Chain0,
            Height0,
            SHA,
            Snapshot,
            BinSnap
        ),
    {ok, Height1} = blockchain_ledger_v1:current_height(Ledger1),
    ct:pal("ledger height AFTER snap load: ~p", [Height1]),

    %% XXX The following ledger update in chain MUST be done manually.
    %% XXX A symptom that it wasn't done: rocksdb badarg.
    Chain1 = blockchain:ledger(Ledger1, Chain0),

    [{chain, Chain1} | Cfg].

snap_download(SnapHeight, Cfg) ->
    PrivDir = ?config(priv_dir, Cfg),
    SnapFileName = lists:flatten(io_lib:format("snap-~b", [SnapHeight])),
    SnapFilePath = filename:join(PrivDir, SnapFileName),
    Cmd =
        %% The -c option in wget effectively memoizes the downloaded file,
        %% since priv_dir is per-suite.
        lists:flatten(io_lib:format(
            "cd ~s && wget -c https://snapshots.helium.wtf/mainnet/~s",
            [PrivDir, SnapFileName]
        )),
    os:cmd(Cmd),
    SnapFilePath.
