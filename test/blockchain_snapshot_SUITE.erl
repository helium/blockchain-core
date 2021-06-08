-module(blockchain_snapshot_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1,
    new_test/1
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
        new_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

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
basic_test(_Config) ->
    LedgerA = ledger(),
    case blockchain_ledger_v1:get_h3dex(LedgerA) of
        #{} ->
            LedgerBoot = blockchain_ledger_v1:new_context(LedgerA),
            blockchain:bootstrap_h3dex(LedgerBoot),
            blockchain_ledger_v1:commit_context(LedgerBoot);
        _ -> ok
    end,
    {ok, SnapshotA} = blockchain_ledger_snapshot_v1:snapshot(LedgerA, []),
    %% make a dir for the loaded snapshot
    {ok, Dir} = file:get_cwd(),
    PrivDir = filename:join([Dir, "priv"]),
    NewDir = PrivDir ++ "/ledger2/",
    ok = filelib:ensure_dir(NewDir),

    ?assertMatch(
        [_|_],
        maps:get(upgrades, SnapshotA, undefined),
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
        maps:get(upgrades, SnapshotB, undefined),
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
        maps:get(upgrades, SnapshotC, undefined),
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

    %% TODO: C has new elements in upgrades. Should we assert something more specific?
    ?assertEqual([upgrades], DiffBC),
    %% Otherwise B and C should be the same:
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

%% utils
-spec snap_hash_without_field(atom(), map()) -> map().
snap_hash_without_field(Field, Snap) ->
    blockchain_ledger_snapshot_v1:hash(maps:remove(Field, Snap)).

ledger() ->
    %% Ledger at height: 194196
    %% ActiveGateway Count: 3023
    {ok, TestDir} = file:get_cwd(),  % this is deep in the test hierarchy

    Comps = filename:split(TestDir),
    Trimmed = lists:reverse(lists:sublist(lists:reverse(Comps), 5, length(Comps))),
    Dir = filename:join(Trimmed),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    %% Path to static ledger tar
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    %% Extract ledger tar if required
    ok = extract_ledger_tar(PrivDir, LedgerTar),
    %% Get the ledger
    Ledger = blockchain_ledger_v1:new(PrivDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    %% If the hexes aren't on the ledger add them
    blockchain:bootstrap_hexes(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    Ledger.

extract_ledger_tar(PrivDir, LedgerTar) ->
    case filelib:is_file(LedgerTar) of
        true ->
            %% if we have already unpacked it, no need to do it again
            LedgerDB = filename:join([PrivDir, "ledger.db"]),
            case filelib:is_dir(LedgerDB) of
                true ->
                    ok;
                false ->
                    %% ledger tar file present, extract
                    erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
            end;
        false ->
            %% ledger tar file not found, download & extract
            ok = ssl:start(),
            {ok, {{_, 200, "OK"}, _, Body}} = httpc:request("https://blockchain-core.s3-us-west-1.amazonaws.com/ledger-387747.tar.gz"),
            ok = file:write_file(filename:join([PrivDir, "ledger.tar.gz"]), Body),
            erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
    end.


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
