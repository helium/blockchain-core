%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Trace ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_snapshot).

-behavior(clique_handler).

-export([register_cli/0]).

-export([snapshot_take/1,
         snapshot_load/1]).

register_cli() ->
    register_all_usage(),
    register_all_cmds().

register_all_usage() ->
    lists:foreach(fun(Args) ->
                          apply(clique, register_usage, Args)
                  end,
                  [
                   snapshot_take_usage(),
                   snapshot_load_usage(),
                   snapshot_diff_usage(),
                   snapshot_info_usage(),
                   snapshot_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   snapshot_take_cmd(),
                   snapshot_load_cmd(),
                   snapshot_diff_cmd(),
                   snapshot_info_cmd(),
                   snapshot_cmd()
                  ]).
%%
%% trace
%%

snapshot_usage() ->
    [["snapshot"],
     ["blockchain snapshot commands\n\n",
      "  snapshot take   - Take a snapshot at the current ledger height.\n",
      "  snapshot load   - Load a snapshot from a file.\n"
      "  snapshot diff   - Load two snapshots from files and find changes.\n"
      "  snapshot info   - Show information about a snapshot in a file.\n"
     ]
    ].

snapshot_cmd() ->
    [
     [["snapshot"], [], [], fun(_, _, _) -> usage end]
    ].


%%
%% trace start
%%

snapshot_take_cmd() ->
    [
     [["snapshot", "take", '*'], [], [], fun snapshot_take/3]
    ].

snapshot_take_usage() ->
    [["snapshot", "take"],
     ["blockchain snapshot take <filename>\n\n",
      "  Take a ledger snapshot at the current height and write it to filename\n"]
    ].

snapshot_take(["snapshot", "take", Filename], [], []) ->
    Ret = snapshot_take(Filename),
    [clique_status:text(io_lib:format("~p", [Ret]))];
snapshot_take(_, _, _) ->
    usage.

snapshot_take(Filename) ->
    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    Blocks = blockchain_ledger_snapshot_v1:get_blocks(Chain),
    {ok, Snapshot} = blockchain_ledger_snapshot_v1:snapshot(Ledger, Blocks),
    {ok, BinSnap} = blockchain_ledger_snapshot_v1:serialize(Snapshot),
    file:write_file(Filename, BinSnap).

snapshot_load_cmd() ->
    [
     [["snapshot", "load", '*'], [], [], fun snapshot_load/3]
    ].

snapshot_load_usage() ->
    [["snapshot", "load"],
     ["blockchain snapshot load <filename>\n\n",
      "  Take a ledger snapshot at the current height and write it to filename\n"]
    ].

snapshot_load(["snapshot", "load", Filename], [], []) ->
    Ret = snapshot_load(Filename),
    [clique_status:text(io_lib:format("~p", [Ret]))];
snapshot_load(_, _, _) ->
    usage.

snapshot_load(Filename) ->
    {ok, BinSnap} = file:read_file(Filename),

    {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
    Hash = blockchain_ledger_snapshot_v1:hash(Snapshot),

    ok = blockchain_worker:install_snapshot(Hash, Snapshot),
    ok.

snapshot_diff_cmd() ->
    [
     [["snapshot", "diff", '*', '*'], [], [], fun snapshot_diff/3]
    ].

snapshot_diff_usage() ->
    [["snapshot", "diff"],
     ["blockchain snapshot diff <filename> <filename>\n\n",
      "  Compare two snapshot files for equality, returns the list of differences or [] if identical\n"]
    ].

snapshot_diff(["snapshot", "diff", AFilename, BFilename], [], []) ->
    Ret = snapshot_diff(AFilename, BFilename),
    [clique_status:text(io_lib:format("~p", [Ret]))];
snapshot_diff(_, _, _) ->
    usage.

snapshot_diff(AFilename, BFilename) ->
    {ok, ABinSnap} = file:read_file(AFilename),
    {ok, BBinSnap} = file:read_file(BFilename),

    {ok, A} = blockchain_ledger_snapshot_v1:deserialize(ABinSnap),
    {ok, B} = blockchain_ledger_snapshot_v1:deserialize(BBinSnap),

    blockchain_ledger_snapshot_v1:diff(A, B).

snapshot_info_cmd() ->
    [
     [["snapshot", "info", '*'], [], [], fun snapshot_info/3]
    ].

snapshot_info_usage() ->
    [["snapshot", "info"],
     ["snapshot info <file>\n\n",
      "  Show information about snapshot in file <file>\n"]
    ].

snapshot_info(["snapshot", "info", Filename], [], []) ->
    {ok, BinSnap} = file:read_file(Filename),
    {ok, Snap} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
    [clique_status:text(io_lib:format("Height ~p\nHash ~p\n", [blockchain_ledger_snapshot_v1:height(Snap),
                                                              blockchain_ledger_snapshot_v1:hash(Snap)]))];
snapshot_info(_, _, _) ->
    usage.
