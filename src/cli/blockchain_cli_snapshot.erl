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
                   snapshot_list_usage(),
                   snapshot_load_height_usage(),
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
                   snapshot_list_cmd(),
                   snapshot_load_height_cmd(),
                   snapshot_cmd()
                  ]).
%%
%% trace
%%

snapshot_usage() ->
    [["snapshot"],
     ["blockchain snapshot commands\n\n",
      "  snapshot take          - Take a snapshot at the current ledger height.\n",
      "  snapshot load     - Load a snapshot from a file.\n"
      "  snapshot diff          - Load two snapshots from files and find changes.\n"
      "  snapshot info          - Show information about a snapshot in a file.\n"
      "  snapshot list          - Show information about the last 5 snapshots.\n"
      "  snapshot load-height   - Load a snapshot from given block height.\n"
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


snapshot_list_cmd() ->
    [
     [["snapshot", "list"], [], [], fun snapshot_list/3]
    ].

snapshot_list_usage() ->
    [["snapshot", "list"],
     ["snapshot list\n\n",
      "  Show information about the last 5 snapshots\n"]
    ].

snapshot_list(["snapshot", "list"], [], []) ->
    Chain = blockchain_worker:blockchain(),
    Snapshots = blockchain:find_last_snapshots(Chain, 5),
    case Snapshots of
        undefined -> ok;
        _ ->
            [ clique_status:text(io_lib:format("Height ~p\nHash ~p\nHave ~p\n", [Height, Hash, element(1, blockchain:get_snapshot(Hash, Chain)) == ok])) || {Height, _, Hash} <- Snapshots ]
    end;
snapshot_list(_, _, _) ->
    usage.

snapshot_load_height_cmd() ->
    [
     [["snapshot", "load-height"], [], [], fun snapshot_load_height/3],
     [["snapshot", "load-height", '*'], [], [], fun snapshot_load_height/3],
     [["snapshot", "load-height", '*'], [],
      [{revalidate, [{shortname, "r"}, {longname, "revalidate"}]}],
      fun snapshot_load_height/3]
    ].

snapshot_load_height_usage() ->
    [["snapshot", "load-height"],
     ["snapshot load-height <block_height> [-r]\n\n",
      "  Take a snapshot and load-height from given <block_height>\n"
      "Options\n\n",
      "  -r, --revalidate\n",
      "    Take a snapshot and load-height from given <block_height> with revalidation\n"
     ]
    ].

snapshot_load_height(["snapshot", "load-height"], [], []) ->
    snapshot_load_latest_available();
snapshot_load_height(["snapshot", "load-height"], [], [{revalidate, _}]) ->
    ok = blockchain_worker:pause_sync(),
    ok = application:set_env(blockchain, force_resync_validation, true),
    snapshot_load_latest_available();
snapshot_load_height(["snapshot", "load-height", BH], [], []) ->
    snapshot_load_from_height(list_to_integer(BH));
snapshot_load_height(["snapshot", "load-height", BH], [], [{revalidate, _}]) ->
    ok = blockchain_worker:pause_sync(),
    ok = application:set_env(blockchain, force_resync_validation, true),
    snapshot_load_from_height(list_to_integer(BH));
snapshot_load_height(_CmdBase, [], []) ->
    usage.

snapshot_load_from_height(BH) ->
    Chain = blockchain_worker:blockchain(),
    {ok, Block} = blockchain:get_block(BH, Chain),
    case blockchain_block_v1:snapshot_hash(Block) of
        <<>> ->
            Text = io_lib:format("No snapshot at block height: ~p", [BH]),
            [clique_status:alert([clique_status:text(Text)])];
        Hash ->
            case blockchain:get_snapshot(Hash, Chain) of
                {ok, Snapshot} ->
                    ok = blockchain_worker:install_snapshot(Hash, Snapshot),
                    [clique_status:text(io_lib:format("Loading snapshot from block_height: ~p with re-validation\n", [BH]))];
                {error, _} ->
                    Text = io_lib:format("No local snapshot at block height: ~p", [BH]),
                    [clique_status:alert([clique_status:text(Text)])]
            end
    end.

snapshot_load_latest_available() ->
    Chain = blockchain_worker:blockchain(),
    {Height, _BlockHash, SnapHash} = hd(blockchain:find_last_snapshots(Chain, 1)),

    case blockchain:get_snapshot(SnapHash, Chain) of
        {ok, Snapshot} ->
            ok = blockchain_worker:install_snapshot(SnapHash, Snapshot),
            [clique_status:text(io_lib:format("Loading snapshot from latest available block~p\n", [Height]))];
        {error, _} ->
            Text = io_lib:format("No local snapshot at block height: ~p", [Height]),
            [clique_status:alert([clique_status:text(Text)])]
    end.
