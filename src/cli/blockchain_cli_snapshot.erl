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
                   snapshot_grab_usage(),
                   snapshot_diff_usage(),
                   snapshot_info_usage(),
                   snapshot_list_usage(),
                   snapshot_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   snapshot_take_cmd(),
                   snapshot_load_cmd(),
                   snapshot_grab_cmd(),
                   snapshot_diff_cmd(),
                   snapshot_info_cmd(),
                   snapshot_list_cmd(),
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
      "  snapshot grab   - Attempt to grab a snapshot from a connected peer.\n"
      "  snapshot diff   - Load two snapshots from files and find changes.\n"
      "  snapshot info   - Show information about a snapshot in a file.\n"
      "  snapshot list   - Show information about the last 5 snapshots.\n"
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
    ok = blockchain_lock:acquire(),
    case blockchain_ledger_snapshot_v1:get_blocks(Chain) of
        {error, encountered_a_rescue_block}=Err ->
            Err;
        {ok, Blocks} ->
            Infos = blockchain_ledger_snapshot_v1:get_infos(Chain),
            {ok, Snapshot} = blockchain_ledger_snapshot_v1:snapshot(Ledger, Blocks, Infos),
            blockchain_lock:release(),
            BinSnap = blockchain_ledger_snapshot_v1:serialize(Snapshot),
            file:write_file(Filename, BinSnap)
    end.

snapshot_load_cmd() ->
    [
     [["snapshot", "load", '*'], [], [], fun snapshot_load/3]
    ].

snapshot_load_usage() ->
    [["snapshot", "load"],
     ["blockchain snapshot load <filename>\n\n",
      "  Load a snapshot from filename\n"]
    ].

snapshot_load(["snapshot", "load", Filename], [], []) ->
    Ret = snapshot_load(Filename),
    [clique_status:text(io_lib:format("~p", [Ret]))];
snapshot_load(_, _, _) ->
    usage.

snapshot_load(Filename) ->
    blockchain_worker:install_snapshot_from_file(Filename).

snapshot_grab_usage() ->
    [["snapshot", "grab"],
     ["blockchain snapshot grab <Height> <Hash> <Filename>\n\n",
      "  Grab a snapshot at specified height and hex encoded snapshot hash from a connected peer\n",
      "  Use curl or wget to pull snapshots from a URL\n"]
    ].

snapshot_grab_cmd() ->
    [
     [["snapshot", "grab", '*', '*', '*' ], [], [], fun snapshot_grab/3]
    ].

snapshot_grab(["snapshot", "grab", HeightStr, HashStr, Filename], [], []) ->
    try
        Height = list_to_integer(HeightStr),
        Hash = hex_to_binary(HashStr),
        {ok, Snapshot} = blockchain_worker:grab_snapshot(Height, Hash),
        %% NOTE: grab_snapshot returns a deserialized snapshot
        file:write_file(Filename, blockchain_ledger_snapshot_v1:serialize(Snapshot))
    catch
        _Type:Error ->
            [clique_status:text(io_lib:format("failed: ~p", [Error]))]
    end;
snapshot_grab([_, _, _, _, _], [], []) ->
    usage.

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
    {ok, A} = blockchain_ledger_snapshot_v1:deserialize({file, AFilename}),
    {ok, B} = blockchain_ledger_snapshot_v1:deserialize({file, BFilename}),

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
    {ok, Snap} = blockchain_ledger_snapshot_v1:deserialize({file, Filename}),
    {ok, {NumBlocks, StartBlockHt, EndBlockHt}} = blockchain_ledger_snapshot_v1:blocks_info(Snap),
    [clique_status:text(io_lib:format("Height ~p\nNumBlocks ~p\nStartBlockHt ~p\nEndBlockHt ~p\nHash ~p (~p)\n",
                                      [blockchain_ledger_snapshot_v1:height(Snap),
                                       NumBlocks,
                                       StartBlockHt,
                                       EndBlockHt,
                                       blockchain_ledger_snapshot_v1:hash(Snap),
                                       binary_to_hex(blockchain_ledger_snapshot_v1:hash(Snap))]
                                     ))];
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
            [ clique_status:text(io_lib:format("Height ~p\nHash ~p (~p)\nHave ~p\n",
                                               [Height, Hash, binary_to_hex(Hash),
                                                element(1, blockchain:get_snapshot(Hash, Chain)) == ok])) || {Height, _, Hash} <- Snapshots ]
    end;
snapshot_list(_, _, _) ->
    usage.

binary_to_hex(Binary) ->
    << <<(hex(H)),(hex(L))>> || <<H:4,L:4>> <= Binary >>.

hex(C) when C < 10 -> $0 + C;
hex(C) -> $a + C - 10.

hex_to_binary(Hex) when is_list(Hex) ->
    hexstr_to_bin(Hex, []).

hexstr_to_bin([], Acc) ->
    list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
    hexstr_to_bin(T, [V | Acc]);
hexstr_to_bin([X|T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", lists:flatten([X,"0"])),
    hexstr_to_bin(T, [V | Acc]).
