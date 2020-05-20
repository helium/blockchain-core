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
                   snapshot_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   snapshot_take_cmd(),
                   snapshot_load_cmd(),
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
    DLedger = blockchain_ledger_v1:mode(delayed, Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    {ok, DHeight} = blockchain_ledger_v1:current_height(DLedger),
    Blocks =
        [begin
             {ok, B} = blockchain:get_block(N, Chain),
             B
         end
         || N <- lists:seq(DHeight, Height)],
    {ok, Snapshot} = blockchain_ledger_snapshot_v1:snapshot(Ledger, Blocks),
    {ok, BinSnap} = blockchain_ledger_snapshot_v1:serialize(Snapshot),
    file:write_file(Filename, BinSnap).

snapshot_load_cmd() ->
    [
     [["snapshot", "load", '*'], [], [], fun snapshot_load/3]
    ].

snapshot_load_usage() ->
    [["snapshot", "load"],
     ["blockchain snapshot take <filename>\n\n",
      "  Take a ledger snapshot at the current height and write it to filename\n"]
    ].

snapshot_load(["snapshot", "load", Filename], [], []) ->
    Ret = snapshot_load(Filename),
    [clique_status:text(io_lib:format("~p", [Ret]))];
snapshot_load(_, _, _) ->
    usage.

snapshot_load(Filename) ->
    {ok, BinSnap} = file:read_file(Filename),
    %% no reason to hang this forever
    ok = blockchain_lock:acquire(15000),
    try
        {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
        Hash = blockchain_ledger_snapshot_v1:hash(Snapshot),

        ok = blockchain_worker:install_snapshot(Hash, Snapshot)
    after
            blockchain_lock:release()
    end,
    ok.
