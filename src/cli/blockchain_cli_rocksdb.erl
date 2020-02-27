-module(blockchain_cli_rocksdb).
-behavior(clique_handler).
-export([register_cli/0]).
-include("blockchain.hrl").

register_cli() ->
    register_all_usage(), register_all_cmds().

register_all_usage() ->
    lists:foreach(fun(Args) ->
                          apply(clique, register_usage, Args)
                  end,
                  [
                   rocksdb_checkpoint_usage(),
                   rocksdb_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   rocksdb_checkpoint_cmd(),
                   rocksdb_cmd()
                  ]).

%%
%% rocksdb
%%

rocksdb_usage() ->
    [["rocksdb"],
     ["blockchain rocksdb commands\n\n",
      "  rocksdb checkpoint - write a rocksdb checkpoint\n"
     ]
    ].

rocksdb_cmd() ->
    [
     [["rocksdb"], [], [], fun(_, _, _) -> usage end]
    ].

%%
%% rocksdb checkpoint
%%

rocksdb_checkpoint_usage() ->
    [["rocksdb", "checkpoint"],
     ["rocksdb checkpoint {directory}\n\n",
      "  Write a checkpoint into the given directory. The directory must not\n",
      "  exist prior to this command.\n"
     ]
    ].

rocksdb_checkpoint_cmd() ->
    [
     [["rocksdb", "checkpoint", '*'], [], [], fun do_checkpoint/3]
    ].

do_checkpoint(["rocksdb", "checkpoint", Directory], [], []) ->
    Blockchain = blockchain_worker:blockchain(),
    case blockchain:checkpoint(Blockchain, Directory) of
        ok ->
            [clique_status:text("ok")];
        Error ->
            Fmt = io_lib:format("error: ~p~n", [Error]),
            [clique_status:alert([clique_status:text(Fmt)])]
    end;
do_checkpoint(_CmdBase, _, _) -> usage.
