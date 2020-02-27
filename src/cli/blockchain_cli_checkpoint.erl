-module(blockchain_cli_checkpoint).
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
                   checkpoint_usage(),
                   checkpoint_blockchain_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   checkpoint_blockchain_cmd(),
                   checkpoint_cmd()
                  ]).

%%
%% checkpoint
%%

checkpoint_usage() ->
    [["checkpoint"],
     ["checkpoint commands\n\n",
      "  checkpoint blockchain - write a blockchain checkpoint\n"
     ]
    ].

checkpoint_cmd() ->
    [
     [["checkpoint"], [], [], fun(_, _, _) -> usage end]
    ].

%%
%% rocksdb checkpoint
%%

checkpoint_blockchain_usage() ->
    [["checkpoint", "blockchain"],
     ["checkpoint blockchain {directory}\n\n",
      "  Write a blockchain checkpoint into the given directory. The directory must not\n",
      "  exist prior to this command.\n"
     ]
    ].

checkpoint_blockchain_cmd() ->
    [
     [["checkpoint", "blockchain", '*'], [], [], fun do_checkpoint/3]
    ].

do_checkpoint(["checkpoint", "blockchain", Directory], [], []) ->
    Blockchain = blockchain_worker:blockchain(),
    case blockchain:checkpoint(Blockchain, Directory) of
        ok ->
            [clique_status:text("ok")];
        Error ->
            Fmt = io_lib:format("error: ~p~n", [Error]),
            [clique_status:alert([clique_status:text(Fmt)])]
    end;
do_checkpoint(_CmdBase, _, _) -> usage.
