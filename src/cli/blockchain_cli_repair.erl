%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Repair ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_repair).

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
                   repair_sync_pause_usage(),
                   repair_sync_cancel_usage(),
                   repair_sync_resume_usage(),
                   repair_sync_state_usage(),
                   repair_analyze_usage(),
                   repair_repair_usage(),
                   repair_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   repair_sync_pause_cmd(),
                   repair_sync_cancel_cmd(),
                   repair_sync_resume_cmd(),
                   repair_sync_state_cmd(),
                   repair_analyze_cmd(),
                   repair_repair_cmd(),
                   repair_cmd()
                  ]).

%%--------------------------------------------------------------------
%% repair
%%--------------------------------------------------------------------
repair_usage() ->
    [["repair"],
     ["blockchain repair commands\n\n",
      "  repair sync_pause     - Temporarily pause transaction sync\n",
      "  repair sync_cancel    - Cancel any in-progress transaction sync.\n",
      "  repair sync_resume    - Resume any paused transaction sync.\n",
      "  repair sync_state     - Show current sync state.\n",
      "  repair analyze        - Display errors in the current blockchain state.\n",
      "  repair repair         - Attempt to repair errors in blockchain state.\n"
     ]
    ].

repair_cmd() ->
    [
     [["repair"], [], [], fun(_, _, _) -> usage end]
    ].

%%--------------------------------------------------------------------
%% repair sync_pause
%%--------------------------------------------------------------------
repair_sync_pause_cmd() ->
    [
     [["repair", "sync_pause"], [], [], fun repair_sync_pause/3]
    ].

repair_sync_pause_usage() ->
    [["repair", "sync_pause"],
     ["repair sync_pause\n\n",
      "  Temporarily suspend sync.\n"
     ]
    ].

repair_sync_pause(["repair", "sync_pause"], [], []) ->
    case blockchain_worker:pause_sync() of
        ok ->
            [clique_status:text("ok")];
        _Other ->
            [clique_status:text("error")]
    end;
repair_sync_pause([], [], []) ->
    usage.

%%--------------------------------------------------------------------
%% repair sync_cancel
%%--------------------------------------------------------------------
repair_sync_cancel_cmd() ->
    [
     [["repair", "sync_cancel"], [], [], fun repair_sync_cancel/3]
    ].

repair_sync_cancel_usage() ->
    [["repair", "sync_cancel"],
     ["repair sync_cancel\n\n",
      "  Cancel current sync.\n"
     ]
    ].

repair_sync_cancel(["repair", "sync_cancel"], [], []) ->
    case blockchain_worker:cancel_sync() of
        ok ->
            [clique_status:text("ok")];
        _Other ->
            [clique_status:text("error")]
    end;
repair_sync_cancel([], [], []) ->
    usage.

%%--------------------------------------------------------------------
%% repair sync_resume
%%--------------------------------------------------------------------
repair_sync_resume_cmd() ->
    [
     [["repair", "sync_resume"], [], [], fun repair_sync_resume/3]
    ].

repair_sync_resume_usage() ->
    [["repair", "sync_resume"],
     ["repair sync_resume\n\n",
      "  Resume sync.\n"
     ]
    ].

repair_sync_resume(["repair", "sync_resume"], [], []) ->
    case blockchain_worker:sync() of
        ok ->
            [clique_status:text("ok")];
        _Other ->
            [clique_status:text("error")]
    end;
repair_sync_resume([], [], []) ->
    usage.

%%--------------------------------------------------------------------
%% repair sync_state
%%--------------------------------------------------------------------
repair_sync_state_cmd() ->
    [
     [["repair", "sync_state"], [], [], fun repair_sync_state/3]
    ].

repair_sync_state_usage() ->
    [["repair", "sync_state"],
     ["repair sync_state\n\n",
      "  Display current sync state (paused or active).\n"
     ]
    ].

repair_sync_state(["repair", "sync_state"], [], []) ->
    case blockchain_worker:sync_paused() of
        true ->
            [clique_status:text("sync paused")];
        false ->
            [clique_status:text("sync active")]
    end;
repair_sync_state([], [], []) ->
    usage.

%%--------------------------------------------------------------------
%% repair analyze
%%--------------------------------------------------------------------
repair_analyze_cmd() ->
    [
     [["repair", "analyze"], [], [], fun repair_analyze/3]
    ].

repair_analyze_usage() ->
    [["repair", "analyze"],
     ["repair analyze\n\n",
      "  Display inconsistencies between current and lagging ledger states.\n"
     ]
    ].

repair_analyze(["repair", "analyze"], [], []) ->
    case blockchain_worker:sync_paused() of
        true ->
            BlkChain = blockchain_worker:blockchain(),
            case blockchain:analyze(BlkChain) of
                ok ->
                    [clique_status:text("no errors detected")];
                {error, Error} ->
                    Fmt = io_lib:format("error: ~p~n", [Error]),
                    [clique_status:alert([clique_status:text(Fmt)])]
            end;
        false ->
            [clique_status:alert([clique_status:text("please pause sync first")])]
    end;
repair_analyze([], [], []) ->
    usage.

%%--------------------------------------------------------------------
%% repair repair
%%--------------------------------------------------------------------
repair_repair_cmd() ->
    [
     [["repair", "repair"], [], [], fun repair_repair/3]
    ].

repair_repair_usage() ->
    [["repair", "repair"],
     ["repair repair\n\n",
      "  Attempt a repair of an inconsistency between current and lagging ledger states.\n"
     ]
    ].

repair_repair(["repair", "repair"], [], []) ->
    case blockchain_worker:sync_paused() of
        true ->
            BlkChain = blockchain_worker:blockchain(),
            case blockchain:repair(BlkChain) of
                ok ->
                    [clique_status:text("ok")];
                Other ->
                    Fmt = io_lib:format("error: ~p~n", [Other]),
                    [clique_status:alert([clique_status:text(Fmt)])]
            end;
        false ->
            [clique_status:alert([clique_status:text("please pause sync first")])]
    end;
repair_repair([], [], []) ->
    usage.
