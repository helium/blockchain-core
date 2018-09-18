%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Trace ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_trace).

-behavior(clique_handler).

-export([register_cli/0]).

register_cli() ->
    register_all_usage(),
    register_all_cmds().

register_all_usage() ->
    lists:foreach(fun(Args) ->
                          apply(clique, register_usage, Args)
                  end,
                 [
                  trace_start_usage(),
                  trace_clear_usage(),
                  trace_usage()
                 ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                 [
                  trace_start_cmd(),
                  trace_clear_cmd(),
                  trace_cmd()
                 ]).
%%
%% trace
%%

trace_usage() ->
    [["trace"],
     ["blockchain trace commands\n\n",
      "  trace start   - Start a trace for a given filter and level.\n",
      "  trace clear   - Clears all installed traces.\n"
     ]
    ].

trace_cmd() ->
    [
     [["trace"], [], [], fun(_, _, _) -> usage end]
    ].


%%
%% trace start
%%

trace_start_cmd() ->
    [
     [["trace", "start"], '_',
      [
       {level, [{shortname, "l"},
               {longname, "level"}]},
       {output, [{shortname, "o"},
                 {longname, "output"}]}
      ], fun trace_start/3]
    ].

trace_start_usage() ->
    [["trace", "start"],
     ["blockchain trace start\n\n",
      "  Start a console trace. Use key=value arguments to set\n",
      "  the trace filters.\n\n",
      "Options\n\n",
      "  -l, --level [none|debug|info|notice|warning|error|critical|alert|emergency]\n",
      "    The loglevel to trace at. Defaults to debug\n",
      "  -o, --output <filename>\n",
      "    The file to output to.\n"
     ]
    ].

trace_start(_CmdBase, [], _) ->
    usage;
trace_start(_CmdBase, Keys, Flags) ->
    case lists:keytake(output, 1, Flags) of
        false -> trace_start_console(Keys, Flags);
        {value, {output, Filename}, RemFlags} ->
            trace_start_file(Keys, Filename, RemFlags)
    end.


trace_start_console(Keys, Flags) ->
    GL = erlang:group_leader(),
    Node = node(GL),
    lager_app:start_handler(lager_event, {lager_console_backend, Node}, [{group_leader, GL}, {level, none}, {id, {lager_console_backend, Node}}]),
    case lager:trace({lager_console_backend, Node},
                     libp2p_lager_metadata:from_strings(Keys),
                     trace_start_level(Flags)) of
        {ok, _} ->
            timer:sleep(infinity);
        {error, Err} ->
            Msg = io_lib:format("~p", [Err]),
            [clique_status:alert([clique_status:text(Msg)])]
    end.

trace_start_file(Keys, Filename, Flags) ->
    case lager:trace_file(Filename, libp2p_lager_metadata:from_strings(Keys),
                         trace_start_level(Flags)) of
        {ok, _} ->
            [clique_status:text("ok")];
        {error, Err} ->
            Msg = io_lib:format("~p", [Err]),
            [clique_status:alert([clique_status:text(Msg)])]
    end.

trace_start_level(Flags) ->
    Level = case lists:keyfind(level, 1, Flags) of
                false -> "debug";
                {level, L} -> L
            end,
    parse_level(Level, debug).

-spec parse_level(string(), lager:log_level()) -> lager:log_level().
parse_level(Level, Default) ->
    LogLevels = [
                 none,
                 debug,
                 info,
                 notice,
                 warning,
                 error,
                 critical,
                 alert,
                 emergency
                ],
    case cuttlefish_datatypes:from_string(Level, {enum, LogLevels}) of
        {error, _} -> Default;
        Value -> Value
    end.

%%
%% trace clear
%%

trace_clear_cmd() ->
    [
     [["trace", "clear"], [], [], fun trace_clear/3]
    ].

trace_clear_usage() ->
    [["trace", "clear"],
     ["blockchain trace clear\n\n",
      "  Clears all console traces.\n"
     ]
    ].

trace_clear(_CmdBase, [], []) ->
    lager:clear_all_traces(),
    [clique_status:text("ok")].
