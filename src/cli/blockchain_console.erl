-module(blockchain_console).

-export([command/1]).

-spec command([string()]) -> rpc_ok | {rpc_error, non_neg_integer()}.
command(Cmd) ->
    %% this is the contents of clique:run but
    %% we want to figure out if the command worked
    %% or not
    M0 = clique_command:match(Cmd),
    M1 = clique_parser:parse(M0),
    M2 = clique_parser:extract_global_flags(M1),
    M3 = clique_parser:validate(M2),
    M4 = clique_command:run(M3),
    clique:print(M4, Cmd),
    case M4 of
        {error, {no_matching_spec, _Spec}} ->
            {rpc_error, 1};
        {_Status, ExitCode, _} when ExitCode == 0 ->
            rpc_ok;
        {_Status, ExitCode, _} ->
            {rpc_error, ExitCode}
    end.
