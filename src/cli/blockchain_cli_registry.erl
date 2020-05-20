-module(blockchain_cli_registry).

-define(CLI_MODULES, [
                      blockchain_cli_peer,
                      blockchain_cli_snapshot,
                      blockchain_cli_ledger,
                      blockchain_cli_repair,
                      blockchain_cli_trace,
                      blockchain_cli_txn,
                      blockchain_cli_sc
                     ]).

-export([register_cli/0]).

register_cli() ->
    clique:register(?CLI_MODULES).
