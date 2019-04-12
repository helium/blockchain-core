%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Txn ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_txn).

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
                   txn_queue_usage(),
                   txn_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   txn_queue_cmd(),
                   txn_cmd()
                  ]).

%%--------------------------------------------------------------------
%% txn
%%--------------------------------------------------------------------
txn_usage() ->
    [["txn"],
     ["blockchain txn commands\n\n",
      "  txn queue             - Show enqueued transactions in the txn_queue.\n"
     ]
    ].

txn_cmd() ->
    [
     [["txn"], [], [], fun(_, _, _) -> usage end]
    ].

%%--------------------------------------------------------------------
%% txn queue
%%--------------------------------------------------------------------
txn_queue_cmd() ->
    [
     [["txn", "queue"], [], [], fun txn_queue/3]
    ].

txn_queue_usage() ->
    [["txn", "queue"],
     ["txn queue\n\n",
      "  Show enqueued transactions for the miner.\n"
     ]
    ].

txn_queue(["txn", "queue"], [], []) ->
    case (catch blockchain_txn_manager:txn_map()) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        [] ->
            [clique_status:text("empty")];
        TxnMap ->
            R = format_txn_queue(TxnMap),
            [clique_status:table(R)]
    end;
txn_queue([], [], []) ->
    usage.

format_txn_queue(TxnMap) ->
    lists:map(fun({TxnHash, Entry}) ->
                      Txn = blockchain_txn_manager:txn(Entry),
                      TxnMod = blockchain_txn:type(Txn),
                      Acceptors = blockchain_txn_manager:acceptors(Entry),
                      Rejectors = blockchain_txn_manager:rejectors(Entry),
                      [
                       {txn_type, atom_to_list(TxnMod)},
                       {txn_hash, io_lib:format("~p", [libp2p_crypto:bin_to_b58(TxnHash)])},
                       {accepted_by, integer_to_list(length(Acceptors))},
                       {rejected_by, integer_to_list(length(Rejectors))}
                      ]
              end, maps:to_list(TxnMap)).
