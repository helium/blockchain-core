%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Txn ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_txn).

-behavior(clique_handler).

-export([register_cli/0]).

-include("blockchain.hrl").
-include("blockchain_shared.hrl").

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
    case (catch blockchain_txn_mgr:txn_list()) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        [] ->
            [clique_status:text("empty")];
        TxnList ->
            R = format_txn_list(TxnList),
            [clique_status:table(R)]
    end;
txn_queue([], [], []) ->
    usage.

format_txn_list(TxnList) ->
    lists:map(fun({Txn, #txn_data{  acceptions = Acceptions,
                                    rejections = Rejections,
                                    recv_block_height = RecvBlockHeight}}) ->
                      TxnMod = blockchain_txn:type(Txn),
                      TxnHash = blockchain_txn:hash(Txn),
                      [
                       {txn_type, atom_to_list(TxnMod)},
                       {txn_hash, io_lib:format("~p", [libp2p_crypto:bin_to_b58(TxnHash)])},
                       {acceptions, length(Acceptions)},
                       {rejections, length(Rejections)},
                       {accepted_block_height, RecvBlockHeight}
                      ]
              end, TxnList).
