-module(blockchain_election).

-export([
         new_group/3,
         has_new_group/1
        ]).

new_group(Ledger, Hash, Size) ->
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),
    Gateways = maps:keys(Gateways0),
    lager:info("hash ~p gateways ~p", [Hash, Gateways]),
    Nodes = lists:sort(Gateways),
    ShuffleNodes = blockchain_utils:shuffle_from_hash(Hash, Nodes),
    lists:sublist(ShuffleNodes, 1, Size).

has_new_group(Txns) ->
    MyAddress = blockchain_swarm:pubkey_bin(),
    case lists:filter(fun(T) ->
                              %% TODO: ideally move to versionless types?
                              blockchain_txn:type(T) == blockchain_txn_consensus_group_v1
                      end, Txns) of
        [Txn] ->
            {true, lists:member(MyAddress, blockchain_txn_consensus_group_v1:members(Txn))};
        [_|_] ->
            lists:foreach(fun(T) ->
                                  case blockchain_txn:type(T) == blockchain_txn_consensus_group_v1 of
                                      true ->
                                          lager:info("txn ~p", [T]);
                                      _ -> ok
                                  end
                          end, Txns),
            error(duplicate_group_txn);
        [] ->
            false
    end.
