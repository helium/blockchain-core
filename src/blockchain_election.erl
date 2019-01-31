-module(blockchain_election).

-export([
         new_group/3,
         has_new_group/1
        ]).

%% we'll need size later
new_group(Ledger, Hash, _Size) ->
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),
    Gateways = maps:keys(Gateways0),
    lager:info("hash ~p gateways ~p", [Hash, Gateways]),
        <<I1:86/integer, I2:85/integer, I3:85/integer>> = Hash,
    rand:seed(exs1024, {I1, I2, I3}),

    %% guarantee that we have a deterministic starting order
    Nodes = lists:sort(Gateways),
    %% for now, just randomize the node order
    Shuf0 = [{rand:uniform(10000000), Node} || Node <- Nodes],
    Shuf = lists:sort(Shuf0),
    %% we should not need this on the mainnet, I'm not sure that it's
    %% fully deterministic anyway.  change when scores
    [Node || {_, Node} <- Shuf].

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
