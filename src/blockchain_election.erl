-module(blockchain_election).

-export([
         new_group/2
        ]).


new_group(Ledger, Hash) ->
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),
    Gateways = maps:keys(Gateways0),
    lager:info("gateways ~p", [Gateways]),
        <<I1:86/integer, I2:85/integer, I3:85/integer>> = Hash,
    rand:seed(exs1024, {I1, I2, I3}),

    %% guarantee that we have a deterministic starting order
    Nodes = lists:sort(Gateways),
    %% for now, just randomize the node order
    Shuf0 = [{rand:uniform(10000000), Node} || Node <- Nodes],
    Shuf = lists:sort(Shuf0),
    [Node || {_, Node} <- Shuf].
