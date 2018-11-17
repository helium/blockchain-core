%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Exporter ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_exporter).

-export([
         export/1,
         export_balances/1,
         export_gateways/1
        ]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec export(blockchain_ledger:ledger()) -> any().
export(Ledger) ->
    [
     {balances, export_balances(Ledger)},
     {gateways, export_gateways(Ledger)}
    ].


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec export_balances(blockchain_ledger:ledger()) -> any().
export_balances(Ledger) ->
    lists:foldl(fun({Address, Entry}, Acc) ->
                        [ {{address, libp2p_crypto:address_to_b58(Address)},
                           {balance, blockchain_ledger:balance(Entry)}} | Acc ]
                end, [], maps:to_list(blockchain_ledger:entries(Ledger))).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec export_gateways(blockchain_ledger:ledger()) -> any().
export_gateways(Ledger) ->
    lists:foldl(fun({GatewayAddress, Gateway}, Acc) ->
                        [ {{gateway_address, libp2p_crypto:address_to_b58(GatewayAddress)},
                           {owner_address, libp2p_crypto:address_to_b58(blockchain_ledger_gateway:owner_address(Gateway))},
                           {location, blockchain_ledger_gateway:location(Gateway)},
                           {last_poc_challenge, blockchain_ledger_gateway:last_poc_challenge(Gateway)},
                           {nonce, blockchain_ledger_gateway:nonce(Gateway)},
                           {score, blockchain_ledger_gateway:score(Gateway)}} | Acc]
                end, [], maps:to_list(blockchain_ledger:active_gateways(Ledger))).
