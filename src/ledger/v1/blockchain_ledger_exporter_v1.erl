%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Exporter ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_exporter_v1).

-export([
    export/1,
    export_accounts/1,
    export_gateways/1
]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec export(blockchain_ledger_v1:ledger()) -> any().
export(Ledger) ->
    [
        {accounts, export_accounts(Ledger)},
        {gateways, export_gateways(Ledger)}
    ].


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec export_accounts(blockchain_ledger_v1:ledger()) -> any().
export_accounts(Ledger) ->
    lists:foldl(
        fun({Address, Entry}, Acc) ->
            [[{address, libp2p_crypto:bin_to_b58(Address)},
              {balance, blockchain_ledger_entry_v1:balance(Entry)}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:entries(Ledger))
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec export_gateways(blockchain_ledger_v1:ledger()) -> any().
export_gateways(Ledger) ->
    lists:foldl(
        fun({GatewayAddress, Gateway}, Acc) ->
            [[{gateway_address, libp2p_crypto:bin_to_b58(GatewayAddress)},
              {owner_address, libp2p_crypto:bin_to_b58(blockchain_ledger_gateway_v1:owner_address(Gateway))},
              {location, blockchain_ledger_gateway_v1:location(Gateway)},
              {nonce, blockchain_ledger_gateway_v1:nonce(Gateway)},
              {score, blockchain_ledger_gateway_v1:score(Gateway)}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:active_gateways(Ledger))
    ).
