%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Exporter ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_exporter_v1).

-export([
    export/1
]).

-spec export(blockchain_ledger_v1:ledger()) -> any().
export(Ledger) ->
    [
        {securities, export_securities(Ledger)},
        {accounts, export_accounts(Ledger)},
        {gateways, export_gateways(Ledger)},
        {chain_vars, export_chain_vars(Ledger)},
        {dcs, export_dcs(Ledger)}
    ].

-spec export_accounts(blockchain_ledger_v1:ledger()) -> list().
export_accounts(Ledger) ->
    lists:foldl(
        fun({Address, Entry}, Acc) ->
            [[{address, libp2p_crypto:bin_to_b58(Address)},
              {balance, blockchain_ledger_entry_v1:balance(Entry)}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:entries(Ledger))
    ).

-spec export_gateways(blockchain_ledger_v1:ledger()) -> list().
export_gateways(Ledger) ->
    lists:foldl(
        fun({GatewayAddress, Gateway}, Acc) ->
            [[{gateway_address, libp2p_crypto:bin_to_b58(GatewayAddress)},
              {owner_address, libp2p_crypto:bin_to_b58(blockchain_ledger_gateway_v2:owner_address(Gateway))},
              {location, blockchain_ledger_gateway_v2:location(Gateway)},
              {nonce, blockchain_ledger_gateway_v2:nonce(Gateway)}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:active_gateways(Ledger))
    ).

-spec export_securities(blockchain_ledger_v1:ledger()) -> list().
export_securities(Ledger) ->
    lists:foldl(
        fun({Address, SecurityEntry}, Acc) ->
            [[{address, libp2p_crypto:bin_to_b58(Address)},
              {token, blockchain_ledger_security_entry_v1:balance(SecurityEntry)}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:securities(Ledger))
    ).

-spec export_dcs(blockchain_ledger_v1:ledger()) -> list().
export_dcs(Ledger) ->
    lists:foldl(
        fun({Address, DCEntry}, Acc) ->
            [[{address, libp2p_crypto:bin_to_b58(Address)},
              {dc_balance, blockchain_ledger_data_credits_entry_v1:balance(DCEntry)}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:dc_entries(Ledger))
    ).

export_chain_vars(Ledger) ->
    lists:sort(blockchain_ledger_v1:snapshot_vars(Ledger)).
