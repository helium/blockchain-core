%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Exporter ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_exporter).

-export([
         export/1,
         export_balances/1,
         export_gateways/1,
         export_htlcs/1
        ]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec export(blockchain_ledger:ledger()) -> any().
export(Ledger) ->
    [
     export_balances(Ledger),
     export_gateways(Ledger),
     export_htlcs(Ledger)
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
%% XXX: No clue wtf these are gonna look like,
%% I'm putting everything from the htlc record in here
-spec export_htlcs(blockchain_ledger:ledger()) -> any().
export_htlcs(Ledger) ->
    lists:foldl(fun({Address, HTLC}, Acc) ->
                        [ {{address, libp2p_crypto:address_to_b58(Address)},
                           {htlc_nonce, blockchain_ledger:htlc_nonce(HTLC)},
                           {htlc_payer, libp2p_crypto:address_to_b58(blockchain_ledger:htlc_payer(HTLC))},
                           {htlc_payee, libp2p_crypto:address_to_b58(blockchain_ledger:htlc_payee(HTLC))},
                           {htlc_hashlock, blockchain_ledger:htlc_hashlock(HTLC)},
                           {htlc_timelock, blockchain_ledger:htlc_timelock(HTLC)}} | Acc ]
                end, [], maps:to_list(blockchain_ledger:htlcs(Ledger))).
