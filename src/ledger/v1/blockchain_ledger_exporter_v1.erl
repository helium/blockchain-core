%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Exporter ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_exporter_v1).

-export([
    export/1,

    export_accounts/1,
    export_gateways/1,
    export_chain_vars/1,
    export_dcs/1,
    export_routes/1,
    export_validators/1,

    minimum_json/1,
    consolidate_accounts/1,
    consolidate_hotspots/1,
    consolidate_routers/1
]).

-define(ZERO, <<"0">>).

minimum_json(Ledger) ->
    #{<<"accounts">> => consolidate_accounts(Ledger),
      <<"hotspots">> => consolidate_hotspots(Ledger),
      <<"routers">> => consolidate_routers(Ledger),
      <<"meta">> => construct_meta(Ledger)}.

construct_meta(Ledger) ->
    {ok, H} = blockchain_ledger_v1:current_height(Ledger),
    #{<<"ledger_height">> => H}.

-spec export(blockchain_ledger_v1:ledger()) -> any().
export(Ledger) ->
    [
        {accounts, export_accounts(Ledger)},
        {gateways, export_gateways(Ledger)},
        {dcs, export_dcs(Ledger)},
        {routes, export_routes(Ledger)},
        {validators, export_validators(Ledger)}
    ].

-spec export_accounts(blockchain_ledger_v1:ledger()) -> list().
export_accounts(Ledger) ->
    lists:foldl(
        fun({Address, Entry}, Acc) ->
            ToAdd0 = [{address, libp2p_crypto:bin_to_b58(Address)},
                      {hnt, integer_to_list(blockchain_ledger_entry_v2:balance(Entry))},
                      {hst, integer_to_list(blockchain_ledger_entry_v2:balance(Entry, hst))},
                      {mobile, integer_to_list(blockchain_ledger_entry_v2:balance(Entry, mobile))},
                      {iot, integer_to_list(blockchain_ledger_entry_v2:balance(Entry, iot))}
                      ],
            ToAdd = lists:filter(fun({_, V}) -> V > 0 end, ToAdd0),
            [ToAdd | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:entries(Ledger))
    ).

-spec export_gateways(blockchain_ledger_v1:ledger()) -> list().
export_gateways(Ledger) ->
    lists:foldl(
        fun({GatewayAddress, Gateway}, Acc) ->
            Loc =
            case blockchain_ledger_gateway_v2:location(Gateway) of
                undefined -> "null";
                L -> integer_to_list(L)
            end,

            IsDataonly =
            case blockchain_ledger_gateway_v2:mode(Gateway) of
                dataonly -> true;
                _ -> false
            end,

            [[{gateway_address, libp2p_crypto:bin_to_b58(GatewayAddress)},
              {owner_address, libp2p_crypto:bin_to_b58(blockchain_ledger_gateway_v2:owner_address(Gateway))},
              {location, Loc},
              {gain, blockchain_ledger_gateway_v2:gain(Gateway)},
              {dataonly, IsDataonly},
              {altitude, blockchain_ledger_gateway_v2:elevation(Gateway)}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:active_gateways(Ledger))
    ).

-spec export_dcs(blockchain_ledger_v1:ledger()) -> list().
export_dcs(Ledger) ->
    lists:foldl(
        fun({Address, DCEntry}, Acc) ->
            [[{address, libp2p_crypto:bin_to_b58(Address)},
              {dc_balance, integer_to_list(blockchain_ledger_data_credits_entry_v1:balance(DCEntry))}] | Acc]
        end,
        [],
        maps:to_list(blockchain_ledger_v1:dc_entries(Ledger))
    ).

-spec export_validators(blockchain_ledger_v1:ledger()) -> list().
export_validators(Ledger) ->
    blockchain_ledger_v1:fold_validators(
      fun(Val, Acc) ->
        [[{owner, libp2p_crypto:bin_to_b58(blockchain_ledger_validator_v1:owner_address(Val))},
          {stake, integer_to_list(blockchain_ledger_validator_v1:stake(Val))}] | Acc]
      end,
      [],
      Ledger).

-spec export_chain_vars(blockchain_ledger_v1:ledger()) -> list().
export_chain_vars(Ledger) ->
    lists:sort(blockchain_ledger_v1:snapshot_vars(Ledger)).

-spec export_routes(blockchain_ledger_v1:ledger()) -> list().
export_routes(Ledger) ->
    {ok, Routes} = blockchain_ledger_v1:get_routes(Ledger),

    lists:foldl(
      fun(RoutingV1, Acc) ->
        RouterAddresses = [libp2p_crypto:bin_to_b58(I) || I <- blockchain_ledger_routing_v1:addresses(RoutingV1)],
        ToAdd = [
                 {oui, blockchain_ledger_routing_v1:oui(RoutingV1)},
                 {owner, libp2p_crypto:bin_to_b58(blockchain_ledger_routing_v1:owner(RoutingV1))},
                 {router_addresses, RouterAddresses}
                ],
        [ToAdd | Acc]
      end,
      [],
      Routes).

consolidate_accounts(Ledger) ->
    Accounts = export_accounts(Ledger),
    DCs = export_dcs(Ledger),
    Vals = export_validators(Ledger),
    AM = lists:foldl(
           fun(Item, Acc) ->
                Value = #{<<"hnt">> => binary:list_to_bin(proplists:get_value(hnt, Item, "0")),
                          <<"mobile">> => binary:list_to_bin(proplists:get_value(mobile, Item, "0")),
                          <<"hst">> => binary:list_to_bin(proplists:get_value(hst, Item, "0")),
                          <<"iot">> => binary:list_to_bin(proplists:get_value(iot, Item, "0"))
                         },
                Key = binary:list_to_bin(proplists:get_value(address, Item)),
                maps:put(Key, Value, Acc)
           end, #{}, Accounts),

    DM = lists:foldl(
           fun(Item, Acc) ->
                Value = #{<<"dc_balance">> => binary:list_to_bin(proplists:get_value(dc_balance, Item, "0"))},
                Key = binary:list_to_bin(proplists:get_value(address, Item)),
                maps:put(Key, Value, Acc)
           end, #{}, DCs),

    VM = lists:foldl(
           fun(Item, Acc) ->
                Key = binary:list_to_bin(proplists:get_value(owner, Item)),
                Value = #{<<"staked_hnt">> => binary:list_to_bin(proplists:get_value(stake, Item, "0"))},
                maps:put(Key, Value, Acc)
           end,
           #{},
           Vals),

    AccountKeys = maps:keys(AM),
    DCKeys = maps:keys(DM),
    ValKeys = maps:keys(VM),

    TotalKeys = sets:to_list(sets:from_list(AccountKeys ++ DCKeys ++ ValKeys)),

    OverallAccounts =
    lists:foldl(
      fun(Key, Acc) ->
              Account = maps:get(Key, AM, undefined),
              DC = maps:get(Key, DM, undefined),
              Val = maps:get(Key, VM, undefined),

              Value =
              case {Account, DC, Val} of

                  {undefined, undefined, undefined} -> #{};
                  {undefined, undefined, V} ->
                      #{<<"hnt">> => ?ZERO,
                        <<"mobile">> => ?ZERO,
                        <<"hst">> => ?ZERO,
                        <<"iot">> => ?ZERO,
                        <<"dc">> => ?ZERO,
                        <<"staked_hnt">> => maps:get(<<"staked_hnt">>, V, ?ZERO)
                       };
                  {undefined, D, undefined} ->
                      #{<<"hnt">> => ?ZERO,
                        <<"mobile">> => ?ZERO,
                        <<"hst">> => ?ZERO,
                        <<"iot">> => ?ZERO,
                        <<"dc">> => maps:get(<<"dc_balance">>, D, ?ZERO),
                        <<"staked_hnt">> => ?ZERO
                       };
                  {undefined, D, V} ->
                      #{<<"hnt">> => ?ZERO,
                        <<"mobile">> => ?ZERO,
                        <<"hst">> => ?ZERO,
                        <<"iot">> => ?ZERO,
                        <<"dc">> => maps:get(<<"dc_balance">>, D, ?ZERO),
                        <<"staked_hnt">> => maps:get(<<"staked_hnt">>, V, ?ZERO)
                       };
                  {A, undefined, undefined} ->
                      M = #{<<"dc">> => ?ZERO, <<"staked_hnt">> => ?ZERO},
                      maps:merge(A, M);
                  {A, undefined, V} ->
                      M = #{<<"dc">> => ?ZERO, <<"staked_hnt">> => maps:get(<<"staked_hnt">>, V, ?ZERO)},
                      maps:merge(A, M);
                  {A, D, undefined} ->
                      M = #{<<"dc">> => maps:get(<<"dc_balance">>, D, ?ZERO), <<"staked_hnt">> => ?ZERO},
                      maps:merge(A, M);
                  {A, D, V} ->
                      M = #{<<"dc">> => maps:get(<<"dc_balance">>, D, ?ZERO),
                            <<"staked_hnt">> => maps:get(<<"staked_hnt">>, V, ?ZERO)},
                      maps:merge(A, M)
              end,
              case map_size(Value) of
                  0 -> Acc;
                  _ -> maps:put(Key, Value, Acc)
              end
      end,
      #{},
      TotalKeys),
    OverallAccounts.

consolidate_hotspots(Ledger) ->
    Hotspots = export_gateways(Ledger),
    HM = lists:foldl(
           fun(Item, Acc) ->
                Value = #{<<"owner">> => binary:list_to_bin(proplists:get_value(owner_address, Item, "null")),
                          <<"location">> => binary:list_to_bin(proplists:get_value(location, Item, "null")),
                          <<"gain">> => proplists:get_value(gain, Item, "null"),
                          <<"altitude">> => proplists:get_value(altitude, Item, "null"),
                          <<"dataonly">> => proplists:get_value(dataonly, Item, false)
                         },
                Key = binary:list_to_bin(proplists:get_value(gateway_address, Item)),
                maps:put(Key, Value, Acc)
           end, #{}, Hotspots),
    HM.

consolidate_routers(Ledger) ->
    Routers = export_routes(Ledger),
    RM = lists:foldl(
           fun(Item, Acc) ->
                Value = #{<<"router_addresses">> => [binary:list_to_bin(I) || I <- proplists:get_value(router_addresses, Item, [])],
                          <<"owner">> => binary:list_to_bin(proplists:get_value(owner, Item, "null"))
                         },
                Key = proplists:get_value(oui, Item),
                maps:put(Key, Value, Acc)
           end, #{}, Routers),

    lists:foldl(
      fun({OUI,
           #{<<"owner">> := Owner, <<"router_addresses">> := RouterAddrs}},
          Acc) ->
              lists:foldl(
                fun(R, Acc2) ->
                        maps:put(R, #{oui => OUI, owner => Owner}, Acc2)
                end, Acc,
                RouterAddrs)
      end, #{}, maps:to_list(RM)).
