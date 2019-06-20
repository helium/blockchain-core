-module(blockchain_election).

-export([
         new_group/3,
         has_new_group/1
        ]).

new_group(Ledger, Hash, Size) ->
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    Gateways1 =
        [begin
             {_, _, Score} = blockchain_ledger_gateway_v1:score(Gw, Height),
             {Score, Addr}
         end
         || {Addr, Gw} <- maps:to_list(Gateways0)],

    Gateways = lists:sort(Gateways1),
    blockchain_utils:rand_from_hash(Hash),
    select(Gateways, Gateways, Size, []).

select(_, _, 0, Acc) ->
    lists:reverse(Acc);
select([], Gateways, Size, Acc) ->
    select(Gateways, Gateways, Size, Acc);
select([{_Score, Gw} | Rest], Gateways, Size, Acc) ->
    case rand:uniform(100) of
        %% this pctage should be chain-var tunable
        N when N =< 60 ->
            select(Rest, lists:keydelete(Gw, 2, Gateways), Size - 1, [Gw | Acc]);
        _ ->
            select(Rest, Gateways, Size, Acc)
    end.

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
