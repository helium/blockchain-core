-module(blockchain_election).

-export([
         new_group/3,
         has_new_group/1
        ]).

new_group(Ledger, Hash, Size) ->
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, SelectPct} = blockchain_ledger_v1:config(election_selection_pct, Ledger),
    {ok, ReplacementFactor} = blockchain_ledger_v1:config(election_replacement_factor, Ledger),

    OldLen = length(OldGroup),
    case Size == OldLen of
        true ->
            Remove = Replace = floor(Size/ReplacementFactor);
        %% growing
        false when Size > OldLen ->
            Remove = 0,
            Replace = Size - OldLen;
        %% shrinking
        false ->
            Remove = OldLen - Size,
            Replace = 0
    end,

    %% annotate with score while removing dupes
    Gateways1 =
        [case lists:member(Addr, OldGroup) of
             true ->
                 [];
             _ ->
                 {_, _, Score} = blockchain_ledger_gateway_v1:score(Gw, Height),
                 {Score, Addr}
         end
         || {Addr, Gw} <- maps:to_list(Gateways0)],

    Gateways2 = lists:flatten(Gateways1),
    Gateways = lists:sort(Gateways2),
    blockchain_utils:rand_from_hash(Hash),
    New = select(Gateways, Gateways, Replace, SelectPct, []),
    OldGroupWrap = lists:zip([ignored || _ <- lists:seq(1, OldLen)], OldGroup),
    Rem = OldGroup -- select(OldGroupWrap, OldGroupWrap, Remove, SelectPct, []),
    Rem ++ New.

select(_, _, 0, _Pct, Acc) ->
    lists:reverse(Acc);
select([], Gateways, Size, Pct, Acc) ->
    select(Gateways, Gateways, Size, Pct, Acc);
select([{_Score, Gw} | Rest], Gateways, Size, Pct, Acc) ->
    case rand:uniform(100) of
        %% this pctage should be chain-var tunable
        N when N =< Pct ->
            select(Rest, lists:keydelete(Gw, 2, Gateways), Size - 1, Pct, [Gw | Acc]);
        _ ->
            select(Rest, Gateways, Size, Pct, Acc)
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
