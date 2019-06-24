-module(blockchain_election).

-export([
         new_group/4,
         has_new_group/1
        ]).

new_group(Chain, Hash, Height, Size) ->
    Ledger = blockchain:ledger(Chain),
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),

    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, RestartInterval} = blockchain_ledger_v1:config(election_restart_interval, Ledger),
    {ok, SelectPct} = blockchain_ledger_v1:config(election_selection_pct, Ledger),
    {ok, ReplacementFactor} = blockchain_ledger_v1:config(election_replacement_factor, Ledger),

    %% TODO: get poc interval properly on the chain
    PoCInterval = blockchain_txn_poc_request_v1:challenge_interval(),

    Counts0 =
        lists:foldl(
          fun(H, Acc) ->
                  {ok, B} = blockchain:get_block(H, Chain),
                  Sigs = blockchain_block:signatures(B),
                  lists:foldl(
                    fun({Addr, _Sig}, Ctrs) ->
                            maps:update_with(Addr, fun(V) -> V + 1 end, 1, Ctrs)
                    end,
                    Acc,
                    Sigs)
          end,
          #{},
          lists:seq(Height - RestartInterval, Height)),

    Counts = [{Count, Addr} || {Addr, Count} <- maps:to_list(Counts0)],

    {_, Group} = lists:unzip(Counts),

    case lists:sort(Group) == lists:sort(OldGroup) of
        true ->
            ok;
        false ->
            throw({error, group_sig_mismatch})
    end,

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

    Gateways1 =
        [begin
             Last0 = last(blockchain_ledger_gateway_v1:last_poc_challenge(Gw)),
             Last = Height - Last0,
             case lists:member(Addr, OldGroup) orelse
                  Last > 3 * PoCInterval of
                 true ->
                     [];
                 _ ->
                     {_, _, Score} = blockchain_ledger_gateway_v1:score(Gw, Height),
                     {Score, Addr}
             end
         end
         || {Addr, Gw} <- maps:to_list(Gateways0)],

    OldGroupSorted = lists:reverse(lists:sort(Counts)),

    Gateways2 = lists:flatten(Gateways1),
    Gateways = lists:sort(Gateways2),
    blockchain_utils:rand_from_hash(Hash),
    New = select(Gateways, Gateways, Replace, SelectPct, []),
    Rem = OldGroup -- select(OldGroupSorted, OldGroupSorted, Remove, SelectPct, []),
    Rem ++ New.

last(undefined) ->
    0;
last(N) when is_integer(N) ->
    N.

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
