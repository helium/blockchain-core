-module(blockchain_election).

-export([
         new_group/3,
         has_new_group/1
        ]).

new_group(Ledger, Hash, Size) ->
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {ok, OldGroup0} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, SelectPct} = blockchain_ledger_v1:config(election_selection_pct, Ledger),
    {ok, ReplacementFactor} = blockchain_ledger_v1:config(election_replacement_factor, Ledger),

    OldLen = length(OldGroup0),
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

    %% TODO: get poc interval properly on the chain
    PoCInterval = blockchain_txn_poc_request_v1:challenge_interval(),

    %% annotate with score while removing dupes
    {OldGroupScored, GatewaysScored} =
        maps:fold(
          fun(Addr, Gw, {Old, Candidates} = Acc) ->
                  Last0 = last(blockchain_ledger_gateway_v1:last_poc_challenge(Gw)),
                  {_, _, Score} = blockchain_ledger_gateway_v1:score(Addr, Gw, Height),
                  Last = Height - Last0,
                  Missing = Last > 3 * PoCInterval,
                  case lists:member(Addr, OldGroup0) of
                      true ->
                          OldGw =
                              case Missing of
                                  %% make sure that non-functioning
                                  %% nodes sort first regardless of score
                                  true ->
                                      {Score - 5, Addr};
                                  _ ->
                                      {Score, Addr}
                              end,
                          {[OldGw | Old], Candidates};
                      _ ->
                          case Missing of
                              %% don't bother to add to the candidate list
                              true ->
                                  Acc;
                              _ ->
                                  {Old, [{Score, Addr} | Candidates]}
                          end
                  end
          end,
          {[], []},
          Gateways0),

    %% sort high to low to prioritize high-scoring gateways for selection
    Gateways = lists:reverse(lists:sort(GatewaysScored)),
    blockchain_utils:rand_from_hash(Hash),
    New = select(Gateways, Gateways, Replace, SelectPct, []),

    %% sort low to high to prioritize low scoring and down gateways
    %% for removal from the group
    OldGroup = lists:sort(OldGroupScored),
    Rem = OldGroup0 -- select(OldGroup, OldGroup, Remove, SelectPct, []),
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
