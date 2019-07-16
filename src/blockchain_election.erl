-module(blockchain_election).

-export([
         new_group/4,
         has_new_group/1
        ]).

new_group(Ledger, Hash, Size, Delay) ->
    Gateways0 = blockchain_ledger_v1:active_gateways(Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {ok, OldGroup0} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, SelectPct} = blockchain_ledger_v1:config(election_selection_pct, Ledger),
    {ok, ReplacementFactor} = blockchain_ledger_v1:config(election_replacement_factor, Ledger),
    %% increase this to make removal more gradual, decrease to make it less so
    {ok, ReplacementSlope} = blockchain_ledger_v1:config(election_replacement_slope, Ledger),
    {ok, Interval} = blockchain:config(election_restart_interval, Ledger),

    OldLen = length(OldGroup0),
    case Size == OldLen of
        true ->
            MinSize = ((OldLen - 1) div 3) + 1, % smallest remainder we will allow
            BaseRemove =  floor(Size/ReplacementFactor), % initial remove size
            Removable = OldLen - MinSize - BaseRemove,
            %% use tanh to get a gradually increasing (but still clamped to 1) value for
            %% scaling the size of removal as delay increases
            %% vihu argues for the logistic function here, for better
            %% control, but tanh is simple
            AdditionalRemove = floor(Removable * math:tanh((Delay/Interval) / ReplacementSlope)),

            Remove = Replace = BaseRemove + AdditionalRemove;
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
                  {_, _, Score} = blockchain_ledger_gateway_v1:score(Addr, Gw, Height, Ledger),
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

    lager:info("scored old group: ~p scored gateways: ~p",
               [tup_to_animal(OldGroupScored), tup_to_animal(GatewaysScored)]),

    %% sort high to low to prioritize high-scoring gateways for selection
    Gateways = lists:reverse(lists:sort(GatewaysScored)),
    blockchain_utils:rand_from_hash(Hash),
    New = select(Gateways, Gateways, min(Replace, length(Gateways)), SelectPct, []),

    %% sort low to high to prioritize low scoring and down gateways
    %% for removal from the group
    OldGroup = lists:sort(OldGroupScored),
    Rem = OldGroup0 -- select(OldGroup, OldGroup, min(Remove, length(New)), SelectPct, []),
    Rem ++ New.

tup_to_animal(TL) ->
    lists:map(fun({Scr, Addr}) ->
                      {ok, N} = erl_angry_purple_tiger:animal_name(Addr),
                      {Scr, N}
              end,
              TL).

last(undefined) ->
    0;
last(N) when is_integer(N) ->
    N.

select(_, [], _, _Pct, Acc) ->
    lists:reverse(Acc);
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
