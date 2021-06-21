-module(blockchain_election).

-export([
         new_group/4,
         has_new_group/1,
         election_info/1, election_info/2,
         icdf_select/3,
         adjust_old_group/2,
         adjust_old_group_v2/2,
         validator_penalties/2
        ]).

-include("blockchain_vars.hrl").
-include("blockchain_caps.hrl").

-import(blockchain_utils, [normalize_float/1]).

-record(val_v1,
        {
         prob :: float(),
         heartbeat :: pos_integer() | undefined,
         addr :: libp2p_crypto:pubkey_bin()
        }).

-ifdef(TEST).

-export([
         val/3, val_addr/1, val_hb/1,
         val_dedup/3,
         determine_sizes_v2_math/6,
         select_removals/6
        ]).

val(Prob, HB, Addr) ->
    #val_v1{prob = Prob,
            heartbeat = HB,
            addr = Addr}.

val_addr(#val_v1{addr = A}) when is_binary(A) ->
    A.

val_hb(#val_v1{heartbeat = HB}) ->
    HB.

-endif.

new_group(Ledger, Hash, Size, Delay) ->
    case blockchain_ledger_v1:config(?election_version, Ledger) of
        {ok, N} when N >= 5 ->
            new_group_v5(Ledger, Hash, Size, Delay);
        {ok, N} when N >= 4 ->
            new_group_v4(Ledger, Hash, Size, Delay);
        {ok, N} when N >= 3 ->
            new_group_v3(Ledger, Hash, Size, Delay);
        {ok, N} when N == 2 ->
            new_group_v2(Ledger, Hash, Size, Delay);
        {error, not_found} ->
            new_group_v1(Ledger, Hash, Size, Delay)
    end.

new_group_v1(Ledger, Hash, Size, Delay) ->
    Gateways0 = gateways_filter(none, Ledger),

    {ok, OldGroup0} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, SelectPct} = blockchain_ledger_v1:config(?election_selection_pct, Ledger),

    OldLen = length(OldGroup0),
    {Remove, Replace} = determine_sizes(Size, OldLen, Delay, Ledger),

    {OldGroupScored, GatewaysScored} = score_dedup(OldGroup0, Gateways0, Ledger),

    lager:debug("scored old group: ~p scored gateways: ~p",
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

new_group_v2(Ledger, Hash, Size, Delay) ->
    {ok, OldGroup0} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, SelectPct} = blockchain_ledger_v1:config(?election_selection_pct, Ledger),
    {ok, RemovePct} = blockchain_ledger_v1:config(?election_removal_pct, Ledger),
    {ok, ClusterRes} = blockchain_ledger_v1:config(?election_cluster_res, Ledger),
    Gateways0 = gateways_filter(ClusterRes, Ledger),

    OldLen = length(OldGroup0),
    {Remove, Replace} = determine_sizes(Size, OldLen, Delay, Ledger),

    %% annotate with score while removing dupes
    {OldGroupScored, GatewaysScored} = score_dedup(OldGroup0, Gateways0, Ledger),

    lager:debug("scored old group: ~p scored gateways: ~p",
                [tup_to_animal(OldGroupScored), tup_to_animal(GatewaysScored)]),

    %% get the locations of the current consensus group at a particular h3 resolution
    Locations = locations(OldGroup0, Gateways0),

    %% sort high to low to prioritize high-scoring gateways for selection
    Gateways = lists:reverse(lists:sort(GatewaysScored)),
    blockchain_utils:rand_from_hash(Hash),
    New = select(Gateways, Gateways, min(Replace, length(Gateways)), SelectPct, [], Locations),

    %% sort low to high to prioritize low scoring and down gateways
    %% for removal from the group
    OldGroup = lists:sort(OldGroupScored),
    Rem = OldGroup0 -- select(OldGroup, OldGroup, min(Remove, length(New)), RemovePct, []),
    Rem ++ New.

new_group_v3(Ledger, Hash, Size, Delay) ->
    {ok, OldGroup0} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, SelectPct} = blockchain_ledger_v1:config(?election_selection_pct, Ledger),
    {ok, RemovePct} = blockchain_ledger_v1:config(?election_removal_pct, Ledger),
    {ok, ClusterRes} = blockchain_ledger_v1:config(?election_cluster_res, Ledger),
    Gateways0 = gateways_filter(ClusterRes, Ledger),

    OldLen = length(OldGroup0),
    {Remove, Replace} = determine_sizes(Size, OldLen, Delay, Ledger),

    %% annotate with score while removing dupes
    {OldGroupScored0, GatewaysScored} = score_dedup(OldGroup0, Gateways0, Ledger),

    OldGroupScored = adjust_old_group(OldGroupScored0, Ledger),

    lager:debug("scored old group: ~p scored gateways: ~p",
                [tup_to_animal(OldGroupScored), tup_to_animal(GatewaysScored)]),

    %% get the locations of the current consensus group at a particular h3 resolution
    Locations = locations(OldGroup0, Gateways0),

    %% sort high to low to prioritize high-scoring gateways for selection
    Gateways = lists:reverse(lists:sort(GatewaysScored)),
    blockchain_utils:rand_from_hash(Hash),
    New = select(Gateways, Gateways, min(Replace, length(Gateways)), SelectPct, [], Locations),

    %% sort low to high to prioritize low scoring and down gateways
    %% for removal from the group
    OldGroup = lists:sort(OldGroupScored),
    Rem = OldGroup0 -- select(OldGroup, OldGroup, min(Remove, length(New)), RemovePct, []),
    Rem ++ New.

new_group_v4(Ledger, Hash, Size, Delay) ->
    {ok, OldGroup0} = blockchain_ledger_v1:consensus_members(Ledger),

    {ok, SelectPct} = blockchain_ledger_v1:config(?election_selection_pct, Ledger),
    {ok, RemovePct} = blockchain_ledger_v1:config(?election_removal_pct, Ledger),
    {ok, ClusterRes} = blockchain_ledger_v1:config(?election_cluster_res, Ledger),
    %% a version of the filter that just gives everyone the same score
    Gateways0 = noscore_gateways_filter(ClusterRes, Ledger),

    OldLen = length(OldGroup0),
    {Remove, Replace} = determine_sizes(Size, OldLen, Delay, Ledger),

    %% remove dupes, sort
    {OldGroupDeduped, Gateways1} = dedup(OldGroup0, Gateways0, Ledger),

    %% adjust for bbas and seen votes
    OldGroupAdjusted = adjust_old_group(OldGroupDeduped, Ledger),

    %% get the locations of the current consensus group at a particular h3 resolution
    Locations = locations(OldGroup0, Gateways0),

    %% deterministically set the random seed
    blockchain_utils:rand_from_hash(Hash),
    %% random shuffle of all gateways
    Gateways = blockchain_utils:shuffle(Gateways1),
    New = select(Gateways, Gateways, min(Replace, length(Gateways)), SelectPct, [], Locations),

    %% sort low to high to prioritize low scoring and down gateways
    %% for removal from the group
    OldGroup = lists:sort(OldGroupAdjusted),
    Rem = OldGroup0 -- select(OldGroup, OldGroup, min(Remove, length(New)), RemovePct, []),
    Rem ++ New.

-spec new_group_v5(Ledger :: blockchain_ledger_v1:ledger(),
                   Hash :: binary(),
                   Size :: non_neg_integer(),
                   Delay :: non_neg_integer()) ->
          [libp2p_crypto:pubkey_bin()].
new_group_v5(Ledger, Hash, Size, Delay) ->
    %% some complications here in the transfer.

    %% we don't want to clutter this code with a bunch of gateway crap.  so we have a special,
    %% simple code path just for the transitional time, and then a clean path for afterwards

    %% deterministically set the random seed
    blockchain_utils:rand_from_hash(Hash),

    {ok, OldGroup0} = blockchain_ledger_v1:consensus_members(Ledger),

    OldLen = length(OldGroup0),
    {Remove, Replace} = determine_sizes_v2(Size, OldLen, Delay, Ledger),

    Validators0 = validators_filter(Ledger),

    %% remove dupes, sort, gather offline nodes.
    {OldGroupDeduped0, Offline, Validators} = val_dedup(OldGroup0, Validators0, Ledger),
    %% random shuffle of all candidate validators, and change format into something idcf can consume
    Validators1 = [{Addr, Prob}
                  || #val_v1{addr = Addr, prob = Prob} <- blockchain_utils:shuffle(Validators)],

    lager:debug("validators ~p ~p", [min(Replace, length(Validators1)), an2(Validators1)]),

    %% select replacement nodes from the candidate pool using iterative icdf
    New = icdf_select(Validators1, min(Replace, length(Validators1)), []),
    lager:debug("validators new ~p", [an(New)]),

    %% limit the number removed to the number of replacement nodes in the case that we're changing
    %% size this election and it would end us up with an undersized group
    NewLen = min(Remove, length(New)),
    ToRem =
        case have_gateways(OldGroup0, Ledger) of
            [] ->
                select_removals(NewLen, OldLen, Size, Offline, OldGroupDeduped0, Ledger);
            Gateways ->
                %% just sort and remove the first removal amount rather than selecting, leaving
                %% validators in to make up the number on the last round if needed.
                lists:sublist(lists:sort(Gateways), 1, NewLen)
        end,
    lager:debug("to rem ~p", [an(ToRem)]),
    %% shuffle the order to diffuse any ordering effects over time
    blockchain_utils:shuffle((OldGroup0 -- ToRem) ++ New).

%% mostly this is out for separate testing
select_removals(NewLen, OldLen, Size, Offline, OldGroupDeduped0, Ledger) ->
    RepLen =
        case NewLen == 0 of
            %% moving down, we need remove bad nodes, not just take the first Size
            true when OldLen > Size -> OldLen - Size;
            true -> 0;
            false -> NewLen
        end,
    %% if there are too many offline nodes, just pick those and re-add the remainder to
    %% the group.  ideally there would be a clean way to just cancel here, but I'm not
    %% sure that's plumbed through well enough.
    {Offline1, OldGroupDeduped, ReplaceFinal} =
        case length(Offline) of
            N when N > RepLen ->
                {ToRemove, Rem} = lists:split(RepLen, Offline),
                {ToRemove, OldGroupDeduped0 ++ Rem, 0};
            Len ->
                %% otherwise just adjust the size for removal selection and use the
                %% groups as-is
                {Offline, OldGroupDeduped0, RepLen - Len}
        end,
    %% adjust for bbas and seen votes
    OldGroupAdjusted = ?MODULE:adjust_old_group_v2(OldGroupDeduped, Ledger),
    lager:debug("old group ~p", [an2(OldGroupAdjusted)]),
    OfflineAddrs = [A || #val_v1{addr = A} <- Offline1],
    OfflineAddrs ++ icdf_select(lists:keysort(1, OldGroupAdjusted), ReplaceFinal, []).

an(M) ->
    lists:map(fun blockchain_utils:addr2name/1, M).

an2(M) ->
    lists:map(fun({Addr, P}) -> {blockchain_utils:addr2name(Addr), P} end, M).

icdf_select(_List, 0, Acc) ->
    Acc;
icdf_select(List, ToSelect, _Acc) when ToSelect > length(List) ->
    {error, not_enough_elements};
icdf_select(List, ToSelect, Acc) ->
    {ok, Elt} = blockchain_utils:icdf_select(List, rand:uniform()),
    icdf_select(lists:keydelete(Elt, 1, List), ToSelect - 1, [Elt | Acc]).

have_gateways(List, Ledger) ->
    lists:filter(
      fun(X) ->
              case blockchain_ledger_v1:get_validator(X, Ledger) of
                  {ok, _} ->
                      false;
                  {error, not_found} ->
                      true
              end
      end, List).

%% all blocks other than the first block in an election epoch contain
%% information about the nodes that consensus members saw during the
%% course of the block, and also information about which BBAs were
%% driven to completion.  Here, we iterate through all of the blocks
%% to gather that information, then summarize it, then apply penalties
%% to the nodes that have not been seen or that are failing to drive
%% their BBA executions to completion.
adjust_old_group(Group, Ledger) ->
    %% ideally would not have to do this but don't want to change the
    %% interface.
    #{start_height := Start,
      curr_height := End} = election_info(Ledger),
    %% annotate the ledger group (which is ordered properly), with each one's index.
    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    {_, Addrs} =
        lists:foldl(
          fun(Addr, {Index, Acc}) ->
                  {Index + 1, Acc#{Addr => Index}}
          end,
          {1, #{}},
          OldGroup),

    Blocks = [begin {ok, Block} = blockchain_ledger_v1:get_block(Ht, Ledger), Block end
              || Ht <- lists:seq(Start + 1, End)],

    Penalties = get_penalties(Blocks, length(Group), Ledger),
    lager:debug("penalties ~p", [Penalties]),

    %% now that we've accumulated all of the penalties, apply them to
    %% adjust the score for this group generation
    lists:map(
      fun({Score, Loc, Addr}) ->
              Index = maps:get(Addr, Addrs),
              Penalty = maps:get(Index, Penalties, 0.0),
              lager:debug("~s ~p ~p", [blockchain_utils:addr2name(Addr), Score, Penalty]),
              {normalize_float(Score - Penalty), Loc, Addr}
      end,
      Group).

adjust_old_group_v2(Group, Ledger) ->
    %% annotate the ledger group (which is ordered properly), with each one's index.
    {ok, OldGroup} = blockchain_ledger_v1:consensus_members(Ledger),
    Penalties = validator_penalties(OldGroup, Ledger),
    lager:debug("penalties ~p", [Penalties]),

    %% now that we've accumulated all of the penalties, apply them to
    %% adjust the score for this group generation
    lists:map(
      fun(#val_v1{prob = Prob, addr = Addr}) ->
              Penalty = maps:get(Addr, Penalties, 0.0),
              {Addr, normalize_float(Prob + Penalty)}
      end,
      Group).

validator_penalties(Group, Ledger) ->
    {_, Addrs} =
        lists:foldl(
          fun(Addr, {Index, Acc}) ->
                  {Index + 1, Acc#{Index => Addr}}
          end,
          {1, #{}},
          Group),

    #{start_height := Start,
      curr_height := End} = election_info(Ledger),
    Blocks = [begin {ok, Block} = blockchain_ledger_v1:get_block(Ht, Ledger), Block end
              || Ht <- lists:seq(Start + 2, End)],

    IndexedPenalties = get_penalties_v2(Blocks, length(Group), Ledger),
    maps:fold(fun(I, Pen, Acc) ->
                      Addr = maps:get(I, Addrs),
                      Acc#{Addr => Pen}
              end, #{}, IndexedPenalties).

calc_age_weighted_penalty(Amt, Limit, Height, Instances) ->
    Tot =
        lists:foldl(
          fun(H, Acc) ->
                  BlocksAgo = Height - H,
                  case BlocksAgo >= Limit of
                      true ->
                          Acc;
                      _ ->
                          %% 1 - ago/limit = linear inverse weighting for recency
                          Acc + (Amt * (1 - (BlocksAgo/Limit)))
                  end
          end,
          0.0,
          Instances),
    blockchain_utils:normalize_float(Tot).

get_penalties(Blocks, Sz, Ledger) ->
    {ok, BBAPenalty} = blockchain_ledger_v1:config(?election_bba_penalty, Ledger),
    {ok, SeenPenalty} = blockchain_ledger_v1:config(?election_seen_penalty, Ledger),

    BBAs = [begin
                BBA0 = blockchain_block_v1:bba_completion(Block),
                blockchain_utils:bitvector_to_map(Sz, BBA0)
            end || Block <- Blocks],

    Seens = [begin
                 Seen0 = blockchain_block_v1:seen_votes(Block),
                 %% votes here are lists of per-participant seen
                 %% information. condense here summarizes them, and a
                 %% node is only voted against if there are 2f+1
                 %% votes against it.
                 condense_votes(Sz, Seen0)
             end || Block <- Blocks],

    lager:debug("ct ~p bbas ~p seens ~p", [length(Blocks), BBAs, Seens]),

    Penalties0 =
        lists:foldl(
          fun(BBA, Acc) ->
                  maps:fold(
                    fun(Idx, false, A) ->
                            maps:update_with(Idx, fun(X) -> X + BBAPenalty end,
                                             BBAPenalty, A);
                       (_, _, A) ->
                            A
                    end,
                    Acc, BBA)
          end,
          #{},
          BBAs),

    lager:debug("penalties0 ~p", [Penalties0]),

    lists:foldl(
      fun(Seen, Acc) ->
              maps:fold(
                fun(Idx, false, A) ->
                        maps:update_with(Idx, fun(X) -> X + SeenPenalty end,
                                         SeenPenalty, A);
                   (_, _, A) ->
                        A
                end,
                Acc, Seen)
      end,
      Penalties0,
      Seens).

get_penalties_v2(Blocks, Sz, Ledger) ->
    {ok, BBAPenalty} = blockchain_ledger_v1:config(?election_bba_penalty, Ledger),
    {ok, SeenPenalty} = blockchain_ledger_v1:config(?election_seen_penalty, Ledger),
    {ok, PenaltyLimit} = blockchain_ledger_v1:config(?penalty_history_limit, Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    BBAs = [begin
                BBA0 = blockchain_block_v1:bba_completion(Block),
                Ht = blockchain_block_v1:height(Block),
                {Ht, blockchain_utils:bitvector_to_map(Sz, BBA0)}
            end || Block <- Blocks],

    BBAPenalties =
        lists:foldl(
          fun({Ht, BBA}, Acc) ->
                  %% we want to generate a list for each idx where we have a height entry for each place
                  %% where the BBA map is false for that index
                  maps:fold(
                    fun(Idx, false, A) ->
                            maps:update_with(Idx, fun(X) -> [Ht | X]  end, A);
                       (_, _, A) -> A
                    end, Acc, BBA)
          end,
          maps:from_list([{I, []} || I <- lists:seq(1, Sz)]),
          BBAs),

    lager:debug("bba pens ~p", [BBAPenalties]),

    Penalties0 =
        maps:map(
          fun(_Idx, HtList) ->
                  calc_age_weighted_penalty(BBAPenalty, PenaltyLimit, Height, HtList)
          end,
          BBAPenalties),

    Seens = [begin
                 Seen0 = blockchain_block_v1:seen_votes(Block),
                 Ht = blockchain_block_v1:height(Block),
                 %% votes here are lists of per-participant seen
                 %% information. condense here summarizes them, and a
                 %% node is only voted against if there are 2f+1
                 %% votes against it.
                 {Ht, condense_votes(Sz, Seen0)}
             end || Block <- Blocks],

    SeenPenalties =
        lists:foldl(
          fun({Ht, Seen}, Acc) ->
                  %% we want to generate a list for each idx where we have a height entry for each place
                  %% where the BBA map is false for that index
                  maps:fold(
                    fun(Idx, false, A) ->
                            maps:update_with(Idx, fun(X) -> [Ht | X]  end, A);
                       (_, _, A) -> A
                    end, Acc, Seen)
          end,
          maps:from_list([{I, []} || I <- lists:seq(1, Sz)]),
          Seens),

    lager:debug("ct ~p bbas ~p seens ~p", [length(Blocks), BBAs, Seens]),
    lager:debug("penalties0 ~p", [Penalties0]),

    maps:fold(
      fun(Idx, HtList, Acc) ->
              SeenPen = calc_age_weighted_penalty(SeenPenalty, PenaltyLimit, Height, HtList),
              maps:update_with(Idx, fun(X) -> SeenPen + X end, Acc)
      end,
      Penalties0,
      SeenPenalties).

condense_votes(Sz, Seen0) ->
    SeenMaps = [blockchain_utils:bitvector_to_map(Sz, S)
                || {_Idx, S} <- Seen0],
    %% 2f + 1 = N - ((N - 1) div 3)
    Threshold = Sz - ((Sz - 1) div 3),
    %% for seen we count `false` votes, which means that if enough other consensus members
    %% report us as not voting, we accrue a per round penalty.
    Counts =
        lists:foldl(
          %% Map here is a per-peer list of whether or not it saw a peer at Idx
          %% participating.  We accumulate these votes in Acc.
          fun(Map, Acc) ->
                  %% the inner fold updates the accumulator with each vote in the map
                  lists:foldl(
                    fun(Idx, A) ->
                            case Map of
                                %% false means we didn't see this peer.
                                #{Idx := false} ->
                                    maps:update_with(Idx, fun(X) -> X + 1 end, 1, A);
                                _ ->
                                    A
                            end
                    end,
                    Acc,
                    lists:seq(1, Sz))
          end,
          #{},
          SeenMaps),
    lager:debug("counts ~p", [Counts]),
    maps:map(fun(_Idx, Ct) ->
                     %% we do the opposite of the intuitive check here, because when we
                     %% use the condensed votes, true is good and false is bad, so we want
                     %% to return false when we have enough votes against a peer
                     Ct < Threshold
             end,
             Counts).

determine_sizes(Size, OldLen, Delay, Ledger) ->
    {ok, ReplacementFactor} = blockchain_ledger_v1:config(?election_replacement_factor, Ledger),
    %% increase this to make removal more gradual, decrease to make it less so
    {ok, ReplacementSlope} = blockchain_ledger_v1:config(?election_replacement_slope, Ledger),
    {ok, Interval} = blockchain:config(?election_restart_interval, Ledger),
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
    {Remove, Replace}.

determine_sizes_v2(Size, OldLen, Delay, Ledger) ->
    {ok, ReplacementFactor} = blockchain_ledger_v1:config(?election_replacement_factor, Ledger),
    %% increase this to make removal more gradual, decrease to make it less so
    {ok, ReplacementSlope} = blockchain_ledger_v1:config(?election_replacement_slope, Ledger),
    {ok, Interval} = blockchain:config(?election_restart_interval, Ledger),
    determine_sizes_v2_math(Size, OldLen, Delay,
                            ReplacementFactor, ReplacementSlope, Interval).

determine_sizes_v2_math(Size,
                        OldLen,
                        Delay,
                        ReplacementFactor,
                        ReplacementSlope,
                        Interval) ->
    case Size == OldLen of
        true ->
            %% lower the replacement ceiling because in big groups, bigger is much much harder
            MinSize = (2 * ((OldLen - 1) div 3)), % smallest remainder we will allow
            BaseRemove = ceil(Size/ReplacementFactor), % initial remove size
            Removable = OldLen - MinSize - BaseRemove,
            %% use tanh to get a gradually increasing (but still clamped to 1) value for
            %% scaling the size of removal as delay increases
            %% vihu argues for the logistic function here, for better
            %% control, but tanh is simple
            AdditionalRemove = floor(Removable * math:tanh((Delay/Interval) / ReplacementSlope)),

            Remove = Replace = BaseRemove + AdditionalRemove;
        %% growing
        false when Size > OldLen ->
            Remove = ceil(OldLen/ReplacementFactor),
            Replace0 = Size - OldLen,
            %% by removing a few, this allows us to grow even when there are dead nodes in the group
            Replace = Replace0 + Remove;
        %% shrinking
        false ->
            Remove = OldLen - Size,
            Replace = 0
    end,
    {Remove, Replace}.

gateways_filter(ClusterRes, Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    blockchain_ledger_v1:cf_fold(
      active_gateways,
      fun({Addr, BinGw}, Acc) ->
              Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
              case blockchain_ledger_gateway_v2:is_valid_capability(Gw, ?GW_CAPABILITY_CONSENSUS_GROUP, Ledger) of
                  true ->
                      Last0 = last(blockchain_ledger_gateway_v2:last_poc_challenge(Gw)),
                      Last = Height - Last0,
                      Loc = location(ClusterRes, Gw),
                      {_, _, Score} = blockchain_ledger_gateway_v2:score(Addr, Gw, Height, Ledger),
                      maps:put(Addr, {Last, Loc, Score}, Acc);
                  false ->
                      Acc

              end
      end,
      #{},
      Ledger).

score_dedup(OldGroup0, Gateways0, Ledger) ->
    PoCInterval = blockchain_utils:challenge_interval(Ledger),

    maps:fold(
      fun(Addr, {Last, Loc, Score}, {Old, Candidates} = Acc) ->
              Missing = Last > 3 * PoCInterval,
              case lists:member(Addr, OldGroup0) of
                  true ->
                      OldGw =
                          case Missing of
                              %% sub 5 to make sure that non-functioning nodes sort first regardless
                              %% of score, making them most likely to be deselected
                              true ->
                                  {Score - 5, Loc, Addr};
                              _ ->
                                  {Score, Loc, Addr}
                          end,
                      {[OldGw | Old], Candidates};
                  _ ->
                      case Missing of
                          %% don't bother to add to the candidate list
                          true ->
                              Acc;
                          _ ->
                              {Old, [{Score, Loc, Addr} | Candidates]}
                      end
              end
      end,
      {[], []},
      Gateways0).

%% we do some duplication here because score is expensive to calculate, so write alternate code
%% paths to avoid it

noscore_gateways_filter(ClusterRes, Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    blockchain_ledger_v1:cf_fold(
      active_gateways,
      fun({Addr, BinGw}, Acc) ->
              Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
              case blockchain_ledger_gateway_v2:is_valid_capability(Gw, ?GW_CAPABILITY_CONSENSUS_GROUP, Ledger) of
                  true ->
                      Last0 = last(blockchain_ledger_gateway_v2:last_poc_challenge(Gw)),
                      Last = Height - Last0,
                      Loc = location(ClusterRes, Gw),
                      %% instead of getting the score, start at 1.0 for all spots
                      %% we need something like a score for sorting the existing consensus group members
                      %% for performance grading
                      maps:put(Addr, {Last, Loc, 1.0}, Acc);
                  false ->
                      lager:debug("filtering gw due to invalid capability: ~p", [Addr]),
                      Acc
              end
      end,
      #{},
      Ledger).

dedup(OldGroup0, Gateways0, Ledger) ->
    PoCInterval = blockchain_utils:challenge_interval(Ledger),

    maps:fold(
      fun(Addr, {Last, Loc, Score}, {Old, Candidates} = Acc) ->
              Missing = Last > 3 * PoCInterval,
              case lists:member(Addr, OldGroup0) of
                  true ->
                      OldGw =
                          case Missing of
                              %% sub 5 to make sure that non-functioning nodes sort first regardless
                              %% of score, making them most likely to be deselected
                              true ->
                                  {Score - 5, Loc, Addr};
                              _ ->
                                  {Score, Loc, Addr}
                          end,
                      {[OldGw | Old], Candidates};
                  _ ->
                      case Missing of
                          %% don't bother to add to the candidate list
                          true ->
                              Acc;
                          _ ->
                              {Old, [{Score, Loc, Addr} | Candidates]}
                      end
              end
      end,
      {[], []},
      Gateways0).

locations(Group, Gws) ->
    GroupGws = maps:with(Group, Gws),
    maps:fold(
      fun(_Addr, {_, P, _}, Acc) ->
              Acc#{P => true}
      end,
      #{},
      GroupGws).

%% for backwards compatibility, generate a location that can never match
location(none, _Gw) ->
    none;
location(Res, Gw) ->
    case blockchain_ledger_gateway_v2:location(Gw) of
        undefined ->
            no_location;
        Loc ->
            h3:parent(Loc, Res)
    end.

tup_to_animal(TL) ->
    lists:map(fun({Scr, _Loc, Addr}) ->
                      {Scr, blockchain_utils:addr2name(Addr)}
              end,
              TL).

last(undefined) ->
    0;
last(N) when is_integer(N) ->
    N.

select(Candidates, Gateways, Size, Pct, Acc) ->
    select(Candidates, Gateways, Size, Pct, Acc, no_loc).


select(_, [], _, _Pct, Acc, _Locs) ->
    lists:reverse(Acc);
select(_, _, 0, _Pct, Acc, _Locs) ->
    lists:reverse(Acc);
select([], Gateways, Size, Pct, Acc, Locs) ->
    select(Gateways, Gateways, Size, Pct, Acc, Locs);
select([{_Score, Loc, Gw} | Rest], Gateways, Size, Pct, Acc, Locs) ->
    case rand:uniform(100) of
        N when N =< Pct ->
            case Locs of
                no_loc ->
                    select(Rest, lists:keydelete(Gw, 3, Gateways), Size - 1, Pct, [Gw | Acc], Locs);
                _ ->
                    %% check if we already have a group member in this h3 hex
                    case maps:is_key(Loc, Locs) of
                        true ->
                            select(Rest, lists:keydelete(Gw, 3, Gateways), Size, Pct, Acc, Locs);
                        _ ->
                            select(Rest, lists:keydelete(Gw, 3, Gateways), Size - 1, Pct,
                                   [Gw | Acc], Locs#{Loc => true})
                    end
            end;
        _ ->
            select(Rest, Gateways, Size, Pct, Acc, Locs)
    end.

has_new_group(Txns) ->
    MyAddress = blockchain_swarm:pubkey_bin(),
    case lists:filter(fun(T) ->
                              %% TODO: ideally move to versionless types?
                              blockchain_txn:type(T) == blockchain_txn_consensus_group_v1
                      end, Txns) of
        [Txn] ->
            Height = blockchain_txn_consensus_group_v1:height(Txn),
            Delay = blockchain_txn_consensus_group_v1:delay(Txn),
            {true,
             lists:member(MyAddress, blockchain_txn_consensus_group_v1:members(Txn)),
             Txn,
             {Height, Delay}};
        [_|_] ->
            lists:foreach(fun(T) ->
                                  case blockchain_txn:type(T) == blockchain_txn_consensus_group_v1 of
                                      true ->
                                          lager:warning("txn ~s", [blockchain_txn:print(T)]);
                                      _ -> ok
                                  end
                          end, Txns),
            error(duplicate_group_txn);
        [] ->
            false
    end.

election_info(Ledger) ->
    %% grab the current height and get the block.
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    election_info(Height, Ledger).

election_info(Height, Ledger) when is_integer(Height) ->
    {ok, Block} = blockchain_ledger_v1:get_block(Height, Ledger),

    %% get the election info
    {Epoch, StartHeight0} = blockchain_block_v1:election_info(Block),

    %% genesis block thinks that the start height is 0, but it is
    %% block 1, so force it.
    StartHeight = max(1, StartHeight0),

    %% get the election txn
    {ok, StartBlock} = blockchain_ledger_v1:get_block(StartHeight, Ledger),
    {ok, Txn} = get_election_txn(StartBlock),
    lager:debug("txn ~s", [blockchain_txn:print(Txn)]),
    ElectionHeight = blockchain_txn_consensus_group_v1:height(Txn),
    ElectionDelay = blockchain_txn_consensus_group_v1:delay(Txn),

    %% wrap it all up as a map

    #{
      epoch => Epoch,
      curr_height => Height,
      start_height => StartHeight,
      election_height => ElectionHeight,
      election_delay => ElectionDelay
     }.

get_election_txn(Block) ->
    Txns = blockchain_block:transactions(Block),
    case lists:filter(
           fun(T) ->
                   blockchain_txn:type(T) == blockchain_txn_consensus_group_v1
           end, Txns) of
        [Txn] ->
            {ok, Txn};
        _ ->
            {error, no_group_txn}
    end.

validators_filter(Ledger) ->
    {ok, MinStake} = blockchain:config(?validator_minimum_stake, Ledger),
    blockchain_ledger_v1:cf_fold(
      validators,
      fun({Addr, BinVal}, Acc) ->
              Val = blockchain_ledger_validator_v1:deserialize(BinVal),
              Stake = blockchain_ledger_validator_v1:stake(Val),
              Status = blockchain_ledger_validator_v1:status(Val),
              Penalty = blockchain_ledger_validator_v1:calculate_penalty_value(Val, Ledger),
              HB = blockchain_ledger_validator_v1:last_heartbeat(Val),
              case Stake >= MinStake andalso Status == staked of
                  true ->
                      maps:put(Addr, #val_v1{addr = Addr,
                                             heartbeat = HB,
                                             prob = Penalty}, Acc);
                  _ -> Acc
              end
      end,
      #{},
      Ledger).

val_dedup(OldGroup0, Validators0, Ledger) ->
    %% filter liveness here
    {ok, HBInterval} = blockchain:config(?validator_liveness_interval, Ledger),
    {ok, HBGrace} = blockchain:config(?validator_liveness_grace_period, Ledger),
    {ok, PenaltyFilter} = blockchain:config(?validator_penalty_filter, Ledger),

    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {Group, OfflineGroup, Pool} =
        maps:fold(
          fun(Addr, Val = #val_v1{heartbeat = Last, prob = Prob},
              {Old, Off, Candidates} = Acc) ->
                  Offline = (Height - Last) > (HBInterval + HBGrace),
                  case lists:member(Addr, OldGroup0) of
                      %% this clause keeps the old group out of the candidate list and additionally
                      %% marks really bad nodes for removal
                      true ->
                          lager:debug("name ~p in off ~p", [blockchain_utils:addr2name(Addr), Offline]),
                          case Offline of
                              true ->
                                  %% try and make sure offline nodes are selected
                                  {Old, [Val | Off], Candidates};
                              _ ->
                                  {[Val | Old], Off, Candidates}
                          end;
                      %% this clause handles generating the list of new nodes
                      %% to potentially add (Candidates)
                      _ ->
                          lager:debug("name ~p out off ~p",
                                      [blockchain_utils:addr2name(Addr), Offline]),
                          case Offline of
                              %% don't bother to add to the candidate list
                              true ->
                                  Acc;
                              _ ->
                                  case PenaltyFilter - Prob of
                                      %% don't even consider until some of
                                      %% these failures have aged out
                                      NewProb when NewProb =< 0.0 -> Acc;
                                      NewProb -> {Old, Off,
                                                  [Val#val_v1{prob = NewProb} | Candidates]}
                                  end
                          end
                  end
          end,
          {[], [], []},
          Validators0),

    %% there have been buggy cases where an unstaked validator has somehow made it into the
    %% group. when this happens, it won't make it into the group, because it has been filtered out
    %% of the broader pool.  in this case, make a new record for it with an extreme chance of being removed.
    case length(Group ++ OfflineGroup) < length(OldGroup0) of
        false -> {Group, OfflineGroup, Pool};
        true ->
            Missing = lists:filter(fun(A) ->
                                           not lists:keymember(A, #val_v1.addr, Group)
                                   end, OldGroup0),
            {Group,
             OfflineGroup ++ [#val_v1{addr = MAddr, prob = 1.0} || MAddr <- Missing],
             Pool}
    end.
