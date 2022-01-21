%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v4 implementation.
%%%
%%% The targeting mechanism is based on the following conditions:
%%% - Filter hotspots which haven't done a poc request for a long time
%%% - Target selection is entirely random
%%% - Uses h3dex for performance
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v4).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_caps.hrl").

-export([
         target/4
        ]).

-spec target(ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
             Hash :: binary(),
             Ledger :: blockchain_ledger_v1:ledger(),
             Vars :: map()) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
target(ChallengerPubkeyBin, Hash, Ledger, Vars) ->
    %% Initialize seed with Hash once
    InitRandState = blockchain_utils:rand_state(Hash),
    %% Get some random hexes
    {HexList, NewRandState} = hex_list(Ledger, InitRandState),
    %% Initial zone to begin targeting into
    {ok, {InitHex, InitHexRandState}} = choose_zone(NewRandState, HexList),
    target_(ChallengerPubkeyBin, Ledger, Vars, HexList, [{InitHex, InitHexRandState}]).

%% @doc Finds a potential target to start the path from.
-spec target_(ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
              Ledger :: blockchain_ledger_v1:ledger(),
              Vars :: map(),
              HexList :: [h3:h3_index()],
              Attempted :: [{h3:h3_index(), rand:state()}]) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
target_(ChallengerPubkeyBin, Ledger, Vars, HexList, [{Hex, HexRandState0} | Tail]=_Attempted) ->
    %% Get a list of gateway pubkeys within this hex
    AddrMap = blockchain_ledger_v1:lookup_gateways_from_hex(Hex, Ledger),
    AddrList0 = lists:flatten(maps:values(AddrMap)),
    %% Remove challenger if present and also remove gateways who haven't challenged

    {HexRandState, AddrList} = limit_addrs(Vars, HexRandState0, AddrList0),

    case filter(AddrList, ChallengerPubkeyBin, Ledger) of
        FilteredList when length(FilteredList) >= 1 ->
            %% Assign probabilities to each of these gateways
            Prob = blockchain_utils:normalize_float(prob_randomness_wt(Vars) * 1.0),
            ProbTargets = lists:map(
                            fun(A) ->
                                    {A, Prob}
                            end,
                            FilteredList),
            %% Sort the scaled probabilities in default order by gateway pubkey_bin
            %% make sure that we carry the rand_state through for determinism
            {RandVal, TargetRandState} = rand:uniform_s(HexRandState),
            {ok, TargetPubkeybin} = blockchain_utils:icdf_select(lists:keysort(1, ProbTargets), RandVal),
            {ok, {TargetPubkeybin, TargetRandState}};
        _ ->
            %% no eligible target in this zone
            %% find a new zone
            {ok, New} = choose_zone(HexRandState, HexList),
            %% remove Hex from attemped, add New to attempted and retry
            target_(ChallengerPubkeyBin, Ledger, Vars, HexList, [New | Tail])
    end.

%% @doc Filter gateways based on these conditions:
%% - Inactive gateways (those which haven't challenged in a long time).
%% - Dont target the challenger gateway itself.
%% - Dont target GWs which do not have the releveant capability
-spec filter(AddrList :: [libp2p_crypto:pubkey_bin()],
             ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
             Ledger :: blockchain_ledger_v1:ledger()) -> [libp2p_crypto:pubkey_bin()].
filter(AddrList, ChallengerPubkeyBin, Ledger) ->
    lists:filter(fun(A) ->
                         {ok, Mode} = blockchain_ledger_v1:find_gateway_mode(A, Ledger),
                         A /= ChallengerPubkeyBin andalso
                         blockchain_ledger_gateway_v2:is_valid_capability(Mode, ?GW_CAPABILITY_POC_CHALLENGEE, Ledger)
                 end,
                 AddrList).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec prob_randomness_wt(Vars :: map()) -> float().
prob_randomness_wt(Vars) ->
    maps:get(poc_v5_target_prob_randomness_wt, Vars).


-spec hex_list(Ledger :: blockchain_ledger_v1:ledger(), RandState :: rand:state()) -> {[{h3:h3_index(), pos_integer()}], rand:state()}.
hex_list(Ledger, RandState) ->
    {ok, Count} = blockchain:config(?poc_target_pool_size, Ledger),
    hex_list(Ledger, RandState, Count, []).

hex_list(_Ledger, RandState, 0, Acc) ->
    %% usort so if we selected duplicates they don't get overselected
    {lists:usort(Acc), RandState};
hex_list(Ledger, RandState, HexCount, Acc) ->
    {ok, Hex, NewRandState} = blockchain_ledger_v1:random_targeting_hex(RandState, Ledger),
    case blockchain_ledger_v1:count_gateways_in_hex(Hex, Ledger) of
        0 ->
            %% this should not happen, but handle it anyway
            hex_list(Ledger, NewRandState, HexCount, Acc);
        GWCount ->
            hex_list(Ledger, NewRandState, HexCount - 1, [{Hex, GWCount}|Acc])
    end.


-spec choose_zone(RandState :: rand:state(),
                  HexList :: [h3:h3_index()]) -> {ok, {h3:h3_index(), rand:state()}}.
choose_zone(RandState, HexList) ->
    {HexVal, HexRandState} = rand:uniform_s(RandState),
    case blockchain_utils:icdf_select(HexList, HexVal) of
        {error, zero_weight} ->
            %% retry
            choose_zone(HexRandState, HexList);
        {ok, Hex} ->
            {ok, {Hex, HexRandState}}
    end.

limit_addrs(#{?poc_witness_consideration_limit := Limit}, RandState, Witnesses) ->
    blockchain_utils:deterministic_subset(Limit, RandState, Witnesses);
limit_addrs(_Vars, RandState, Witnesses) ->
    {RandState, Witnesses}.
