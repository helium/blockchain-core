%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v3 implementation.
%%%
%%% The targeting mechanism is based on the following conditions:
%%% - Filter hotspots which haven't done a poc request for a long time
%%% - Target selection is entirely random
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v3).

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
    %% Get all hexes once
    HexList = sorted_hex_list(Ledger),
    %% Initialize seed with Hash once
    InitRandState = blockchain_utils:rand_state(Hash),
    %% Initial zone to begin targeting into
    {ok, {InitHex, InitHexRandState}} = choose_zone(InitRandState, HexList),
    target_(ChallengerPubkeyBin, Ledger, Vars, HexList, [{InitHex, InitHexRandState}]).

%% @doc Finds a potential target to start the path from.
-spec target_(ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
              Ledger :: blockchain_ledger_v1:ledger(),
              Vars :: map(),
              HexList :: [h3:h3_index()],
              Attempted :: [{h3:h3_index(), rand:state()}]) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
target_(ChallengerPubkeyBin, Ledger, Vars, HexList, [{Hex, HexRandState0} | Tail]=_Attempted) ->
    %% Get a list of gateway pubkeys within this hex
    {ok, AddrList0} = blockchain_ledger_v1:get_hex(Hex, Ledger),
    %% Remove challenger if present and also remove gateways who haven't challenged
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {HexRandState, AddrList} = limit_addrs(Vars, HexRandState0, AddrList0),

    case filter(AddrList, ChallengerPubkeyBin, Ledger, Height, Vars) of
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
             Ledger :: blockchain_ledger_v1:ledger(),
             Height :: non_neg_integer(),
             Vars :: map()) -> [libp2p_crypto:pubkey_bin()].
filter(AddrList, ChallengerPubkeyBin, Ledger, Height, Vars) ->
    lists:filter(fun(A) ->
                         {ok, Mode} = blockchain_ledger_v1:find_gateway_mode(A, Ledger),
                         A /= ChallengerPubkeyBin andalso
                         is_active(A, Height, Vars, Ledger) andalso
                         blockchain_ledger_gateway_v2:is_valid_capability(Mode, ?GW_CAPABILITY_POC_CHALLENGEE, Ledger)
                 end,
                 AddrList).

-spec is_active(Gateway :: libp2p_crypto:pubkey_bin(),
                Height :: non_neg_integer(),
                Vars :: map(),
                Ledger :: blockchain_ledger_v1:ledger()) -> boolean().
is_active(Gateway, Height, Vars, Ledger) ->
    case blockchain_ledger_v1:find_gateway_last_challenge(Gateway, Ledger) of
        {ok, undefined} ->
            %% No POC challenge, don't include
            false;
        {ok, C} ->
            case application:get_env(blockchain, disable_poc_v4_target_challenge_age, false) of
                true ->
                    %% Likely disabled for testing
                    true;
                false ->
                    %% Check challenge age is recent depending on the set chain var
                    (Height - C) < challenge_age(Vars)
            end
    end.

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec challenge_age(Vars :: map()) -> pos_integer().
challenge_age(Vars) ->
    maps:get(poc_v4_target_challenge_age, Vars).

-spec prob_randomness_wt(Vars :: map()) -> float().
prob_randomness_wt(Vars) ->
    maps:get(poc_v5_target_prob_randomness_wt, Vars).


-spec sorted_hex_list(Ledger :: blockchain_ledger_v1:ledger()) -> [{h3:h3_index(), pos_integer()}].
sorted_hex_list(Ledger) ->
    %% Grab the list of parent hexes
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case get({'$cache_hex_list', Height}) of
        undefined ->
            {ok, Hexes} = blockchain_ledger_v1:get_hexes_list(Ledger),
            put({'$cache_hex_list', Height}, Hexes),
            Hexes;
        Hexes ->
            Hexes
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

-spec limit_addrs(map(), RandState, [Witness]) -> {RandState, [Witness]}.
limit_addrs(Vars, RandState, Witnesses) when is_map(Vars) ->
    %% XXX Value could literally be 'undefined' or just missing,
    %%     so defaulting to 'undefined' handles both cases:
    case maps:get(?poc_witness_consideration_limit, Vars, undefined) of
        undefined ->
            {RandState, Witnesses};
        Limit when is_integer(Limit) ->
            blockchain_utils:deterministic_subset(Limit, RandState, Witnesses)
    end.
