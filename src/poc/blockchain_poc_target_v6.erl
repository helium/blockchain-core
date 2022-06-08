%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v6 implementation.
%%%
%%% The targeting mechanism is based on the following conditions:
%%% - Deterministically identify a target hex based on public key
%%% - Deterministically select a challengee from target region based on private key
%%% - v6 utilises h3dex for more efficient targeting and GC
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v6).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_caps.hrl").

-export([
    target_zone/2,
    target/5
]).

-spec target_zone(
    RandState :: rand:state(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> {ok, {[h3:h3_index()], h3:h3_index(), rand:state()}} | {error, any()}.
target_zone(RandState, Ledger) ->
    %% Get some random hexes
    {HexList, NewRandState} = hex_list(Ledger, RandState),
    %% Initialize seed with Hash once
    %% Initial zone to begin targeting into
    case choose_zone(NewRandState, HexList) of
        {error, _} = ErrorResp -> ErrorResp;
        {ok, {Hex, HexRandState}} -> {ok, {HexList, Hex, HexRandState}}
    end.

%% @doc Finds all valid gateways in specified zone
-spec gateways_for_zone(
    ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map(),
    HexList :: [h3:h3_index()],
    Attempted :: [{h3:h3_index(), rand:state()}]
) -> {ok, libp2p_crypto:pubkey_bin(), rand:state()} | {error, any()}.
gateways_for_zone(
    ChallengerPubkeyBin,
    Ledger,
    Vars,
    HexList,
    Attempted
) ->
    {ok, Count} = blockchain:config(?poc_target_pool_size, Ledger),
    gateways_for_zone(ChallengerPubkeyBin,Ledger,Vars,HexList,Attempted, Count).

-spec gateways_for_zone(
    ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map(),
    HexList :: [h3:h3_index()],
    Attempted :: [{h3:h3_index(), rand:state()}],
    NumAttempts :: integer()
) -> {ok, libp2p_crypto:pubkey_bin(), rand:state()} | {error, any()}.
gateways_for_zone(
    _ChallengerPubkeyBin,
    _Ledger,
    _Vars,
    _HexList,
    _Attempted,
    0
)->
    {error, no_gateways_found};
gateways_for_zone(
    ChallengerPubkeyBin,
    Ledger,
    Vars,
    HexList,
    [{Hex, HexRandState0} | Tail] = _Attempted,
    NumAttempts
) ->
    %% Get a list of gateway pubkeys within this hex
    AddrMap = blockchain_ledger_v1:lookup_gateways_from_hex(Hex, Ledger),
    AddrList0 = lists:flatten(maps:values(AddrMap)),
    lager:debug("gateways for hex ~p: ~p", [Hex, AddrList0]),
    %% Limit max number of potential targets in the zone
    case find_active_addr(Vars, HexRandState0, AddrList0, Ledger) of
        {ok, Addr, HexRandState} ->
            {ok, Addr, HexRandState};
        {no_target, HexRandState} ->
            lager:debug("*** failed to find any filtered gateways for hex ~p, trying again", [Hex]),
            %% no eligible target in this zone
            %% find a new zone
            case choose_zone(HexRandState, HexList) of
                {error, _} = ErrorResp -> ErrorResp;
                {ok, New} ->
                    %% remove Hex from attempted, add New to attempted and retry
                    gateways_for_zone(ChallengerPubkeyBin, Ledger, Vars, HexList, [New | Tail], NumAttempts - 1)
            end
    end.

-spec target(
    ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
    InitTargetRandState :: rand:state(),
    ZoneRandState :: rand:state(),
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map()
) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}} | {error, any()}.
target(ChallengerPubkeyBin, InitTargetRandState, ZoneRandState, Ledger, Vars) ->
    %% Initial zone to begin targeting into
    case target_zone(ZoneRandState, Ledger) of
        {error, _} = ErrorResp ->
            ErrorResp;
        {ok, {HexList, Hex, _HexRandState}} ->
            target_(
                ChallengerPubkeyBin,
                InitTargetRandState,
                Ledger,
                Vars,
                HexList,
                Hex
            )
    end.

%% @doc Finds a potential target to start the path from.
-spec target_(
    ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
    InitTargetRandState :: rand:state(),
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map(),
    HexList :: [h3:h3_index()],
    InitHex :: h3:h3_index()
) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}} | {error, any()}.
target_(
    ChallengerPubkeyBin,
    InitTargetRandState,
    Ledger,
    Vars,
    HexList,
    InitHex
) ->
    case gateways_for_zone(
           ChallengerPubkeyBin,
           Ledger, Vars, HexList,
           [{InitHex, InitTargetRandState}]) of
        {error, no_gateways_found} ->
            {error, no_gateways_found};
        {error, empty_hex_list} ->
            {error, no_gateways_found};
        {ok, TargetPubkeyBin, RandState} ->
            {ok, {TargetPubkeyBin, RandState}}
        end.

-spec find_active_addr(
    Vars :: map(),
    RandState :: rand:state(),
    AddrList :: [libp2p_crypto:pubkey_bin()],
    Ledger :: blockchain_ledger_v1:ledger()
) -> {ok, libp2p_crypto:pubkey_bin(), rand:state()} |
          {no_target, rand:state()}.
find_active_addr(Vars, RandState, AddrList, Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ActivityFilterEnabled =
        case blockchain:config(poc_activity_filter_enabled, Ledger) of
            {ok, V} -> V;
            _ -> false
        end,
    MaxActivityAge = max_activity_age(Vars),
    {ShuffledList, RandState1} = blockchain_utils:shuffle(AddrList, RandState),

    case find_active_addr_(ShuffledList, ActivityFilterEnabled,
                           Height, MaxActivityAge, Ledger) of
        {ok, Addr} ->
            {ok, Addr, RandState1};
        not_found ->
            {no_target, RandState1}
    end.

find_active_addr_([], _Filter, _Height, _MaxAge, _Ledger) ->
    not_found;
find_active_addr_([Addr | Tail], FilterEnabled, Height, MaxActivityAge, Ledger) ->
    {ok, Gateway} = blockchain_ledger_v1:find_gateway_info(Addr, Ledger),
    Mode = blockchain_ledger_gateway_v2:mode(Gateway),
    LastActivity = blockchain_ledger_gateway_v2:last_poc_challenge(Gateway),
    case is_active(FilterEnabled, LastActivity, MaxActivityAge, Height) andalso
        blockchain_ledger_gateway_v2:is_valid_capability(
          Mode,
          ?GW_CAPABILITY_POC_CHALLENGEE,
          Ledger
         ) of
        true ->
            {ok, Addr};
        _ ->
            find_active_addr_(Tail, FilterEnabled, Height, MaxActivityAge, Ledger)
    end.

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------

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
            hex_list(Ledger, NewRandState, HexCount -1, Acc);
        GWCount ->
            hex_list(Ledger, NewRandState, HexCount - 1, [{Hex, GWCount}|Acc])
    end.

-spec choose_zone(
    RandState :: rand:state(),
    HexList :: [h3:h3_index()]
) -> {ok, {h3:h3_index(), rand:state()}} | {error, empty_hex_list}.
choose_zone(_RandState, [] = _HexList) ->
    {error, empty_hex_list};
choose_zone(RandState, HexList) ->
    {HexVal, HexRandState} = rand:uniform_s(RandState),
    case blockchain_utils:icdf_select(HexList, HexVal) of
        {error, zero_weight} ->
            %% retry
            choose_zone(HexRandState, HexList);
        {ok, Hex} ->
            lager:debug("choose hex success, found hex ~p", [Hex]),
            {ok, {Hex, HexRandState}}
    end.

-spec is_active(ActivityFilterEnabled :: boolean(),
                LastActivity :: pos_integer(),
                MaxActivityAge :: pos_integer(),
                Height :: pos_integer()) -> boolean().
is_active(true, undefined, _MaxActivityAge, _Height) ->
    false;
is_active(true, LastActivity, MaxActivityAge, Height) ->
    (Height - LastActivity) < MaxActivityAge;
is_active(_ActivityFilterEnabled, _Gateway, _Height, _Vars) ->
    true.

-spec max_activity_age(Vars :: map()) -> pos_integer().
max_activity_age(Vars) ->
    case maps:get(harmonize_activity_on_hip17_interactivity_blocks, Vars, false) of
        true -> maps:get(hip17_interactivity_blocks, Vars);
        false -> maps:get(poc_v4_target_challenge_age, Vars)
    end.
