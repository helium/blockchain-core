%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v5 implementation.
%%%
%%% The targeting mechanism is based on the following conditions:
%%% - Deterministically identify a target hex based on public key
%%% - Deterministically select a challengee from target region based on private key
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v5).

-include_lib("blockchain/include/blockchain_utils.hrl").
-include_lib("blockchain/include/blockchain_vars.hrl").
-include_lib("blockchain/include/blockchain_caps.hrl").

-export([
    target_zone/2,
    target/5
]).

-spec target_zone(
    RandState :: rand:state(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> {ok, {[h3:h3_index()], h3:h3_index(), rand:state()}} | {error, any()}.
target_zone(RandState, Ledger) ->
    %% Get all hexes once
    HexList = sorted_hex_list(Ledger),
    %% Initialize seed with Hash once
    %% Initial zone to begin targeting into
    case choose_zone(RandState, HexList) of
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
) -> {ok, [libp2p_crypto:pubkey_bin()]} | {error, any()}.
gateways_for_zone(
    ChallengerPubkeyBin,
    Ledger,
    Vars,
    HexList,
    [{Hex, HexRandState0} | Tail] = _Attempted
) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    %% Get a list of gateway pubkeys within this hex
    {ok, AddrList0} = blockchain_ledger_v1:get_hex(Hex, Ledger),
    lager:debug("gateways for hex ~p: ~p", [Hex, AddrList0]),
    %% Limit max number of potential targets in the zone
    {HexRandState, AddrList} = limit_addrs(Vars, HexRandState0, AddrList0),
    case filter(AddrList, Ledger, Height, Vars) of
        FilteredList when length(FilteredList) >= 1 ->
            lager:debug("*** filtered gateways for hex ~p: ~p", [Hex, FilteredList]),
            {ok, FilteredList};
        _ ->
            lager:debug("*** failed to find gateways for hex ~p, trying again", [Hex]),
            %% no eligible target in this zone
            %% find a new zone
            case choose_zone(HexRandState, HexList) of
                {error, _} = ErrorResp -> ErrorResp;
                {ok, New} ->
                    %% remove Hex from attemped, add New to attempted and retry
                    gateways_for_zone(ChallengerPubkeyBin, Ledger, Vars, HexList, [New | Tail])
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
    %% Get all hexes once
    HexList = sorted_hex_list(Ledger),
    %% Initial zone to begin targeting into
    case choose_zone(ZoneRandState, HexList) of
        {error, _} = ErrorResp ->
            ErrorResp;
        {ok, {InitHex, InitHexRandState}} ->
            target_(
                ChallengerPubkeyBin,
                InitTargetRandState,
                Ledger,
                Vars,
                HexList,
                InitHex,
                InitHexRandState
            )
    end.

%% @doc Finds a potential target to start the path from.
-spec target_(
    ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
    InitTargetRandState :: rand:state(),
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map(),
    HexList :: [h3:h3_index()],
    InitHex :: h3:h3_index(),
    InitHexRandState :: rand:state()
) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
target_(
    ChallengerPubkeyBin,
    InitTargetRandState,
    Ledger,
    Vars,
    HexList,
    InitHex,
    InitHexRandState
) ->
    {ok, ZoneGWs} = gateways_for_zone(ChallengerPubkeyBin, Ledger, Vars, HexList, [
        {InitHex, InitHexRandState}
    ]),
    %% Assign probabilities to each of these gateways
    ProbTargetMap = lists:foldl(
        fun(A, Acc) ->
            Prob = blockchain_utils:normalize_float(prob_randomness_wt(Vars) * 1.0),
            maps:put(A, Prob, Acc)
        end,
        #{},
        ZoneGWs
    ),
    %% Sort the scaled probabilities in default order by gateway pubkey_bin
    %% make sure that we carry the rand_state through for determinism
    {RandVal, TargetRandState} = rand:uniform_s(InitTargetRandState),
    {ok, TargetPubkeybin} = blockchain_utils:icdf_select(
        lists:keysort(1, maps:to_list(ProbTargetMap)),
        RandVal
    ),
    {ok, {TargetPubkeybin, TargetRandState}}.

%% @doc Filter gateways based on these conditions:
%% - gateways which do not have the relevant capability
-spec filter(
    AddrList :: [libp2p_crypto:pubkey_bin()],
    Ledger :: blockchain_ledger_v1:ledger(),
    Height :: non_neg_integer(),
    Vars :: map()
) -> [libp2p_crypto:pubkey_bin()].
filter(AddrList, Ledger, Height, Vars) ->
    ActivityFilterEnabled =
        case blockchain:config(poc_activity_filter_enabled, Ledger) of
            {ok, V} -> V;
            _ -> false
        end,
    MaxActivityAge = max_activity_age(Vars),
    lists:filter(
        fun(A) ->
            {ok, Gateway} = blockchain_ledger_v1:find_gateway_info(A, Ledger),
            Mode = blockchain_ledger_gateway_v2:mode(Gateway),
            LastActivity = blockchain_ledger_gateway_v2:last_poc_challenge(Gateway),
            is_active(ActivityFilterEnabled, LastActivity, MaxActivityAge, Height) andalso
                blockchain_ledger_gateway_v2:is_valid_capability(
                    Mode,
                    ?GW_CAPABILITY_POC_CHALLENGEE,
                    Ledger
                )
        end,
        AddrList
    ).

%%%-------------------------------------------------------------------
%% Helpers
%%%-------------------------------------------------------------------
-spec prob_randomness_wt(Vars :: map()) -> float().
prob_randomness_wt(Vars) ->
    maps:get(poc_v5_target_prob_randomness_wt, Vars).

-spec sorted_hex_list(Ledger :: blockchain_ledger_v1:ledger()) -> [h3:h3_index()].
sorted_hex_list(Ledger) ->
    %% Grab the list of parent hexes
    %%
    %% On a new network, there will be _no_ hexes
    case blockchain_ledger_v1:get_hexes(Ledger) of
        {ok, Hexes} -> lists:keysort(1, maps:to_list(Hexes));
        _ -> []
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
            lager:debug("choose zone success, found hex ~p", [Hex]),
            {ok, {Hex, HexRandState}}
    end.

limit_addrs(#{?poc_witness_consideration_limit := Limit}, RandState, Witnesses) ->
    blockchain_utils:deterministic_subset(Limit, RandState, Witnesses);
limit_addrs(_Vars, RandState, Witnesses) ->
    {RandState, Witnesses}.

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
