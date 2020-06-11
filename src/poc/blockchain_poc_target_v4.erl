%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v4 implementation.
%%%
%%% Builds on top of target_v3
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v4).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-export([
         target/4
        ]).

-spec target(ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
             Hash :: binary(),
             Ledger :: blockchain_ledger_v1:ledger(),
             Vars :: list()) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
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
              Vars :: list(),
              HexList :: [h3:h3_index()],
              Attempted :: [{h3:h3_index(), rand:state()}]) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}}.
target_(ChallengerPubkeyBin, Ledger, Vars, HexList, [{Hex, HexRandState} | Tail]=_Attempted) ->
    %% Get a list of gateway pubkeys within this hex
    {ok, AddrList} = blockchain_ledger_v1:get_hex(Hex, Ledger),
    %% Remove challenger if present and also remove gateways who haven't challenged
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    case filter(AddrList, ChallengerPubkeyBin, Ledger, Height, Vars) of
        FilteredList when length(FilteredList) >= 1 ->
            %% Assign probabilities to each of these gateways
            ProbTargetMap = lists:foldl(fun(A, Acc) ->
                                                Prob = blockchain_utils:normalize_float(prob_randomness_wt(Vars) * 1.0),
                                                maps:put(A, Prob, Acc)
                                        end,
                                        #{},
                                        FilteredList),
            %% Sort the scaled probabilities in default order by gateway pubkey_bin
            %% make sure that we carry the rand_state through for determinism
            {RandVal, TargetRandState} = rand:uniform_s(HexRandState),
            {ok, TargetPubkeybin} = blockchain_utils:icdf_select(lists:keysort(1, maps:to_list(ProbTargetMap)), RandVal),
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
-spec filter(AddrList :: [libp2p_crypto:pubkey_bin()],
             ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
             Ledger :: blockchain_ledger_v1:ledger(),
             Height :: non_neg_integer(),
             Vars :: list()) -> [libp2p_crypto:pubkey_bin()].
filter(AddrList, ChallengerPubkeyBin, Ledger, Height, Vars) ->
    lists:filter(fun(A) ->
                         A /= ChallengerPubkeyBin andalso
                         is_active(A, Ledger, Height, Vars)
                 end,
                 AddrList).

-spec is_active(GwPubkeyBin :: libp2p_crypto:pubkey_bin(),
                Ledger :: blockchain_ledger_v1:ledger(),
                Height :: non_neg_integer(),
                Vars :: list()) -> boolean().
is_active(GwPubkeyBin, Ledger, Height, Vars) ->
    {ok, Gateway} = blockchain_ledger_v1:find_gateway_info(GwPubkeyBin, Ledger),
    case blockchain_ledger_gateway_v2:last_poc_challenge(Gateway) of
        undefined ->
            %% No POC challenge, don't include
            false;
        C ->
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
-spec challenge_age(Vars :: list()) -> pos_integer().
challenge_age(Vars) ->
    proplists:get_value(poc_v4_target_challenge_age, Vars).

-spec prob_randomness_wt(Vars :: list()) -> float().
prob_randomness_wt(Vars) ->
    proplists:get_value(poc_v5_target_prob_randomness_wt, Vars).


-spec sorted_hex_list(Ledger :: blockchain_ledger_v1:ledger()) -> [h3:h3_index()].
sorted_hex_list(Ledger) ->
    %% Grab the list of parent hexes
    {ok, Hexes} = blockchain_ledger_v1:get_hexes(Ledger),
    lists:keysort(1, maps:to_list(Hexes)).

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
