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

-export([
         target/4
        ]).

%% @doc Finds a potential target to start the path from.
-spec target(ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
             Hash :: binary(),
             Ledger :: blockchain:ledger(),
             Vars :: map()) -> {ok, {libp2p_crypto:pubkey_bin(), rand:state()}} | {error, no_target}.
target(ChallengerPubkeyBin, Hash, Ledger, Vars) ->
    %% Get the zone to target within
    {ok, {Hex, HexRandState}} = target_hex(Hash, Ledger),
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
            {error, no_target}
    end.

%% @doc Filter gateways based on these conditions:
%% - Inactive gateways (those which haven't challenged in a long time).
%% - Dont target the challenger gateway itself.
-spec filter(AddrList :: [libp2p_crypto:pubkey_bin()],
             ChallengerPubkeyBin :: libp2p_crypto:pubkey_bin(),
             Ledger :: blockchain_ledger_v1:ledger(),
             Height :: non_neg_integer(),
             Vars :: map()) -> [libp2p_crypto:pubkey_bin()].
filter(AddrList, ChallengerPubkeyBin, Ledger, Height, Vars) ->
    lists:filter(fun(A) ->
                         A /= ChallengerPubkeyBin andalso
                         is_active(A, Ledger, Height, Vars)
                 end,
                 AddrList).

-spec is_active(GwPubkeyBin :: libp2p_crypto:pubkey_bin(),
                Ledger :: blockchain_ledger_v1:ledger(),
                Height :: non_neg_integer(),
                Vars :: map()) -> boolean().
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
-spec challenge_age(Vars :: map()) -> pos_integer().
challenge_age(Vars) ->
    maps:get(poc_v4_target_challenge_age, Vars).

-spec prob_randomness_wt(Vars :: map()) -> float().
prob_randomness_wt(Vars) ->
    maps:get(poc_v5_target_prob_randomness_wt, Vars).

-spec target_hex(Hash :: binary(),
                 Ledger :: blockchain:ledger()) -> h3:h3_index().
target_hex(Hash, Ledger) ->
    %% Grab the list of parent hexes
    {ok, Hexes} = blockchain_ledger_v1:get_hexes(Ledger),
    HexList = lists:keysort(1, maps:to_list(Hexes)),

    %% choose hex via CDF
    InitRandState = blockchain_utils:rand_state(Hash),
    {HexVal, HexRandState} = rand:uniform_s(InitRandState),
    {ok, Hex} = blockchain_utils:icdf_select(HexList, HexVal),
    {ok, {Hex, HexRandState}}.
