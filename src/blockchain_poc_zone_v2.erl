%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_zone_v2 implementation.
%%%
%%% TODO: The zoning mechanism is based on the following conditions:
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_zone_v2).

-export([
         pick/3
        ]).

%% @doc Pick a random zone
-spec pick(Hash :: binary(),
           Ledger :: blockchain_ledger_v1:ledger(),
           Vars :: map()) -> blockchain_utils:gateway_score_map().
pick(Hash, Ledger, Vars) ->
    %% Get zones
    Zones = blockchain_utils:zones(Ledger, Vars),
    Entropy = blockchain_utils:rand_state(Hash),
    SortedZoneIndices = lists:sort(maps:keys(Zones)),
    {RandIndex, _} = rand:uniform_s(length(SortedZoneIndices), Entropy),
    PickedZoneIndex = lists:nth(RandIndex, SortedZoneIndices),
    %% It's impossible to build a zone without having a gateway score map in it.
    maps:get(PickedZoneIndex, Zones).
