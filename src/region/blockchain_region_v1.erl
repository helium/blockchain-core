%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region API ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_v1).

-include("blockchain_vars.hrl").

-export([get_all_regions/1, h3_to_region/2, h3_in_region/3]).

-type regions() :: [atom()].

-export_type([atom/0, regions/0]).

-define(H3_IN_REGION_CACHE, h3_in_region).
-define(H3_TO_REGION_CACHE, h3_to_region).

%% 24 hours in seconds
-define(H3_TO_REGION_CACHE_TIMEOUT, 60 * 60 * 24).
%% 24 hours in seconds
-define(H3_IN_REGION_CACHE_TIMEOUT, 60 * 60 * 24).

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec get_all_regions(Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, regions()} | {error, any()}.
get_all_regions(Ledger) ->
    case blockchain:config(?regulatory_regions, Ledger) of
        {ok, Bin} ->
            {ok, [list_to_atom(I) || I <- string:tokens(binary:bin_to_list(Bin), ",")]};
        _ ->
            {error, regulatory_regions_not_set}
    end.

-spec h3_to_region(H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
h3_to_region(H3, Ledger) ->
    {ok, VarsNonce} = blockchain_ledger_v1:vars_nonce(Ledger),
    e2qc:cache(
        ?H3_TO_REGION_CACHE,
        {VarsNonce, H3},
        ?H3_TO_REGION_CACHE_TIMEOUT,
        fun() ->
            h3_to_region_(H3, Ledger)
        end
    ).

-spec h3_in_region(
    H3 :: h3:h3_index(),
    RegionVar :: atom(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> boolean() | {error, any()}.
h3_in_region(H3, RegionVar, Ledger) ->
    {ok, VarsNonce} = blockchain_ledger_v1:vars_nonce(Ledger),
    e2qc:cache(
        ?H3_IN_REGION_CACHE,
        {VarsNonce, H3, RegionVar},
        ?H3_IN_REGION_CACHE_TIMEOUT,
        fun() ->
            h3_in_region_(H3, RegionVar, Ledger)
        end
    ).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------
-spec region_(
    Regions :: regions(),
    H3 :: h3:h3_index(),
    Ledger :: blockchain_ledger_v1:ledger()
) ->
    {ok, atom()} | {error, any()}.
region_([], H3, _Ledger) ->
    {error, {unknown_region, H3}};
region_([ToCheck | Remaining], H3, Ledger) ->
    case h3_in_region(H3, ToCheck, Ledger) of
        {error, _} = Error -> Error;
        false -> region_(Remaining, H3, Ledger);
        true -> {ok, ToCheck}
    end.

-spec h3_to_region_(H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
h3_to_region_(H3, Ledger) ->
    case get_all_regions(Ledger) of
        {ok, Regions} ->
            region_(Regions, H3, Ledger);
        E ->
            E
    end.

-spec h3_in_region_(
    H3 :: h3:h3_index(),
    RegionVar :: atom(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> boolean() | {error, any()}.
h3_in_region_(H3, RegionVar, Ledger) ->
    case blockchain:config(RegionVar, Ledger) of
        {ok, Bin} ->
            try h3:contains(H3, Bin) of
                false ->
                    false;
                {true, _Parent} ->
                    true
            catch
                What:Why:Stack ->
                    lager:error("Unable to get region, What: ~p, Why: ~p, Stack: ~p", [
                        What,
                        Why,
                        Stack
                    ]),
                    {error, {h3_contains_failed, Why}}
            end;
        _ ->
            {error, {region_var_not_set, RegionVar}}
    end.
