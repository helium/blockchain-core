%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Specific APIs
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region).

-include("blockchain_vars.hrl").

-export([get_all_regions/1, lookup_region/2, get_regulatory_regions_var/1]).

-type regions() :: [string()].

-spec get_all_regions(Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, regions()} | {error, regulatory_regions_not_set}.
get_all_regions(Ledger) ->
    case blockchain:config(?regulatory_regions, Ledger) of
        {ok, RegionStr} ->
            get_regulatory_regions_var(RegionStr);
        _ ->
            {error, regulatory_regions_not_set}
    end.

lookup_region(_H3, _Ledger) ->
    ok.

-spec get_regulatory_regions_var(Value :: binary()) -> {ok, regions()} | {error, any()}.
get_regulatory_regions_var(Value) ->
    try
        ValueList = string:tokens(binary:bin_to_list(Value), ","),
        {ok, ValueList}
    catch
        What:Why:Stack ->
            lager:error("Unable to get_regulatory_regions_var, What: ~p, Why: ~p, Stack: ~p", [
                What,
                Why,
                Stack
            ]),
            {error, {unable_to_get_regulatory_regions_var, Value}}
    end.
