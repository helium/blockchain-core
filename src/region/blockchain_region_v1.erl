%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region API ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_v1).

-include("blockchain_vars.hrl").

-export([get_all_regions/1, h3_to_region/2, h3_in_region/3]).

-type region_var() ::
    'region_as923_1'
    | 'region_as923_2'
    | 'region_as923_3'
    | 'region_au915'
    | 'region_cn470'
    | 'region_eu433'
    | 'region_eu868'
    | 'region_in865'
    | 'region_kr920'
    | 'region_ru864'
    | 'region_us915'.

-type regions() :: [region_var()].

-export_type([region_var/0, regions/0]).

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
    {ok, region_var()} | {error, any()}.
h3_to_region(H3, Ledger) ->
    case get_all_regions(Ledger) of
        {ok, Regions} ->
            region_(Regions, H3, Ledger);
        E ->
            E
    end.

-spec h3_in_region(
    H3 :: h3:h3_index(),
    RegionVar :: region_var(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> boolean() | {error, any()}.
h3_in_region(H3, RegionVar, Ledger) ->
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

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------
-spec region_(
    Regions :: regions(),
    H3 :: h3:h3_index(),
    Ledger :: blockchain_ledger_v1:ledger()
) ->
    {ok, region_var()} | {error, any()}.
region_([], _H3, _Ledger) ->
    {error, not_found};
region_([ToCheck | Remaining], H3, Ledger) ->
    case h3_in_region(H3, ToCheck, Ledger) of
        {error, _} = Error -> Error;
        false -> region_(Remaining, H3, Ledger);
        true -> {ok, ToCheck}
    end.
