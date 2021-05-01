%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region API ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_v1).

-include("blockchain_vars.hrl").

-export([get_all_regions/1, region/2, get_regulatory_regions_var/1]).

%% [us915, au915, .... ]
-type str_regions() :: [string()].
-type regions() :: [atom()].

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec get_all_regions(Ledger :: blockchain_ledger_v1:ledger()) -> {ok, regions()} | {error, any()}.
get_all_regions(Ledger) ->
    case blockchain:config(?regulatory_regions, Ledger) of
        {ok, RegionStr} ->
            case get_regulatory_regions_var(RegionStr) of
                {ok, RegionStrList} ->
                    %% NOTE: There isn't much value to convert these to atoms probably but
                    %% it makes things easy to pattern match on
                    {ok, [list_to_atom(R) || R <- RegionStrList]};
                {error, _} = E ->
                    E
            end;
        _ ->
            {error, regulatory_regions_not_set}
    end.

-spec region(H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
region(H3, Ledger) ->
    {ok, Regions} = get_all_regions(Ledger),
    region_(Regions, H3, Ledger).

-spec get_regulatory_regions_var(Value :: binary()) -> {ok, str_regions()} | {error, any()}.
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

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

-spec actual_region_var(atom()) -> atom().
actual_region_var(as923_1) -> ?region_as923_1;
actual_region_var(as923_2) -> ?region_as923_2;
actual_region_var(as923_3) -> ?region_as923_3;
actual_region_var(au915) -> ?region_au915;
actual_region_var(cn779) -> ?region_cn779;
actual_region_var(eu433) -> ?region_eu433;
actual_region_var(eu868) -> ?region_eu868;
actual_region_var(in865) -> ?region_in865;
actual_region_var(kr920) -> ?region_kr920;
actual_region_var(ru864) -> ?region_ru864;
actual_region_var(us915) -> ?region_us915.

-spec region_(Regions :: regions(), H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
region_([], _H3, _Ledger) ->
    {error, not_found};
region_([ToCheck | Remaining], H3, Ledger) ->
    RegionVar = actual_region_var(ToCheck),
    case blockchain:config(RegionVar, Ledger) of
        {ok, Bin} ->
            try h3:contains(H3, Bin) of
                false ->
                    region_(Remaining, H3, Ledger);
                {true, _Parent} ->
                    %% TODO: This return string is just a placeholder, return something more
                    %% meaningful here, perhaps region_params(ToCheck)
                    {ok, ToCheck}
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
            {error, {region_char_var_not_set, RegionVar}}
    end.
