%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region API ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_v1).

-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         get_all_regions/1, get_all_region_bins/1,
         h3_to_region/2, h3_to_region/3,
         h3_in_region/3, h3_in_region/4
        ]).

-type regions() :: [atom()].

-export_type([atom/0, regions/0]).

% key: {has_aux, vars_nonce, h3}
-define(H3_TO_REGION_CACHE, h3_to_region).
-define(POLYFILL_RESOLUTION, 7).

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec get_all_regions(Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, regions()} | {error, any()}.
get_all_regions(Ledger) ->
    case blockchain:config(?regulatory_regions, Ledger) of
        {ok, Bin} ->
            {ok, lists:map(fun(R) -> list_to_atom(binary_to_list(R)) end, binary:split(Bin, <<",">>, [global, trim]))};
        _ ->
            {error, regulatory_regions_not_set}
    end.

-spec get_all_region_bins(Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, #{atom() => binary()}} | {error, any()}.
get_all_region_bins(Ledger) ->
    case get_all_regions(Ledger) of
        {ok, Regions} ->
            Map = lists:foldl(
                    fun(Reg, Acc) ->
                            case blockchain:config(Reg, Ledger) of
                                {ok, Bin} ->
                                    Acc#{Reg => Bin};
                                _ -> Acc
                            end
                    end, #{}, Regions),
            {ok, Map};
        Error ->
            Error
    end.

-spec h3_to_region(H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
h3_to_region(H3, Ledger) ->
    h3_to_region(H3, Ledger, no_prefetch).

-spec h3_to_region(H3 :: h3:h3_index(),
                   Ledger :: blockchain_ledger_v1:ledger(),
                   RegionBins :: no_prefetch | #{atom() => binary()}) ->
    {ok, atom()} | {error, any()}.
h3_to_region(H3, Ledger, RegionBins) ->
    {ok, VarsNonce} = blockchain_ledger_v1:vars_nonce(Ledger),
    %% maybe allow this to be passed in?
    Res = polyfill_resolution(Ledger),
    HasAux = blockchain_ledger_v1:has_aux(Ledger),
    Parent = h3:parent(H3, Res),
    e2qc:cache(
        ?H3_TO_REGION_CACHE,
        {HasAux, VarsNonce, Parent},
        fun() ->
                MaybeBins =
                    case RegionBins of
                        no_prefetch -> get_all_region_bins(Ledger);
                        {error, _} = Err -> Err;
                        B -> {ok, B}
                    end,
                case MaybeBins of
                    {ok, Bins} ->
                        h3_to_region_(Parent, Bins);
                    {error, _} = Error -> Error
                end
        end
     ).

-spec h3_in_region(
    H3 :: h3:h3_index(),
    RegionVar :: atom(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> boolean() | {error, any()}.
h3_in_region(H3, RegionVar, Ledger) ->
    Res = polyfill_resolution(Ledger),
    Parent = h3:parent(H3, Res),
    case h3_to_region(Parent, Ledger) of
        {ok, Region} -> Region == RegionVar;
        Other -> Other
    end.

-spec h3_in_region(
    H3 :: h3:h3_index(),
    RegionVar :: atom(),
    Ledger :: blockchain_ledger_v1:ledger(),
    RegionBins :: #{atom() => binary()}
) -> boolean() | {error, any()}.
h3_in_region(H3, RegionVar, Ledger, RegionBins) ->
    Res = polyfill_resolution(Ledger),
    Parent = h3:parent(H3, Res),
    case h3_to_region(Parent, Ledger, RegionBins) of
        {ok, Region} -> Region == RegionVar;
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------
-spec h3_to_region_(H3 :: h3:h3_index(),
                    RegionBins :: #{atom() => binary()}) ->
    {ok, atom()} | {error, any()}.
h3_to_region_(H3, RegionBins) ->

    IsH3InRegion =
    fun({Region, RegionBin}) ->
            case h3_in_region_(H3, RegionBin) of
                true -> Region;
                _ -> undefined
            end
    end,

    PotentialRegions = blockchain_utils:pmap(IsH3InRegion, maps:to_list(RegionBins)),

    IsDefined = fun(I) -> I /= undefined end,

    case lists:filter(IsDefined, PotentialRegions) of
        [ ] -> {error, {unknown_region, H3}};
        Filtered when length(Filtered) > 1 ->
            %% More than one element in Filtered regions, pick the "firstmatch" from the
            %% original regions list
            Region = firstmatch(maps:keys(RegionBins), Filtered),
            lager:info("H3: ~p, Filtered regions: ~p, Selected: ~p", [H3, Filtered, Region]),
            {ok, Region};
        [Region] ->
            %% Filtered to a single region, return
            {ok, Region}
    end.

-spec firstmatch(Regions :: regions(), Filtered :: regions()) -> atom().
firstmatch(Regions, Filtered) ->
    IndexedRegions =
    lists:foldl(fun(Region, Acc) ->
                        case walk_regions(Regions, Region) of
                            {error, notfound} -> Acc;
                            Found -> [Found | Acc]
                        end
                end, [], Filtered),
    {_, Region} = hd(lists:keysort(1, IndexedRegions)),
    Region.

-spec walk_regions(Regions :: regions(), ToCheck :: atom()) -> {error, notfound} | {non_neg_integer(), atom()}.
walk_regions(Regions, ToCheck) ->
    IndexedRegions = lists:zip(lists:seq(1, length(Regions)), Regions),
    case lists:dropwhile(fun({_Index, Region}) -> Region /= ToCheck end, IndexedRegions) of
        [] -> {error, notfound};
        [{_Index, _Region}=X | _] -> X
    end.

-spec h3_in_region_(
    H3 :: h3:h3_index(),
    RegionBin :: binary()
) -> boolean() | {error, any()}.
h3_in_region_(H3, RegionBin) ->
    try h3:contains(H3, RegionBin) of
        false ->
            false;
        {true, _Parent} ->
            true
    catch
        What:Why:Stack ->
            lager:error("Unable to get region, What: ~p, Why: ~p, Stack: ~p",
                        [
                         What,
                         Why,
                         Stack
                        ]),
            {error, {h3_contains_failed, Why}}
    end.

polyfill_resolution(Ledger) ->
    case blockchain_ledger_v1:config(?polyfill_resolution, Ledger) of
        {ok, Res} -> Res;
        _ -> ?POLYFILL_RESOLUTION
    end.

-ifdef(TEST).

lookup_test() ->
    %% ans: region_as923_1
    Regions = [region_as923_1,region_as923_2,region_as923_3,region_as923_4,
               region_au915,region_cn470,region_eu433,region_eu868,region_in865,
               region_kr920,region_ru864,region_us915],
    %% ans: region_cn470
    Regions2 = [region_cn470, region_as923_1,region_as923_2,region_as923_3,
                region_as923_4,region_au915,region_eu433,region_eu868,region_in865,
                region_kr920,region_ru864,region_us915],
    %% ans: region_as923_1
    Regions3 = [region_us915,region_as923_2,region_as923_3,region_as923_4,region_au915,
                region_eu433,region_as923_1,region_eu868,region_in865,region_kr920,
                region_cn470,region_ru864],
    %% ans: region_cn470
    Regions4 = [region_us915,region_as923_2,region_as923_3,region_as923_4,region_au915,
                region_eu433,region_cn470,region_eu868,region_in865,region_kr920,
                region_as923_1,region_ru864],

    Filtered1 = [region_as923_1, region_cn470],
    Filtered2 = [region_cn470, region_as923_1],

    ?assertEqual(region_as923_1, firstmatch(Regions, Filtered1)),
    ?assertEqual(region_as923_1, firstmatch(Regions, Filtered2)),
    ?assertEqual(region_cn470, firstmatch(Regions2, Filtered1)),
    ?assertEqual(region_cn470, firstmatch(Regions2, Filtered2)),
    ?assertEqual(region_as923_1, firstmatch(Regions3, Filtered1)),
    ?assertEqual(region_as923_1, firstmatch(Regions3, Filtered2)),
    ?assertEqual(region_cn470, firstmatch(Regions4, Filtered1)),
    ?assertEqual(region_cn470, firstmatch(Regions4, Filtered2)).

-endif.
