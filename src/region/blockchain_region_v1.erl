%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region API ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_v1).

-include("blockchain_vars.hrl").

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
    {ok, [{atom(), binary() | {error, any()}}]} | {error, any()}.
get_all_region_bins(Ledger) ->
    case get_all_regions(Ledger) of
        {ok, Regions} ->
            Map = lists:foldl(
                    fun(Reg, Acc) ->
                            case blockchain:config(Reg, Ledger) of
                                {ok, Bin} ->
                                    [{Reg, Bin}|Acc];
                                _ ->
                                    [{error, {region_var_not_set, Reg}}|Acc]
                            end
                    end, [], Regions),
            {ok, lists:reverse(Map)};
        Error ->
            Error
    end.

-spec h3_to_region(H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
h3_to_region(H3, Ledger) ->
    h3_to_region(H3, Ledger, no_prefetch).

-spec h3_to_region(H3 :: h3:h3_index(),
                   Ledger :: blockchain_ledger_v1:ledger(),
                   RegionBins :: no_prefetch | [{atom(), binary() | {error, term()}}]) ->
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
    RegionBins :: [{atom(), binary() | {error, any()}}]
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
-spec region_(
    Regions :: regions(),
    H3 :: h3:h3_index()
) ->
    {ok, atom()} | {error, any()}.
region_([], H3) ->
    {error, {unknown_region, H3}};
region_([{ToCheck, Bin} | Remaining], H3) ->
    case h3_in_region_(H3, Bin) of
        {error, _} = Error -> Error;
        false -> region_(Remaining, H3);
        true -> {ok, ToCheck}
    end.

-spec h3_to_region_(H3 :: h3:h3_index(),
                    RegionBins :: [{atom(), binary() | {error, any()}}]) ->
    {ok, atom()} | {error, any()}.
h3_to_region_(H3, RegionBins) ->
    region_(RegionBins, H3).

-spec h3_in_region_(
    H3 :: h3:h3_index(),
    RegionBin :: binary()
) -> boolean() | {error, any()}.
h3_in_region_(_H3, {error, _}=Error) ->
    Error;
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
