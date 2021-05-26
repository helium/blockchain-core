%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region API ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_v1).

-include("blockchain_vars.hrl").

-export([get_all_regions/1, region/2]).

%% [us915, au915, .... ]
-type regions() :: [atom()].

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec get_all_regions(Ledger :: blockchain_ledger_v1:ledger()) -> {ok, regions()} | {error, any()}.
get_all_regions(Ledger) ->
    case blockchain:config(?regulatory_regions, Ledger) of
        {ok, Bin} ->
            {ok, [list_to_atom(I) || I <- string:tokens(binary:bin_to_list(Bin), ",")]};
        _ ->
            {error, regulatory_regions_not_set}
    end.

-spec region(H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
region(H3, Ledger) ->
    {ok, Regions} = get_all_regions(Ledger),
    region_(Regions, H3, Ledger).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

-spec atom_to_region_var(atom()) -> atom().
atom_to_region_var(as923_1) -> ?region_as923_1;
atom_to_region_var(as923_2) -> ?region_as923_2;
atom_to_region_var(as923_3) -> ?region_as923_3;
atom_to_region_var(au915) -> ?region_au915;
atom_to_region_var(cn470) -> ?region_cn470;
atom_to_region_var(eu433) -> ?region_eu433;
atom_to_region_var(eu868) -> ?region_eu868;
atom_to_region_var(in865) -> ?region_in865;
atom_to_region_var(kr920) -> ?region_kr920;
atom_to_region_var(ru864) -> ?region_ru864;
atom_to_region_var(us915) -> ?region_us915.

-spec region_(Regions :: regions(), H3 :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, atom()} | {error, any()}.
region_([], _H3, _Ledger) ->
    {error, not_found};
region_([ToCheck | Remaining], H3, Ledger) ->
    RegionVar = atom_to_region_var(ToCheck),
    case blockchain:config(RegionVar, Ledger) of
        {ok, Bin} ->
            try h3:contains(H3, Bin) of
                false ->
                    region_(Remaining, H3, Ledger);
                {true, _Parent} ->
                    {ok, RegionVar}
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
