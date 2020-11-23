-module(blockchain_hex).

-export([var_map/1, scale/4, densities/1]).

-include("blockchain_vars.hrl").

-type density_map() :: #{h3:h3_index() => pos_integer()}.
-type densities() :: {density_map(), density_map()}.
-type var_map() :: #{non_neg_integer() => map()}.
-type hex_resolutions() :: [non_neg_integer()].
-type locations() :: #{h3:h3_index() => [libp2p_crypto:pubkey_bin(),...]}.

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------

-spec densities(Ledger :: blockchain_ledger_v1:ledger()) -> densities().
densities(Ledger) ->
    %% build a map of required chain variables, example:
    %% #{0 => #{n => N, density_tgt => T, density_max =M}, ....}
    VarMap = var_map(Ledger),
    %% Filter out gateways with no location
    Locations = filtered_locations(Ledger),
    %% Calculate clipped and unclipped densities
    densities(VarMap, Locations).

-spec scale(
    Location :: h3:h3_index(),
    TargetRes :: non_neg_integer(),
    UnclippedDensities :: density_map(),
    ClippedDensities :: density_map()
) -> float().
scale(Location, TargetRes, UnclippedDensities, ClippedDensities) ->
    case TargetRes >= h3:get_resolution(Location) of
        true ->
            maps:get(Location, ClippedDensities) / maps:get(Location, UnclippedDensities);
        false ->
            lists:foldl(
              fun(R, Acc) ->
                      Parent = h3:parent(Location, R),
                      Acc * (maps:get(Parent, ClippedDensities) / maps:get(Parent, UnclippedDensities))
              end,
              1.0,
              lists:seq(TargetRes, 0, -1)
             )
    end.

-spec var_map(Ledger :: blockchain_ledger_v1:ledger()) -> var_map().
var_map(Ledger) ->
    [N0, Tgt0, Max0] = get_density_var(?hip17_res_0, Ledger),
    [N1, Tgt1, Max1] = get_density_var(?hip17_res_1, Ledger),
    [N2, Tgt2, Max2] = get_density_var(?hip17_res_2, Ledger),
    [N3, Tgt3, Max3] = get_density_var(?hip17_res_3, Ledger),
    [N4, Tgt4, Max4] = get_density_var(?hip17_res_4, Ledger),
    [N5, Tgt5, Max5] = get_density_var(?hip17_res_5, Ledger),
    [N6, Tgt6, Max6] = get_density_var(?hip17_res_6, Ledger),
    [N7, Tgt7, Max7] = get_density_var(?hip17_res_7, Ledger),
    [N8, Tgt8, Max8] = get_density_var(?hip17_res_8, Ledger),
    [N9, Tgt9, Max9] = get_density_var(?hip17_res_9, Ledger),
    [N10, Tgt10, Max10] = get_density_var(?hip17_res_10, Ledger),
    [N11, Tgt11, Max11] = get_density_var(?hip17_res_11, Ledger),
    [N12, Tgt12, Max12] = get_density_var(?hip17_res_12, Ledger),

    #{
        0 => #{n => N0, density_tgt => Tgt0, density_max => Max0},
        1 => #{n => N1, density_tgt => Tgt1, density_max => Max1},
        2 => #{n => N2, density_tgt => Tgt2, density_max => Max2},
        3 => #{n => N3, density_tgt => Tgt3, density_max => Max3},
        4 => #{n => N4, density_tgt => Tgt4, density_max => Max4},
        5 => #{n => N5, density_tgt => Tgt5, density_max => Max5},
        6 => #{n => N6, density_tgt => Tgt6, density_max => Max6},
        7 => #{n => N7, density_tgt => Tgt7, density_max => Max7},
        8 => #{n => N8, density_tgt => Tgt8, density_max => Max8},
        9 => #{n => N9, density_tgt => Tgt9, density_max => Max9},
        10 => #{n => N10, density_tgt => Tgt10, density_max => Max10},
        11 => #{n => N11, density_tgt => Tgt11, density_max => Max11},
        12 => #{n => N12, density_tgt => Tgt12, density_max => Max12}
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec filtered_locations(Ledger :: blockchain_ledger_v1:ledger()) -> locations().
filtered_locations(Ledger) ->
    blockchain_ledger_v1:get_h3dex(Ledger).

-spec hex_resolutions(VarMap :: var_map()) -> hex_resolutions().
hex_resolutions(VarMap) ->
    %% [12, 11, ... 0]
    lists:reverse(lists:sort(maps:keys(VarMap))).

-spec densities(VarMap :: var_map(), Locations :: locations()) -> densities().
densities(VarMap, Locations) ->
    %% Head = 12, Tail = [11, 10, ... 0]
    [Head | Tail] = hex_resolutions(VarMap),

    %% find parent hexs to all hotspots at highest resolution in chain variables
    {ParentHexes, InitialDensities} = maps:fold(fun(Hex, GWs, {HAcc, MAcc}) ->
                                    ParentHex = h3:parent(Hex, Head),
                                    case maps:find(ParentHex, MAcc) of
                                        error ->
                                            {[ParentHex|HAcc], maps:put(ParentHex, length(GWs), MAcc)};
                                        {ok, OldCount} ->
                                            {HAcc, maps:put(ParentHex, OldCount + length(GWs), MAcc)}
                                    end
                            end, {[], #{}}, Locations),

    {UDensities, Densities} = build_densities(
        VarMap,
        ParentHexes,
        {InitialDensities, InitialDensities},
        Tail
    ),

    {UDensities, Densities}.

-spec build_densities(var_map(), locations(), densities(), [non_neg_integer()]) -> densities().
build_densities(_VarMap, _ParentHexes, {UAcc, Acc}, []) ->
    {UAcc, Acc};
build_densities(VarMap, ParentHexes, {UAcc, Acc}, [Res | Tail]) ->
    ChildHexes = lists:usort(ParentHexes),

    ChildToParents = [{Hex, h3:parent(Hex, Res)} || Hex <- ChildHexes],

    {UM0, M0} = unclipped_densities(ChildToParents, {UAcc, Acc}),

    OccupiedHexesThisRes = lists:usort([ThisParentHex || {_, ThisParentHex} <- ChildToParents]),

    {UM1, M1} = lists:foldl(
        fun(ThisResHex, {UAcc3, Acc3}) ->
            OccupiedCount = occupied_count(Res, VarMap, ThisResHex, M0),

            Limit = limit(Res, VarMap, OccupiedCount),

            {
                maps:put(ThisResHex, maps:get(ThisResHex, M0), UAcc3),
                maps:put(ThisResHex, min(Limit, maps:get(ThisResHex, M0)), Acc3)
            }
        end,
        {UM0, M0},
        OccupiedHexesThisRes
    ),

    build_densities(VarMap, OccupiedHexesThisRes, {UM1, M1}, Tail).

-spec limit(
    Res :: non_neg_integer(),
    VarMap :: var_map(),
    OccupiedCount :: non_neg_integer()
) -> non_neg_integer().
limit(Res, VarMap, OccupiedCount) ->
    min(
        maps:get(density_max, maps:get(Res, VarMap)),
        maps:get(density_tgt, maps:get(Res, VarMap)) *
            max((OccupiedCount - maps:get(n, maps:get(Res, VarMap))), 1)
    ).

-spec occupied_count(
    Res :: non_neg_integer(),
    VarMap :: var_map(),
    ThisResHex :: h3:h3_index(),
    DensityMap :: density_map()
) -> non_neg_integer().
occupied_count(Res, VarMap, ThisResHex, DensityMap) ->
    H3Neighbors = h3:k_ring(ThisResHex, 1),
    lists:foldl(
        fun(Neighbor, Acc) ->
            ToAdd =
                case
                    maps:get(Neighbor, DensityMap, 0) >=
                        maps:get(density_tgt, maps:get(Res, VarMap))
                of
                    false -> 0;
                    true -> 1
                end,
            Acc + ToAdd
        end,
        0,
        H3Neighbors
    ).

-spec unclipped_densities(locations(), densities()) -> densities().
unclipped_densities(ChildToParents, {UAcc, Acc}) ->
    lists:foldl(
        fun({ChildHex, ThisParentHex}, {UAcc2, Acc2}) ->
            {maps:update_with(
                    ThisParentHex,
                    fun(V) -> V + maps:get(ChildHex, Acc, 0) end,
                    maps:get(ChildHex, Acc, 0),
                    UAcc2
                ),
                maps:update_with(
                    ThisParentHex,
                    fun(V) -> V + maps:get(ChildHex, Acc, 0) end,
                    maps:get(ChildHex, Acc, 0),
                    Acc2
                )}
        end,
        {UAcc, Acc},
        ChildToParents
    ).

-spec get_density_var(
    Var :: atom(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> [pos_integer()].
get_density_var(Var, Ledger) ->
    {ok, Bin} = blockchain:config(Var, Ledger),
    [N, Tgt, Max] = [list_to_integer(I) || I <- string:tokens(binary:bin_to_list(Bin), ",")],
    [N, Tgt, Max].
