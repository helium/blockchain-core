-module(blockchain_hex).

-export([densities/1]).

-include("blockchain_vars.hrl").

-type densities() :: {density_map(), density_map()}.
-type density_map() :: #{h3:h3_index() => pos_integer()}.

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------

-spec densities(Ledger :: blockchain_ledger_v1:ledger()) -> densities().
densities(Ledger) ->
    VarMap = var_map(Ledger),
    Locations = filtered_locations(Ledger),
    densities(VarMap, Locations).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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

filtered_locations(Ledger) ->
    AG = blockchain_ledger_v1:active_gateways(Ledger),
    UnfilteredLocs = [blockchain_ledger_gateway_v2:location(G) || G <- maps:values(AG)],
    lists:filter(fun(L) -> L /= undefined end, UnfilteredLocs).

hex_resolutions(VarMap) ->
    %% [12, 11, ... 0]
    lists:reverse(lists:sort(maps:keys(VarMap))).

densities(VarMap, Locations) ->
    %% Head = 12, Tail = [11, 10, ... 0]
    [Head | Tail] = hex_resolutions(VarMap),

    %% find parent hexs to all hotspots at highest resolution in chain variables
    ParentHexes = [h3:parent(Hex, Head) || Hex <- Locations],

    InitialDensities = init_densities(ParentHexes, #{}),

    {UDensities, Densities} = build_densities(
        VarMap,
        ParentHexes,
        {InitialDensities, InitialDensities},
        Tail
    ),

    {UDensities, Densities}.

init_densities(ParentHexes, Init) ->
    %% increment density for all parent hotspots by 1 for each hotspot in that hex
    lists:foldl(
        fun(Hex, Acc) ->
            maps:update_with(
                Hex,
                fun(V) -> V + 1 end,
                1,
                Acc
            )
        end,
        Init,
        ParentHexes
    ).

build_densities(_VarMap, _ParentHexes, {UAcc, Acc}, []) ->
    {UAcc, Acc};
build_densities(VarMap, ParentHexes, {UAcc, Acc}, [Res | Tail]) ->
    %% child_hexs needs to be unique so we usort from parent_hexs list
    ChildHexes = lists:usort(ParentHexes),

    %% create list of tuples of child hex and parent at this res
    %% this avoids calling h3:parent repeatedly
    ChildToParents = [{Hex, h3:parent(Hex, Res)} || Hex <- ChildHexes],

    %% calculate unclipped densities for all occupied hexs at this res
    {UM0, M0} = unclipped_densities(ChildToParents, {UAcc, Acc}),

    %% clip densities based on neighbors, this should also be unique
    OccupiedHexesThisRes = lists:usort([ThisParentHex || {_, ThisParentHex} <- ChildToParents]),

    {UM1, M1} = lists:foldl(
        fun(ThisResHex, {UAcc3, Acc3}) ->
            OccupiedCount = occupied_count(Res, VarMap, ThisResHex, M0),

            %% limit from formula in HIP
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

limit(Res, VarMap, OccupiedCount) ->
    %% limit from formula in HIP
    min(
        maps:get(density_max, maps:get(Res, VarMap)),
        maps:get(density_tgt, maps:get(Res, VarMap)) *
            max((OccupiedCount - maps:get(n, maps:get(Res, VarMap))), 1)
    ).

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

get_density_var(Var, Ledger) ->
    {ok, Bin} = blockchain:config(Var, Ledger),
    [N, Tgt, Max] = [list_to_integer(I) || I <- string:tokens(binary:bin_to_list(Bin), ",")],
    [N, Tgt, Max].
