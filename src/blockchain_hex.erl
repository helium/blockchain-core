-module(blockchain_hex).

-export([var_map/1, scale/2, densities/2]).

-include("blockchain_vars.hrl").
-include_lib("common_test/include/ct.hrl").

-type density_map() :: #{h3:h3_index() => pos_integer()}.
-type densities() :: {UnclippedDensities :: density_map(), ClippedDensities :: density_map()}.
-type var_map() :: #{non_neg_integer() => map()}.
-type locations() :: #{h3:h3_index() => [libp2p_crypto:pubkey_bin(), ...]}.
-type h3_indices() :: [h3:h3_index()].

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------
-spec densities(H3Index :: h3:h3_index(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, densities()} | {error, any()}.
densities(H3Index, Ledger) ->
    %% build a map of required chain variables, example:
    %% #{0 => #{n => N, density_tgt => T, density_max =M}, ....}
    case var_map(Ledger) of
        {error, Reason}=E ->
            lager:error("error, reason: ~p, e: ~p", [Reason, E]),
            {error, {hip17_vars_not_set, Reason}};
        {ok, VarMap} ->
            Locations = blockchain_ledger_v1:lookup_gateways_from_hex(h3:parent(H3Index,
                                                                                h3:get_resolution(H3Index) - 1),
                                                                      Ledger),
            %% Calculate clipped and unclipped densities
            Densities = densities(H3Index, VarMap, Locations, Ledger),
            {ok, Densities}
    end.

-spec scale(
    Location :: h3:h3_index(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> float().
scale(Location, Ledger) ->
    {ok, {UnclippedDensities, ClippedDensities}} = densities(Location, Ledger),
    maps:get(Location, ClippedDensities) / maps:get(Location, UnclippedDensities).


-spec var_map(Ledger :: blockchain_ledger_v1:ledger()) -> {error, any()} | {ok, var_map()}.
var_map(Ledger) ->

    case get_density_var(?hip17_res_0, Ledger) of
        {error, _}=E1 -> E1;
        {ok, [N0, Tgt0, Max0]} ->
            case get_density_var(?hip17_res_1, Ledger) of
                {error, _}=E2 -> E2;
                {ok, [N1, Tgt1, Max1]} ->
                    case get_density_var(?hip17_res_2, Ledger) of
                        {error, _}=E3 -> E3;
                        {ok, [N2, Tgt2, Max2]} ->
                            case get_density_var(?hip17_res_3, Ledger) of
                                {error, _}=E4 -> E4;
                                {ok, [N3, Tgt3, Max3]} ->
                                    case get_density_var(?hip17_res_4, Ledger) of
                                        {error, _}=E5 -> E5;
                                        {ok, [N4, Tgt4, Max4]} ->
                                            case get_density_var(?hip17_res_5, Ledger) of
                                                {error, _}=E6 -> E6;
                                                {ok, [N5, Tgt5, Max5]} ->
                                                    case get_density_var(?hip17_res_6, Ledger) of
                                                        {error, _}=E7 -> E7;
                                                        {ok, [N6, Tgt6, Max6]} ->
                                                            case get_density_var(?hip17_res_7, Ledger) of
                                                                {error, _}=E8 -> E8;
                                                                {ok, [N7, Tgt7, Max7]} ->
                                                                    case get_density_var(?hip17_res_8, Ledger) of
                                                                        {error, _}=E9 -> E9;
                                                                        {ok, [N8, Tgt8, Max8]} ->
                                                                            case get_density_var(?hip17_res_9, Ledger) of
                                                                                {error, _}=E10 -> E10;
                                                                                {ok, [N9, Tgt9, Max9]} ->
                                                                                    case get_density_var(?hip17_res_10, Ledger) of
                                                                                        {error, _}=E11 -> E11;
                                                                                        {ok, [N10, Tgt10, Max10]} ->
                                                                                            case get_density_var(?hip17_res_11, Ledger) of
                                                                                                {error, _}=E12 -> E12;
                                                                                                {ok, [N11, Tgt11, Max11]} ->
                                                                                                    case get_density_var(?hip17_res_12, Ledger) of
                                                                                                        {error, _}=E13 -> E13;
                                                                                                        {ok, [N12, Tgt12, Max12]} ->
                                                                                                            VarMap = #{
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
                                                                                                             },
                                                                                                            {ok, VarMap}
                                                                                                    end
                                                                                            end
                                                                                    end
                                                                            end
                                                                    end
                                                            end
                                                    end
                                            end
                                    end
                            end
                    end
            end
    end.


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec densities(H3Root :: h3:h3_index(),
                VarMap :: var_map(),
                Locations :: locations(),
                Ledger :: blockchain_ledger_v1:ledger()) -> densities().
densities(H3Root, VarMap, Locations, Ledger) ->
    case maps:size(Locations) of
        0 -> {#{}, #{}};
        _ ->
            UpperBoundRes = lists:max([h3:get_resolution(H3) || H3 <- maps:keys(Locations)]),
            LowerBoundRes = h3:get_resolution(H3Root),
            ct:pal("UpperBoundRes: ~p, LowerBoundRes: ~p", [UpperBoundRes, LowerBoundRes]),

            [Head | Tail] = lists:seq(UpperBoundRes, LowerBoundRes, -1),

            %% find parent hexs to all hotspots at highest resolution in chain variables
            {ParentHexes, InitialDensities} = maps:fold(
                                                fun(Hex, GWs, {HAcc, MAcc}) ->
                                                        ParentHex = h3:parent(Hex, Head),
                                                        case maps:find(ParentHex, MAcc) of
                                                            error ->
                                                                {[ParentHex | HAcc], maps:put(ParentHex, length(GWs), MAcc)};
                                                            {ok, OldCount} ->
                                                                {HAcc, maps:put(ParentHex, OldCount + length(GWs), MAcc)}
                                                        end
                                                end,
                                                {[], #{}},
                                                Locations
                                               ),

            {UDensities, Densities} = build_densities(
                                        H3Root,
                                        Ledger,
                                        VarMap,
                                        ParentHexes,
                                        {InitialDensities, InitialDensities},
                                        Tail
                                       ),

            {UDensities, Densities}
    end.

-spec build_densities(h3:h3_index(),
                      blockchain_ledger_v1:ledger(),
                      var_map(),
                      h3_indices(),
                      densities(),
                      [non_neg_integer()]) -> densities().
build_densities(_H3Root, _Ledger, _VarMap, _ParentHexes, {UAcc, Acc}, []) ->
    {UAcc, Acc};
build_densities(H3Root, Ledger, VarMap, ChildHexes, {UAcc, Acc}, [Res | Tail]) ->
    UD = unclipped_densities(ChildHexes, Res, Acc),
    UM0 = maps:merge(UAcc, UD),
    M0 = maps:merge(Acc, UD),

    OccupiedHexesThisRes = maps:keys(UD),

    DensityTarget = maps:get(density_tgt, maps:get(Res, VarMap)),

    M1 = lists:foldl(
        fun(ThisResHex, Acc3) ->
            OccupiedCount = occupied_count(DensityTarget, ThisResHex, UD),
            Limit = limit(Res, VarMap, OccupiedCount),

            ct:pal("Limit: ~p, OccupiedCount: ~p", [Limit, OccupiedCount]),

            maps:put(ThisResHex, min(Limit, maps:get(ThisResHex, M0)), Acc3)
        end,
        M0,
        OccupiedHexesThisRes
    ),

    build_densities(H3Root, Ledger, VarMap, OccupiedHexesThisRes, {UM0, M1}, Tail).

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
        DensityTarget :: non_neg_integer(),
        ThisResHex :: h3:h3_index(),
        DensityMap :: density_map()
       ) -> non_neg_integer().
occupied_count(DensityTarget, ThisResHex, DensityMap) ->
    H3Neighbors = h3:k_ring(ThisResHex, 1),

    lists:foldl(
        fun(Neighbor, Acc) ->
                case maps:get(Neighbor, DensityMap, 0) >= DensityTarget of
                    false -> Acc;
                    true -> Acc + 1
                end
        end,
        0,
        H3Neighbors
    ).

-spec unclipped_densities(h3_indices(), non_neg_integer(), density_map()) -> density_map().
unclipped_densities(ChildToParents, Res, Acc) ->
    lists:foldl(
        fun(ChildHex, Acc2) ->
            ThisParentHex = h3:parent(ChildHex, Res),
            maps:update_with(
                ThisParentHex,
                fun(V) -> V + maps:get(ChildHex, Acc, 0) end,
                maps:get(ChildHex, Acc, 0),
                Acc2
            )
        end,
        #{},
        ChildToParents
    ).

-spec get_density_var(
    Var :: atom(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> {error, any()} | {ok, [pos_integer()]}.
get_density_var(Var, Ledger) ->
    case blockchain:config(Var, Ledger) of
        {error, _} = E ->
            E;
        {ok, Bin} ->
            [N, Tgt, Max] = [
                list_to_integer(I)
                || I <- string:tokens(binary:bin_to_list(Bin), ",")
            ],
            {ok, [N, Tgt, Max]}
    end.
