-module(blockchain_hex).

-export([var_map/1, scale/3, destroy_memoization/0]).

-ifdef(TEST).
-export([densities/3]).
-endif.

-include("blockchain_vars.hrl").
-include_lib("common_test/include/ct.hrl").

-define(MEMO_TBL, '__blockchain_hex_memoization_tbl').
-define(ETS_OPTS, [named_table, public]).

-type density_map() :: #{h3:h3_index() => pos_integer()}.
-type densities() :: {UnclippedDensities :: density_map(), ClippedDensities :: density_map()}.
-type var_map() :: #{0..12 => map()}.
-type locations() :: #{h3:h3_index() => [libp2p_crypto:pubkey_bin(), ...]}.
-type h3_indices() :: [h3:h3_index()].

-export_type([var_map/0]).

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------
-spec destroy_memoization() -> true.
%% @doc This call will destroy the memoization context used during a rewards
%% calculation.
destroy_memoization() -> ets:delete(?MEMO_TBL).

-spec scale(
    Location :: h3:h3_index(),
    VarMap :: var_map(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> float().
%% @doc Given a hex location, return the rewards scaling factor. This call is
%% memoized.
scale(Location, VarMap, Ledger) ->
    case lookup(Location) of
        {ok, Scale} -> Scale;
        not_found ->
            memoize(Location, do_scale(Location, VarMap, Ledger))
    end.


%% TODO: This ought to be stored in the ledger because it won't change much? ever?
%% after it's been computed. Seems dumb to calculate it every single time we pay
%% out rewards.
-spec var_map(Ledger :: blockchain_ledger_v1:ledger()) -> {error, any()} | {ok, var_map()}.
%% @doc This function returns a map of hex resolutions mapped to hotspot density targets and
%% maximums. These numbers are used during PoC witness and challenge rewards calculations.
var_map(Ledger) ->
    ResolutionVars = [
        ?hip17_res_0,
        ?hip17_res_1,
        ?hip17_res_2,
        ?hip17_res_3,
        ?hip17_res_4,
        ?hip17_res_5,
        ?hip17_res_6,
        ?hip17_res_7,
        ?hip17_res_8,
        ?hip17_res_9,
        ?hip17_res_10,
        ?hip17_res_11,
        ?hip17_res_12
    ],

    {_I, Errors, M} = lists:foldl(
        fun(A, {I, Errors, Acc}) ->
            case get_density_var(A, Ledger) of
                {error, _} = E ->
                    {I + 1, [{A, E} | Errors], Acc};
                {ok, [N, Tgt, Max]} ->
                    {I + 1, Errors,
                        maps:put(
                            I,
                            #{
                                n => N,
                                tgt => Tgt,
                                max => Max
                            },
                            Acc
                        )}
            end
        end,
        {0, [], #{}},
        ResolutionVars
    ),

    case Errors of
        [] -> {ok, M};
        Errors -> {error, Errors}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
-spec lookup(Key :: term()) -> {ok, Result :: term()} | not_found.
lookup(Key) ->
    try
        case ets:lookup(?MEMO_TBL, Key) of
            [{_Key, Res}] -> {ok, Res};
            [] -> not_found
        end
    catch
        %% if the table doesn't exist yet, create it and return `not_found'
        _:badarg ->
            _Name = ets:new(?MEMO_TBL, ?ETS_OPTS),
            not_found
    end.

-spec memoize(Key :: term(), Result :: term()) -> Result :: term().
memoize(Key, Result) ->
    true = ets:insert(?MEMO_TBL, {Key, Result}),
    Result.

-spec do_scale(
    Location :: h3:h3_index(),
    VarMap :: var_map(),
    Ledger :: blockchain_ledger_v1:ledger() ) -> float().
do_scale(Location, VarMap, Ledger) ->
    {UnclippedDensities, ClippedDensities} = densities(Location, VarMap, Ledger),
    maps:get(Location, ClippedDensities) / maps:get(Location, UnclippedDensities).

-spec densities(
    H3Index :: h3:h3_index(),
    VarMap :: var_map(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> densities().
densities(H3Index, VarMap, Ledger) ->
    Locations = blockchain_ledger_v1:lookup_gateways_from_hex(h3:k_ring(H3Index, 1), Ledger),
    %% Calculate clipped and unclipped densities
    densities(H3Index, VarMap, Locations, Ledger).

-spec densities(
    H3Root :: h3:h3_index(),
    VarMap :: var_map(),
    Locations :: locations(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> densities().
densities(H3Root, VarMap, Locations, Ledger) ->
    case maps:size(Locations) of
        0 ->
            {#{}, #{}};
        _ ->
            UpperBoundRes = lists:max([h3:get_resolution(H3) || H3 <- maps:keys(Locations)]),
            LowerBoundRes = h3:get_resolution(H3Root),
            ct:pal("UpperBoundRes: ~p, LowerBoundRes: ~p", [UpperBoundRes, LowerBoundRes]),

            [Head | Tail] = lists:seq(UpperBoundRes, LowerBoundRes, -1),

            %% find parent hexs to all hotspots at highest resolution in chain variables
            {ParentHexes, InitialDensities} =
                maps:fold(
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

            build_densities(
                H3Root,
                Ledger,
                VarMap,
                ParentHexes,
                {InitialDensities, InitialDensities},
                Tail
            )
    end.

-spec build_densities(
    h3:h3_index(),
    blockchain_ledger_v1:ledger(),
    var_map(),
    h3_indices(),
    densities(),
    [non_neg_integer()]
) -> densities().
build_densities(_H3Root, _Ledger, _VarMap, _ParentHexes, {UAcc, Acc}, []) ->
    {UAcc, Acc};
build_densities(H3Root, Ledger, VarMap, ChildHexes, {UAcc, Acc}, [Res | Tail]) ->
    UD = unclipped_densities(ChildHexes, Res, Acc),
    UM0 = maps:merge(UAcc, UD),
    M0 = maps:merge(Acc, UD),

    OccupiedHexesThisRes = maps:keys(UD),

    DensityTarget = maps:get(tgt, maps:get(Res, VarMap)),

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
    Res :: 0..12,
    VarMap :: var_map(),
    OccupiedCount :: non_neg_integer()
) -> non_neg_integer().
limit(Res, VarMap, OccupiedCount) ->
    min(
        maps:get(max, maps:get(Res, VarMap)),
        maps:get(tgt, maps:get(Res, VarMap)) *
            max((OccupiedCount - maps:get(n, maps:get(Res, VarMap))), 1)
    ).

-spec occupied_count(
    DensityTarget :: 0..12,
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

-spec unclipped_densities(h3_indices(), 0..12, density_map()) -> density_map().
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
