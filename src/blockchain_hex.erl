-module(blockchain_hex).

-export([var_map/1,
         scale/2, scale/4,
         destroy_memoization/0]).

-ifdef(TEST).
-export([densities/3]).
-endif.

-include("blockchain_vars.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SCALE_MEMO_TBL, '__blockchain_hex_scale_memoization_tbl').
-define(DENSITY_MEMO_TBL, '__blockchain_hex_density_memoization_tbl').
-define(ETS_OPTS, [named_table, public]).

-type density_map() :: #{h3:h3_index() => non_neg_integer()}.
-type densities() :: {UnclippedDensities :: density_map(), ClippedDensities :: density_map()}.
-type var_map() :: #{0..12 => map()}.
-type locations() :: #{h3:h3_index() => [libp2p_crypto:pubkey_bin(), ...]}.
-type h3_indices() :: [h3:h3_index()].
-type tblnames() :: ?SCALE_MEMO_TBL|?DENSITY_MEMO_TBL.

-export_type([var_map/0]).

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------
-spec destroy_memoization() -> true.
%% @doc This call will destroy the memoization context used during a rewards
%% calculation.
destroy_memoization() ->
    try ets:delete(?SCALE_MEMO_TBL) catch _:_ -> true end,
    try ets:delete(?DENSITY_MEMO_TBL) catch _:_ -> true end.

%% @doc This call is for blockchain_etl to use directly
-spec scale(Location :: h3:h3_index(),
            Ledger :: blockchain_ledger_v1:ledger()) -> {error, any()} | {ok, float()}.
scale(Location, Ledger) ->
    case var_map(Ledger) of
        {error, _}=E -> E;
        {ok, VarMap} ->
            case get_target_res(Ledger) of
                {error, _}=E -> E;
                {ok, TargetRes} ->
                    S = scale(Location, VarMap, TargetRes, Ledger),
                    {ok, S}
            end
    end.

-spec scale(
    Location :: h3:h3_index(),
    VarMap :: var_map(),
    TargetRes :: 0..12,
    Ledger :: blockchain_ledger_v1:ledger()
) -> float().
%% @doc Given a hex location, return the rewards scaling factor. This call is
%% memoized.
scale(Location, VarMap, TargetRes, Ledger) ->
    case lookup(Location, ?SCALE_MEMO_TBL) of
        {ok, Scale} -> Scale;
        not_found ->
            memoize(?SCALE_MEMO_TBL, Location,
                    calculate_scale(Location, VarMap, TargetRes, Ledger))
    end.

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
-spec lookup(Key :: term(),
             TblName :: tblnames()) ->
    {ok, Result :: term()} | not_found.
lookup(Key, TblName) ->
    try
        case ets:lookup(TblName, Key) of
            [{_Key, Res}] -> {ok, Res};
            [] -> not_found
        end
    catch
        %% if the table doesn't exist yet, create it and return `not_found'
        error:badarg ->
            _ = maybe_start(TblName),
            not_found
    end.

-spec maybe_start(TblName :: tblnames()) -> tblnames().
maybe_start(TblName) ->
    try
        _TblName = ets:new(TblName, ?ETS_OPTS)
    catch
        error:badarg -> TblName
    end.

-spec memoize(TblName :: tblnames(),
              Key :: term(),
              Result :: term()) -> Result :: term().
memoize(TblName, Key, Result) ->
    true = ets:insert(TblName, {Key, Result}),
    Result.

-spec calculate_scale(
    Location :: h3:h3_index(),
    VarMap :: var_map(),
    TargetRes :: 0..12,
    Ledger :: blockchain_ledger_v1:ledger() ) -> float().
calculate_scale(Location, VarMap, TargetRes, Ledger) ->
    %% hip0017 states to go from R -> 0 and take a product of the clipped(parent)/unclipped(parent)
    %% however, we specify the lower bound instead of going all the way down to 0

    R = h3:get_resolution(Location),

    %% Calculate densities at the outermost hex
    OuterMostParent = h3:parent(Location, TargetRes),
    {UnclippedDensities, ClippedDensities} = densities(OuterMostParent, VarMap, Ledger),

    lists:foldl(fun(Res, Acc) ->
                        Parent = h3:parent(Location, Res),
                        case maps:get(Parent, UnclippedDensities) of
                            0 -> Acc;
                            Unclipped -> Acc * (maps:get(Parent, ClippedDensities) / Unclipped)
                        end
                end, 1.0, lists:seq(R, TargetRes, -1)).


-spec densities(
    H3Index :: h3:h3_index(),
    VarMap :: var_map(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> densities().
densities(H3Index, VarMap, Ledger) ->
    case lookup(H3Index, ?DENSITY_MEMO_TBL) of
        {ok, Densities} -> Densities;
        not_found -> memoize(?DENSITY_MEMO_TBL, H3Index,
                            calculate_densities(H3Index, VarMap, Ledger))
    end.

-spec calculate_densities(
    H3Index :: h3:h3_index(),
    VarMap :: var_map(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> densities().
calculate_densities(H3Index, VarMap, Ledger) ->
    InteractiveBlocks = case blockchain_ledger_v1:config(?hip17_interactivity_blocks, Ledger) of
                            {ok, V} -> V;
                            {error, not_found} -> 0 % XXX what should this value be?
                        end,
    Locations = blockchain_ledger_v1:lookup_gateways_from_hex(h3:k_ring(H3Index, 2), Ledger),

    Interactive = case application:get_env(blockchain, hip17_test_mode, false) of
                      true ->
                          %% HIP17 test mode, no interactive filtering
                          Locations;
                      false ->
                          maps:map(
                            fun(_K, V) ->
                                    filter_interactive_gws(V, InteractiveBlocks, Ledger)
                            end, Locations)
                  end,

    %% Calculate clipped and unclipped densities
    densities(H3Index, VarMap, Interactive, Ledger).

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

            [Head | Tail] = lists:seq(UpperBoundRes, LowerBoundRes, -1),

            %% find parent hexes to all hotspots at highest resolution in chain variables
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
    [0..15]
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
            maps:put(ThisResHex, min(Limit, maps:get(ThisResHex, M0)), Acc3)
        end,
        M0,
        OccupiedHexesThisRes
    ),

    build_densities(H3Root, Ledger, VarMap, OccupiedHexesThisRes, {UM0, M1}, Tail).

-spec filter_interactive_gws( GWs :: [libp2p_crypto:pubkey_bin(), ...],
                              InteractiveBlocks :: pos_integer(),
                              Ledger :: blockchain_ledger_v1:ledger()) ->
    [libp2p_crypto:pubkey_bin(), ...].
%% @doc This function filters a list of gateway addresses which are considered
%% "interactive" for the purposes of HIP17 based on the last block when it
%% responded to a POC challenge compared to the current chain height.
filter_interactive_gws(GWs, InteractiveBlocks, Ledger) ->
    {ok, CurrentHeight} = blockchain_ledger_v1:current_height(Ledger),
    lists:filter(fun(GWAddr) ->
                         case blockchain_ledger_v1:find_gateway_info(GWAddr, Ledger) of
                             {ok, GWInfo} ->
                                 case blockchain_ledger_gateway_v2:last_poc_challenge(GWInfo) of
                                     undefined -> false;
                                     LastChallenge ->
                                         (CurrentHeight - LastChallenge) =< InteractiveBlocks
                                 end;
                             {error, not_found} -> false
                         end
                 end, GWs).

-spec limit(
    Res :: 0..12,
    VarMap :: var_map(),
    OccupiedCount :: non_neg_integer()
) -> non_neg_integer().
limit(Res, VarMap, OccupiedCount) ->
    VarAtRes = maps:get(Res, VarMap),
    DensityMax = maps:get(max, VarAtRes),
    DensityTgt = maps:get(tgt, VarAtRes),
    N = maps:get(n, VarAtRes),
    Max = max(((OccupiedCount - N) + 1), 1),
    min(DensityMax, DensityTgt * Max).

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

-spec get_target_res(Ledger :: blockchain_ledger_v1:ledger()) -> {error, any()} | {ok, non_neg_integer()}.
get_target_res(Ledger) ->
    case blockchain:config(?density_tgt_res, Ledger) of
        {error, _}=E -> E;
        {ok, V} -> {ok, V}
    end.

