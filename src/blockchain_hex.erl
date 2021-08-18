-module(blockchain_hex).

-export([var_map/1,
         scale/2, scale/4,
         destroy_memoization/0]).

-ifdef(TEST).
%% -export([densities/3]).
-endif.

-include("blockchain_vars.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SCALE_MEMO_TBL, '__blockchain_hex_scale_memoization_tbl').
-define(DENSITY_MEMO_TBL, '__blockchain_hex_density_memoization_tbl').
-define(PRE_MEMO_TBL, '__blockchain_hex_prememoization_tbl').
-define(PRE_CLIP_MEMO_TBL, '__blockchain_hex_denasdsdsity_prememoization_tbl').
-define(ETS_OPTS, [named_table, public]).

%% -type density_map() :: #{h3:h3_index() => non_neg_integer()}.
%% -type densities() :: ClippedDensities :: density_map().
-type var_map() :: #{0..12 => map()}.
%% -type h3_indices() :: [h3:h3_index()].
-type tblnames() :: ?SCALE_MEMO_TBL|?DENSITY_MEMO_TBL.

-export_type([var_map/0]).

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------
-spec destroy_memoization() -> true.
%% @doc This call will destroy the memoization context used during a rewards
%% calculation.
destroy_memoization() ->
    try ets:delete(?PRE_MEMO_TBL) catch _:_ -> true end,
    try ets:delete(?PRE_CLIP_MEMO_TBL) catch _:_ -> true end,
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
                    try
                        S = scale(Location, VarMap, TargetRes, Ledger),
                        {ok, S}
                    catch What:Why:ST ->
                        {ok, CurHeight} = blockchain_ledger_v1:current_height(Ledger),
                        lager:error("failed to calculate scale for location: ~p, ~p:~p:~p", [Location, What, Why, ST]),
                        {error, {failed_scale_calc, Location, CurHeight}}
                    end
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
    maybe_precalc(Ledger),
    case lookup(Location, ?SCALE_MEMO_TBL) of
        {ok, Scale} -> Scale;
        not_found ->
            Ret =
            memoize(?SCALE_MEMO_TBL, Location,
                    calculate_scale(Location, VarMap, TargetRes, Ledger)),
            lager:info("scale ~p ~p ~p", [Location, TargetRes, Ret]),
            Ret
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

dlookup(Key) ->
    case ets:lookup(?PRE_MEMO_TBL, Key) of
        [{_Key, Res}] -> Res;
        [] -> 0
    end.

clookup(Key) ->
    case ets:lookup(?PRE_CLIP_MEMO_TBL, Key) of
        [{_Key, Res}] -> Res;
        [] -> 0
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
calculate_scale(Location, _VarMap, TargetRes, _Ledger) ->
    %% hip0017 states to go from R -> 0 and take a product of the clipped(parent)/unclipped(parent)
    %% however, we specify the lower bound instead of going all the way down to 0

    R = h3:get_resolution(Location),
    %% {R, OuterMostParent} = {h3:get_resolution(Location),
    %%                         h3:parent(Location, TargetRes)},

    %% %% Calculate densities at the outermost hex
    %% ClippedDensities = densities(OuterMostParent, VarMap, Ledger),

    %% lager:info("outermost ~p", [OuterMostParent]),

    lists:foldl(fun(Res, Acc) ->
                        Parent = h3:parent(Location, Res),
                        case dlookup(Parent) of
                            0 -> Acc;
                            Unclipped -> Acc * (clookup(Parent) / Unclipped)
                        end
                end, 1.0, lists:seq(R, TargetRes, -1)).


%% -spec densities(
%%     H3Index :: h3:h3_index(),
%%     VarMap :: var_map(),
%%     Ledger :: blockchain_ledger_v1:ledger()
%% ) -> densities().
%% densities(H3Index, VarMap, Ledger) ->
%%     case lookup(H3Index, ?DENSITY_MEMO_TBL) of
%%         {ok, Densities} -> Densities;
%%         not_found -> memoize(?DENSITY_MEMO_TBL, H3Index,
%%                              calculate_densities(H3Index, VarMap, Ledger))
%%     end.

maybe_precalc(Ledger) ->
    case ets:info(?PRE_MEMO_TBL) of
        undefined ->
            precalc(Ledger);
        _ ->
            ok
    end.

precalc(Ledger) ->
    {ok, VarMap} = var_map(Ledger),
    Start = erlang:monotonic_time(millisecond),
    InteractiveBlocks =
        case blockchain_ledger_v1:config(?hip17_interactivity_blocks, Ledger) of
            {ok, V} -> V;
            {error, not_found} -> 0 % XXX what should this value be?
        end,
    {ok, CurrentHeight} = blockchain_ledger_v1:current_height(Ledger),
    ets:new(?PRE_MEMO_TBL, [named_table, public]),
    ets:new(?PRE_CLIP_MEMO_TBL, [named_table, public]),
    TCt =
    blockchain_ledger_v1:cf_fold(
      active_gateways,
      fun({_Addr, BinGw}, Acc) ->
              G = blockchain_ledger_gateway_v2:deserialize(BinGw),
              L = blockchain_ledger_gateway_v2:location(G),
              LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(G),
              case LastChallenge /= undefined andalso
                  (CurrentHeight - LastChallenge) =< InteractiveBlocks of
                  true ->
                      case L of
                          undefined -> Acc;
                          _ ->
                              lists:foreach(
                                fun(H) ->
                                ets:update_counter(?PRE_MEMO_TBL, H, 1, {H, 0})
                                end,
                                [h3:parent(L, N) || N <- lists:seq(4, 12)]),
                              Acc + 1
                      end;
                  _ -> Acc
              end
      end, 0, Ledger),
    ets:foldl(fun({Hex, Ct}, _) ->
                      Res = h3:get_resolution(Hex),
                      DensityTarget = maps:get(tgt, maps:get(Res, VarMap)),
                      OccupiedCount = occupied_count(DensityTarget, Hex),
                      Limit = limit(Res, VarMap, OccupiedCount),
                      Actual = min(Limit, Ct),
                      ets:update_counter(?PRE_CLIP_MEMO_TBL, Hex, Actual, {Hex, 0})
              end, foo, ?PRE_MEMO_TBL),

    End = erlang:monotonic_time(millisecond),
    lager:info("ets ~p ~p ~p ~p", [ets:info(?PRE_MEMO_TBL, size), TCt, InteractiveBlocks, End-Start]).

%% -spec calculate_densities(
%%     H3Index :: h3:h3_index(),
%%     VarMap :: var_map(),
%%     Ledger :: blockchain_ledger_v1:ledger()
%% ) -> densities().
%% calculate_densities(H3Index, VarMap, Ledger) ->
%%     %% Calculate clipped and densities
%%     densities_(H3Index, VarMap, Ledger).

%% -spec densities_(
%%     H3Root :: h3:h3_index(),
%%     VarMap :: var_map(),
%%     Ledger :: blockchain_ledger_v1:ledger()
%% ) -> densities().
%% densities_(H3Root, VarMap, Ledger) ->
%%     Lower = case blockchain_ledger_v1:config(?hip17_resolution_limit, Ledger) of
%%                 {ok, Limit} ->
%%                     Limit;
%%                 {error, not_found} ->
%%                     h3:get_resolution(H3Root)
%%             end,
%%     Tail = lists:seq(9, Lower, -1),

%%     InitialHexes0 = h3:k_ring(H3Root, 2),
%%     InitialHexes = lists:flatmap(fun(H) ->
%%                                          h3:children(H, 10)
%%                                  end, InitialHexes0),

%%     %% lager:info("init ~p lower ~p", [InitialHexes, Lower]),

%%     %% find parent hexes to all hotspots at highest resolution in chain variables
%%     InitialDensities =
%%         lists:foldl(
%%           fun(Hex, Acc) ->
%%                   case dlookup(Hex) of
%%                       0 -> Acc;
%%                       Ct ->
%%                           maps:put(Hex, Ct, Acc)
%%                   end
%%           end,
%%           #{},
%%           InitialHexes
%%          ),

%%     build_densities(
%%       H3Root,
%%       Ledger,
%%       VarMap,
%%       InitialHexes,
%%       InitialDensities,
%%       Tail
%%      ).

%% -spec build_densities(
%%     h3:h3_index(),
%%     blockchain_ledger_v1:ledger(),
%%     var_map(),
%%     h3_indices(),
%%     densities(),
%%     [0..15]
%% ) -> densities().
%% build_densities(_H3Root, _Ledger, _VarMap, _ParentHexes, Acc, []) ->
%%     Acc;
%% build_densities(H3Root, Ledger, VarMap, ChildHexes, Acc, [Res | Tail]) ->
%%     UD = unclipped_densities(ChildHexes, Res),
%%     M0 = maps:merge(Acc, UD),

%%     lager:info("root ~p size ~p res ~p", [H3Root, maps:size(M0), Res]),

%%     OccupiedHexesThisRes = maps:keys(UD),

%%     DensityTarget = maps:get(tgt, maps:get(Res, VarMap)),

%%     M1 = lists:foldl(
%%         fun(ThisResHex, Acc3) ->
%%             OccupiedCount = occupied_count(DensityTarget, ThisResHex, UD),
%%             Limit = limit(Res, VarMap, OccupiedCount),
%%             maps:put(ThisResHex, min(Limit, maps:get(ThisResHex, M0)), Acc3)
%%         end,
%%         M0,
%%         OccupiedHexesThisRes
%%     ),

%%     build_densities(H3Root, Ledger, VarMap, OccupiedHexesThisRes, M1, Tail).


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
    ThisResHex :: h3:h3_index()
) -> non_neg_integer().
occupied_count(DensityTarget, ThisResHex) ->
    H3Neighbors = h3:k_ring(ThisResHex, 1),

    lists:foldl(
        fun(Neighbor, Acc) ->
            case dlookup(Neighbor) >= DensityTarget of
                false -> Acc;
                true -> Acc + 1
            end
        end,
        0,
        H3Neighbors
    ).

%% -spec unclipped_densities(h3_indices(), 0..12) -> density_map().
%% unclipped_densities(ChildToParents, Res) ->
%%     lists:foldl(
%%       fun(ChildHex, Acc2) ->
%%               case parent(ChildHex, Res) of
%%                   too_low -> Acc2;
%%                   Parent ->
%%                       case dlookup(ChildHex) of
%%                           0 -> Acc2;
%%                           Ct ->
%%                               maps:update_with(
%%                                 Parent,
%%                                 fun(V) -> V + Ct end,
%%                                 Ct,
%%                                 Acc2
%%                                )
%%                       end
%%               end
%%       end,
%%       #{},
%%       ChildToParents
%%      ).

%% parent(Hex, Res) ->
%%     try
%%         h3:parent(Hex, Res)
%%     catch _:_ ->
%%             too_low
%%     end.

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

