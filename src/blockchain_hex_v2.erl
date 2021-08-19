-module(blockchain_hex_v2).

-export([
         scale/2, scale/4,
         destroy_memoization/0
        ]).

-ifdef(TEST).
%% -export([densities/3]).
-endif.

-include("blockchain_vars.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PRE_UNCLIP_TBL, '__blockchain_hex_unclipped_tbl').
-define(PRE_CLIP_TBL, '__blockchain_hex_clipped_tbl').

-define(ETS_OPTS, [named_table, public]).

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------
-spec destroy_memoization() -> true.
%% @doc This call will destroy the memoization context used during a rewards
%% calculation.
destroy_memoization() ->
    try ets:delete(?PRE_CLIP_TBL) catch _:_ -> true end,
    try ets:delete(?PRE_UNCLIP_TBL) catch _:_ -> true end.

%% @doc This call is for blockchain_etl to use directly
-spec scale(Location :: h3:h3_index(),
            Ledger :: blockchain_ledger_v1:ledger()) -> {error, any()} | {ok, float()}.
scale(Location, Ledger) ->
    case blockchain_hex_v1:var_map(Ledger) of
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
    VarMap :: blockhain_hex_v1:var_map(),
    TargetRes :: 0..12,
    Ledger :: blockchain_ledger_v1:ledger()
) -> float().
%% @doc Given a hex location, return the rewards scaling factor. This call is
%% memoized.
scale(Location, _VarMap, TargetRes, Ledger) ->
    maybe_precalc(Ledger),
    %% hip0017 states to go from R -> 0 and take a product of the clipped(parent)/unclipped(parent)
    %% however, we specify the lower bound instead of going all the way down to 0

    R = h3:get_resolution(Location),

    lists:foldl(fun(Res, Acc) ->
                        Parent = h3:parent(Location, Res),
                        case ulookup(Parent) of
                            0 -> Acc;
                            Unclipped -> Acc * (clookup(Parent) / Unclipped)
                        end
                end, 1.0, lists:seq(R, TargetRes, -1)).


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ulookup(Key) ->
    case ets:lookup(?PRE_UNCLIP_TBL, Key) of
        [{_Key, Res}] -> Res;
        [] -> 0
    end.

clookup(Key) ->
    case ets:lookup(?PRE_CLIP_TBL, Key) of
        [{_Key, Res}] -> Res;
        [] -> 0
    end.

maybe_precalc(Ledger) ->
    case ets:info(?PRE_UNCLIP_TBL) of
        undefined ->
            precalc(Ledger);
        _ ->
            ok
    end.

precalc(Ledger) ->
    {ok, VarMap} = blockchain_hex_v1:var_map(Ledger),
    Start = erlang:monotonic_time(millisecond),
    InteractiveBlocks =
        case blockchain_ledger_v1:config(?hip17_interactivity_blocks, Ledger) of
            {ok, V} -> V;
            {error, not_found} -> 0 % XXX what should this value be?
        end,
    {ok, CurrentHeight} = blockchain_ledger_v1:current_height(Ledger),
    ets:new(?PRE_UNCLIP_TBL, [named_table, public]),
    ets:new(?PRE_CLIP_TBL, [named_table, public]),
    UsedResolutions = [N || N <- lists:seq(0, 12), maps:get(tgt, maps:get(N, VarMap)) /= 100000],
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
                                            ets:update_counter(?PRE_UNCLIP_TBL, H, 1, {H, 0})
                                end,
                                    %% dropping this to 10 is a big speedup, but perhaps unsafe?
                                    [h3:parent(L, N) || N <- UsedResolutions]),
                                  Acc + 1
                          end;
                      _ -> Acc
                  end
          end, 0, Ledger),
    %% pre-unfold these because we access them a lot.
    Vars0 =
        [begin
             VarAtRes = maps:get(Res, VarMap),
             N = maps:get(n, VarAtRes),
             Tgt = maps:get(tgt, VarAtRes),
             Max = maps:get(max, VarAtRes),
             {N, Tgt, Max}
         end
         || Res <- lists:seq(1, 12)],  %% use the whole thing here for numbering
    Vars = list_to_tuple(Vars0),
    ets:foldl(fun({Hex, Ct}, _) ->
                      Res = h3:get_resolution(Hex),
                      DensityTarget = element(2, element(Res, Vars)),
                      OccupiedCount = occupied_count(DensityTarget, Hex),
                      Limit = limit(Res, Vars, OccupiedCount),
                      Actual = min(Limit, Ct),
                      ets:update_counter(?PRE_CLIP_TBL, Hex, Actual, {Hex, 0})
              end, foo, ?PRE_UNCLIP_TBL),

    End = erlang:monotonic_time(millisecond),
    lager:info("ets ~p ~p ~p ~p", [ets:info(?PRE_UNCLIP_TBL, size), TCt, InteractiveBlocks, End-Start]).

-spec limit(
    Res :: 0..12,
    VarMap :: blockchain_hex_v1:var_map(),
    OccupiedCount :: non_neg_integer()
) -> non_neg_integer().
limit(Res, Vars, OccupiedCount) ->
    VarAtRes = element(Res, Vars),
    N = element(1, VarAtRes),
    DensityTgt = element(2, VarAtRes),
    DensityMax = element(3, VarAtRes),
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
            case ulookup(Neighbor) >= DensityTarget of
                false -> Acc;
                true -> Acc + 1
            end
        end,
        0,
        H3Neighbors
    ).

-spec get_target_res(Ledger :: blockchain_ledger_v1:ledger()) -> {error, any()} | {ok, non_neg_integer()}.
get_target_res(Ledger) ->
    case blockchain:config(?density_tgt_res, Ledger) of
        {error, _}=E -> E;
        {ok, V} -> {ok, V}
    end.

