-module(blockchain_hex).

-export([
         var_map/1,
         scale/2, scale/4,
         destroy_memoization/0,

         %% exported for dialyzer reasons
         precalc/2
        ]).

-include("blockchain_vars.hrl").

-define(PRE_UNCLIP_TBL, '__blockchain_hex_unclipped_tbl').
-define(PRE_CLIP_TBL, '__blockchain_hex_clipped_tbl').

-define(ETS_OPTS, []).

-type var_map() :: #{0..12 => map()}.
-export_type([var_map/0]).

-ifdef(TEST).

-export([
         lookup/2
        ]).

-endif.

%%--------------------------------------------------------------------
%% Public functions
%%--------------------------------------------------------------------
-spec destroy_memoization() -> true.
%% @doc This call will destroy the memoization context used during a rewards
%% calculation.
destroy_memoization() ->
    _ = erase(?PRE_CLIP_TBL),
    _ = erase(?PRE_UNCLIP_TBL),
    true.

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
scale(Location, _VarMap, TargetRes, Ledger) ->
    maybe_precalc(Ledger),
    %% hip0017 states to go from R -> 0 and take a product of the clipped(parent)/unclipped(parent)
    %% however, we specify the lower bound instead of going all the way down to 0

    R = h3:get_resolution(Location),
    UnclipETS = get(?PRE_UNCLIP_TBL),
    ClipETS = get(?PRE_CLIP_TBL),

    lists:foldl(fun(Res, Acc) ->
                        Parent = h3:parent(Location, Res),
                        case lookup(UnclipETS, Parent) of
                            0 -> Acc;
                            Unclipped -> Acc * (lookup(ClipETS, Parent) / Unclipped)
                        end
                end, 1.0, lists:seq(R, TargetRes, -1)).


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

-spec lookup(Tbl :: ets:table(), Key :: h3:h3_index()) -> non_neg_integer().
lookup(Tbl, Key) ->
    case ets:lookup(Tbl, Key) of
        [{_Key, Res}] -> Res;
        [] -> 0
    end.

-spec maybe_precalc(Ledger :: blockchain_ledger_v1:ledger()) -> ok.
maybe_precalc(Ledger) ->
    case get(?PRE_UNCLIP_TBL) of
        undefined ->
            precalc(Ledger);
        _ ->
            ok
    end.

-spec precalc(Ledger :: blockchain_ledger_v1:ledger()) -> ok.
precalc(Ledger) ->
    precalc(false, Ledger).

-spec precalc(boolean(), Ledger :: blockchain_ledger_v1:ledger()) -> ok.
precalc(Testing, Ledger) ->
    {ok, VarMap} = var_map(Ledger),
    Start = erlang:monotonic_time(millisecond),
    InteractiveBlocks =
        case blockchain_ledger_v1:config(?hip17_interactivity_blocks, Ledger) of
            {ok, V} -> V;
            {error, not_found} -> 0 % XXX what should this value be?
        end,
    {ok, CurrentHeight} = blockchain_ledger_v1:current_height(Ledger),
    UnclipETS = ets:new(?PRE_UNCLIP_TBL, ?ETS_OPTS),
    ClipETS = ets:new(?PRE_CLIP_TBL, ?ETS_OPTS),
    put(?PRE_UNCLIP_TBL, UnclipETS),
    put(?PRE_CLIP_TBL, ClipETS),

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

    UsedResolutions =
        case Testing of
            false ->
                [N || N <- lists:seq(0, 12), maps:get(tgt, maps:get(N, VarMap)) /= 100000];
            true -> lists:seq(1, 11)
        end,

    %% This won't do the same thing as the old code if we make it so that we care about the
    %% densities at 11 and 12.  it's not clear how they would differ, we'd need to experiment.
    MaxRes = min(12, lists:max(UsedResolutions) + 1),
    TestMode = application:get_env(blockchain, hip17_test_mode, false),
    InitHexes0 =
        blockchain_ledger_v1:cf_fold(
          active_gateways,
          fun({_Addr, BinGw}, Acc) ->
                  %% todo explore alternate serialization formats where we can get the location and
                  %% the last challenge without fully deserializing the gateway
                  G = blockchain_ledger_gateway_v2:deserialize(BinGw),
                  L = blockchain_ledger_gateway_v2:location(G),
                  LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(G),
                  case (LastChallenge /= undefined
                        andalso (CurrentHeight - LastChallenge) =< InteractiveBlocks)
                      orelse TestMode of
                      true ->
                          case L of
                              undefined -> Acc;
                              _ ->
                                  Hex = h3:parent(L, MaxRes),
                                  ets:update_counter(UnclipETS, Hex, 1, {Hex, 0}),
                                  ets:update_counter(ClipETS, Hex, 1, {Hex, 0}),
                                  [Hex | Acc]
                          end;
                      _ -> Acc
                  end
          end, [], Ledger),

    InitHexes = lists:usort(InitHexes0),

    %% starting from the bottom grab each level and fold through it, calculating the unclipped
    %% density from the level below?
    lists:foldl(
      fun(Level, Acc) ->
              Acc1 =
                  lists:foldl(
                    fun(Hex, A) ->
                            ResHex = h3:parent(Hex, Level),
                            Ct = lookup(ClipETS, Hex),
                            ets:update_counter(UnclipETS, ResHex, Ct, {ResHex, 0}),
                            ets:update_counter(ClipETS, ResHex, Ct, {ResHex, 0}),
                            [ResHex | A]
                    end, [], Acc),
              Acc2 = lists:usort(Acc1),
              lists:foreach(
                fun(ResHex) ->
                        DensityTarget = element(2, element(Level, Vars)),
                        OccupiedCount = occupied_count(DensityTarget, ResHex, ClipETS),
                        Limit = limit(Level, Vars, OccupiedCount),
                        Ct = lookup(UnclipETS, ResHex),
                        Actual = min(Limit, Ct),
                        ets:insert(ClipETS, {ResHex, Actual})
                end, Acc2),
              Acc2
      end,
      InitHexes,
      lists:reverse(UsedResolutions)),  %% go from the bottom here

    End = erlang:monotonic_time(millisecond),
    lager:info("ets ~p ~p", [ets:info(UnclipETS, size), End-Start]).

-spec limit(
    Res :: 0..12,
    VarTuple :: tuple(),
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
    ThisResHex :: h3:h3_index(),
    ClipETS :: ets:table()
) -> non_neg_integer().
occupied_count(DensityTarget, ThisResHex, ClipETS) ->
    H3Neighbors = h3:k_ring(ThisResHex, 1),

    lists:foldl(
        fun(Neighbor, Acc) ->
            case lookup(ClipETS, Neighbor) >= DensityTarget of
                false -> Acc;
                true -> Acc + 1
            end
        end,
        0,
        H3Neighbors
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

