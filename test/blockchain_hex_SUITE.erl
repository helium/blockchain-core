-module(blockchain_hex_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain_vars.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    full_known_values_test/1,
    known_values_test/1,
    known_differences_test/1,
    scale_test/1,
    h3dex_test/1,
    export_scale_test/1
]).

%% Values taken from python model
-define(KNOWN, [
    {631210968849144319, 1},
    {631179325713598463, 2},
    {631196205757572607, 1},
    {631243921328349695, 1},
    {631243921527814655, 3},
    {631211399470291455, 1},
    {577234808489377791, 2319},
    {577023702256844799, 449},
    {577692205326532607, 1039},
    {577340361605644287, 6},
    {576812596024311807, 54},
    {577621836582354943, 5},
    {577164439745200127, 1459},
    {579768083279773695, 2},
    {577305177233555455, 5},
    {576601489791778815, 9},
    {576636674163867647, 56},
    {576777411652222975, 3},
    {577481099093999615, 43},
    {578114417791598591, 1},
    {577269992861466623, 1},
    {577586652210266111, 24},
    {577762574070710271, 381},
    {577199624117288959, 1817},
    {577058886628933631, 1},
    {578325524024131583, 1},
    {577832942814887935, 26},
    {576953333512667135, 2},
    {579838452023951359, 1},
    {579451423930974207, 1},
    {576918149140578303, 600},
    {599663374669709311, 3},
    {599238598109167615, 1},
    {599653418935517183, 4},
    {600256126327455743, 2},
    {600176343014965247, 1},
    {599718760420474879, 2},
    {599736317173039103, 1},
    {599721183855771647, 2},
    {599685859897245695, 1}
]).

%% There are 1952 differences when we calculate clipped vs unclipped densities
-define(KNOWN_CLIP_VS_UNCLIPPED, 1952).

%% Some known differences between clipped vs unclipped densities
-define(KNOWN_DIFFERENCES, #{
    617712130192310271 => {1, 2},
    617761313717485567 => {1, 2},
    617733151106007039 => {2, 3},
    617684909104562175 => {2, 3},
    617700174986739711 => {1, 2},
    617700552987901951 => {2, 4},
    617700552891170815 => {2, 4},
    617733123580887039 => {1, 2},
    617733151091589119 => {1, 3},
    617743888390553599 => {1, 3},
    617733269885550591 => {1, 2}
}).

%% Value taken from hip17
-define(KNOWN_RES_FOR_SCALING, "8828361563fffff").

all() ->
    [
        known_values_test,
        known_differences_test,
        scale_test,
        h3dex_test
        %% export_scale_test
    ].

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    LedgerURL = "https://blockchain-core.s3-us-west-1.amazonaws.com/ledger-586724.tar.gz",
    Ledger = blockchain_ct_utils:ledger(hip17_vars(), LedgerURL),

    ok = application:set_env(blockchain, hip17_test_mode, true),

    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    blockchain:bootstrap_h3dex(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    blockchain_ledger_v1:compact(Ledger),

    %% Check that the pinned ledger is at the height we expect it to be
    {ok, 586724} = blockchain_ledger_v1:current_height(Ledger),

    %% Check that the vars are correct, one is enough...
    {ok, VarMap} = blockchain_hex:var_map(Ledger),
    ct:pal("var_map: ~p", [VarMap]),
    #{n := 1, tgt := 250, max := 800} = maps:get(4, VarMap),

    [
        {ledger, Ledger},
        {var_map, VarMap}
        | Config
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST SUITE TEARDOWN
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    %% destroy the downloaded ledger
    blockchain_ct_utils:destroy_ledger(),
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%% XXX: Remove this test when done cross-checking with python
full_known_values_test(Config) ->
    Ledger = ?config(ledger, Config),

    {ok, [List]} = file:consult("/tmp/tracker.erl"),

    %% assert some known values calculated from the python model (thanks @para1!)
    true = lists:all(
        fun({Hex, Res, UnclippedValue, _Limit, ClippedValue}) ->
            case h3:get_resolution(Hex) of
                0 ->
                    true;
                _ ->
                    {ok, VarMap} = blockchain_hex:var_map(Ledger),

                    {UnclippedDensities, ClippedDensities} = blockchain_hex:densities(
                        Hex,
                        VarMap,
                        Ledger
                    ),

                    Dex = blockchain_ledger_v1:lookup_gateways_from_hex(Hex, Ledger),
                    Spots = [blockchain_utils:addr2name(I) || I <- lists:flatten(maps:values(Dex))],
                    NumSpots = length(Spots),

                    ct:pal(
                        "Hex: ~p, Res: ~p, PythonUnclippedDensity: ~p, CalculatedUnclippedDensity: ~p, PythonClippedDensity: ~p, CalculatedClippedDensity: ~p, NumSpots: ~p, Spots: ~p",
                        [
                            Hex,
                            Res,
                            UnclippedValue,
                            maps:get(Hex, UnclippedDensities),
                            ClippedValue,
                            maps:get(Hex, ClippedDensities),
                            NumSpots,
                            Spots
                        ]
                    ),

                    UnclippedValue == maps:get(Hex, UnclippedDensities) andalso
                        ClippedValue == maps:get(Hex, ClippedDensities)
            end
        end,
        List
    ),

    ok.

known_values_test(Config) ->
    Ledger = ?config(ledger, Config),

    %% assert some known values calculated from the python model (thanks @para1!)
    true = lists:all(
        fun({Hex, Density}) ->
            case h3:get_resolution(Hex) of
                0 ->
                    true;
                _ ->
                    {ok, VarMap} = blockchain_hex:var_map(Ledger),
                    {_, ClippedDensities} = blockchain_hex:densities(Hex, VarMap, Ledger),

                    ct:pal("~p ~p", [
                        Density,
                        maps:get(Hex, ClippedDensities)
                    ]),

                    Density == maps:get(Hex, ClippedDensities)
            end
        end,
        ?KNOWN
    ),

    ok.

known_differences_test(Config) ->
    Ledger = ?config(ledger, Config),

    true = lists:all(
        fun({Hex, {Clipped, Unclipped}}) ->
            {ok, VarMap} = blockchain_hex:var_map(Ledger),
            {UnclippedDensities, ClippedDensities} = blockchain_hex:densities(Hex, VarMap, Ledger),
            GotUnclipped = maps:get(Hex, UnclippedDensities),
            GotClipped = maps:get(Hex, ClippedDensities),
            ct:pal(
                "Hex: ~p, Clipped: ~p, GotClipped: ~p, Unclipped: ~p, GotUnclipped: ~p",
                [Hex, Clipped, GotClipped, Unclipped, GotUnclipped]
            ),
            Unclipped == GotUnclipped andalso Clipped == GotClipped
        end,
        maps:to_list(?KNOWN_DIFFERENCES)
    ),

    ok.

scale_test(Config) ->
    Ledger = ?config(ledger, Config),
    VarMap = ?config(var_map, Config),
    TargetResolutions = lists:seq(3, 10),
    KnownHex = h3:from_string("8c2836152804dff"),

    Result = lists:foldl(
               fun(TargetRes, Acc) ->
                       Scale = blockchain_hex:scale(KnownHex, VarMap, TargetRes, Ledger),
                       ct:pal("TargetRes: ~p, Scale: ~p", [TargetRes, Scale]),
                       blockchain_hex:destroy_memoization(),
                       maps:put(TargetRes, Scale, Acc)
               end,
               #{},
               TargetResolutions
              ),

    ct:pal("Result: ~p", [Result]),

    "0.18" = io_lib:format("~.2f", [maps:get(8, Result)]),

    ok.

h3dex_test(Config) ->
    Ledger = ?config(ledger, Config),

    %% A known hotspot hex at res=12, there's only one here
    Hex = 631236347406370303,
    HexPubkeyBin =
        <<0, 161, 86, 254, 148, 82, 27, 153, 2, 52, 158, 118, 1, 178, 133, 150, 238, 135, 228, 40,
            114, 253, 149, 194, 89, 170, 68, 170, 122, 230, 130, 196, 139>>,

    Gateways = blockchain_ledger_v1:lookup_gateways_from_hex(Hex, Ledger),

    GotPubkeyBin = hd(maps:get(Hex, Gateways)),

    ?assertEqual(1, map_size(Gateways)),
    ?assertEqual(GotPubkeyBin, HexPubkeyBin),

    ct:pal("Hex: ~p", [Hex]),
    ct:pal("Gateways: ~p", [Gateways]),

    ok.

export_scale_test(Config) ->
    Ledger = ?config(ledger, Config),
    VarMap = ?config(var_map, Config),

    %% A list of possible density target resolution we'd output the scales at
    DensityTargetResolutions = lists:seq(3, 10),

    %% Only do this for gateways with known locations
    GatewaysWithLocs = gateways_with_locs(Ledger),

    ok = export_scale_data(
        Ledger,
        VarMap,
        DensityTargetResolutions,
        GatewaysWithLocs
    ),

    ok.

%%--------------------------------------------------------------------
%% CHAIN VARIABLES
%%--------------------------------------------------------------------

hip17_vars() ->
    #{
        hip17_res_0 => <<"2,100000,100000">>,
        hip17_res_1 => <<"2,100000,100000">>,
        hip17_res_2 => <<"2,100000,100000">>,
        hip17_res_3 => <<"2,100000,100000">>,
        hip17_res_4 => <<"1,250,800">>,
        hip17_res_5 => <<"1,100,400">>,
        hip17_res_6 => <<"1,25,100">>,
        hip17_res_7 => <<"2,5,20">>,
        hip17_res_8 => <<"2,1,4">>,
        hip17_res_9 => <<"2,1,2">>,
        hip17_res_10 => <<"2,1,1">>,
        hip17_res_11 => <<"2,100000,100000">>,
        hip17_res_12 => <<"2,100000,100000">>,
        density_tgt_res => 8,
        hip17_interactivity_blocks => 1200 * 3
    }.

%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------

gateways_with_locs(Ledger) ->
    AG = blockchain_ledger_v1:active_gateways(Ledger),

    maps:fold(
        fun(Addr, GW, Acc) ->
            case blockchain_ledger_gateway_v2:location(GW) of
                undefined -> Acc;
                Loc -> [{blockchain_utils:addr2name(Addr), Loc} | Acc]
            end
        end,
        [],
        AG
    ).

export_scale_data(Ledger, VarMap, DensityTargetResolutions, GatewaysWithLocs) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    %% Calculate scale at each density target res for eventual comparison
    lists:foreach(
        fun(TargetRes) ->
            %% Export scale data for every single gateway to a gps file
            Scales = lists:foldl(
                fun({GwName, Loc}, Acc) ->
                    Scale = blockchain_hex:scale(
                        Loc,
                        VarMap#{density_tgt_res => TargetRes},
                        TargetRes,
                        Ledger
                    ),
                    [{GwName, Loc, Scale} | Acc]
                end,
                [],
                GatewaysWithLocs
            ),

            Fname = "/tmp/scale_" ++ integer_to_list(TargetRes),
            ok = export_gps_file(Fname, Scales, Height),
            blockchain_hex:destroy_memoization()
        end,
        DensityTargetResolutions
    ).

export_gps_file(Fname, Scales, Height) ->
    Header = ["name,latitude,longitude,h3,color,height,desc"],

    Data = lists:foldl(
        fun({Name, H3, ScaleVal}, Acc) ->
            {Lat, Long} = h3:to_geo(H3),
            ToAppend =
                Name ++
                    "," ++
                    io_lib:format("~.20f", [Lat]) ++
                    "," ++
                    io_lib:format("~.20f", [Long]) ++
                    "," ++
                    integer_to_list(H3) ++
                    "," ++
                    color(ScaleVal) ++
                    "," ++
                    integer_to_list(Height) ++
                    "," ++
                    io_lib:format("scale: ~p", [ScaleVal]),
            [ToAppend | Acc]
        end,
        [],
        Scales
    ),

    TotalData = Header ++ Data,

    LineSep = io_lib:nl(),
    Print = [string:join(TotalData, LineSep), LineSep],
    file:write_file(Fname, Print),
    ok.

color(1.0) -> "green";
color(V) when V =< 0.1 -> "red";
color(V) when V =< 0.3 -> "orange";
color(V) when V =< 0.5 -> "yellow";
color(V) when V =< 0.8 -> "cyan";
color(_) -> "blue".
