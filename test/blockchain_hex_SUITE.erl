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
    known_scale_test/1,
    known_values_test/1,
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

%% Value taken from hip17
-define(KNOWN_RES_FOR_SCALING, "8828361563fffff").

all() ->
    [
        known_scale_test,
        known_values_test,
        h3dex_test
        %% export_scale_test
    ].

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    LedgerURL = "https://blockchain-core.s3-us-west-1.amazonaws.com/ledger-632627.tar.gz",
    Ledger = blockchain_ct_utils:ledger(hip17_vars(), LedgerURL),

    ok = application:set_env(blockchain, hip17_test_mode, true),

    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    blockchain:bootstrap_h3dex(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    blockchain_ledger_v1:compact(Ledger),

    application:ensure_all_started(lager),

    %% Check that the pinned ledger is at the height we expect it to be
    {ok, 632627} = blockchain_ledger_v1:current_height(Ledger),

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
    blockchain_hex:destroy_memoization(),
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
known_scale_test(Config) ->
    Ledger = ?config(ledger, Config),
    blockchain_hex:precalc(true, ?config(ledger, Config)),
    PythonScaledURL = "https://blockchain-core.s3-us-west-1.amazonaws.com/hip17-python-scaled-632627.erl",
    Fname = "hip17-python-scaled-632627.erl",
    FPath = filename:join(["/tmp/", Fname]),

    ok = ssl:start(),
    {ok, {{_, 200, "OK"}, _, Body}} = httpc:request(PythonScaledURL),
    ok = file:write_file(FPath, Body),

    {ok, [List]} = file:consult(FPath),

    %% assert some known values calculated from the python model (thanks @para1!)
    true = lists:all(
        fun({Hex, Scale}) ->
            case h3:get_resolution(Hex) of
                0 ->
                    true;
                _ ->
                    {ok, GotScale} = blockchain_hex:scale(Hex, Ledger),
                    ct:pal("GotScale: ~p, Scale: ~p", [GotScale, Scale]),
                    (GotScale >= Scale - 0.001) andalso (GotScale =< Scale + 0.001)
            end
        end,
        List
    ),

    ok.

known_values_test(Config) ->

    %% assert some known values calculated from the python model (thanks @para1!)
    blockchain_hex:precalc(true, ?config(ledger, Config)),
    true = lists:all(
        fun({Hex, Density}) ->
            case h3:get_resolution(Hex) of
                0 ->
                    true;
                Res ->
                    GotDensity = blockchain_hex:clookup(h3:parent(Hex, Res)),

                    ct:pal("hex ~p ~p ~p ~p",
                           [
                            Hex,
                            Res,
                            Density,
                            GotDensity
                           ]),

                    Density == GotDensity
            end
        end,
        ?KNOWN
    ),

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
    GatewaysWithLocs = active_gateways_with_locs(Ledger),

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
        density_tgt_res => 4,
        hip17_interactivity_blocks => 1200 * 3
    }.

%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------

active_gateways_with_locs(Ledger) ->
    AG = blockchain_ledger_v1:active_gateways(Ledger),
    {ok, H} = blockchain_ledger_v1:current_height(Ledger),
    {ok, InteractiveBlocks} = blockchain:config(?hip17_interactivity_blocks, Ledger),

    maps:fold(
        fun(Addr, GW, Acc) ->
            case blockchain_ledger_gateway_v2:location(GW) of
                undefined -> Acc;
                Loc ->
                    case blockchain_ledger_gateway_v2:last_poc_challenge(GW) of
                        undefined -> Acc;
                        L ->
                            case (H - L) =< InteractiveBlocks of
                                false -> Acc;
                                true -> [{Addr, blockchain_utils:addr2name(Addr), Loc} | Acc]
                            end
                    end
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
                fun({_GwAddr, GwName, Loc}, Acc) ->
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
