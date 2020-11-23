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
    non_zero_test/1,
    known_values_test/1,
    known_differences_test/1,
    scale_test/1
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
        non_zero_test,
        known_values_test,
        known_differences_test,
        scale_test
    ].

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    LedgerURL = "https://blockchain-core.s3-us-west-1.amazonaws.com/ledger-586724.tar.gz",
    Ledger = blockchain_ct_utils:ledger(hip17_vars(), LedgerURL),

    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    blockchain:bootstrap_h3dex(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),

    %% Check that the pinned ledger is at the height we expect it to be
    {ok, 586724} = blockchain_ledger_v1:current_height(Ledger),

    %% Check that the vars are correct, one is enough...
    VarMap = blockchain_hex:var_map(Ledger),
    Res4 = maps:get(4, VarMap),
    1 = maps:get(n, Res4),
    250 = maps:get(density_tgt, Res4),
    800 = maps:get(density_max, Res4),

    {Time, {UnclippedDensities, ClippedDensities}} = timer:tc(
        fun() ->
            blockchain_hex:densities(Ledger)
        end
    ),
    ct:pal("density calculation time: ~pms", [Time / 1000]),

    ct:pal("density took ~p", [Time]),
    %% Check that the time is less than 1000ms
    %?assert(1000 =< Time),

    [
        {ledger, Ledger},
        {clipped, ClippedDensities},
        {unclipped, UnclippedDensities},
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

non_zero_test(Config) ->
    ClippedDensities = ?config(clipped, Config),

    %% assert that there are no 0 values for density
    %% we count a hotspot at res 12 (max resolution) as 1
    true = lists:all(fun(V) -> V /= 0 end, maps:values(ClippedDensities)),
    ok.

known_values_test(Config) ->
    ClippedDensities = ?config(clipped, Config),
    Ledger = ?config(ledger, Config),

    %% assert some known values calculated from the python model (thanks @para1!)
    true = lists:all(
        fun({Hex, Density}) ->
            ct:pal("~p ~p", [Density, maps:size(blockchain_ledger_v1:lookup_gateways_from_hex(Hex, Ledger))]),
            Density == maps:get(Hex, ClippedDensities)
        end,
        ?KNOWN
    ),

    ok.

known_differences_test(Config) ->
    UnclippedDensities = ?config(unclipped, Config),
    ClippedDensities = ?config(clipped, Config),

    Differences = lists:foldl(
        fun({Hex, ClippedDensity}, Acc) ->
            UnclippedDensity = maps:get(Hex, UnclippedDensities),
            case ClippedDensity /= UnclippedDensity of
                false ->
                    Acc;
                true ->
                    maps:put(Hex, {ClippedDensity, UnclippedDensity}, Acc)
            end
        end,
        #{},
        maps:to_list(ClippedDensities)
    ),

    ?assertEqual(?KNOWN_CLIP_VS_UNCLIPPED, map_size(Differences)),

    true = lists:all(
        fun({Hex, {Clipped, Unclipped}}) ->
            Unclipped == maps:get(Hex, UnclippedDensities) andalso
                Clipped == maps:get(Hex, ClippedDensities)
        end,
        maps:to_list(?KNOWN_DIFFERENCES)
    ),

    ok.

scale_test(Config) ->
    UnclippedDensities = ?config(unclipped, Config),
    ClippedDensities = ?config(clipped, Config),

    Another = h3:from_string("8c2836152804dff"),

    ok = lists:foreach(fun(I) ->
                               Scale = blockchain_hex:scale(Another, I, UnclippedDensities, ClippedDensities),
                               ct:pal("Res: ~p, Scale: ~p", [I, Scale])
                       end, lists:seq(12, 0, -1)),

    %% TODO: Assert checks from the python model

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
        density_tgt_res => 8
    }.
