%%
%% Steps:
%% - Download a ledger from S3
%% - Change assert res to 9 (This gets applied as a chain var hook) on the aux ledger
%% - Check that all gws switched to res9 h3
%%

-module(blockchain_change_assert_res_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1
]).

all() ->
    [
        basic_test
    ].

init_per_suite(Config) ->
    S3URL = "https://blockchain-core.s3-us-west-1.amazonaws.com/ledger-865940.tar.gz",
    Ledger = blockchain_ct_utils:ledger(#{}, S3URL),
    [{ledger, Ledger} | Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

basic_test(Config) ->
    %% Check we have the correct ledger from S3
    Ledger = ?config(ledger, Config),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    ct:pal("Height: ~p", [Height]),
    ?assertEqual(865940, Height),

    %% Check we can construct an aux ledger and it matches the height
    AuxLedger = blockchain_ledger_v1:new_aux("aux.db", Ledger),
    {ok, AuxHeight} = blockchain_ledger_v1:current_height(AuxLedger),
    ct:pal("AuxHeight: ~p", [AuxHeight]),
    ?assertEqual(865940, Height),

    %% Check that every single hotspot on the ledger either has loc at res=12
    true = lists:all(fun(Loc) -> h3:get_resolution(Loc) == 12 end, get_locations(Ledger)),

    %% Get the aux ledger
    AL = blockchain_ledger_v1:mode(aux, AuxLedger),

    %% Set aux vars on the aux ledger
    ok = blockchain_ledger_v1:set_aux_vars(aux_vars(), AL),

    %% Check that every single hotspot on the aux-ledger either has loc at res=9
    true = lists:all(fun(Loc) -> h3:get_resolution(Loc) == 9 end, get_locations(AL)),

    ok.

get_locations(Ledger) ->
    AG = blockchain_ledger_v1:active_gateways(Ledger),
    maps:fold(
        fun(_GwAddr, Gw, Acc) ->
            case blockchain_ledger_gateway_v2:location(Gw) of
                undefined -> Acc;
                Loc -> [Loc | Acc]
            end
        end,
        [],
        AG
    ).

aux_vars() ->
    #{
        ?switch_assert_res_9 => true,
        ?min_assert_h3_res => 9,
        ?poc_v4_parent_res => 8
    }.
