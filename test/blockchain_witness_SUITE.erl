-module(blockchain_witness_SUITE).

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
    with_vars_max_witnessing_test/1,
    without_vars_max_witnessing_test/1
]).

-define(POC_WITNESSING_MAX, 5).
%% WITNESS has seen BEACONER more than 5 times in the pinned ledger
-define(BEACONER, "11UAmz1HPHKbNXE5Wd5un5Koe6TLS4Qdn13ChXFhw4NBgWF2ehX").
-define(WITNESS, "112A3u9QpjQmzfk5WPLCCDUoME3hsPRWTGUuWooxXbw8KsxAHJDL").

all() ->
    [
        with_vars_max_witnessing_test,
        without_vars_max_witnessing_test
    ].

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    LedgerURL = "https://blockchain-core.s3-us-west-1.amazonaws.com/ledger-687973.tar.gz",
    Ledger = blockchain_ct_utils:download_ledger(LedgerURL),
    [{ledger, Ledger} | Config].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    Ledger0 = ?config(ledger, Config),

    ExtraVars =
        case TestCase of
            without_vars_max_witnessing_test -> #{};
            with_vars_max_witnessing_test -> poc_max_witnessing_var()
        end,

    Ledger = blockchain_ct_utils:apply_extra_vars(ExtraVars, Ledger0),

    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    blockchain:bootstrap_h3dex(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    blockchain_ledger_v1:compact(Ledger),

    %% Check that the pinned ledger is at the height we expect it to be
    {ok, 687973} = blockchain_ledger_v1:current_height(Ledger),

    [
        {ledger, Ledger},
        {beaconer_pubkey_bin, libp2p_crypto:b58_to_bin(?BEACONER)},
        {witness_pubkey_bin, libp2p_crypto:b58_to_bin(?WITNESS)}
        | Config
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, Config) ->
    Ledger0 = ?config(ledger, Config),
    Ledger = blockchain_ct_utils:remove_var(poc_witnessing_max, Ledger0),
    [{ledger, Ledger} | Config].

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

with_vars_max_witnessing_test(Config) ->
    Ledger = ?config(ledger, Config),
    WitnessPubkeyBin = ?config(witness_pubkey_bin, Config),
    BeaconerPubkeyBin = ?config(beaconer_pubkey_bin, Config),
    {ok, ?POC_WITNESSING_MAX} = blockchain:config(?poc_witnessing_max, Ledger),
    {ok, BeaconGW} = blockchain_gateway_cache:get(BeaconerPubkeyBin, Ledger),

    %% As the vars are set, it's expected that this check returns false
    false = blockchain_txn_poc_receipts_v1:check_witness_allowed_count(
        BeaconGW,
        WitnessPubkeyBin,
        Ledger
    ),

    ok.

without_vars_max_witnessing_test(Config) ->
    Ledger = ?config(ledger, Config),
    WitnessPubkeyBin = ?config(witness_pubkey_bin, Config),
    BeaconerPubkeyBin = ?config(beaconer_pubkey_bin, Config),
    {error, not_found} = blockchain:config(?poc_witnessing_max, Ledger),
    {ok, BeaconGW} = blockchain_gateway_cache:get(BeaconerPubkeyBin, Ledger),

    %% The var is not set, this should return true
    true = blockchain_txn_poc_receipts_v1:check_witness_allowed_count(
        BeaconGW,
        WitnessPubkeyBin,
        Ledger
    ),

    ok.

%%--------------------------------------------------------------------
%% CHAIN VARIABLES
%%--------------------------------------------------------------------

poc_max_witnessing_var() ->
    #{poc_witnessing_max => ?POC_WITNESSING_MAX}.
