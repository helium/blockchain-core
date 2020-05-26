-module(blockchain_price_oracle_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

all() -> [
    validate_initial_state,
    submit_prices,
    calculate_price_even,
    calculate_price_odd,
    submit_bad_public_key
].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config).

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_, _Config) ->
    catch gen_server:stop(blockchain_sup),
    application:unset_env(blockchain, assumed_valid_block_hash),
    application:unset_env(blockchain, assumed_valid_block_height),
    application:unset_env(blockchain, honor_quick_sync),
    persistent_term:erase(blockchain_core_assumed_valid_block_hash_and_height),
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

validate_initial_state(_Config) ->
    %% the initial state should be a price of 0
    %% should have 3 oracle keys
    ok.

submit_prices(_Config) ->
    %%  
    %%
    ok.

%%--------------------------------------------------------------------
%% TEST HELPERS
%%--------------------------------------------------------------------
prices() -> [ 10, 20, 30 ].

random_price(Prices) ->
    Pos = rand:uniform(length(Prices)),
    lists:nth(Pos, Prices).
