-module(blockchain_state_channel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1
]).

-include("blockchain.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%%   Running tests for this suite
%% @end
%%--------------------------------------------------------------------
all() ->
    [
        basic_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config0) ->
    BaseDir = "data/blockchain_state_channel_SUITE/" ++ erlang:atom_to_list(TestCase),
    [{base_dir, BaseDir} |Config0].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic_test(Config) ->
    BaseDir = proplists:get_value(base_dir, Config),
    {ok, Sup} = blockchain_state_channel_sup:start_link([BaseDir]),

    ?assert(erlang:is_process_alive(Sup)),
    ?assertEqual({ok, 0}, blockchain_state_channel_server:credits()),
    ?assertEqual({ok, 0}, blockchain_state_channel_server:nonce()),


    true = erlang:exit(Sup, normal),
    ok.
