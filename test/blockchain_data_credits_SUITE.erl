-module(blockchain_data_credits_SUITE).

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

init_per_testcase(_TestCase, Config0) ->
    blockchain_ct_utils:init_per_testcase(_TestCase, [{"T", 2}, {"N", 1}|Config0]).

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
    blockchain_ct_utils:end_per_testcase(_TestCase, Config).

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic_test(Config) ->
    [RouterNode, GatewayNode] = proplists:get_value(nodes, Config, []),

    ct:pal("RouterNode: ~p GatewayNode: ~p", [RouterNode, GatewayNode]),

    Keys = libp2p_crypto:generate_keys(ecc_compact),
    ok = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [Keys, 100]),

    #{public := PubKey} = Keys,
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, ChannelServer} = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [PubKeyBin]),
    ?assertEqual({ok, 100}, ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])),

    RouterPubKeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ok = ct_rpc:call(GatewayNode, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])
    end, 10, 500),
    ok.
