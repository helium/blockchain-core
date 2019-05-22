-module(blockchain_data_credits_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1,
    retry_test/1,
    restart_channel_test/1,
    restart_monitor_test/1
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
        basic_test,
        retry_test,
        restart_channel_test,
        restart_monitor_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(_TestCase, Config0) ->
    blockchain_ct_utils:init_per_testcase(_TestCase, [{"T", 3}, {"N", 1}|Config0]).

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
    [RouterNode, GatewayNode1, GatewayNode2] = proplists:get_value(nodes, Config, []),

    ct:pal("RouterNode: ~p GatewayNode1: ~p GatewayNode2: ~p", [RouterNode, GatewayNode1, GatewayNode2]),

    % Simulate Atom burn to Data Credits
    Keys = libp2p_crypto:generate_keys(ecc_compact),
    ok = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [Keys, 100]),

    % Check that 100 credits was added
    #{public := PubKey} = Keys,
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, ChannelServer} = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [PubKeyBin]),
    ?assertEqual({ok, 100}, ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])),

    % Make a payment request from GatewayNode1 of 10 credits
    RouterPubKeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    % Checking that we have 90 credits now
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])
    end, 10, 500),

    % Make a payment request from GatewayNode1 of 10 credits
    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    % Checking that we have 80 credits now
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 80} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])
    end, 10, 500),

    % Make a payment request from GatewayNode2 of 10 credits (to make sure it got the history)
    ok = ct_rpc:call(GatewayNode2, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    % Checking that we have 70 credits now
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 70} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])
    end, 10, 500),

    % Checking clients states (height and credits)
    {ok, GatewayNode1Client} = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, channel_client, [RouterPubKeyBin]),
    {ok, GatewayNode2Client} = ct_rpc:call(GatewayNode2, blockchain_data_credits_clients_monitor, channel_client, [RouterPubKeyBin]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 3} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, height, [GatewayNode1Client])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 3} == ct_rpc:call(GatewayNode2, blockchain_data_credits_channel_client, height, [GatewayNode2Client])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 70} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, credits, [GatewayNode1Client])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 70} == ct_rpc:call(GatewayNode2, blockchain_data_credits_channel_client, credits, [GatewayNode2Client])
    end, 10, 500),

    ok.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
retry_test(Config) ->
    [RouterNode, GatewayNode1, GatewayNode2] = proplists:get_value(nodes, Config, []),

    ct:pal("RouterNode: ~p GatewayNode1: ~p GatewayNode2: ~p", [RouterNode, GatewayNode1, GatewayNode2]),

    % Simulate Atom burn to Data Credits
    Keys = libp2p_crypto:generate_keys(ecc_compact),
    ok = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [Keys, 100]),

    % Check that 100 credits was added
    #{public := PubKey} = Keys,
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, ChannelServer} = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [PubKeyBin]),
    ?assertEqual({ok, 100}, ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])),

    % Make a payment request from GatewayNode1 of 10 credits
    RouterPubKeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    % Checking that we have 90 credits now
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])
    end, 10, 500),

    ok = ct_rpc:call(RouterNode, meck, new, [blockchain_data_credits_channel_server, [passthrough]]),
    % Needed for meck, don't ask me why...
    timer:sleep(1),
    ok = ct_rpc:call(RouterNode, meck, expect, [blockchain_data_credits_channel_server, payment_req, fun(_A, _B) ->
        ok
    end]),

    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])
    end, 10, 500),

    ?assert(ct_rpc:call(RouterNode, meck, validate, [blockchain_data_credits_channel_server])),
    ct_rpc:call(RouterNode, meck, unload, [blockchain_data_credits_channel_server]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 80} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer])
    end, 100, 600),

    ok.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
restart_channel_test(Config) ->
    [RouterNode, GatewayNode1, GatewayNode2] = proplists:get_value(nodes, Config, []),

    ct:pal("RouterNode: ~p GatewayNode1: ~p GatewayNode2: ~p", [RouterNode, GatewayNode1, GatewayNode2]),

    % Simulate Atom burn to Data Credits
    Keys = libp2p_crypto:generate_keys(ecc_compact),
    ok = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [Keys, 100]),

    % Check that 100 credits was added
    #{public := PubKey} = Keys,
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, ChannelServer0} = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [PubKeyBin]),
    ?assertEqual({ok, 100}, ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer0])),

    % Make a payment request from GatewayNode1 of 10 credits
    RouterPubKeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    % Checking that we have 90 credits now
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer0])
    end, 10, 500),

    % Kill channel server and let it restart
    ok = ct_rpc:call(RouterNode, gen_server, stop, [ChannelServer0, crash, 5000]),
    timer:sleep(5000),
    {ok, ChannelServer1} = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [PubKeyBin]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer1])
    end, 10, 500),

   % Kill channel client and let it restart
    {ok, GatewayNode1Client0} = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, channel_client, [RouterPubKeyBin]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 1} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, height, [GatewayNode1Client0])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, credits, [GatewayNode1Client0])
    end, 10, 500),

    ok = ct_rpc:call(GatewayNode1, gen_server, stop, [GatewayNode1Client0, crash, 5000]),
    timer:sleep(5000),

    {ok, GatewayNode1Client1} = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, channel_client, [RouterPubKeyBin]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 1} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, height, [GatewayNode1Client1])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, credits, [GatewayNode1Client1])
    end, 10, 500),
    ok.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
restart_monitor_test(Config) ->
    [RouterNode, GatewayNode1, GatewayNode2] = proplists:get_value(nodes, Config, []),

    ct:pal("RouterNode: ~p GatewayNode1: ~p GatewayNode2: ~p", [RouterNode, GatewayNode1, GatewayNode2]),

    % Simulate Atom burn to Data Credits
    Keys = libp2p_crypto:generate_keys(ecc_compact),
    ok = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [Keys, 100]),

    % Check that 100 credits was added
    #{public := PubKey} = Keys,
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, ChannelServer0} = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [PubKeyBin]),
    ?assertEqual({ok, 100}, ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer0])),

    % Make a payment request from GatewayNode1 of 10 credits
    RouterPubKeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    % Checking that we have 90 credits now
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer0])
    end, 10, 500),

    {ok, GatewayNode1Client0} = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, channel_client, [RouterPubKeyBin]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 1} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, height, [GatewayNode1Client0])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, credits, [GatewayNode1Client0])
    end, 10, 500),

    % Kill monitor client and let it restart
    ok = ct_rpc:call(GatewayNode1, gen_server, stop, [blockchain_data_credits_clients_monitor, crash, 5000]),
    timer:sleep(5000),
    
    {ok, GatewayNode1Client1} = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, channel_client, [RouterPubKeyBin]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 1} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, height, [GatewayNode1Client1])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 90} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, credits, [GatewayNode1Client1])
    end, 10, 500),

    % Make another payment to see if client is still working
    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 2} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, height, [GatewayNode1Client1])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 80} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, credits, [GatewayNode1Client1])
    end, 10, 500),

    % Kill monitor server and let it restart
    ok = ct_rpc:call(RouterNode, gen_server, stop, [blockchain_data_credits_servers_monitor, crash, 5000]),
    timer:sleep(5000),

    {ok, ChannelServer1} = ct_rpc:call(RouterNode, blockchain_data_credits_servers_monitor, channel_server, [PubKeyBin]),

    % Checking that we still have 80 credits
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 80} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer1])
    end, 10, 500),

    % Make another payment to see if client is still working
    ok = ct_rpc:call(GatewayNode1, blockchain_data_credits_clients_monitor, payment_req, [RouterPubKeyBin, 10]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 3} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, height, [GatewayNode1Client1])
    end, 10, 500),
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 70} == ct_rpc:call(GatewayNode1, blockchain_data_credits_channel_client, credits, [GatewayNode1Client1])
    end, 10, 500),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 70} == ct_rpc:call(RouterNode, blockchain_data_credits_channel_server, credits, [ChannelServer1])
    end, 10, 500),

    ok.