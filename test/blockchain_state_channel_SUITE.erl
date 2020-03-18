-module(blockchain_state_channel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1,
    zero_test/1,
    full_test/1,
    expired_test/1,
    replay_test/1,
    multiple_test/1
]).

-include("blockchain.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

all() ->
    [
        basic_test,
        zero_test,
        full_test,
        expired_test,
        replay_test,
        multiple_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    {ok, GenesisMembers, ConsensusMembers, Keys} = test_utils:init_chain(Balance, {PrivKey, PubKey}, true),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    ok = lists:foreach(fun(Entry) ->
                               Balance = blockchain_ledger_entry_v1:balance(Entry),
                               0 = blockchain_ledger_entry_v1:nonce(Entry)
                       end, maps:values(Entries)),

    [
     {balance, Balance},
     {sup, Sup},
     {pubkey, PubKey},
     {privkey, PrivKey},
     {opts, Opts},
     {chain, Chain},
     {swarm, Swarm},
     {n, N},
     {consensus_members, ConsensusMembers},
     {genesis_members, GenesisMembers},
     Keys
     | Config0
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(basic_test, _Config) ->
    ok;
end_per_testcase(Test, Config) ->
    blockchain_ct_utils:end_per_testcase(Test, Config).


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------
basic_test(Config) ->

    [{Addr, {PubKey, PrivKey, SigFun}} | _] = ?config(genesis_members, Config),
    ct:pal("Addr: ~p, Pubkey: ~p, PrivKey: ~p, SigFun: ~p", [Addr, PubKey, PrivKey, SigFun]),

    ID = <<"ID1">>,

    ?assertEqual({error, not_found}, blockchain_state_channels_server:credits(ID)),
    ?assertEqual({error, not_found}, blockchain_state_channels_server:nonce(ID)),

    ok = blockchain_state_channels_server:burn(ID, 10),
    ?assertEqual({ok, 10}, blockchain_state_channels_server:credits(ID)),
    ?assertEqual({ok, 0}, blockchain_state_channels_server:nonce(ID)),

    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Req0 = blockchain_state_channel_request_v1:new(PubKeyBin, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    Req = blockchain_state_channel_request_v1:sign(Req0, SigFun),
    ok = blockchain_state_channels_server:request(Req),

    ?assertEqual({ok, 9}, blockchain_state_channels_server:credits(ID)),
    ?assertEqual({ok, 1}, blockchain_state_channels_server:nonce(ID)),

    ok.

zero_test(Config) ->
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    Swarm = ?config(swarm, Config),

    [{_RouterAddr, {RouterPubkey, _RouterPrivKey, RouterSigFun}},
     {_GatewayAddr, {GatewayPubkey, _GatewayPrivKey, _GatewaySigFun}} | _] = ?config(genesis_members, Config),

    % Step 1: Create OUI txn
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterP2PAddress = libp2p_crypto:pubkey_bin_to_p2p(RouterPubkeyBin),
    OUI = 1,
    OUITxn = blockchain_txn_oui_v1:new(RouterPubkeyBin, [erlang:list_to_binary(RouterP2PAddress)], OUI, 1, 0),
    SignedOUITxn = blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun),

    % Step 2: Create state channel open zero txn
    ID = blockchain_state_channel_v1:zero_id(),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, 0, 100, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),

    % Step 3: Create update oui txn
    GatewayPubkeyBin = libp2p_crypto:pubkey_to_bin(GatewayPubkey),
    UpdateGWOuiTxn = blockchain_txn_update_gateway_oui_v1:new(GatewayPubkeyBin, OUI, 1, 1),
    SignedUpdateGWOuiTxn0 = blockchain_txn_update_gateway_oui_v1:gateway_owner_sign(UpdateGWOuiTxn, RouterSigFun),
    SignedUpdateGWOuiTxn1 = blockchain_txn_update_gateway_oui_v1:oui_owner_sign(SignedUpdateGWOuiTxn0, RouterSigFun),

    % Step 4: Adding block
    Txns = [SignedOUITxn, SignedSCOpenTxn, SignedUpdateGWOuiTxn1],
    Block1 = test_utils:create_block(ConsensusMembers, Txns),
    ct:pal("Block1: ~p", [Block1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, self()),

    % Step 5: Checking that state channel got created properly
    Ledger = blockchain:ledger(Chain),
    {ok, SC} = blockchain_ledger_v1:find_state_channel(ID, RouterPubkeyBin, Ledger),
    ?assertEqual(ID, blockchain_ledger_state_channel_v1:id(SC)),
    ?assertEqual(RouterPubkeyBin, blockchain_ledger_state_channel_v1:owner(SC)),
    ?assertEqual(0, blockchain_ledger_state_channel_v1:amount(SC)),

    {ok, 0} = blockchain_state_channels_server:credits(ID),
    {ok, 0} = blockchain_state_channels_server:nonce(ID),

    % Step 6: Sending packet with same OUI
    Packet0 = blockchain_helium_packet_v1:new(1, <<"sup">>),
    PacketInfo0 = {Packet0, <<"devaddr">>, 1, <<"mic1">>},
    ok = blockchain_state_channels_client:packet(PacketInfo0),

    % Step 7: Checking state channel on server/client (balance did not update but nonce did)
    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("server state: ~p", [blockchain_state_channels_server:state()]),
        {ok, 0} == blockchain_state_channels_server:credits(ID) andalso
        {ok, 1} == blockchain_state_channels_server:nonce(ID)
    end, 30, timer:seconds(1)),

    ct:pal("new credits: ~p", [blockchain_state_channels_server:credits(ID)]),
    ct:pal("new nonce: ~p", [blockchain_state_channels_server:nonce(ID)]),

     % Step 8: Sending packet with same OUI and a payload
    Payload1 = crypto:strong_rand_bytes(120),
    Packet1 = blockchain_helium_packet_v1:new(1, Payload1),
    PacketInfo1 = {Packet1, <<"devaddr">>, 1, <<"mic1">>},
    ok = blockchain_state_channels_client:packet(PacketInfo1),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("server state: ~p", [blockchain_state_channels_server:state()]),
        {ok, 0} == blockchain_state_channels_server:credits(ID) andalso
        {ok, 2} == blockchain_state_channels_server:nonce(ID)
    end, 30, timer:seconds(1)),

    ok.

full_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    % Some madness to make submit txn work and "create a block"
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck, new, [blockchain_worker, [passthrough]]),
    timer:sleep(10),
    ok = ct_rpc:call(RouterNode, meck, expect, [blockchain_worker, submit_txn, fun(T) ->
        Self ! {txn, T},
        ok
    end]),
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} -> ok
    after 1000 ->
        ct:fail("txn test timeout")
    end,

    % Step 1: Create OUI txn
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterP2PAddress = ct_rpc:call(RouterNode, libp2p_swarm, p2p_address, [RouterSwarm]),
    OUITxn = blockchain_txn_oui_v1:new(RouterPubkeyBin, [erlang:list_to_binary(RouterP2PAddress)], 1, 1, 0),
    SignedOUITxn = blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun),

    % Step 2: Create state channel open txn
    TotalDC = 10,
    ID = crypto:strong_rand_bytes(32),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, TotalDC, 100, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    ct:pal("RouterChain: ~p", [RouterChain]),
    Block0 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]]),
    ct:pal("Block0: ~p", [Block0]),
    _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block0, RouterChain, Self]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        {ok, 2} == ct_rpc:call(GatewayNode1, blockchain, height, [C])
    end, 30, timer:seconds(1)),

    % Step 4: Checking that state channel got created properly
    RouterLedger = blockchain:ledger(RouterChain),
    {ok, SC} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID, RouterPubkeyBin, RouterLedger]),
    ?assertEqual(ID, blockchain_ledger_state_channel_v1:id(SC)),
    ?assertEqual(RouterPubkeyBin, blockchain_ledger_state_channel_v1:owner(SC)),
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [Self]),
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("MARKER1 ~p", [ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])]),
        {ok, 5} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID]) andalso
        {ok, 1} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("MARKER2 ~p", [ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])]),
        {ok, 5} == ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])
    end, 30, timer:seconds(1)),

    % Step 7: Making sure packet got transmitted
    receive
        {packet, P0} ->
            ?assertEqual(Packet0, P0)
    after 10000 ->
        ct:fail("packet timeout")
    end,

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [undefined]),
    Payload1 = crypto:strong_rand_bytes(120),
    Packet1 = blockchain_helium_packet_v1:new(1, Payload1),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 0} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID]) andalso
        {ok, 2} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 0} == ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])
    end, 30, timer:seconds(1)),

    % Step 8: Adding close txn to blockchain
    receive
        {txn, Txn} ->
            Block1 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block1, RouterChain, Self])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    % Step 9: Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 3} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])
    end, 30, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
    ok.

expired_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    % Some madness to make submit txn work and "create a block"
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck, new, [blockchain_worker, [passthrough]]),
    timer:sleep(10),
    ok = ct_rpc:call(RouterNode, meck, expect, [blockchain_worker, submit_txn, fun(T) ->
        Self ! {txn, T},
        ok
    end]),
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} -> ok
    after 1000 ->
        ct:fail("txn test timeout")
    end,

    % Step 1: Create OUI txn
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterP2PAddress = ct_rpc:call(RouterNode, libp2p_swarm, p2p_address, [RouterSwarm]),
    OUITxn = blockchain_txn_oui_v1:new(RouterPubkeyBin, [erlang:list_to_binary(RouterP2PAddress)], 1, 1, 0),
    SignedOUITxn = blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun),

    % Step 2: Create state channel open txn
    TotalDC = 10,
    ID = crypto:strong_rand_bytes(32),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, TotalDC, 20, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    Block0 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]]),
    _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block0, RouterChain, Self]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        {ok, 2} == ct_rpc:call(GatewayNode1, blockchain, height, [C])
    end, 30, timer:seconds(1)),

    % Step 4: Checking that state channel got created properly
    RouterLedger = blockchain:ledger(RouterChain),
    {ok, SC} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID, RouterPubkeyBin, RouterLedger]),
    ?assertEqual(ID, blockchain_ledger_state_channel_v1:id(SC)),
    ?assertEqual(RouterPubkeyBin, blockchain_ledger_state_channel_v1:owner(SC)),
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [Self]),
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 5} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID]) andalso
        {ok, 1} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 5} == ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])
    end, 30, timer:seconds(1)),

    % Step 7: Making sure packet got transmitted
    receive
        {packet, P0} ->
            ?assertEqual(Packet0, P0)
    after 10000 ->
        ct:fail("packet timeout")
    end,

    % Step 5: Adding some blocks to get the state channel to expire
    lists:foreach(
        fun(_) ->
            B = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, []]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, B, RouterChain, Self])
        end,
        lists:seq(1, 20)
    ),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 22} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    % Step 8: Adding close txn to blockchain
    receive
        {txn, Txn} ->
            Block1 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block1, RouterChain, Self])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    % Step 9: Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 23} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channels_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
    ok.

replay_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    % Some madness to make submit txn work and "create a block"
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck, new, [blockchain_worker, [passthrough]]),
    timer:sleep(10),
    ok = ct_rpc:call(RouterNode, meck, expect, [blockchain_worker, submit_txn, fun(T) ->
        Self ! {txn, T},
        ok
    end]),
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} -> ok
    after 1000 ->
        ct:fail("txn test timeout")
    end,

    % Step 1: Create OUI txn
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterP2PAddress = ct_rpc:call(RouterNode, libp2p_swarm, p2p_address, [RouterSwarm]),
    OUITxn = blockchain_txn_oui_v1:new(RouterPubkeyBin, [erlang:list_to_binary(RouterP2PAddress)], 1, 1, 0),
    SignedOUITxn = blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun),

    % Step 2: Create state channel open txn
    TotalDC = 10,
    ID = crypto:strong_rand_bytes(32),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, TotalDC, 100, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    ct:pal("RouterChain: ~p", [RouterChain]),
    Block0 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]]),
    ct:pal("Block0: ~p", [Block0]),
    _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block0, RouterChain, Self]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        {ok, 2} == ct_rpc:call(GatewayNode1, blockchain, height, [C])
    end, 30, timer:seconds(1)),

    % Step 4: Checking that state channel got created properly
    RouterLedger = blockchain:ledger(RouterChain),
    {ok, SC} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID, RouterPubkeyBin, RouterLedger]),
    ?assertEqual(ID, blockchain_ledger_state_channel_v1:id(SC)),
    ?assertEqual(RouterPubkeyBin, blockchain_ledger_state_channel_v1:owner(SC)),
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [Self]),
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("MARKER1 ~p", [ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])]),
        {ok, 5} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID]) andalso
        {ok, 1} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("MARKER2 ~p", [ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])]),
        {ok, 5} == ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])
    end, 30, timer:seconds(1)),

    % Step 7: Making sure packet got transmitted
    receive
        {packet, P0} ->
            ?assertEqual(Packet0, P0)
    after 10000 ->
        ct:fail("packet timeout")
    end,

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [undefined]),
    Payload1 = crypto:strong_rand_bytes(120),
    Packet1 = blockchain_helium_packet_v1:new(1, Payload1),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 0} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID]) andalso
        {ok, 2} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 0} == ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])
    end, 30, timer:seconds(1)),

    % Step 8: Adding close txn to blockchain
    receive
        {txn, Txn} ->
            Block1 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block1, RouterChain, Self])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    % Step 9: Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 3} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])
    end, 30, timer:seconds(1)),

    % Step 10: Recreating the state channel open txn with the same nonce
    RouterChain2 = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterLedger2 = blockchain:ledger(RouterChain2),

    ct:pal("DCEntry: ~p", [ct_rpc:call(RouterNode, blockchain_ledger_v1, find_dc_entry, [RouterPubkeyBin, RouterLedger2])]),

    ReplayTotalDC = 20,
    ReplayID = crypto:strong_rand_bytes(32),
    ReplaySCOpenTxn = blockchain_txn_state_channel_open_v1:new(ReplayID, RouterPubkeyBin, ReplayTotalDC, 100, 1),
    ReplaySignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(ReplaySCOpenTxn, RouterSigFun),
    ct:pal("ReplaySignedSCOpenTxn: ~p", [ReplaySignedSCOpenTxn]),
    ReplayIsValid = ct_rpc:call(RouterNode,
                                blockchain_txn_state_channel_open_v1,
                                is_valid,
                                [ReplaySignedSCOpenTxn, RouterChain2]),

    %% Step 11: check whether the replay sc open txn is valid?
    ?assertEqual({error, {bad_nonce, {state_channel_open, 1, 2}}}, ReplayIsValid),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
    ok.

multiple_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    % Some madness to make submit txn work and "create a block"
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck, new, [blockchain_worker, [passthrough]]),
    timer:sleep(10),
    ok = ct_rpc:call(RouterNode, meck, expect, [blockchain_worker, submit_txn, fun(T) ->
        Self ! {txn, T},
        ok
    end]),
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} -> ok
    after 1000 ->
        ct:fail("txn test timeout")
    end,

    % Step 1: Create OUI txn
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterP2PAddress = ct_rpc:call(RouterNode, libp2p_swarm, p2p_address, [RouterSwarm]),
    OUITxn = blockchain_txn_oui_v1:new(RouterPubkeyBin, [erlang:list_to_binary(RouterP2PAddress)], 1, 1, 0),
    SignedOUITxn = blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun),

    % Step 2: Create state channel open txn
    TotalDC = 10,
    ID1 = crypto:strong_rand_bytes(32),
    SCOpenTxn1 = blockchain_txn_state_channel_open_v1:new(ID1, RouterPubkeyBin, TotalDC, 20, 1),
    SignedSCOpenTxn1 = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn1, RouterSigFun),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    Block1 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn1]]),
    _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block1, RouterChain, Self]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        {ok, 2} == ct_rpc:call(GatewayNode1, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    % Step 4: Checking that state channel got created properly
    RouterLedger = blockchain:ledger(RouterChain),
    {ok, SC1} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID1, RouterPubkeyBin, RouterLedger]),
    ?assertEqual(ID1, blockchain_ledger_state_channel_v1:id(SC1)),
    ?assertEqual(RouterPubkeyBin, blockchain_ledger_state_channel_v1:owner(SC1)),
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC1)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID1])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID1])),

    % Step 5: Adding some blocks to get the state channel to expire
    lists:foreach(
        fun(_) ->
            B = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, []]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, B, RouterChain, Self])
        end,
        lists:seq(1, 20)
    ),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 22} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    % Step 8: Adding close txn to blockchain
    receive
        {txn, Txn1} ->
            B1 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn1]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, B1, RouterChain, Self])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    % Step 9: Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 23} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID1])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channels_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),


    % Step 10: Create another state channel
    ID2 = crypto:strong_rand_bytes(32),
    SCOpenTxn2 = blockchain_txn_state_channel_open_v1:new(ID2, RouterPubkeyBin, TotalDC, 20, 2),
    SignedSCOpenTxn2 = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn2, RouterSigFun),

    Block2 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedSCOpenTxn2]]),
    _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block2, RouterChain, Self]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        {ok, 24} == ct_rpc:call(GatewayNode1, blockchain, height, [C])
    end, 10, timer:seconds(1)),
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 24} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    % Step 11: Checking that state channel got created properly
    {ok, SC2} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID2, RouterPubkeyBin, RouterLedger]),
    ?assertEqual(ID2, blockchain_ledger_state_channel_v1:id(SC2)),
    ?assertEqual(RouterPubkeyBin, blockchain_ledger_state_channel_v1:owner(SC2)),
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC2)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID2])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID2])),

    % Step 12: Adding some blocks to get the state channel to expire
    lists:foreach(
        fun(_) ->
            B = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, []]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, B, RouterChain, Self])
        end,
        lists:seq(1, 20)
    ),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 44} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    % Step 13: Adding close txn to blockchain
    receive
        {txn, Txn2} ->
            B2 = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn2]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, B2, RouterChain, Self])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    % Step 14: Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 45} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID2])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channels_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
    ok.
