-module(blockchain_state_channel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1,
    full_test/1,
    expired_test/1,
    replay_test/1,
    multiple_test/1,
    multi_owner_multi_sc_test/1
]).

-include("blockchain.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

all() ->
    [
        basic_test,
        full_test,
        expired_test,
        replay_test,
        multiple_test,
        multi_owner_multi_sc_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(basic_test, Config) ->
    BaseDir = "data/blockchain_state_channel_SUITE/" ++ erlang:atom_to_list(basic_test),
    [{base_dir, BaseDir} |Config];
init_per_testcase(Test, Config) ->
    InitConfig = blockchain_ct_utils:init_per_testcase(Test, Config),
    Nodes = ?config(nodes, InitConfig),
    Balance = 5000,
    NumConsensusMembers = ?config(num_consensus_members, InitConfig),

    %% accumulate the address of each node
    Addrs = lists:foldl(fun(Node, Acc) ->
                                Addr = ct_rpc:call(Node, blockchain_swarm, pubkey_bin, []),
                                [Addr | Acc]
                        end, [], Nodes),

    ConsensusAddrs = lists:sublist(lists:sort(Addrs), NumConsensusMembers),

    {InitialVars, _Config} = blockchain_ct_utils:create_vars(#{num_consensus_members => NumConsensusMembers}),

    % Create genesis block
    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance) || Addr <- Addrs],
    GenDCsTxs = [blockchain_txn_dc_coinbase_v1:new(Addr, Balance) || Addr <- Addrs],
    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new(ConsensusAddrs, <<"proof">>, 1, 0),

    GenGwTxns = [blockchain_txn_gen_gateway_v1:new(Addr, Addr, h3:from_geo({37.780586, -122.469470}, 13), 0)
                 || Addr <- Addrs],

    Txs = InitialVars ++ GenPaymentTxs ++ GenDCsTxs ++ GenGwTxns ++ [GenConsensusGroupTx],
    GenesisBlock = blockchain_block:new_genesis_block(Txs),

    %% tell each node to integrate the genesis block
    lists:foreach(fun(Node) ->
                          ?assertMatch(ok, ct_rpc:call(Node, blockchain_worker, integrate_genesis_block, [GenesisBlock]))
                  end, Nodes),

    %% wait till each worker gets the gensis block
    ok = lists:foreach(
           fun(Node) ->
                   ok = blockchain_ct_utils:wait_until(
                          fun() ->
                                  C0 = ct_rpc:call(Node, blockchain_worker, blockchain, []),
                                  {ok, Height} = ct_rpc:call(Node, blockchain, height, [C0]),
                                  ct:pal("node ~p height ~p", [Node, Height]),
                                  Height == 1
                          end, 100, 100)
           end, Nodes),

    ok = check_genesis_block(InitConfig, GenesisBlock),
    ConsensusMembers = get_consensus_members(InitConfig, ConsensusAddrs),
    [{consensus_members, ConsensusMembers} | InitConfig].

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
    application:ensure_all_started(throttle),
    application:ensure_all_started(lager),
    BaseDir = ?config(base_dir, Config),
    SwarmOpts = [
        {libp2p_nat, [{enabled, false}]},
        {base_dir, BaseDir}
    ],
    {ok, Swarm} = libp2p_swarm:start(basic_test, SwarmOpts),

    meck:unload(),
    meck:new(blockchain_swarm, [passthrough]),
    meck:expect(blockchain_swarm, swarm, fun() -> Swarm end),
    meck:new(blockchain_event, [passthrough]),
    meck:expect(blockchain_event, add_handler, fun(_) -> ok end),
    meck:new(blockchain_worker, [passthrough]),
    meck:expect(blockchain_worker, blockchain, fun() -> blockchain end),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, ledger, fun(_) -> ledger end),
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, find_scs_by_owner, fun(_, _) -> {ok, #{}} end),

    {ok, Sup} = blockchain_state_channel_sup:start_link([BaseDir]),
    ID = <<"ID1">>,

    ?assert(erlang:is_process_alive(Sup)),
    ?assertEqual({error, not_found}, blockchain_state_channels_server:nonce(ID)),

    true = erlang:exit(Sup, normal),
    ok = libp2p_swarm:stop(Swarm),
    ?assert(meck:validate(blockchain_swarm)),
    meck:unload(blockchain_swarm),
    ?assert(meck:validate(blockchain_event)),
    meck:unload(blockchain_event),
    ?assert(meck:validate(blockchain_worker)),
    meck:unload(blockchain_worker),
    ?assert(meck:validate(blockchain)),
    meck:unload(blockchain),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ok.

full_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    % Some madness to make submit txn work and "create a block"
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck_test_util, forward_submit_txn, [Self]),

    %% ok = ct_rpc:call(RouterNode, meck_test_util, expect, [[blockchain_worker, submit_txn, F]]),
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),

    receive
        {txn, test} ->
            ct:pal("Got txn test"),
            ok
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
    ID = crypto:strong_rand_bytes(24),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, 100, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    ct:pal("RouterChain: ~p", [RouterChain]),
    {ok, Block0} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]]),
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

    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 1} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    % Step 7: Making sure packet got transmitted
    receive
        {packet, P0} ->
            ?assertEqual(Packet0, P0)
    after 10000 ->
        ct:fail("packet timeout")
    end,

    % Step 5: Sending 1 packet
    Payload1 = crypto:strong_rand_bytes(120),
    Packet1 = blockchain_helium_packet_v1:new(1, Payload1),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 2} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    % Step 8: Adding close txn to blockchain
    receive
        {txn, Txn} ->
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block1, RouterChain, Self])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    % Step 9: Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 3} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 30, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
    ok.

expired_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck_test_util, forward_submit_txn, [Self]),

    %% Check that this works
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} ->
            ct:pal("Got txn test"),
            ok
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
    ID = crypto:strong_rand_bytes(32),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, 20, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    {ok, Block0} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]]),
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

    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 1} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
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
            {ok, B} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, []]),
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
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
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
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_sc_ids_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
    ok.

replay_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck_test_util, forward_submit_txn, [Self]),

    %% Check that this works
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} ->
            ct:pal("Got txn test"),
            ok
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
    ID = crypto:strong_rand_bytes(120),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, 100, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    ct:pal("RouterChain: ~p", [RouterChain]),
    {ok, Block0} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]]),
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

    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 1} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    % Step 7: Making sure packet got transmitted
    receive
        {packet, P0} ->
            ?assertEqual(Packet0, P0)
    after 10000 ->
        ct:fail("packet timeout")
    end,

    % Step 5: Sending 1 packet
    Payload1 = crypto:strong_rand_bytes(120),
    Packet1 = blockchain_helium_packet_v1:new(1, Payload1),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1]),

    % Step 6: Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 2} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    % Step 8: Adding close txn to blockchain
    receive
        {txn, Txn} ->
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block1, RouterChain, Self])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    % Step 9: Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
        {ok, 3} == ct_rpc:call(RouterNode, blockchain, height, [C])
    end, 30, timer:seconds(1)),

    % Step 10: Recreating the state channel open txn with the same nonce
    RouterChain2 = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterLedger2 = blockchain:ledger(RouterChain2),

    ct:pal("DCEntry: ~p", [ct_rpc:call(RouterNode, blockchain_ledger_v1, find_dc_entry, [RouterPubkeyBin, RouterLedger2])]),

    ReplayID = crypto:strong_rand_bytes(24),
    ReplaySCOpenTxn = blockchain_txn_state_channel_open_v1:new(ReplayID, RouterPubkeyBin, 100, 1),
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

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = ct_rpc:call(RouterNode, meck_test_util, forward_submit_txn, [Self]),

    %% Check that this works
    ok = ct_rpc:call(RouterNode, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} ->
            ct:pal("Got txn test"),
            ok
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
    ID1 = crypto:strong_rand_bytes(24),
    SCOpenTxn1 = blockchain_txn_state_channel_open_v1:new(ID1, RouterPubkeyBin, 20, 1),
    SignedSCOpenTxn1 = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn1, RouterSigFun),

    % Step 3: Adding block
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn1]]),
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

    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID1])),

    % Step 5: Adding some blocks to get the state channel to expire
    lists:foreach(
        fun(_) ->
            {ok, B} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, []]),
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
            {ok, B1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn1]]),
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
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_sc_ids_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),


    % Step 10: Create another state channel
    ID2 = crypto:strong_rand_bytes(24),
    SCOpenTxn2 = blockchain_txn_state_channel_open_v1:new(ID2, RouterPubkeyBin, 20, 2),
    SignedSCOpenTxn2 = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn2, RouterSigFun),

    {ok, Block2} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedSCOpenTxn2]]),
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

    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID2])),

    % Step 12: Adding some blocks to get the state channel to expire
    lists:foreach(
        fun(_) ->
            {ok, B} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, []]),
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
            {ok, B2} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn2]]),
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
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_sc_ids_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
    ok.

multi_owner_multi_sc_test(Config) ->
    [RouterNode1, RouterNode2, GatewayNode1 | _] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    RouterPubkeyBin1 = ct_rpc:call(RouterNode1, blockchain_swarm, pubkey_bin, []),
    RouterPubkeyBin2 = ct_rpc:call(RouterNode2, blockchain_swarm, pubkey_bin, []),

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = ct_rpc:call(RouterNode1, meck_test_util, forward_submit_txn, [Self]),
    ok = ct_rpc:call(RouterNode2, meck_test_util, forward_submit_txn, [Self]),

    %% Check that this works
    ok = ct_rpc:call(RouterNode1, blockchain_worker, submit_txn, [test]),
    receive
        {txn, test} ->
            ct:pal("Got txn test"),
            ok
    after 1000 ->
        ct:fail("txn test timeout")
    end,

    %% Create OUI txn for RouterNode1
    SignedOUITxn1 = create_oui_txn(RouterNode1, 1),

    %% Create 3 SCs for RouterNode1
    Expiry = 20,
    ID11 = crypto:strong_rand_bytes(24),
    ID12 = crypto:strong_rand_bytes(24),
    ID13 = crypto:strong_rand_bytes(24),
    SignedSCOpenTxn11 = create_sc_open_txn(RouterNode1, ID11, Expiry, 1),
    SignedSCOpenTxn12 = create_sc_open_txn(RouterNode1, ID12, Expiry, 2),
    SignedSCOpenTxn13 = create_sc_open_txn(RouterNode1, ID13, Expiry, 3),

    % Adding block with first set of txns
    Txns1 = [SignedOUITxn1,
             SignedSCOpenTxn11,
             SignedSCOpenTxn12,
             SignedSCOpenTxn13],
    RouterChain1 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
    RouterSwarm1 = ct_rpc:call(RouterNode1, blockchain_swarm, swarm, []),
    {ok, Block2} = ct_rpc:call(RouterNode1, test_utils, create_block, [ConsensusMembers, Txns1]),
    _ = ct_rpc:call(RouterNode1, blockchain_gossip_handler, add_block, [RouterSwarm1, Block2, RouterChain1, Self]),
    ct:pal("Block2: ~p", [Block2]),

    %% Wait till the block is propagated
    ok = blockchain_ct_utils:wait_until(fun() ->
        C1 = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        C2 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
        C3 = ct_rpc:call(RouterNode2, blockchain_worker, blockchain, []),
        {ok, 2} == ct_rpc:call(GatewayNode1, blockchain, height, [C1]) andalso
        {ok, 2} == ct_rpc:call(RouterNode1, blockchain, height, [C2]) andalso
        {ok, 2} == ct_rpc:call(RouterNode2, blockchain, height, [C3])
    end, 10, timer:seconds(1)),

    %% Create OUI txn for RouterNode2
    SignedOUITxn2 = create_oui_txn(RouterNode2, 2),
    ID21 = crypto:strong_rand_bytes(24),
    ID22 = crypto:strong_rand_bytes(24),
    ID23 = crypto:strong_rand_bytes(24),

    %% Create OUI txn for RouterNode2
    SignedSCOpenTxn21 = create_sc_open_txn(RouterNode2, ID21, Expiry, 1),
    SignedSCOpenTxn22 = create_sc_open_txn(RouterNode2, ID22, Expiry, 2),
    SignedSCOpenTxn23 = create_sc_open_txn(RouterNode2, ID23, Expiry, 3),

    %% Create second set of txns
    Txns2 = [SignedOUITxn2,
             SignedSCOpenTxn21,
             SignedSCOpenTxn22,
             SignedSCOpenTxn23],

    %% Adding block with second set of txns
    RouterChain2 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
    {ok, Block3} = ct_rpc:call(RouterNode1, test_utils, create_block, [ConsensusMembers, Txns2]),
    ct:pal("Block3: ~p", [Block3]),
    _ = ct_rpc:call(RouterNode1, blockchain_gossip_handler, add_block, [RouterSwarm1, Block3, RouterChain2, Self]),

    %% Wait till the block is propagated
    ok = blockchain_ct_utils:wait_until(fun() ->
        C1 = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        C2 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
        C3 = ct_rpc:call(RouterNode2, blockchain_worker, blockchain, []),
        {ok, 3} == ct_rpc:call(GatewayNode1, blockchain, height, [C1]) andalso
        {ok, 3} == ct_rpc:call(RouterNode1, blockchain, height, [C2]) andalso
        {ok, 3} == ct_rpc:call(RouterNode2, blockchain, height, [C3])
    end, 10, timer:seconds(1)),

    %% Checking that state channels got created properly
    RouterChain3 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
    RouterLedger1 = ct_rpc:call(RouterNode1, blockchain, ledger, [RouterChain3]),

    RouterChain4 = ct_rpc:call(RouterNode2, blockchain_worker, blockchain, []),
    RouterLedger2 = ct_rpc:call(RouterNode2, blockchain, ledger, [RouterChain4]),
    {ok, SC11} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_state_channel,
                             [ID11, RouterPubkeyBin1, RouterLedger1]),
    {ok, SC12} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_state_channel,
                             [ID12, RouterPubkeyBin1, RouterLedger1]),
    {ok, SC13} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_state_channel,
                             [ID13, RouterPubkeyBin1, RouterLedger1]),
    {ok, SC21} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_state_channel,
                             [ID21, RouterPubkeyBin2, RouterLedger2]),
    {ok, SC22} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_state_channel,
                             [ID22, RouterPubkeyBin2, RouterLedger2]),
    {ok, SC23} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_state_channel,
                             [ID23, RouterPubkeyBin2, RouterLedger2]),

    {ok, R1} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin1, RouterLedger1]),

    3 = maps:size(R1),
    true = lists:usort([ID11, ID12, ID13]) == lists:usort(maps:keys(R1)),

    {ok, R2} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin2, RouterLedger2]),

    3 = maps:size(R2),
    true = lists:usort([ID21, ID22, ID23]) == lists:usort(maps:keys(R2)),

    6 = length(lists:usort([SC11, SC12, SC13, SC21, SC22, SC23])),

    %% Check that the state channels being created are legit
    ?assertEqual(ID11, blockchain_ledger_state_channel_v1:id(SC11)),
    ?assertEqual(RouterPubkeyBin1, blockchain_ledger_state_channel_v1:owner(SC11)),
    ?assertEqual(ID12, blockchain_ledger_state_channel_v1:id(SC12)),
    ?assertEqual(RouterPubkeyBin1, blockchain_ledger_state_channel_v1:owner(SC12)),
    ?assertEqual(ID13, blockchain_ledger_state_channel_v1:id(SC13)),
    ?assertEqual(RouterPubkeyBin1, blockchain_ledger_state_channel_v1:owner(SC13)),
    ?assertEqual(ID21, blockchain_ledger_state_channel_v1:id(SC21)),
    ?assertEqual(RouterPubkeyBin2, blockchain_ledger_state_channel_v1:owner(SC21)),
    ?assertEqual(ID22, blockchain_ledger_state_channel_v1:id(SC22)),
    ?assertEqual(RouterPubkeyBin2, blockchain_ledger_state_channel_v1:owner(SC22)),
    ?assertEqual(ID23, blockchain_ledger_state_channel_v1:id(SC23)),
    ?assertEqual(RouterPubkeyBin2, blockchain_ledger_state_channel_v1:owner(SC23)),

    %% Add 20 more blocks to get the state channel to expire
    ok = lists:foreach(
           fun(_) ->
                   {ok, B} = ct_rpc:call(RouterNode1, test_utils, create_block, [ConsensusMembers, []]),
                   _ = ct_rpc:call(RouterNode1, blockchain_gossip_handler, add_block, [RouterSwarm1, B, RouterChain3, Self])
           end,
           lists:seq(1, 20)
          ),

    %% At this point we should be at genesis (1) + block2 + block3 + 20 more blocks
    ok = blockchain_ct_utils:wait_until(fun() ->
        C1 = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        C2 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
        C3 = ct_rpc:call(RouterNode2, blockchain_worker, blockchain, []),
        {ok, 23} == ct_rpc:call(GatewayNode1, blockchain, height, [C1]) andalso
        {ok, 23} == ct_rpc:call(RouterNode1, blockchain, height, [C2]) andalso
        {ok, 23} == ct_rpc:call(RouterNode2, blockchain, height, [C3])
    end, 10, timer:seconds(1)),


    %% At this point, we know that Block2's sc open txns must have expired
    %% So we do the dumbest possible thing and match each one
    %% Checking that the IDs are atleast coherent

    ok = check_all_closed([ID11, ID12, ID13]),

    %% Add 3 more blocks to trigger sc close for sc open in Block3
    ok = lists:foreach(
           fun(_) ->
                   {ok, B} = ct_rpc:call(RouterNode1, test_utils, create_block, [ConsensusMembers, []]),
                   _ = ct_rpc:call(RouterNode1, blockchain_gossip_handler, add_block, [RouterSwarm1, B, RouterChain3, Self])
           end,
           lists:seq(1, 3)
          ),


    {ok, R11} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin1, RouterLedger1]),

    3 = maps:size(R11),

    {ok, R22} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin2, RouterLedger2]),

    3 = maps:size(R22),


    %% At this point we should be at Block26
    ok = blockchain_ct_utils:wait_until(fun() ->
        C1 = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        C2 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
        C3 = ct_rpc:call(RouterNode2, blockchain_worker, blockchain, []),
        {ok, 26} == ct_rpc:call(GatewayNode1, blockchain, height, [C1]) andalso
        {ok, 26} == ct_rpc:call(RouterNode1, blockchain, height, [C2]) andalso
        {ok, 26} == ct_rpc:call(RouterNode2, blockchain, height, [C3])
    end, 10, timer:seconds(1)),

    RouterNode1Chain = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
    RouterNode1Height = ct_rpc:call(RouterNode1, blockchain, height, [RouterNode1Chain]),
    RouterNode2Chain = ct_rpc:call(RouterNode2, blockchain_worker, blockchain, []),
    RouterNode2Height = ct_rpc:call(RouterNode2, blockchain, height, [RouterNode2Chain]),

    ct:pal("Routernode1, height: ~p", [RouterNode1Height]),
    ct:pal("Routernode2, height: ~p", [RouterNode2Height]),

    %% And the related sc_close for sc_open in Block3 must have fired

    ok = check_all_closed([ID21, ID22, ID23]),
    ok.


%% ------------------------------------------------------------------
%% Helper functions
%% ------------------------------------------------------------------

check_genesis_block(Config, GenesisBlock) ->
    Nodes = ?config(nodes, Config),
    lists:foreach(fun(Node) ->
                          Blockchain = ct_rpc:call(Node, blockchain_worker, blockchain, []),
                          {ok, HeadBlock} = ct_rpc:call(Node, blockchain, head_block, [Blockchain]),
                          {ok, WorkerGenesisBlock} = ct_rpc:call(Node, blockchain, genesis_block, [Blockchain]),
                          {ok, Height} = ct_rpc:call(Node, blockchain, height, [Blockchain]),
                          ?assertEqual(GenesisBlock, HeadBlock),
                          ?assertEqual(GenesisBlock, WorkerGenesisBlock),
                          ?assertEqual(1, Height)
                  end, Nodes).

get_consensus_members(Config, ConsensusAddrs) ->
    Nodes = ?config(nodes, Config),
    lists:keysort(1, lists:foldl(fun(Node, Acc) ->
                                         Addr = ct_rpc:call(Node, blockchain_swarm, pubkey_bin, []),
                                         case lists:member(Addr, ConsensusAddrs) of
                                             false -> Acc;
                                             true ->
                                                 {ok, Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Node, blockchain_swarm, keys, []),
                                                 [{Addr, Pubkey, SigFun} | Acc]
                                         end
                                 end, [], Nodes)).

create_oui_txn(RouterNode, OUI) ->
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterP2PAddress = ct_rpc:call(RouterNode, libp2p_swarm, p2p_address, [RouterSwarm]),
    OUITxn = blockchain_txn_oui_v1:new(RouterPubkeyBin, [erlang:list_to_binary(RouterP2PAddress)], OUI, 1, 0),
    blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun).

create_sc_open_txn(RouterNode, ID, Expiry, Nonce) ->
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, Expiry, Nonce),
    blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun).

check_all_closed([]) ->
    ok;
check_all_closed(IDs) ->
    receive
        {txn, Txn} ->
            check_all_closed([ ID || ID <- IDs, ID /= blockchain_state_channel_v1:id(blockchain_txn_state_channel_close_v1:state_channel(Txn)) ])
    after 1000 ->
              ct:fail("still unclosed ~p", [IDs])
    end.
