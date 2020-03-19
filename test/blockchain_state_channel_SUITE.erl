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
    meck:new(blockchain_state_channel_request_v1, [passthrough]),
    meck:expect(blockchain_state_channel_request_v1, is_valid, fun(_, _) -> true end),
    meck:new(blockchain_event, [passthrough]),
    meck:expect(blockchain_event, add_handler, fun(_) -> ok end),
    meck:new(blockchain_worker, [passthrough]),
    meck:expect(blockchain_worker, blockchain, fun() -> blockchain end),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, ledger, fun(_) -> ledger end),
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, find_state_channels_by_owner, fun(_, _) -> {error, meck} end),

    {ok, Sup} = blockchain_state_channel_sup:start_link([BaseDir]),
    ID = <<"ID1">>,

    ?assert(erlang:is_process_alive(Sup)),
    ?assertEqual({error, not_found}, blockchain_state_channels_server:credits(ID)),
    ?assertEqual({error, not_found}, blockchain_state_channels_server:nonce(ID)),

    ok = blockchain_state_channels_server:burn(ID, 10),
    ?assertEqual({ok, 10}, blockchain_state_channels_server:credits(ID)),
    ?assertEqual({ok, 0}, blockchain_state_channels_server:nonce(ID)),

    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Req0 = blockchain_state_channel_request_v1:new(PubKeyBin, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    Req = blockchain_state_channel_request_v1:sign(Req0, SigFun),
    ok = blockchain_state_channels_server:request(Req),

    ?assertEqual({ok, 9}, blockchain_state_channels_server:credits(ID)),
    ?assertEqual({ok, 1}, blockchain_state_channels_server:nonce(ID)),

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
    ?assert(meck:validate(blockchain_state_channel_request_v1)),
    meck:unload(blockchain_state_channel_request_v1),
    ok.

zero_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    % Step 1: Create OUI txn
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterP2PAddress = ct_rpc:call(RouterNode, libp2p_swarm, p2p_address, [RouterSwarm]),
    OUI = 1,
    OUITxn = blockchain_txn_oui_v1:new(RouterPubkeyBin, [erlang:list_to_binary(RouterP2PAddress)], OUI, 1, 0),
    SignedOUITxn = blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun),

    % Step 2: Create state channel open zero txn
    ID = blockchain_state_channel_v1:zero_id(),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, 0, 100, 1),
    SignedSCOpenTxn = blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun),

    % Step 3: Create add gateway txn (making Gateqay node a gateway and router 1 its owner)
    {ok, GatewayPubkey, GatewaySigFun, _} = ct_rpc:call(GatewayNode1, blockchain_swarm, keys, []),
    GatewayPubkeyBin = libp2p_crypto:pubkey_to_bin(GatewayPubkey),

    UpdateGWOuiTxn = blockchain_txn_update_gateway_oui_v1:new(GatewayPubkeyBin, OUI, 1, 1),
    SignedUpdateGWOuiTxn0 = blockchain_txn_update_gateway_oui_v1:gateway_owner_sign(UpdateGWOuiTxn, GatewaySigFun),
    SignedUpdateGWOuiTxn1 = blockchain_txn_update_gateway_oui_v1:oui_owner_sign(SignedUpdateGWOuiTxn0, RouterSigFun),

    % Step 4: Adding block
    {ok, Block0} = ct_rpc:call(RouterNode,
                         test_utils,
                         create_block,
                         [ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn, SignedUpdateGWOuiTxn1]]),
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [RouterSwarm, Block0, RouterChain, self()]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        C = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
        {ok, 2} == ct_rpc:call(GatewayNode1, blockchain, height, [C])
    end, 60, timer:seconds(2)),

    % Step 5: Checking that state channel got created properly
    RouterLedger = blockchain:ledger(RouterChain),
    {ok, SC} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID, RouterPubkeyBin, RouterLedger]),
    ?assertEqual(ID, blockchain_ledger_state_channel_v1:id(SC)),
    ?assertEqual(RouterPubkeyBin, blockchain_ledger_state_channel_v1:owner(SC)),
    ?assertEqual(0, blockchain_ledger_state_channel_v1:amount(SC)),

    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 6: Sending packet with same OUI
    Packet0 = blockchain_helium_packet_v1:new(1, <<"sup">>),
    PacketInfo0 = {Packet0, <<"devaddr">>, 1, <<"mic1">>},
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [PacketInfo0]),

    % Step 7: Checking state channel on server/client (balance did not update but nonce did)
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 0} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID]) andalso
        {ok, 1} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("MARKER ~p", [ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])]),
        {ok, 0} == ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])
    end, 30, timer:seconds(1)),

     % Step 8: Sending packet with same OUI and a payload
    Payload1 = crypto:strong_rand_bytes(24),
    Packet1 = blockchain_helium_packet_v1:new(1, Payload1),
    PacketInfo1 = {Packet1, <<"devaddr">>, 1, <<"mic1">>},
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [PacketInfo1]),

    % Step 9: Checking state channel on server/client (balance did not update but nonce did)
    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, 0} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID]) andalso
        {ok, 2} == ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ct:pal("MARKER ~p", [ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])]),
        {ok, 0} == ct_rpc:call(GatewayNode1, blockchain_state_channels_client, credits, [ID])
    end, 30, timer:seconds(1)),

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
    TotalDC = 10,
    ID = crypto:strong_rand_bytes(24),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, TotalDC, 100, 1),
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
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [Self]),
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    PacketInfo0 = {Packet0, <<"devaddr">>, 1, <<"mic1">>},
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [PacketInfo0]),

    {ok, State} = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, state, []),
    ok = ct:pal("state: ~p", [State]),

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
    PacketInfo1 = {Packet1, <<"devaddr">>, 2, <<"mic1">>},
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [PacketInfo1]),

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

    ok = blockchain_ct_utils:wait_until(fun() ->
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])
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
    TotalDC = 10,
    ID = crypto:strong_rand_bytes(32),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, TotalDC, 20, 1),
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
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [Self]),
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    PacketInfo0 = {Packet0, <<"devaddr">>, 1, <<"mic1">>},
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [PacketInfo0]),

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
    TotalDC = 10,
    ID = crypto:strong_rand_bytes(120),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, TotalDC, 100, 1),
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
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])),
    ?assertEqual({ok, 0}, ct_rpc:call(RouterNode, blockchain_state_channels_server, nonce, [ID])),

    % Step 5: Sending 1 packet
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, packet_forward, [Self]),
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new(1, Payload0),
    PacketInfo0 = {Packet0, <<"devaddr">>, 1, <<"mic1">>},
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [PacketInfo0]),

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
    PacketInfo1 = {Packet1, <<"devaddr">>, 1, <<"mic1">>},
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [PacketInfo1]),

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

    ok = blockchain_ct_utils:wait_until(fun() ->
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID])
    end, 30, timer:seconds(1)),

    % Step 10: Recreating the state channel open txn with the same nonce
    RouterChain2 = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterLedger2 = blockchain:ledger(RouterChain2),

    ct:pal("DCEntry: ~p", [ct_rpc:call(RouterNode, blockchain_ledger_v1, find_dc_entry, [RouterPubkeyBin, RouterLedger2])]),

    ReplayTotalDC = 20,
    ReplayID = crypto:strong_rand_bytes(24),
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
    TotalDC = 10,
    ID1 = crypto:strong_rand_bytes(24),
    SCOpenTxn1 = blockchain_txn_state_channel_open_v1:new(ID1, RouterPubkeyBin, TotalDC, 20, 1),
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
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC1)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID1])),
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
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID1])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channels_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),


    % Step 10: Create another state channel
    ID2 = crypto:strong_rand_bytes(24),
    SCOpenTxn2 = blockchain_txn_state_channel_open_v1:new(ID2, RouterPubkeyBin, TotalDC, 20, 2),
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
    ?assertEqual(TotalDC, blockchain_ledger_state_channel_v1:amount(SC2)),

    ?assertEqual({ok, TotalDC}, ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID2])),
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
        {error, not_found} == ct_rpc:call(RouterNode, blockchain_state_channels_server, credits, [ID2])
    end, 10, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        {ok, []} == ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channels_by_owner, [RouterPubkeyBin, RouterLedger])
    end, 10, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_worker]),
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
