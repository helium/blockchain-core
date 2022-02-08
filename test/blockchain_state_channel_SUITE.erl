-module(blockchain_state_channel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_ct_utils.hrl").

-export([
    groups/0,
    all/0,
    test_cases/0,
    init_per_group/2, end_per_group/2,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    full_test/1,
    overspent_test/1,
    dup_packets_test/1,
    cached_routing_test/1,
    max_actor_test/1,
    max_actor_cache_eviction_test/1,
    replay_test/1,
    multiple_test/1,
    multi_owner_multi_sc_test/1,
    multi_active_sc_test/1,
    open_without_oui_test/1,
    max_scs_open_test/1,
    max_scs_open_v2_test/1,
    sc_dispute_prevention_test/1,
    oui_not_found_test/1,
    unknown_owner_test/1,
    crash_single_sc_test/1,
    crash_multi_sc_test/1,
    crash_sc_sup_test/1,
    hotspot_in_router_oui_test/1,
    default_routers_test/1
]).

-include("blockchain.hrl").
-include("blockchain_utils.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

groups() ->
    [{sc_libp2p,
      [],
      test_cases()
     },
     {sc_grpc,
      [],
      test_cases()
     }].

all() ->
    [{group, sc_libp2p}, {group, sc_grpc}].

test_cases() ->
    [
        full_test,
        overspent_test,
        dup_packets_test,
        cached_routing_test,
        max_actor_test,
        max_actor_cache_eviction_test,
        replay_test,
        multiple_test,
        multi_owner_multi_sc_test,
        multi_active_sc_test,
        open_without_oui_test,
        max_scs_open_test,
        max_scs_open_v2_test,
        sc_dispute_prevention_test,
        oui_not_found_test,
        unknown_owner_test,
        crash_single_sc_test,
        crash_multi_sc_test,
        crash_sc_sup_test,
        hotspot_in_router_oui_test,
        default_routers_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    [{sc_client_transport_handler, blockchain_state_channel_handler} | Config].

%% NOTE: If you're running individual tests 'sc_client_transport_handler` will be unset.
%% Run with --group=(sc_libp2p | sc_grpc)
init_per_group(sc_libp2p, Config) ->
    [{sc_client_transport_handler, blockchain_state_channel_handler} | Config];
init_per_group(sc_grpc, Config) ->
    [{sc_client_transport_handler, blockchain_grpc_sc_client_test_handler} | Config].

debug_modules_for_node(_, _, []) ->
    ok;
debug_modules_for_node(Node, Filename, [Module | Rest]) ->
    {ok, _} = ct_rpc:call(
                Node,
                lager,
                trace_file,
                [Filename, [{module, Module}], debug]
               ),
    debug_modules_for_node(Node, Filename, Rest).


init_per_testcase(Test, Config) ->
    application:ensure_all_started(throttle),
    application:ensure_all_started(lager),

    InitConfig0 = blockchain_ct_utils:init_base_dir_config(?MODULE, Test, Config),
    InitConfig = blockchain_ct_utils:init_per_testcase(Test, InitConfig0),

    Nodes = ?config(nodes, InitConfig),
    Balance = 50000,
    NumConsensusMembers = ?config(num_consensus_members, InitConfig),

    [RouterNode, GatewayNode1|_] = Nodes,
    Dir = os:getenv("SC_DIR", ""),
    debug_modules_for_node(
      RouterNode,
      Dir ++ "sc_server.log",
      [blockchain_state_channel_v1,
       blockchain_state_channels_cache,
       blockchain_state_channels_handler,
       blockchain_state_channels_server,
       blockchain_state_channels_worker,
       blockchain_txn_state_channel_close_v1]
     ),
    debug_modules_for_node(
      GatewayNode1,
      Dir ++ "sc_client_1.log",
      [blockchain_state_channel_v1,
       blockchain_state_channels_client,
       blockchain_state_channels_handler]
     ),

    %% accumulate the address of each node
    Addrs = lists:foldl(fun(Node, Acc) ->
                                Addr = ct_rpc:call(Node, blockchain_swarm, pubkey_bin, []),
                                [Addr | Acc]
                        end, [], Nodes),

    ConsensusAddrs = lists:sublist(lists:sort(Addrs), NumConsensusMembers),

    %% the SC tests use the first two nodes as the gateway and router
    %% for the GRPC group to work we need to ensure these two nodes are connected to each other
    %% in blockchain_ct_utils:init_per_testcase the nodes are connected to a majority of the group
    %% but that does not guarantee these two nodes will be connected
    [RouterNode, GatewayNode|_] = Nodes,
    [RouterNodeAddr, GatewayNodeAddr|_] = Addrs,
    ok = blockchain_ct_utils:wait_until(
             fun() ->
                     lists:all(
                       fun({Node, AddrToConnectToo}) ->
                               try
                                   GossipPeers = ct_rpc:call(Node, blockchain_swarm, gossip_peers, [], 500),
                                   ct:pal("~p connected to peers ~p", [Node, GossipPeers]),
                                   case lists:member(libp2p_crypto:pubkey_bin_to_p2p(AddrToConnectToo), GossipPeers) of
                                       true -> true;
                                       false ->
                                           ct:pal("~p is not connected to desired peer ~p", [Node, AddrToConnectToo]),
                                           Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, [], 500),
                                           CRes = ct_rpc:call(Node, libp2p_swarm, connect, [Swarm, AddrToConnectToo], 500),
                                           ct:pal("Connecting ~p to ~p: ~p", [Node, AddrToConnectToo, CRes]),
                                           false
                                   end
                               catch _C:_E ->
                                       false
                               end
                       end, [{RouterNode, GatewayNodeAddr}, {GatewayNode, RouterNodeAddr}])
             end, 200, 150),

    SCDisputeStrat = case Test == sc_dispute_prevention_test of
                         false -> 0;
                         true -> 1
                     end,
    DefaultVars = #{num_consensus_members => NumConsensusMembers},
    ExtraVars = #{
        max_open_sc => 2,
        min_expire_within => 10,
        max_xor_filter_size => 1024*100,
        max_xor_filter_num => 5,
        max_subnet_size => 65536,
        min_subnet_size => 8,
        max_subnet_num => 20,
        sc_grace_blocks => 5,
        dc_payload_size => 24,
        sc_max_actors => 100,
        sc_version => 2, %% we are focring 2 for all test as 1 is just rly old now
        sc_dispute_strategy_version => SCDisputeStrat
    },

    {InitialVars, {master_key, MasterKey}} = blockchain_ct_utils:create_vars(maps:merge(DefaultVars, ExtraVars)),

    % Create genesis block
    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance) || Addr <- Addrs],
    GenDCsTxs = [blockchain_txn_dc_coinbase_v1:new(Addr, Balance) || Addr <- Addrs],
    GenPriceOracle = blockchain_txn_gen_price_oracle_v1:new(100000000), % 1 dollar
    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new(ConsensusAddrs, <<"proof">>, 1, 0),

    %% Make one consensus member the owner of all gateways
    GenGwTxns = [blockchain_txn_gen_gateway_v1:new(Addr, hd(ConsensusAddrs), h3:from_geo({37.780586, -122.469470}, 13), 0)
                 || Addr <- Addrs],

    Txs = InitialVars ++ [GenPriceOracle] ++ GenPaymentTxs ++ GenDCsTxs ++ GenGwTxns ++ [GenConsensusGroupTx],
    GenesisBlock = blockchain_block:new_genesis_block(Txs),

    %% tell each node to integrate the genesis block
    lists:foreach(
        fun(Node) ->
            ?assertMatch(ok, ct_rpc:call(Node, blockchain_worker, integrate_genesis_block, [GenesisBlock]))
        end,
        Nodes
    ),

    %% wait till each worker gets the genesis block
    ok = lists:foreach(
        fun(Node) ->
            ok = blockchain_ct_utils:wait_until(
                fun() ->
                    C0 = ct_rpc:call(Node, blockchain_worker, blockchain, []),
                    {ok, Height} = ct_rpc:call(Node, blockchain, height, [C0]),
                    ct:pal("node ~p height ~p", [Node, Height]),
                    Height == 1
                end,
                100,
                100
            )
        end,
        Nodes
    ),

    ok = check_genesis_block(InitConfig, GenesisBlock),
    ConsensusMembers = get_consensus_members(InitConfig, ConsensusAddrs),
    [{consensus_members, ConsensusMembers}, {master_key, MasterKey} | InitConfig].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(basic_test, _Config) ->
    ok;
end_per_testcase(Test, Config) ->
    blockchain_ct_utils:end_per_testcase(Test, Config).

end_per_group(_, _Config) ->
    ok.

end_per_suite(_) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

full_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ct:pal("RouterNode ~p", [RouterNode]),
    ct:pal("Gateway node1 ~p", [GatewayNode1]),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID1, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),
    %% Add block with oui and sc open txns
    {ok, Block2} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block2: ~p", [Block2]),
    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block2, RouterChain, Self, RouterSwarm]),
    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID1),

    %% Check that the state channel is in server
    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
        maps:is_key(ID1, ActiveSCs)
    end, 30, timer:seconds(1)),

    %% Check that the state channel is active and running
    SCWorkerPid1 = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_active_pid, [ID1]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        erlang:is_pid(SCWorkerPid1) andalso ct_rpc:call(RouterNode, erlang, is_process_alive, [SCWorkerPid1])
    end, 30, timer:seconds(1)),

    SignedSC = ct_rpc:call(RouterNode, blockchain_state_channels_worker, get, [SCWorkerPid1, 100]),
    ?assertEqual(ok, blockchain_state_channel_v1:validate(SignedSC)),

    %% Open another SC that will NOT expire
    ID2 = crypto:strong_rand_bytes(32),
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, 30, 1, 2),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),
    %% Add block with oui and sc open txns
    {ok, Block3} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedSCOpenTxn2]),
    ct:pal("Block3: ~p", [Block3]),
    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block3, RouterChain, Self, RouterSwarm]),
    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 3),
    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID2),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID1, 1),

    %% Sending another packet
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ct:pal("Gateway node1 ~p", [GatewayNode1]),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID1, 2),

    %% Adding 15 fake blocks to get the state channel to expire (on top of the 3 first one)
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    %% Adding close txn to blockchain
    SCOpenBlockHash = blockchain_block:hash_block(Block2),
    receive
        {txn, Txn} ->
            true = check_sc_close(Txn, ID1, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                             blockchain_helium_packet_v1:payload(Packet1)]),
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block1, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 19),

    %% Check that the state channel is not active and not running
    ok = blockchain_ct_utils:wait_until(fun() ->
        ct_rpc:call(RouterNode, erlang, is_process_alive, [SCWorkerPid1]) == false
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
        maps:is_key(ID1, ActiveSCs) == false
    end, 30, timer:seconds(1)),

    SCWorkerPid2 = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_active_pid, [ID2]),
    ?assert(erlang:is_pid(SCWorkerPid2)),
    ok = blockchain_ct_utils:wait_until(fun() ->
        ct_rpc:call(RouterNode, erlang, is_process_alive, [SCWorkerPid2])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
        maps:is_key(ID2, ActiveSCs)
    end, 30, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),

    ok.

overspent_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ct:pal("RouterNode ~p", [RouterNode]),
    ct:pal("Gateway node1 ~p", [GatewayNode1]),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce1 = 1,
    SignedSCOpenTxn1 = create_sc_open_txn(RouterNode, ID1, ExpireWithin, 1, Nonce1),
    ct:pal("SignedSCOpenTxn1: ~p", [SignedSCOpenTxn1]),

    %% Add block with oui and sc open txns
    {ok, Block2} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn1]),
    ct:pal("Block2: ~p", [Block2]),
    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block2, RouterChain, Self, RouterSwarm]),
    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID1),

    %% Check that the state channel is in server and active
    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
        maps:is_key(ID1, ActiveSCs)
    end, 30, timer:seconds(1)),

    %% Check that the state channel is active and running
    SCWorkerPid1 = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_active_pid, [ID1]),
    ok = blockchain_ct_utils:wait_until(fun() ->
        erlang:is_pid(SCWorkerPid1) andalso ct_rpc:call(RouterNode, erlang, is_process_alive, [SCWorkerPid1])
    end, 30, timer:seconds(1)),

    ID2 = crypto:strong_rand_bytes(32),
    Nonce2 = 2,
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, ExpireWithin, 1, Nonce2),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),

    %% Add block with oui and sc open txns
    {ok, Block3} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedSCOpenTxn2]),
    ct:pal("Block3: ~p", [Block3]),
    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block3, RouterChain, Self, RouterSwarm]),
    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 3),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID2),

    %% Check that the state channel is NOT active
    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
        maps:is_key(ID2, ActiveSCs) == false
    end, 30, timer:seconds(1)),

    %% Sending 21 packets
    lists:foreach(
        fun(_I) ->
            DevNonce = crypto:strong_rand_bytes(2),
            Packet = blockchain_ct_utils:join_packet(?APPKEY, DevNonce, 0.0),
            ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet, [], 'US915'])
        end,
        lists:seq(1, 20)
    ),

    %% Check that the OVERSPENT state channel is not active and not running
    ok = blockchain_ct_utils:wait_until(fun() ->
        ct_rpc:call(RouterNode, erlang, is_process_alive, [SCWorkerPid1]) == false
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
        maps:is_key(ID1, ActiveSCs) == false
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        SCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, []),
        {SC, _, _} = maps:get(ID1, SCs, undefined),
        SC =/= undefined andalso blockchain_state_channel_v1:nonce(SC) == 20
    end, 30, timer:seconds(1)),

    %% Check that the second state channel is active and running
    SCWorkerPid2 = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_active_pid, [ID2]),
    ?assert(erlang:is_pid(SCWorkerPid2)),
    ok = blockchain_ct_utils:wait_until(fun() ->
        ct_rpc:call(RouterNode, erlang, is_process_alive, [SCWorkerPid2])
    end, 30, timer:seconds(1)),

    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
        maps:is_key(ID2, ActiveSCs)
    end, 30, timer:seconds(1)),

    %% Sending 2 packets
    lists:foreach(
        fun(_I) ->
            DevNonce = crypto:strong_rand_bytes(2),
            Packet = blockchain_ct_utils:join_packet(?APPKEY, DevNonce, 0.0),
            ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet, [], 'US915'])
        end,
        lists:seq(1, 2)
    ),

    ok = blockchain_ct_utils:wait_until(fun() ->
        SCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, []),
        {SC, _, _} = maps:get(ID2, SCs, undefined),
        SC =/= undefined andalso blockchain_state_channel_v1:nonce(SC) == 2
    end, 30, timer:seconds(1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

dup_packets_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{16#deadbeef, 16#deadc0de}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce, 1000),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending 1 packet
    Payload0 = crypto:strong_rand_bytes(120),
    Packet0 = blockchain_helium_packet_v1:new({eui, 16#deadbeef, 16#deadc0de}, Payload0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),
    
    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),
    ct:pal("[~p:~p:~p] MARKER ~p~n", [?MODULE, ?FUNCTION_NAME, ?LINE, {0, Payload0}]),

    %% Sending another packet
    Payload1 = crypto:strong_rand_bytes(120),
    Packet1 = blockchain_helium_packet_v1:new({devaddr, 1207959553}, Payload1),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 2),
    ct:pal("[~p:~p:~p] MARKER ~p~n", [?MODULE, ?FUNCTION_NAME, ?LINE, {1, Payload1}]),

    %% Sending the same packet again
    Payload2 = crypto:strong_rand_bytes(120),
    Packet2 = blockchain_helium_packet_v1:new({devaddr, 1207959553}, Payload2),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet2, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 3),
    ct:pal("[~p:~p:~p] MARKER ~p~n", [?MODULE, ?FUNCTION_NAME, ?LINE, {2, Payload2}]),

    %% Sending Packet1 again
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 4),

    %% Adding 20 fake blocks to get the state channel to expire
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 17),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            true = check_sc_close(Txn, ID, SCOpenBlockHash, [Payload0, Payload1, Payload2]),
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block1, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

cached_routing_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    OUI = 1,
    SignedOUITxn = create_oui_txn(OUI, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block0: ~p", [Block0]),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),

    %% Checking that we have a cached route
    _RoutingInfo = blockchain_helium_packet_v1:routing_info(Packet0),
    Stats0 = ct_rpc:call(GatewayNode1, e2qc, stats, [sc_client_routing]),
    ?assert(proplists:get_value(q1size, Stats0) > 0),

    %% send a routing txn to clear cache
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RoutingTxn = blockchain_txn_routing_v1:update_router_addresses(OUI, RouterPubkeyBin, [], 2),
    SignedRoutingTxn = blockchain_txn_routing_v1:sign(RoutingTxn, RouterSigFun),

    %% Add block with oui and sc open txns
    {ok, Block1} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedRoutingTxn]),
    ct:pal("Block1: ~p", [Block1]),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block1, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 3),

    Stats1 = ct_rpc:call(GatewayNode1, e2qc, stats, [sc_client_routing]),
    ?assertEqual(0, proplists:get_value(q1size, Stats1)),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

max_actor_cache_eviction_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(24),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID1, ExpireWithin, 1, Nonce, 10000),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

     %% Create state channel open txn
    ID2 = crypto:strong_rand_bytes(24),
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, ExpireWithin, 1, Nonce+1, 10000),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn, SignedSCOpenTxn2]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    _SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID1),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID1, 0),

    ct:pal("ID1: ~p", [libp2p_crypto:bin_to_b58(ID1)]),
    ct:pal("ID2: ~p", [libp2p_crypto:bin_to_b58(ID2)]),

    MaxActorsAllowed = ct_rpc:call(RouterNode, blockchain_state_channel_v1, max_actors_allowed, [blockchain:ledger(RouterChain)]),
    ct:pal("MaxActorsAllowed: ~p", [MaxActorsAllowed]),

    %% Get active SC before sending MaxActorsAllowed + 1 packets from diff hotspots
    ActiveSCIDs0 = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, []),
    ?assertEqual([ID1], maps:keys(ActiveSCIDs0)),
    {_StateChannel1, _SCState1, SCPid} = maps:get(ID1, ActiveSCIDs0),

    CreateActorFun = fun() ->
                             Actor=#{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
                             Actor#{
                                    pubkey_bin => libp2p_crypto:pubkey_to_bin(PubKey),
                                    sig_fun => libp2p_crypto:mk_sig_fun(PrivKey)
                                   }
                     end,

    SendPacketFun = fun(#{pubkey_bin := PubKeyBin, sig_fun := SigFun} = _Actor) ->
        Packet = blockchain_helium_packet_v1:new(
            lorawan,
            crypto:strong_rand_bytes(20),
            erlang:system_time(millisecond),
            -100.0,
            915.2,
            "SF8BW125",
            -12.0,
            {devaddr, 1207959553}
        ),
        Offer0 = blockchain_state_channel_offer_v1:from_packet(Packet, PubKeyBin, 'US915'),
        Offer1 = blockchain_state_channel_offer_v1:sign(Offer0, SigFun),
        RouterLedger = blockchain:ledger(RouterChain),
        ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, handle_offer, [Offer1, sc_packet_test_handler, RouterLedger, Self]),
        {ok, Offer1}
    end,

    CreateActorAndSendPacketFun = fun(_Idx) ->
        Actor = CreateActorFun(),
        {ok, _} = SendPacketFun(Actor),
        Actor
    end,

    %% Sending MaxActorsAllowed + 1 packets. Allowing a little time for second
    %% state channel to be opened after filling the first with actors.
    _ = lists:map(CreateActorAndSendPacketFun, lists:seq(1, MaxActorsAllowed)),
    timer:sleep(100),
    _ = CreateActorAndSendPacketFun(101),

    %% Checking that new SC ID is not old SC ID
    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCIDs1 = maps:keys(ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, [])),
        ct:pal("ActiveSCIDs1: ~p", [ActiveSCIDs1]),
        lists:sort([ID1, ID2]) == lists:sort(ActiveSCIDs1)
    end, 30, timer:seconds(1)),

    %% ============================================================================
    %% Test really starts here, let's make a known actor
    #{pubkey_bin := KnownActorPubKeyBin} = KnownActor = CreateActorFun(),
    ok = ct_rpc:call(RouterNode, blockchain_state_channels_cache, insert_hotspot, [KnownActorPubKeyBin, SCPid]),

    %% Sending packet from gateway forced into the state channel cache of the already full SC, expecting a rejection.
    {ok, Offer0} = SendPacketFun(KnownActor),
    Rejection0 = blockchain_state_channel_rejection_v1:new(blockchain_state_channel_offer_v1:packet_hash(Offer0)),
    ok =
        receive
             {send_rejection, Rejection0} ->
                ok
        after 1000 ->
                ct:fail("Rejection not received, State channel should be full and reject this offer")
        end,

    %% Sending a packet from same previous gateway, should be purchased now, and in a different SC cache.
    {ok, Offer1} = SendPacketFun(KnownActor),
    Rejection1 = blockchain_state_channel_rejection_v1:new(blockchain_state_channel_offer_v1:packet_hash(Offer1)),
    ok =
        receive
            {send_rejection, Rejection1} ->
                ct:fail("Rejection received, Actor should have been moved to a different state channel with space")
        after 1000 ->
                ok
        end,

    %% Make sure the cached SC pid is not the same as the old one somehow
    ?assertNotEqual(SCPid, ct_rpc:call(RouterNode, blockchain_state_channels_cache, lookup_hotspot, [KnownActorPubKeyBin])),

    ok.


max_actor_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(24),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID1, ExpireWithin, 1, Nonce, 10000),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

     %% Create state channel open txn
    ID2 = crypto:strong_rand_bytes(24),
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, ExpireWithin, 1, Nonce+1, 10000),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn, SignedSCOpenTxn2]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    _SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID1),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID1, 0),

    ct:pal("ID1: ~p", [libp2p_crypto:bin_to_b58(ID1)]),
    ct:pal("ID2: ~p", [libp2p_crypto:bin_to_b58(ID2)]),

    MaxActorsAllowed = ct_rpc:call(RouterNode, blockchain_state_channel_v1, max_actors_allowed, [blockchain:ledger(RouterChain)]),
    ct:pal("MaxActorsAllowed: ~p", [MaxActorsAllowed]),

    %% Get active SC before sending MaxActorsAllowed + 1 packets from diff hotspots
    ActiveSCIDs0 = maps:keys(ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, [])),
    ?assertEqual([ID1], ActiveSCIDs0),

    SendPacketFun = fun(#{public := PubKey, secret := PrivKey} = _Actor) ->
        PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
        SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
        Packet = blockchain_helium_packet_v1:new(
            lorawan,
            crypto:strong_rand_bytes(20),
            erlang:system_time(millisecond),
            -100.0,
            915.2,
            "SF8BW125",
            -12.0,
            {devaddr, 1207959553}
        ),
        Offer0 = blockchain_state_channel_offer_v1:from_packet(Packet, PubKeyBin, 'US915'),
        Offer1 = blockchain_state_channel_offer_v1:sign(Offer0, SigFun),
        RouterLedger = blockchain:ledger(RouterChain),
        ok = ct_rpc:call(RouterNode, blockchain_state_channels_server, handle_offer, [Offer1, sc_packet_test_handler, RouterLedger, Self])
    end,

    CreateActorAndSendPacketFun = fun(_Idx) ->
        Actor = libp2p_crypto:generate_keys(ecc_compact),
        ok = SendPacketFun(Actor),
        Actor
    end,

    %% Sending MaxActorsAllowed + 1 packets. Allowing a little time for second
    %% state channel to be opened after filling the first with actors.
    Actors0 = lists:map(CreateActorAndSendPacketFun, lists:seq(1, MaxActorsAllowed)),
    timer:sleep(100),
    Actors = Actors0 ++ [CreateActorAndSendPacketFun(101)],

    %% Checking that new SC ID is not old SC ID
    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCIDs1 = maps:keys(ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, [])),
        ct:pal("ActiveSCIDs1: ~p", [ActiveSCIDs1]),
        lists:sort([ID1, ID2]) == lists:sort(ActiveSCIDs1)
    end, 30, timer:seconds(1)),


    ActiveSCIDs = maps:keys(ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, [])),
    ct:pal("[~p:~p:~p] MARKER ~p~n", [?MODULE, ?FUNCTION_NAME, ?LINE, ActiveSCIDs]),
    ?assertEqual(2, erlang:length(ActiveSCIDs)),
    ?assertEqual(lists:sort([ID1, ID2]), lists:sort(ActiveSCIDs)),

    SCA1 = get_active_state_channel(RouterNode, ID1),
    SCB1 = get_active_state_channel(RouterNode, ID2),

    ?assertEqual(MaxActorsAllowed, erlang:length(blockchain_state_channel_v1:summaries(SCA1)), "first state channel is full"),
    ?assertEqual(1, erlang:length(blockchain_state_channel_v1:summaries(SCB1)), "second state channel has 1 actor"),

    ?assertEqual(MaxActorsAllowed, blockchain_state_channel_v1:total_packets(SCA1), "first state channel has 1 packet per summary"),
    ?assertEqual(1, blockchain_state_channel_v1:total_packets(SCB1), "second state channel has 1 packet"),

    % We are resending packets from same actor to make sure they still make it in there and in the right state channel
    lists:foreach(SendPacketFun, lists:reverse(Actors)),

    %% Make sure both state channels are still active
    ok = blockchain_ct_utils:wait_until(fun() ->
        ActiveSCIDs2 = maps:keys(ct_rpc:call(RouterNode, blockchain_state_channels_server, get_actives, [])),
        ct:pal("ActiveSCIDs2: ~p", [ActiveSCIDs2]),
        lists:sort([ID1, ID2]) == lists:sort(ActiveSCIDs2)
    end, 30, timer:seconds(1)),

    SCA2 = get_active_state_channel(RouterNode, ID1),
    SCB2 = get_active_state_channel(RouterNode, ID2),

    ?assertEqual(MaxActorsAllowed, erlang:length(blockchain_state_channel_v1:summaries(SCA2)), "first state channel is full"),
    ?assertEqual(1, erlang:length(blockchain_state_channel_v1:summaries(SCB2)), "second state channel has 1 actor"),

    ?assertEqual(MaxActorsAllowed*2, blockchain_state_channel_v1:total_packets(SCA2), "first state channel has 2 packets for every actor"),
    ?assertEqual(2, blockchain_state_channel_v1:total_packets(SCB2), "second state channel has 2 packet for 1 actor"),

    ok.

replay_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Adding block
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),

    %% Sending another packet
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 2),

    %% Add some fake blocks
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 17),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            true = check_sc_close(Txn, ID, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                             blockchain_helium_packet_v1:payload(Packet1)]),
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block1, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    %% Recreating the state channel open txn with the same nonce
    RouterChain2 = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterLedger2 = blockchain:ledger(RouterChain2),

    ct:pal("DCEntry: ~p", [ct_rpc:call(RouterNode, blockchain_ledger_v1, find_dc_entry, [RouterPubkeyBin, RouterLedger2])]),

    ReplayID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    ReplaySignedSCOpenTxn = create_sc_open_txn(RouterNode, ReplayID, ExpireWithin, 1, Nonce),
    ct:pal("ReplaySignedSCOpenTxn: ~p", [ReplaySignedSCOpenTxn]),

    {error, {invalid_txns, [{ReplaySignedSCOpenTxn, _InvalidReason}]}} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [ReplaySignedSCOpenTxn]]),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

multiple_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    ct:pal("RouterNode: ~p", [RouterNode]),

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),

    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 20,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Adding block
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block0: ~p", [Block0]),
    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Add some fake blocks
    FakeBlocks = 20,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 22),

    %% Adding close txn to blockchain
    receive
        {txn, Txn1} ->
            ?assertEqual(blockchain_txn_state_channel_close_v1, blockchain_txn:type(Txn1)),
            {ok, B1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn1]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B1, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Waiting for close txn to be mine
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 23),

    %% Create another state channel
    ID2 = crypto:strong_rand_bytes(24),
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, ExpireWithin, 1, Nonce + 1),

    {ok, Block2} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedSCOpenTxn2]]),
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block2, RouterChain, Self, RouterSwarm]),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 24),
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 24),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID2),

    %% Add some fake blocks
    FakeBlocks = 20,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 44),

    %% Adding close txn to blockchain
    receive
        {txn, Txn2} ->
            ?assertEqual(blockchain_txn_state_channel_close_v1, blockchain_txn:type(Txn2)),
            {ok, B2} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn2]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B2, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    ok = blockchain_ct_utils:wait_until_height(RouterNode, 45),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

multi_owner_multi_sc_test(Config) ->
    [RouterNode1, RouterNode2 | _] = ?config(nodes, Config),

    Dir = os:getenv("SC_DIR", ""),
    {ok, _} = ct_rpc:call(
        RouterNode2,
        lager,
        trace_file,
        [Dir ++ "sc_server2.log", [{module, blockchain_state_channels_server}], debug]
    ),
    {ok, _} = ct_rpc:call(
        RouterNode2,
        lager,
        trace_file,
        [Dir ++ "sc_server2.log", [{module, blockchain_state_channel_handler}], debug]
    ),
    {ok, _} = ct_rpc:call(
        RouterNode2,
        lager,
        trace_file,
        [Dir ++ "sc_server2.log", [{module, blockchain_state_channel_v1}], debug]
    ),
    {ok, _} = ct_rpc:call(
        RouterNode2,
        lager,
        trace_file,
        [Dir ++ "sc_server2.log", [{module, blockchain_state_channels_worker}], debug]
    ),
    {ok, _} = ct_rpc:call(
        RouterNode2,
        lager,
        trace_file,
        [Dir ++ "sc_server2.log", [{module, blockchain_state_channels_cache}], debug]
    ),

    ConsensusMembers = ?config(consensus_members, Config),

    RouterPubkeyBin1 = ct_rpc:call(RouterNode1, blockchain_swarm, pubkey_bin, []),
    RouterPubkeyBin2 = ct_rpc:call(RouterNode2, blockchain_swarm, pubkey_bin, []),

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode1, Self),
    ok = setup_meck_txn_forwarding(RouterNode2, Self),

    %% Create OUI txn for RouterNode1
    SignedOUITxn1 = create_oui_txn(1, RouterNode1, [{0, 0}], 8),

    %% Create 3 SCs for RouterNode1
    Expiry = 20,
    ID11 = crypto:strong_rand_bytes(24),
    ID12 = crypto:strong_rand_bytes(24),
    SignedSCOpenTxn11 = create_sc_open_txn(RouterNode1, ID11, Expiry, 1, 1),
    SignedSCOpenTxn12 = create_sc_open_txn(RouterNode1, ID12, Expiry, 1, 2),

    % Adding block with first set of txns
    Txns1 = [SignedOUITxn1,
             SignedSCOpenTxn11,
             SignedSCOpenTxn12],

    RouterChain1 = ct_rpc:call(RouterNode1, blockchain_worker, blockchain, []),
    RouterSwarm1 = ct_rpc:call(RouterNode1, blockchain_swarm, swarm, []),
    {ok, Block2} = ct_rpc:call(RouterNode1, test_utils, create_block, [ConsensusMembers, Txns1]),
    _ = ct_rpc:call(RouterNode1, blockchain_gossip_handler, add_block, [Block2, RouterChain1, Self, RouterSwarm1]),
    ct:pal("Block2: ~p", [Block2]),

    RouterPubkeyBin1 = ct_rpc:call(RouterNode1, blockchain_swarm, pubkey_bin, []),
    {ok, _RouterPubkey, RouterSigFun1, _} = ct_rpc:call(RouterNode1, blockchain_swarm, keys, []),
    RouterPubkeyBin2 = ct_rpc:call(RouterNode2, blockchain_swarm, pubkey_bin, []),
    RoutingTxn = blockchain_txn_routing_v1:update_router_addresses(1, RouterPubkeyBin1, [RouterPubkeyBin2], 1),
    SignedRoutingTxn = blockchain_txn_routing_v1:sign(RoutingTxn, RouterSigFun1),
    ct:pal("SignedRoutingTxn: ~p", [SignedRoutingTxn]),
    {ok, Block3} = add_block(RouterNode1, RouterChain1, ConsensusMembers, [SignedRoutingTxn]),
    ct:pal("Block3: ~p", [Block3]),
    ok = ct_rpc:call(RouterNode1, blockchain_gossip_handler, add_block, [Block3, RouterChain1, Self, RouterSwarm1]),

    %% Wait till the block is propagated
    ok = blockchain_ct_utils:wait_until_height(RouterNode1, 3),
    ok = blockchain_ct_utils:wait_until_height(RouterNode2, 3),

    %% Create OUI txn for RouterNode2
    SignedOUITxn2 = create_oui_txn(2, RouterNode2, [{0, 0}], 8),
    ID21 = crypto:strong_rand_bytes(24),
    ID22 = crypto:strong_rand_bytes(24),

    %% Create OUI txn for RouterNode2
    SignedSCOpenTxn21 = create_sc_open_txn(RouterNode2, ID21, Expiry, 1, 1),
    SignedSCOpenTxn22 = create_sc_open_txn(RouterNode2, ID22, Expiry, 1, 2),

    %% Create second set of txns
    Txns2 = [SignedOUITxn2,
             SignedSCOpenTxn21,
             SignedSCOpenTxn22],

    %% Adding block with second set of txns
    {ok, Block4} = ct_rpc:call(RouterNode1, test_utils, create_block, [ConsensusMembers, Txns2]),
    ct:pal("Block4: ~p", [Block4]),
    _ = ct_rpc:call(RouterNode1, blockchain_gossip_handler, add_block, [Block4, RouterChain1, Self, RouterSwarm1]),

    %% Wait till the block is propagated
    ok = blockchain_ct_utils:wait_until_height(RouterNode1, 4),
    ok = blockchain_ct_utils:wait_until_height(RouterNode2, 4),

    %% Checking that state channels got created properly
    RouterLedger1 = ct_rpc:call(RouterNode1, blockchain, ledger, [RouterChain1]),

    RouterChain4 = ct_rpc:call(RouterNode2, blockchain_worker, blockchain, []),
    RouterLedger2 = ct_rpc:call(RouterNode2, blockchain, ledger, [RouterChain4]),
    {ok, SC11} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_state_channel,
                             [ID11, RouterPubkeyBin1, RouterLedger1]),
    {ok, SC12} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_state_channel,
                             [ID12, RouterPubkeyBin1, RouterLedger1]),
    {ok, SC21} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_state_channel,
                             [ID21, RouterPubkeyBin2, RouterLedger2]),
    {ok, SC22} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_state_channel,
                             [ID22, RouterPubkeyBin2, RouterLedger2]),

    {ok, R1} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin1, RouterLedger1]),

    2 = maps:size(R1),
    true = lists:usort([ID11, ID12]) == lists:usort(maps:keys(R1)),

    {ok, R2} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin2, RouterLedger2]),

    2 = maps:size(R2),
    true = lists:usort([ID21, ID22]) == lists:usort(maps:keys(R2)),

    4 = length(lists:usort([SC11, SC12, SC21, SC22])),

    %% Check that the state channels being created are legit
    ?assertEqual(ID11, blockchain_ledger_state_channel_v2:id(SC11)),
    ?assertEqual(RouterPubkeyBin1, blockchain_ledger_state_channel_v2:owner(SC11)),
    ?assertEqual(ID12, blockchain_ledger_state_channel_v2:id(SC12)),
    ?assertEqual(RouterPubkeyBin1, blockchain_ledger_state_channel_v2:owner(SC12)),
    ?assertEqual(ID21, blockchain_ledger_state_channel_v2:id(SC21)),
    ?assertEqual(RouterPubkeyBin2, blockchain_ledger_state_channel_v2:owner(SC21)),
    ?assertEqual(ID22, blockchain_ledger_state_channel_v2:id(SC22)),
    ?assertEqual(RouterPubkeyBin2, blockchain_ledger_state_channel_v2:owner(SC22)),

    %% Add 20 more blocks to get the state channel to expire
    FakeBlocks = 20,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode1, RouterSwarm1, RouterChain1, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode1, 24),
    ok = blockchain_ct_utils:wait_until_height(RouterNode2, 24),

    %% At this point, we know that Block2's sc open txns must have expired
    %% So we do the dumbest possible thing and match each one
    %% Checking that the IDs are atleast coherent
    ok = check_all_closed([ID11, ID12]),

    %% Add 3 more blocks to trigger sc close for sc open in Block3
    MoreFakeBlocks = 3,
    ok = add_and_gossip_fake_blocks(MoreFakeBlocks, ConsensusMembers, RouterNode1, RouterSwarm1, RouterChain1, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode1, 27),
    ok = blockchain_ct_utils:wait_until_height(RouterNode2, 27),

    {ok, R11} = ct_rpc:call(RouterNode1, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin1, RouterLedger1]),

    2 = maps:size(R11),

    {ok, R22} = ct_rpc:call(RouterNode2, blockchain_ledger_v1, find_scs_by_owner,
                             [RouterPubkeyBin2, RouterLedger2]),

    2 = maps:size(R22),

    %% And the related sc_close for sc_open in Block3 must have fired
    ok = check_all_closed([ID21, ID22]),
    ok.

multi_active_sc_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Forward this process's submit_txn to meck_test_util which
    %% sends this process a msg reply back which we later handle
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 45,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Adding block
    {ok, Block2} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block2: ~p", [Block2]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block2),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block2, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    %% HEIGHT MARKER -> 2
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),

    %% Add some fake blocks
    FakeBlocks = 20,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 22),

    %% HEIGHT MARKER -> 22

    %% Open another state channel while the previous one is still active
    ID2 = crypto:strong_rand_bytes(24),
    ExpireWithin2 = 90,
    Nonce2 = 2,
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, ExpireWithin2, 1, Nonce2),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),

    %% Adding block
    {ok, Block23} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedSCOpenTxn2]),
    ct:pal("Block23: ~p", [Block23]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash1 = blockchain_block:hash_block(Block23),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block23, RouterChain, Self, RouterSwarm]),

    %% HEIGHT MARKER -> 23
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 23),

    %% At this point both the state channels are open, check
    ?assertEqual(2, maps:size(ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, []))),

    %% Sending 1 packet, this should use the previously opened state channel
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),

    ok = expect_nonce_for_state_channel(RouterNode, ID, 2),

    %% Add more fake blocks so that the first state_channel expires
    MoreFakeBlocks = 25,
    ok = add_and_gossip_fake_blocks(MoreFakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),

    %% HEIGHT MARKER -> 48
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 48),

    %% At this point the first state channel must have expired
    ok = blockchain_ct_utils:wait_until(fun() ->
        SCs = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, []),
        {SC, SCState, Pid} = maps:get(ID, SCs),
        SC =/= undefined andalso SCState == expired andalso ct_rpc:call(RouterNode, erlang, is_process_alive, [Pid]) == false
    end, 10, timer:seconds(1)),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            ct:pal("Txn: ~p", [Txn]),
            true = check_sc_close(Txn, ID, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                             blockchain_helium_packet_v1:payload(Packet1)]),
            {ok, Block49} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block49, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% HEIGHT MARKER -> 49
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 49),

    %% Check that it's gone from the sc server
    ok = blockchain_ct_utils:wait_until(fun() ->
        R = ct_rpc:call(RouterNode, blockchain_state_channels_server, get_all, []),
        [ID2] == maps:keys(R)
    end, 10, timer:seconds(1)),
    
    %% Wait 1 sec before sending more packets
    ok = timer:sleep(timer:seconds(1)),

    %% Send more packets, this should use the newly active state channel
    DevNonce2 = crypto:strong_rand_bytes(2),
    Packet2 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce2, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet2, [], 'US915']),

    ok = expect_nonce_for_state_channel(RouterNode, ID2, 1),

    DevNonce3 = crypto:strong_rand_bytes(2),
    Packet3 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce3, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet3, [], 'US915']),

    ok = expect_nonce_for_state_channel(RouterNode, ID2, 2),

    %% Add more fake blocks to get the second sc to expire
    EvenMoreFakeBlocks = 65,
    ok = add_and_gossip_fake_blocks(EvenMoreFakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 114),

    %% Adding close txn to blockchain
    receive
        {txn, Txn2} ->
            ct:pal("Txn2: ~p", [Txn2]),
            true = check_sc_close(Txn2, ID2, SCOpenBlockHash1, [blockchain_helium_packet_v1:payload(Packet2),
                                                                blockchain_helium_packet_v1:payload(Packet3)]),
            {ok, Block150} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn2]]),
            _ = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block150, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

open_without_oui_test(Config) ->
    [RouterNode |_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),

    %% Create state channel open txn without any oui
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Adding block
    {error, {invalid_txns, [{SignedSCOpenTxn, _InvalidReason}]}} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedSCOpenTxn]),

    ok.

max_scs_open_test(Config) ->
    [RouterNode |_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    ExpireWithin = 11,

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(24),
    Nonce1 = 1,
    SignedSCOpenTxn1 = create_sc_open_txn(RouterNode, ID1, ExpireWithin, 1, Nonce1),
    ct:pal("SignedSCOpenTxn1: ~p", [SignedSCOpenTxn1]),

    %% Create state channel open txn
    ID2 = crypto:strong_rand_bytes(24),
    Nonce2 = 2,
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, ExpireWithin, 1, Nonce2),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),

    %% Create state channel open txn
    ID3 = crypto:strong_rand_bytes(24),
    Nonce3 = 3,
    SignedSCOpenTxn3 = create_sc_open_txn(RouterNode, ID3, ExpireWithin, 1, Nonce3),
    ct:pal("SignedSCOpenTxn3: ~p", [SignedSCOpenTxn3]),

    %% Adding block
    {error, {invalid_txns, _}} = add_block(RouterNode,
                                            RouterChain,
                                            ConsensusMembers,
                                            [SignedOUITxn,
                                             SignedSCOpenTxn1,
                                             SignedSCOpenTxn2,
                                             SignedSCOpenTxn3]),

    ok.

max_scs_open_v2_test(Config) ->
    [RouterNode |_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    {ok, RouterPubkey, _RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    RouterLedger = blockchain:ledger(RouterChain),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(24),
    Nonce1 = 1,
    SignedSCOpenTxn1 = create_sc_open_txn(RouterNode, ID1, 12, 1, Nonce1),
    ct:pal("SignedSCOpenTxn1: ~p", [SignedSCOpenTxn1]),

    %% Create state channel open txn
    ID2 = crypto:strong_rand_bytes(24),
    Nonce2 = 2,
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, 20, 1, Nonce2),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),

    %% Adding block with state channels
    {ok, B2} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn1, SignedSCOpenTxn2]),
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B2, RouterChain, Self, RouterSwarm]),

    ok = blockchain_ct_utils:wait_until_height(RouterNode, 2),

    OpenSCCountForOwner0 = ct_rpc:call(RouterNode, blockchain_ledger_v1, count_open_scs_for_owner, [[ID1, ID2], RouterPubkeyBin, RouterLedger]),
    ?assertEqual(2, OpenSCCountForOwner0),

    %% Wait for first sc to expire
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 17),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            {ok, B18} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B18, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    %% Check new function, we should only have 1 SC open now
    OpenSCCountForOwner1 = ct_rpc:call(RouterNode, blockchain_ledger_v1, count_open_scs_for_owner, [[ID1, ID2], RouterPubkeyBin, RouterLedger]),
    ?assertEqual(1, OpenSCCountForOwner1),

    %% Try to add anohter SC (it should fail, the var is not in)
    ID3 = crypto:strong_rand_bytes(24),
    Nonce3 = 3,
    SignedSCOpenTxn3 = create_sc_open_txn(RouterNode, ID3, 20, 1, Nonce3),
    ct:pal("SignedSCOpenTxn3: ~p", [SignedSCOpenTxn3]),
    {error, {invalid_txns, _}} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedSCOpenTxn3]]),

    %% Add sc_only_count_open_active var=true
    {Priv, _Pub} = ?config(master_key, Config),
    Vars = #{sc_only_count_open_active => true},
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    SignedVarTxn = blockchain_txn_vars_v1:proof(VarTxn, Proof),
    {ok, B19} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedVarTxn]]),
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B19, RouterChain, Self, RouterSwarm]),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 19),

    %% Pass var delay
    ok = add_and_gossip_fake_blocks(11, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 30),

    %% Make sure var is set
    SCOnlyCountOpenActive = ct_rpc:call(RouterNode, blockchain, config, [sc_only_count_open_active, RouterLedger]),
    ?assertEqual({ok, true}, SCOnlyCountOpenActive),

    %% Make sure we can open another SC now
    {ok, _Block31} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [SignedSCOpenTxn3]]),
    ok.

sc_dispute_prevention_test(Config) ->
    [RouterNode, GatewayNode1, GatewayNode2 |_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% NOTE: sc_dispute_strategy_version chain var is toggled for this test in init_per_test_case/2

    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),
    ok = setup_meck_txn_forwarding(GatewayNode1, Self),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterLedger = blockchain:ledger(RouterChain),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),

    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),

    {ok, Gateway1Pubkey, Gateway1SigFun, _} = ct_rpc:call(GatewayNode1, blockchain_swarm, keys, []),
    Gateway1PubkeyBin = libp2p_crypto:pubkey_to_bin(Gateway1Pubkey),

    {ok, Gateway2Pubkey, Gateway2SigFun, _} = ct_rpc:call(GatewayNode2, blockchain_swarm, keys, []),
    Gateway2PubkeyBin = libp2p_crypto:pubkey_to_bin(Gateway2Pubkey),

    ct:pal("Pubkeys: ~n~p",
           [[
             {routernode, RouterPubkeyBin},
             {gateway_1, Gateway1PubkeyBin},
             {gateway_2, Gateway2PubkeyBin}
            ]]),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% ===================================================================
    %% - open state channel

    ID1 = crypto:strong_rand_bytes(24),
    Nonce1 = 1,
    SignedSCOpenTxn1 = create_sc_open_txn(RouterNode, ID1, 12, 1, Nonce1, 99),

    %% Adding block with state channels
    {ok, B2} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn1]),
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B2, RouterChain, Self, RouterSwarm]),

    ok = blockchain_ct_utils:wait_until_height(RouterNode, 2),

    %% sanity check
    OpenSCCountForOwner0 = ct_rpc:call(RouterNode, blockchain_ledger_v1, count_open_scs_for_owner, [[ID1], RouterPubkeyBin, RouterLedger]),
    ?assertEqual(1, OpenSCCountForOwner0),


    %% Helpers
    AddFakeBlocksFn =
        fun(NumBlocks, ExpectedBlock, Nodes) ->
                ok = add_and_gossip_fake_blocks(NumBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
                lists:foreach(fun(Node) ->
                                      ok = blockchain_ct_utils:wait_until_height(Node, ExpectedBlock)
                              end, Nodes)
                end,

    SendPacketsFn = fun(NumPackets, Gateway) ->
                         lists:foreach(
                           fun(_) ->
                                   DevNonce0 = crypto:strong_rand_bytes(2),
                                   Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
                                   ok = ct_rpc:call(Gateway, blockchain_state_channels_client, packet, [Packet0, [], 'US915'])
                           end,
                           lists:seq(1, NumPackets)
                          )
                 end,

    %% Wait until Gateways have gotten blocks with OUI txn to send packets
    AddFakeBlocksFn(3, 5, [RouterNode, GatewayNode1]),

    %% ===================================================================
    %% Sending 10 packet from first gateway
    SendPacketsFn(20, GatewayNode1),
    AddFakeBlocksFn(1, 6, [RouterNode, GatewayNode1]),

    %% Send packets from another gateway
    %% Gateway2 needs to be involved state channel to dispute
    SendPacketsFn(20, GatewayNode2),
    AddFakeBlocksFn(1, 7, [RouterNode, GatewayNode1, GatewayNode2]),

    %% ===================================================================
    %% Wait until we can get a state channel with both summaries
    %% Failures to dial during this test can cause failures here
    ok = test_utils:wait_until(
           fun() ->
                   case get_active_state_channel(RouterNode, ID1) of
                       worker_not_started -> {false, worker_not_started};
                       SC ->
                           case length(blockchain_state_channel_v1:summaries(SC)) of
                               2 -> true;
                               C -> {false, summary_count, C}
                           end
                   end
           end, 100, 100),

    SC0 = get_active_state_channel(RouterNode, ID1),
    ct:pal("Routernode SC: ~p", [lager:pr(SC0, blockchain_state_channel_v1)]),

    %% ===================================================================
    %% Let the state channel expire and add to the chain
    AddFakeBlocksFn(8, 15, [RouterNode]),

    %% Adding the close txn to the chain
    receive
        {txn, Txn} ->
            %% routernode closing the state channel
            {ok, B18} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn], #{}, false]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B18, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("close txn timeout")
    end,

    %% ===================================================================
    %% We've added the state channel to the chain. Rewards should be able to be
    %% determined, and there should be some for the gateways involved.

    ok = blockchain_ct_utils:wait_until_height(RouterNode, 16),

    %% REVIEW: How can I assert something here about the rewards?
    %% Nothing has been disputed yet.
    %{ok, Rewards1} = ct_rpc:call(RouterNode, blockchain_txn_rewards_v2, calculate_rewards_metadata, [5, 16, RouterChain]),
    %?assertNotEqual(#{}, maps:get(dc_rewards, Rewards1)),
    %ct:pal("PubkeyBins: ~n~p", [[{routernode, RouterPubkeyBin}, {gateway_1, Gateway1PubkeyBin}, {gateway_2, Gateway2PubkeyBin}]]),
    %ct:pal("potential Rewards: ~p", [lager:pr(Rewards1, blockchain_txn_rewards_v2)]),

    %% ===================================================================
    %% Make two disputes that are both valid before they are submitted

    {SC1, true} = blockchain_state_channel_v1:update_summary_for(
            Gateway2PubkeyBin,
            blockchain_state_channel_summary_v1:new(Gateway2PubkeyBin, 11, 22),
            SC0,
            90),

    SignedSC1 = blockchain_state_channel_v1:sign(SC1, RouterSigFun),
    Dispute1 = blockchain_txn_state_channel_close_v1:new(SC0, SignedSC1, Gateway1PubkeyBin),
    SignedTxn1 = blockchain_txn_state_channel_close_v1:sign(Dispute1, Gateway1SigFun),

    %% ----
    {SC2, true} = blockchain_state_channel_v1:update_summary_for(
            Gateway2PubkeyBin,
            blockchain_state_channel_summary_v1:new(Gateway2PubkeyBin, 22, 33),
            SC0,
            90),

    SignedSC2 = blockchain_state_channel_v1:sign(SC2, RouterSigFun),
    Dispute2 = blockchain_txn_state_channel_close_v1:new(SC0, SignedSC2, Gateway2PubkeyBin),
    SignedTxn2 = blockchain_txn_state_channel_close_v1:sign(Dispute2, Gateway2SigFun),

    Res1 = ct_rpc:call(RouterNode, blockchain_txn_state_channel_close_v1, is_valid, [SignedTxn1, RouterChain]),
    ?assertEqual(ok, Res1, "Our first dispute close is valid"),

    Res2 = ct_rpc:call(RouterNode, blockchain_txn_state_channel_close_v1, is_valid, [SignedTxn2, RouterChain]),
    ?assertEqual(ok, Res2, "Our second dispute close is valid"),

    %% Should not be able to create a block with more than 1 dispute
    {error, {invalid_txns, [_]}} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedTxn1, SignedTxn2]),

    %% ===================================================================
    %% Submit one fo the close txns to put SC0 in dispute
    {ok, B3} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedTxn1]),
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B3, RouterChain, Self, RouterSwarm]),

    %% wait until this block has made it everywhere
    AddFakeBlocksFn(1, 18, [RouterNode, GatewayNode1, GatewayNode1]),

    %% Check that the state that was first closed by routernode, is in dispute
    {ok, LedgerSC} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID1, RouterPubkeyBin, RouterLedger]),
    ct:pal("Ledger SC: ~p", [lager:pr(LedgerSC, ledger_state_channel_v2)]),
    ?assertEqual(dispute, blockchain_ledger_state_channel_v2:close_state(LedgerSC)),

    %% ===================================================================
    %% The unsubmitted close dispute is no longer valid
    Res3 = ct_rpc:call(RouterNode, blockchain_txn_state_channel_close_v1, is_valid, [SignedTxn2, RouterChain]),
    ct:pal("Trying to create block with bad txn: ~p", [Res3]),
    ?assertEqual({error, already_disputed}, Res3, "Our second dispute close is not valid"),

    %% ===================================================================
    %% Move past the grace period and check that the no rewards are generated
    AddFakeBlocksFn(2, 20, [RouterNode, GatewayNode1, GatewayNode2]),
    {ok, Rewards2} = ct_rpc:call(RouterNode, blockchain_txn_rewards_v2, calculate_rewards_metadata, [5, 20, RouterChain]),
    %% there should be no rewards here
    ?assertEqual(#{}, maps:get(dc_rewards, Rewards2)),
    ct:pal("PubkeyBins: ~n~p", [[{routernode, RouterPubkeyBin}, {gateway_1, Gateway1PubkeyBin}, {gateway_2, Gateway2PubkeyBin}]]),
    ct:pal("disputed Rewards: ~p", [lager:pr(Rewards2, blockchain_txn_rewards_v2)]),


    ok.


oui_not_found_test(Config) ->
    [RouterNode |_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(24),
    ExpireWithin = 11,
    Nonce1 = 1,

    SignedSCOpenTxn1 = create_sc_open_txn(RouterNode, ID1, ExpireWithin, 2, Nonce1),
    ct:pal("SignedSCOpenTxn1: ~p", [SignedSCOpenTxn1]),

    %% Adding block
    {error, {invalid_txns, _}} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn1]),

    ok.

unknown_owner_test(Config) ->
    [RouterNode, PayerNode |_] = ?config(nodes, Config),
    Self = self(),

    ConsensusMembers = ?config(consensus_members, Config),
    ct:pal("ConsensusMembers: ~p", [ConsensusMembers]),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    {ok, PayerPubkey, _, _} = ct_rpc:call(PayerNode, blockchain_swarm, keys, []),
    PayerPubkeyBin = libp2p_crypto:pubkey_to_bin(PayerPubkey),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    {ok, B0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn]),
    ct:pal("B0: ~p", [B0]),
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B0, RouterChain, Self, RouterSwarm]),

    RoutingTxn = blockchain_txn_routing_v1:update_router_addresses(1, RouterPubkeyBin, [PayerPubkeyBin], 1),
    ct:pal("RoutingTxn: ~p", [RoutingTxn]),
    SignedRoutingTxn = blockchain_txn_routing_v1:sign(RoutingTxn, RouterSigFun),
    ct:pal("SignedRoutingTxn: ~p", [SignedRoutingTxn]),

    {ok, B1} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedRoutingTxn]),
    ct:pal("B1: ~p", [B1]),
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [B1, RouterChain, Self, RouterSwarm]),

    Ledger = ct_rpc:call(RouterNode, blockchain, ledger, [RouterChain]),
    ct:pal("Ledger: ~p", [Ledger]),
    ct:pal("Routing: ~p", [ct_rpc:call(RouterNode, blockchain_ledger_v1, find_routing, [1, Ledger])]),

    %% Create state channel open txn
    ID1 = crypto:strong_rand_bytes(24),
    ExpireWithin = 11,
    Nonce1 = 1,
    SignedSCOpenTxn1 = create_sc_open_txn(RouterNode, ID1, ExpireWithin, 1, Nonce1),
    ct:pal("SignedSCOpenTxn1: ~p", [SignedSCOpenTxn1]),

    %% Adding block
    {error, {invalid_txns, _}} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedSCOpenTxn1]),

    ok.

crash_single_sc_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% look at sc server before sending the packet
    {_, _, _} = debug(RouterNode),

    %% wait a sec before sending a packet
    timer:sleep(timer:seconds(1)),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ct:pal("Packet0: ~p", [blockchain_utils:bin_to_hex(blockchain_helium_packet_v1:encode(Packet0))]),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),

    %% Check stuff before crash =====================================
    ct:pal("********** before crash *****************"),
    {P0, _, _} = debug(RouterNode),

    %% Crash =====================================
    ok = blockchain_ct_utils:wait_until(fun() ->
        Crash = ct_rpc:call(RouterNode, erlang, exit, [P0, kill]),
        P = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channels_server]),
        ct:pal("Crashing sc server: ~p, result: ~p", [P, Crash]),
        P /= P0 andalso Crash == true andalso P /= undefined
    end, 30, timer:seconds(1)),

    %% Wait a couple seconds for the state of the sc_server to update?
    timer:sleep(timer:seconds(2)),

    %% Check server after crash =====================================
    ct:pal("********** after crash *****************"),
    {_, _, _} = debug(RouterNode),

    %% Sending another packet
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),
    ct:pal("Packet1: ~p", [blockchain_utils:bin_to_hex(blockchain_helium_packet_v1:encode(Packet1))]),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 2),

    %% Adding 30 fake blocks to get the state channel to expire
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 17),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            true = check_sc_close(Txn, ID, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                             blockchain_helium_packet_v1:payload(Packet1)]),
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block1, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

crash_multi_sc_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create first state channel open txn
    ID1 = crypto:strong_rand_bytes(24),
    ExpireWithin1 = 11,
    Nonce1 = 1,
    SignedSCOpenTxn1 = create_sc_open_txn(RouterNode, ID1, ExpireWithin1, 1, Nonce1),
    ct:pal("SignedSCOpenTxn1: ~p", [SignedSCOpenTxn1]),

    %% Create second state channel open txn
    ID2 = crypto:strong_rand_bytes(24),
    ExpireWithin2 = 21,
    Nonce2 = 2,
    SignedSCOpenTxn2 = create_sc_open_txn(RouterNode, ID2, ExpireWithin2, 1, Nonce2),
    ct:pal("SignedSCOpenTxn2: ~p", [SignedSCOpenTxn2]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn1, SignedSCOpenTxn2]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID1),
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID2),

    %% Activate second state channel on channels server
    _ = ct_rpc:call(RouterNode, erlang, send, [blockchain_state_channels_server, get_new_active]),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID1, 0),
    ok = expect_nonce_for_state_channel(RouterNode, ID2, 0),

    %% look at sc server before sending the packet
    {_, _, _} = debug(RouterNode),

    %% wait a sec before sending a packet
    timer:sleep(timer:seconds(1)),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ct:pal("Packet0: ~p", [blockchain_utils:bin_to_hex(blockchain_helium_packet_v1:encode(Packet0))]),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        case {get_nonce_for_state_channel(RouterNode, ID1), get_nonce_for_state_channel(RouterNode, ID2)} of
            {0, 1} -> true;
            {1, 0} -> true;
            _Nonce -> {incorrect_nonce, _Nonce}
        end
    end, 30, timer:seconds(1)),

    %% Check stuff before crash =====================================
    ct:pal("********** before crash *****************"),
    {P0, _, _} = debug(RouterNode),

    %% Crash =====================================
    ok = blockchain_ct_utils:wait_until(fun() ->
        Crash = ct_rpc:call(RouterNode, erlang, exit, [P0, kill]),
        P = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channels_server]),
        ct:pal("Crashing sc server: ~p, result: ~p", [P, Crash]),
        P /= P0 andalso Crash == true andalso P /= undefined
    end, 30, timer:seconds(1)),

    %% Wait a couple seconds for the state of the sc_server to update?
    timer:sleep(timer:seconds(2)),

    %% Check server after crash =====================================
    ct:pal("********** after crash *****************"),
    {_, _, _} = debug(RouterNode),

    %% Make sure we have 2 active state channels. The 2nd won't have opened
    %% because there was no packets in it.
    _ = ct_rpc:call(RouterNode, erlang, send, [blockchain_state_channels_server, get_new_active]),

    %% Sending another packet
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),
    ct:pal("Packet1: ~p", [blockchain_utils:bin_to_hex(blockchain_helium_packet_v1:encode(Packet1))]),

    %% Checking state channel on server/client
    ok = blockchain_ct_utils:wait_until(fun() ->
        case {get_nonce_for_state_channel(RouterNode, ID1), get_nonce_for_state_channel(RouterNode, ID2)} of
            {_, 2} -> true;
            {2, _} -> true;
            _Nonce -> {incorrect_nonce, _Nonce}
        end
    end, 30, timer:seconds(1)),

    %% Get the Nonce of the state channels before we expire the first, so we can
    %% use it to determine which close txns we should be expecting.
    NonceSC1 = get_nonce_for_state_channel(RouterNode, ID1),
    NonceSC2 = get_nonce_for_state_channel(RouterNode, ID2),

    %% Adding 30 fake blocks to get the first state channel to expire
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 17),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            case NonceSC1 of
                0 ->
                    %% This guy did nothing
                    ok;
                1 ->
                    %% It sent one packet
                    true = check_sc_close(Txn, ID1, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0)]);
                2 ->
                    %% It sent both packets
                    true = check_sc_close(Txn, ID1, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                                      blockchain_helium_packet_v1:payload(Packet1)])
            end,

            {ok, Block23} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block23, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    %% Adding 20 more fake blocks to get the second state channels to expire
    MoreFakeBlocks = 8,
    ok = add_and_gossip_fake_blocks(MoreFakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 26),

    %% Adding close txn to blockchain
    receive
        {txn, Txn2} ->
            case NonceSC2 of
                0 ->
                    %% This guy did nothing
                    ok;
                1 ->
                    %% It sent one packet
                    true = check_sc_close(Txn2, ID2, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet1)]);
                2 ->
                    %% It sent both packets
                    true = check_sc_close(Txn2, ID2, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                                       blockchain_helium_packet_v1:payload(Packet1)])
            end,
            {ok, Block44} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn2]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block44, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 27),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

crash_sc_sup_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),

    %% =====================================
    SupPid = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channel_sup]),
    ct:pal("Before crash sc_sup pid: ~p", [SupPid]),

    P0 = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channels_server]),

    %% crash blockchain_state_channels_server twice within five seconds
    ok = blockchain_ct_utils:wait_until(fun() ->
        Crash1 = ct_rpc:call(RouterNode, erlang, exit, [P0, kill]),
        P_1 = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channels_server]),
        ct:pal("First sc_server crash: ~p, pid before: ~p, after: ~p", [Crash1, P0, P_1]),
        P_1 /= P0 andalso Crash1 == true andalso P_1 /= undefined
    end, 2, timer:seconds(1)),

    P1 = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channels_server]),

    ok = blockchain_ct_utils:wait_until(fun() ->
        Crash2 = ct_rpc:call(RouterNode, erlang, exit, [P1, kill]),
        P_2 = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channels_server]),
        ct:pal("Second sc_server crash: ~p pid before: ~p, after: ~p", [Crash2, P1, P_2]),
        SecondCrashSupPid = ct_rpc:call(RouterNode, erlang, whereis, [blockchain_state_channel_sup]),
        ct:pal("Second crash sc sup pid: ~p", [SecondCrashSupPid]),
        P_2 /= P1 andalso SecondCrashSupPid /= undefined andalso SecondCrashSupPid /= SupPid andalso Crash2 == true andalso P_2 /= undefined
    end, 2, timer:seconds(1)),
    %% =====================================

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),

    %% Sending another packet
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 2),

    %% Adding 20 fake blocks to get the state channel to expire
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 17),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            true = check_sc_close(Txn, ID, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                             blockchain_helium_packet_v1:payload(Packet1)]),
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block1, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),
    ok.

hotspot_in_router_oui_test(Config) ->
    [RouterNode, GatewayNode1|_] = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),
    GatewayPubkeyBin = ct_rpc:call(GatewayNode1, blockchain_swarm, pubkey_bin, []),
    {ok, _RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    OUI = 1,
    SignedOUITxn = create_oui_txn(OUI, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    ct:pal("ConsensusMembers: ~p", [ConsensusMembers]),
    RouterLedger0 = ct_rpc:call(RouterNode, blockchain, ledger, [RouterChain]),
    {ok, GwInfo0} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_gateway_info, [GatewayPubkeyBin, RouterLedger0]),
    OwnerAddress = ct_rpc:call(RouterNode, blockchain_ledger_gateway_v2, owner_address, [GwInfo0]),
    {_, _, OwnerSigFun} = lists:keyfind(OwnerAddress, 1, ConsensusMembers),
    SignedUpdateGatewayOUITxn = create_update_gateway_oui_txn(GatewayNode1, OUI, 1, OwnerSigFun, RouterSigFun),
    ct:pal("SignedUpdateGatewayOUITxn: ~p", [SignedUpdateGatewayOUITxn]),

    %% Add block with oui and update_gateway_oui txns
    {ok, Block2} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedUpdateGatewayOUITxn]),
    ct:pal("Block2: ~p", [Block2]),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block2, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Check that hotspot is in the same oui as the router
    GatewayNode1Chain = ct_rpc:call(GatewayNode1, blockchain_worker, blockchain, []),
    GatewayNodeLedger = ct_rpc:call(GatewayNode1, blockchain, ledger, [GatewayNode1Chain]),
    {ok, GwInfo} = ct_rpc:call(GatewayNode1, blockchain_ledger_v1, find_gateway_info, [GatewayPubkeyBin,
                                                                                       GatewayNodeLedger]),
    GwOUI = ct_rpc:call(GatewayNode1, blockchain_ledger_gateway_v2, oui, [GwInfo]),
    ?assertEqual(OUI, GwOUI),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Add block with oui and sc open txns
    {ok, Block3} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedSCOpenTxn]),
    ct:pal("Block3: ~p", [Block3]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block3),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block3, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 3),

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending 1 packet
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending another packet
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, [], 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Adding 20 fake blocks to get the state channel to expire
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            true = check_sc_close(Txn, ID, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                             blockchain_helium_packet_v1:payload(Packet1)]),
            {ok, Block4} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block4, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 19),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),

    ok.

default_routers_test(Config) ->
    [RouterNode, GatewayNode1|_] = Nodes = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    %% Get router chain, swarm and pubkey_bin
    RouterChain = ct_rpc:call(RouterNode, blockchain_worker, blockchain, []),
    RouterSwarm = ct_rpc:call(RouterNode, blockchain_swarm, swarm, []),
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Check that the meck txn forwarding works
    Self = self(),
    ok = setup_meck_txn_forwarding(RouterNode, Self),

    %% Create OUI txn
    SignedOUITxn = create_oui_txn(1, RouterNode, [{0, 0}], 8),
    ct:pal("SignedOUITxn: ~p", [SignedOUITxn]),

    %% Create state channel open txn
    ID = crypto:strong_rand_bytes(32),
    ExpireWithin = 11,
    Nonce = 1,
    SignedSCOpenTxn = create_sc_open_txn(RouterNode, ID, ExpireWithin, 1, Nonce),
    ct:pal("SignedSCOpenTxn: ~p", [SignedSCOpenTxn]),

    %% Add block with oui and sc open txns
    {ok, Block0} = add_block(RouterNode, RouterChain, ConsensusMembers, [SignedOUITxn, SignedSCOpenTxn]),
    ct:pal("Block0: ~p", [Block0]),

    %% Get sc open block hash for verification later
    SCOpenBlockHash = blockchain_block:hash_block(Block0),

    %% Fake gossip block
    ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block0, RouterChain, Self, RouterSwarm]),

    %% Wait till the block is gossiped
    ok = blockchain_ct_utils:wait_until_height(GatewayNode1, 2),

    %% Set app env to use default routers on all nodes
    _ = blockchain_ct_utils:pmap(
          fun(Node) ->
                  ct_rpc:call(Node, application, set_env, [blockchain, use_oui_routers, false])
          end, Nodes),
    timer:sleep(timer:seconds(1)),

    %% Include the router in default routers list
    DefaultRouters = [libp2p_crypto:pubkey_bin_to_p2p(RouterPubkeyBin)],

    %% Checking that state channel got created properly
    true = check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID),

    %% Check that the nonce of the sc server is okay
    ok = expect_nonce_for_state_channel(RouterNode, ID, 0),

    %% Sending first packet and routing using the default routers
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet0, DefaultRouters, 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 1),

    %% Sending another packet
    DevNonce1 = crypto:strong_rand_bytes(2),
    Packet1 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce1, 0.0),
    ok = ct_rpc:call(GatewayNode1, blockchain_state_channels_client, packet, [Packet1, DefaultRouters, 'US915']),

    %% Checking state channel on server/client
    ok = expect_nonce_for_state_channel(RouterNode, ID, 2),

    %% Adding 20 fake blocks to get the state channel to expire
    FakeBlocks = 15,
    ok = add_and_gossip_fake_blocks(FakeBlocks, ConsensusMembers, RouterNode, RouterSwarm, RouterChain, Self),
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 17),

    %% Adding close txn to blockchain
    receive
        {txn, Txn} ->
            true = check_sc_close(Txn, ID, SCOpenBlockHash, [blockchain_helium_packet_v1:payload(Packet0),
                                                             blockchain_helium_packet_v1:payload(Packet1)]),
            {ok, Block1} = ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, [Txn]]),
            ok = ct_rpc:call(RouterNode, blockchain_gossip_handler, add_block, [Block1, RouterChain, Self, RouterSwarm])
    after 10000 ->
        ct:fail("txn timeout")
    end,

    %% Wait for close txn to appear
    ok = blockchain_ct_utils:wait_until_height(RouterNode, 18),

    ok = ct_rpc:call(RouterNode, meck, unload, [blockchain_txn_mgr]),

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


create_oui_txn(OUI, RouterNode, [], SubnetSize) ->
    create_oui_txn(OUI, RouterNode, [{16#deadbeef, 16#deadc0de}], SubnetSize);
create_oui_txn(OUI, RouterNode, EUIs, SubnetSize) ->
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    {Filter, _} = xor16:to_bin(xor16:new([ <<DevEUI:64/integer-unsigned-little, AppEUI:64/integer-unsigned-little>> || {DevEUI, AppEUI} <- EUIs], fun xxhash:hash64/1)),
    OUITxn = blockchain_txn_oui_v1:new(OUI, RouterPubkeyBin, [RouterPubkeyBin], Filter, SubnetSize),
    blockchain_txn_oui_v1:sign(OUITxn, RouterSigFun).

create_sc_open_txn(RouterNode, ID, Expiry, OUI, Nonce) ->
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, Expiry, OUI, Nonce, 20),
    blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun).

create_sc_open_txn(RouterNode, ID, Expiry, OUI, Nonce, Amount) ->
    {ok, RouterPubkey, RouterSigFun, _} = ct_rpc:call(RouterNode, blockchain_swarm, keys, []),
    RouterPubkeyBin = libp2p_crypto:pubkey_to_bin(RouterPubkey),
    SCOpenTxn = blockchain_txn_state_channel_open_v1:new(ID, RouterPubkeyBin, Expiry, OUI, Nonce, Amount),
    blockchain_txn_state_channel_open_v1:sign(SCOpenTxn, RouterSigFun).

create_update_gateway_oui_txn(GatewayNode, OUI, Nonce, OwnerSigFun, RouterSigFun) ->
    {ok, GatewayPubkey, _, _} = ct_rpc:call(GatewayNode, blockchain_swarm, keys, []),
    GatewayPubkeyBin = libp2p_crypto:pubkey_to_bin(GatewayPubkey),
    UpdateGatewayOUITxn = blockchain_txn_update_gateway_oui_v1:new(GatewayPubkeyBin, OUI, Nonce),

    STx0 = blockchain_txn_update_gateway_oui_v1:gateway_owner_sign(UpdateGatewayOUITxn, OwnerSigFun),
    blockchain_txn_update_gateway_oui_v1:oui_owner_sign(STx0, RouterSigFun).

check_all_closed([]) ->
    ok;
check_all_closed(IDs) ->
    receive
        {txn, Txn} ->
            check_all_closed([ ID || ID <- IDs, ID /= blockchain_state_channel_v1:id(blockchain_txn_state_channel_close_v1:state_channel(Txn)) ])
    after 1000 ->
              ct:fail("still unclosed ~p", [IDs])
    end.

check_sc_open(RouterNode, RouterChain, RouterPubkeyBin, ID) ->
    RouterLedger = blockchain:ledger(RouterChain),
    {ok, SC} = ct_rpc:call(RouterNode, blockchain_ledger_v1, find_state_channel, [ID, RouterPubkeyBin, RouterLedger]),
    SCModule = case erlang:element(1, SC) == ledger_state_channel_v2 of 
        true -> blockchain_ledger_state_channel_v2;
        false -> blockchain_ledger_state_channel_v1
    end,
    ID == SCModule:id(SC) andalso RouterPubkeyBin == SCModule:owner(SC).

add_block(RouterNode, RouterChain, ConsensusMembers, Txns) ->
    ct:pal("RouterChain: ~p", [RouterChain]),
    ct_rpc:call(RouterNode, test_utils, create_block, [ConsensusMembers, Txns]).

add_and_gossip_fake_blocks(NumFakeBlocks, ConsensusMembers, Node, Swarm, Chain, From) ->
    lists:foreach(
        fun(_) ->
            {ok, B} = ct_rpc:call(Node, test_utils, create_block, [ConsensusMembers, []]),
            _ = ct_rpc:call(Node, blockchain_gossip_handler, add_block, [B, Chain, From, Swarm])
        end,
        lists:seq(1, NumFakeBlocks)
    ).

setup_meck_txn_forwarding(Node, From) ->
    ok = ct_rpc:call(Node, meck_test_util, forward_submit_txn, [From]),
    ok = ct_rpc:call(Node, blockchain_txn_mgr, submit, [fake_txn, fun(_) -> ok end]),
    receive
        {txn, fake_txn} ->
            ct:pal("Got fake_txn test"),
            ok
    after 1000 ->
        ct:fail("txn test timeout")
    end.

check_sc_close(Txn, ID, SCOpenBlockHash, Payloads) ->
    case blockchain_txn_state_channel_close_v1 == blockchain_txn:type(Txn) of
        true ->
            case blockchain_state_channel_v1:id(blockchain_txn_state_channel_close_v1:state_channel(Txn)) == ID of
                true ->
                    ExpectedTree =
                        lists:foldl(
                            fun skewed:add/2,
                            skewed:new(SCOpenBlockHash),
                            Payloads
                        ),

                    Hash = blockchain_state_channel_v1:root_hash(blockchain_txn_state_channel_close_v1:state_channel(Txn)),
                    ExpectedHash = skewed:root_hash(ExpectedTree),
                    case Hash == ExpectedHash of
                        true ->
                            true;
                        false ->
                            {error, {root_hash_mismatch, ExpectedHash, Hash}}
                    end;
                false ->
                    {error, id_mismatch}
            end;
        false ->
            {error, {unexpected_type, blockchain_txn:type(Txn)}}
    end.

debug(Node) ->
    P = ct_rpc:call(Node, erlang, whereis, [blockchain_state_channels_server]),
    ct:pal("sc_server pid: ~p", [P]),

    S = maps:keys(ct_rpc:call(Node, blockchain_state_channels_server, get_all, [])),
    ct:pal("state_channels: ~p", [S]),

    A = maps:keys(ct_rpc:call(Node, blockchain_state_channels_server, get_actives, [])),
    ct:pal("active: ~p", [A]),
    {P, S, A}.

expect_nonce_for_state_channel(RouterNode, SCID, ExpectedNonce) ->
    ok = blockchain_ct_utils:wait_until(fun() ->
        case get_nonce_for_state_channel(RouterNode, SCID) of
            ExpectedNonce -> true;
            N -> {bad_nonce, expected, ExpectedNonce, got, N}
        end
    end, 30, timer:seconds(1)).

get_nonce_for_state_channel(RouterNode, SCID) ->
    case get_active_state_channel(RouterNode, SCID) of
        worker_not_started -> worker_not_started;
        {badrpc, _} = Err -> Err;
        SC -> blockchain_state_channel_v1:nonce(SC)
    end.

get_active_state_channel(RouterNode, SCID) ->
    case ct_rpc:call(RouterNode, blockchain_state_channels_server, get_active_pid, [SCID]) of
        undefined ->
            worker_not_started;
        SCWorkerPid ->
            ct_rpc:call(RouterNode, blockchain_state_channels_worker, get, [SCWorkerPid, 10])
    end.
