-module(blockchain_packet_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_ct_utils.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    direct_send_packet_test/1,
    direct_response_packet_test/1
]).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

all() ->
    [
        direct_send_packet_test,
        direct_response_packet_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    [
        {sc_client_transport_handler, blockchain_packet_handler}
        | Config
    ].

end_per_suite(_) -> ok.

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
    application:ensure_all_started(telemetry),

    InitConfig0 = blockchain_ct_utils:init_base_dir_config(?MODULE, Test, Config),
    InitConfig = blockchain_ct_utils:init_per_testcase(Test, InitConfig0),

    InitNodes = ?config(nodes, InitConfig),
    Balance = 50000,
    NumConsensusMembers = ?config(num_consensus_members, InitConfig),

    %% Make a map Node => Addr
    NodeAddrList = lists:foldl(
        fun(Node, Acc) ->
            [{Node, ct_rpc:call(Node, blockchain_swarm, pubkey_bin, [])} | Acc]
        end,
        [],
        InitNodes
    ),
    Addrs = [Addr || {_, Addr} <- NodeAddrList],

    ConsensusAddrs = lists:sublist(lists:sort(Addrs), NumConsensusMembers),

    %% The SC tests use the first two nodes as the gateway and router.
    %% For the GRPC group to work we need to ensure these two nodes are
    %% connected to each other in blockchain_ct_utils:init_per_testcase().
    %% The nodes are connected to a majority of the group, but that does not
    %% guarantee these two nodes are connected.

    [RouterNode, GatewayNode] =
        blockchain_ct_utils:find_connected_node_pair(NodeAddrList),
    Nodes =
        [RouterNode, GatewayNode] ++ (InitNodes -- [RouterNode, GatewayNode]),

    Dir = os:getenv("SC_DIR", ""),
    debug_modules_for_node(
        RouterNode,
        Dir ++ "sc_server.log",
        [
            blockchain_state_channel_v1,
            blockchain_state_channels_cache,
            blockchain_state_channel_handler,
            blockchain_state_channels_server,
            blockchain_state_channels_worker,
            blockchain_txn_state_channel_close_v1,
            blockchain_state_channel_sup,
            sc_packet_test_handler,
            blockchain_packet_client,
            blockchain_state_channel_common
        ]
    ),
    debug_modules_for_node(
        GatewayNode,
        Dir ++ "sc_client_1.log",
        [
            blockchain_state_channel_v1,
            blockchain_state_channels_client,
            blockchain_state_channel_handler,
            blockchain_state_channel_sup,
            sc_packet_test_handler,
            blockchain_packet_client,
            blockchain_state_channel_common
        ]
    ),

    DefaultVars = #{num_consensus_members => NumConsensusMembers},
    ExtraVars = #{
        max_open_sc => 2,
        min_expire_within => 10,
        max_xor_filter_size => 1024 * 100,
        max_xor_filter_num => 5,
        max_subnet_size => 65536,
        min_subnet_size => 8,
        max_subnet_num => 20,
        sc_grace_blocks => 5,
        dc_payload_size => 24,
        sc_max_actors => 100,
        %% we are focring 2 for all test as 1 is just rly old now
        sc_version => 2,
        sc_dispute_strategy_version => 0
    },

    {InitialVars, {master_key, MasterKey}} = blockchain_ct_utils:create_vars(
        maps:merge(DefaultVars, ExtraVars)
    ),

    % Create genesis block
    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance) || Addr <- Addrs],
    GenDCsTxs = [blockchain_txn_dc_coinbase_v1:new(Addr, Balance) || Addr <- Addrs],
    % 1 dollar
    GenPriceOracle = blockchain_txn_gen_price_oracle_v1:new(100000000),
    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new(ConsensusAddrs, <<"proof">>, 1, 0),

    %% Make one consensus member the owner of all gateways
    GenGwTxns = [
        blockchain_txn_gen_gateway_v1:new(
            Addr, hd(ConsensusAddrs), h3:from_geo({37.780586, -122.469470}, 13), 0
        )
     || Addr <- Addrs
    ],

    Txs = lists:flatten([
        InitialVars,
        [GenPriceOracle],
        GenPaymentTxs,
        GenDCsTxs,
        GenGwTxns,
        [GenConsensusGroupTx]
    ]),
    GenesisBlock = blockchain_block:new_genesis_block(Txs),

    %% tell each node to integrate the genesis block
    lists:foreach(
        fun(Node) ->
            ?assertMatch(
                ok, ct_rpc:call(Node, blockchain_worker, integrate_genesis_block, [GenesisBlock])
            )
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

    ok = check_genesis_block(Nodes, GenesisBlock),
    ConsensusMembers = get_consensus_members(Nodes, ConsensusAddrs),
    [
        {connected_nodes, [RouterNode, GatewayNode]},
        {routernode, RouterNode},
        {gatewaynode, GatewayNode},
        {nodes, Nodes},
        {consensus_members, ConsensusMembers},
        {master_key, MasterKey}
        | proplists:delete(nodes, InitConfig)
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(Test, Config) ->
    blockchain_ct_utils:end_per_testcase(Test, Config).

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

direct_send_packet_test(Config) ->
    Nodes = ?config(nodes, Config),
    RouterNode = ?config(routernode, Config),
    GatewayNode = ?config(gatewaynode, Config),

    %% Get router pubkey_bin
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Break if someone sends an offer
    Self = self(),
    ok = ct_rpc:call(RouterNode, application, set_env, [
        blockchain, sc_packet_handler_offer_fun, fun(_, _) -> throw(no_more_offers) end
    ]),
    %% Forward uplinks
    ok = ct_rpc:call(RouterNode, application, set_env, [
        blockchain, sc_packet_handler_packet_fun, fun(P, HPid) -> Self ! {packet, P, HPid} end
    ]),
    %% Break if there's a purchase
    ok = ct_rpc:call(GatewayNode, application, set_env, [
        blockchain, sc_client_handle_purchase_fun, fun(_) -> throw(no_more_purchasing) end
    ]),
    %% Forward downlinks
    ok = ct_rpc:call(GatewayNode, application, set_env, [
        blockchain, sc_client_handle_response_fun, fun(Resp) -> Self ! {client_response, Resp} end
    ]),

    %% Set app env to use default routers on all nodes
    _ = blockchain_ct_utils:pmap(
        fun(Node) ->
            ct_rpc:call(Node, application, set_env, [blockchain, use_oui_routers, false])
        end,
        Nodes
    ),
    timer:sleep(timer:seconds(1)),

    %% Include the router in default routers list
    DefaultRouters = [libp2p_crypto:pubkey_bin_to_p2p(RouterPubkeyBin)],

    %% Sending first packet and routing using the default routers
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode, blockchain_packet_client, packet, [
        Packet0, DefaultRouters, 'US915'
    ]),

    ok =
        receive
            {packet, _P} -> ok
        after 2000 -> ct:fail(no_packet_arrived)
        end,

    ok.

direct_response_packet_test(Config) ->
    Nodes = ?config(nodes, Config),
    RouterNode = ?config(routernode, Config),
    GatewayNode = ?config(gatewaynode, Config),

    %% Get router pubkey_bin
    RouterPubkeyBin = ct_rpc:call(RouterNode, blockchain_swarm, pubkey_bin, []),

    %% Break if someone sends an offer
    Self = self(),
    ok = ct_rpc:call(RouterNode, application, set_env, [
        blockchain, sc_packet_handler_offer_fun, fun(_, _) -> throw(no_more_offers) end
    ]),
    %% Forward uplinks
    ok = ct_rpc:call(RouterNode, application, set_env, [
        blockchain, sc_packet_handler_packet_fun, fun(P, HPid) -> Self ! {packet, P, HPid} end
    ]),
    %% Break if there's a purchase
    ok = ct_rpc:call(GatewayNode, application, set_env, [
        blockchain, sc_client_handle_purchase_fun, fun(_) -> throw(no_more_purchasing) end
    ]),
    %% Forward downlinks
    ok = ct_rpc:call(GatewayNode, application, set_env, [
        blockchain, sc_client_handle_response_fun, fun(Resp) -> Self ! {client_response, Resp} end
    ]),

    %% Set app env to use default routers on all nodes
    _ = blockchain_ct_utils:pmap(
        fun(Node) ->
            ct_rpc:call(Node, application, set_env, [blockchain, use_oui_routers, false])
        end,
        Nodes
    ),
    timer:sleep(timer:seconds(1)),

    %% Include the router in default routers list
    DefaultRouters = [libp2p_crypto:pubkey_bin_to_p2p(RouterPubkeyBin)],

    %% Sending first packet and routing using the default routers
    DevNonce0 = crypto:strong_rand_bytes(2),
    Packet0 = blockchain_ct_utils:join_packet(?APPKEY, DevNonce0, 0.0),
    ok = ct_rpc:call(GatewayNode, blockchain_packet_client, packet, [
        Packet0, DefaultRouters, 'US915'
    ]),

    ResponsePid =
        receive
            {packet, _P, RespPid} -> RespPid
        after 2000 -> ct:fail(no_packet_arrived)
        end,

    Response = blockchain_state_channel_response_v1:new(true, undefined),
    _Pid = ct_rpc:call(RouterNode, blockchain_packet_client, send_response, [ResponsePid, Response]),

    ok =
        receive
            {client_response, _Resp} -> ok
        after 2000 -> ct:fail(no_response_arrived)
        end,

    ok.

%% ------------------------------------------------------------------
%% Helper functions
%% ------------------------------------------------------------------

check_genesis_block(Nodes, GenesisBlock) ->
    lists:foreach(
        fun(Node) ->
            Blockchain = ct_rpc:call(Node, blockchain_worker, blockchain, []),
            {ok, HeadBlock} = ct_rpc:call(Node, blockchain, head_block, [Blockchain]),
            {ok, WorkerGenesisBlock} = ct_rpc:call(Node, blockchain, genesis_block, [Blockchain]),
            {ok, Height} = ct_rpc:call(Node, blockchain, height, [Blockchain]),
            ?assertEqual(GenesisBlock, HeadBlock),
            ?assertEqual(GenesisBlock, WorkerGenesisBlock),
            ?assertEqual(1, Height)
        end,
        Nodes
    ).

get_consensus_members(Nodes, ConsensusAddrs) ->
    lists:keysort(
        1,
        lists:foldl(
            fun(Node, Acc) ->
                Addr = ct_rpc:call(Node, blockchain_swarm, pubkey_bin, []),
                case lists:member(Addr, ConsensusAddrs) of
                    false ->
                        Acc;
                    true ->
                        {ok, Pubkey, SigFun, _ECDHFun} = ct_rpc:call(
                            Node, blockchain_swarm, keys, []
                        ),
                        [{Addr, Pubkey, SigFun} | Acc]
                end
            end,
            [],
            Nodes
        )
    ).
