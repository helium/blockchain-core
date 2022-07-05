-module(blockchain_txn_mgr_SUITE).

-include("blockchain.hrl").
-include("blockchain_ct_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_payment_v1_pb.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

-export([
    submit_and_query_test/1,
    grpc_submit_and_query_test/1
]).

all() ->
    [
        submit_and_query_test,
        grpc_submit_and_query_test
    ].

%% Setup --------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    InitConfig0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    InitConfig = blockchain_ct_utils:init_per_testcase(TestCase, InitConfig0),

    Nodes = ?config(nodes, InitConfig),
    Balance = 5000,
    NumConsensusMembers = ?config(num_consensus_members, InitConfig),

    Addrs = lists:foldl(
        fun(Node, Acc) ->
            [ct_rpc:call(Node, blockchain_swarm, pubkey_bin, []) | Acc]
        end, [], Nodes),

    ConsensusAddrs = lists:sublist(lists:sort(Addrs), NumConsensusMembers),
    {InitVars, _} = blockchain_ct_utils:create_vars(#{num_consensus_members => NumConsensusMembers}),

    GenPaymentTxns = [blockchain_txn_coinbase_v1:new(Addr, Balance) || Addr <- Addrs],
    GenConsensusGroupTxn = blockchain_txn_consensus_group_v1:new(ConsensusAddrs, <<"proof">>, 1, 0),

    Users = t_user:n_new(2),
    AddrBalPairs = [{t_user:addr(U), Balance} || U <- Users],
    GenUserTxns = [blockchain_txn_coinbase_v1:new(A, B) || {A, B} <- AddrBalPairs],

    Txns = InitVars ++ GenPaymentTxns ++ GenUserTxns ++ [GenConsensusGroupTxn],
    GenesisBlock = blockchain_block:new_genesis_block(Txns),

    lists:foreach(
        fun(Node) ->
            ?assertMatch(ok, ct_rpc:call(Node, blockchain_worker, integrate_genesis_block, [GenesisBlock]))
        end, Nodes),

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

    ok = check_genesis_block(Nodes, GenesisBlock),
    ConsensusMembers = get_consensus_members(Nodes, ConsensusAddrs),

    %% Add the txn stream handler with a bogus "accepted" return value
    ok = lists:foreach(
             fun(Node) ->
                 ct_rpc:call(Node,
                             libp2p_swarm,
                             add_stream_handler,
                             [ blockchain_swarm:tid(),
                               ?TX_PROTOCOL_V3,
                               {libp2p_framed_stream, server, [ blockchain_txn_handler,
                                                                ?TX_PROTOCOL_V3,
                                                                self(),
                                                                fun(_, _) -> {{ok, 2, 4}, 1} end ]}
                             ])
             end, Nodes),

    [{users, Users},
     {nodes, Nodes},
     {consensus_members, ConsensusMembers}
     | InitConfig].

end_per_testcase(TestCase, Config) ->
    blockchain_ct_utils:end_per_testcase(TestCase, Config).

%% Cases --------------------------------------------------------

submit_and_query_test(Cfg) ->
    [N1 | _Nodes] = ?config(nodes, Cfg),

    [U1, U2] = ?config(users, Cfg),
    Txn = t_txn:pay(U1, U2, 1000, 1),

    {ok, Key, Height} = ct_rpc:call(N1, blockchain_txn_mgr, grpc_submit, [Txn]),
    ct:pal("Received ~p from txn submitted at height ~p", [Key, Height]),

    ?assert(is_integer(binary_to_integer(Key))),
    ?assertEqual(1, Height),

    ok = ct_rpc:call(N1, blockchain_txn_mgr, force_process_cached_txns, []),

    ok = blockchain_ct_utils:wait_until(fun() ->
                                            {ok, pending, Status} = ct_rpc:call(N1, blockchain_txn_mgr, txn_status, [Key]),
                                            length(maps:get(acceptors, Status, [])) > 0
                                        end, 100, 200),

    {ok, pending, #{key := Key,
                    recv_block_height := RecvHeight,
                    height := CurHeight,
                    acceptors := [{_, AccH, QPos, QLen} = Acc | _],
                    rejectors := []}} = ct_rpc:call(N1, blockchain_txn_mgr, txn_status, [Key]),

    ct:pal("Accepted by : ~p", [Acc]),
    ?assertEqual(Height, RecvHeight),
    ?assert(CurHeight >= RecvHeight),
    ?assertEqual(1, AccH),
    ?assertEqual({2, 4}, {QPos, QLen}),
    ok.

grpc_submit_and_query_test(Cfg) ->
    [N1 | _Nodes] = ?config(nodes, Cfg),
    [U1, U2] = ?config(users, Cfg),
    Amount = 1000,

    #blockchain_txn_payment_v1_pb{payer = SrcPub,
                                  payee = DstPub,
                                  amount = Amount,
                                  fee = Fee,
                                  nonce = Nonce,
                                  signature = Sig} = t_txn:pay(U1, U2, Amount, 1),
    TxnMap = #{payer => SrcPub,
               payee => DstPub,
               amount => Amount,
               fee => Fee,
               nonce => Nonce,
               signature => Sig},

    N1GrpcPort = node_grpc_port(N1),
    ct:pal("Dialing node ~p on grpc port ~p", [N1, N1GrpcPort]),

    {ok, GrpcConn} = grpc_client:connect(tcp, "127.0.0.1", N1GrpcPort),
    {ok, #{result := SubmitResult}} = grpc_client:unary(GrpcConn,
                                                        #{txn => #{txn => {payment, TxnMap}}},
                                                        'helium.transaction',
                                                        submit,
                                                        transaction_client_pb,
                                                        [{timeout, 1000}]),

    Key = maps:get(key, SubmitResult),
    ?assert(is_integer(binary_to_integer(Key))),
    ?assertEqual(1, maps:get(recv_height, SubmitResult)),
    ct:pal("Submit txn request result ~p", [SubmitResult]),

    ok = ct_rpc:call(N1, blockchain_txn_mgr, force_process_cached_txns, []),

    ok = blockchain_ct_utils:wait_until(fun() ->
                                            {ok, pending, Status} = ct_rpc:call(N1, blockchain_txn_mgr, txn_status, [Key]),
                                            length(maps:get(acceptors, Status, [])) > 0
                                        end, 100, 200),

    {ok, #{result := QueryResult}} = grpc_client:unary(GrpcConn,
                                                       #{key => Key},
                                                       'helium.transaction',
                                                       query,
                                                       transaction_client_pb,
                                                       [{timeout, 1000}]),

    ct:pal("Query txn request result ~p", [QueryResult]),

    ?assertEqual(pending, maps:get(status, QueryResult)),
    ?assertEqual([], maps:get(rejectors, QueryResult)),
    [#{queue_pos := QPos, queue_len := QLen, height := AccH} | _] = maps:get(acceptors, QueryResult),
    ?assertEqual({2, 4}, {QPos, QLen}),
    ?assertEqual(1, AccH),
    ok.

%% --------------------------------------------------------------------

check_genesis_block(Nodes, GenesisBlock) ->
    lists:foreach(fun(Node) ->
                          Blockchain = ct_rpc:call(Node, blockchain_worker, blockchain, []),
                          {ok, HeadBlock} = ct_rpc:call(Node, blockchain, head_block, [Blockchain]),
                          {ok, WorkerGenesisBlock} = ct_rpc:call(Node, blockchain, genesis_block, [Blockchain]),
                          {ok, Height} = ct_rpc:call(Node, blockchain, height, [Blockchain]),
                          ?assertEqual(GenesisBlock, HeadBlock),
                          ?assertEqual(GenesisBlock, WorkerGenesisBlock),
                          ?assertEqual(1, Height)
                  end, Nodes).

get_consensus_members(Nodes, ConsensusAddrs) ->
    lists:keysort(1, lists:foldl(fun(Node, Acc) ->
                                         Addr = ct_rpc:call(Node, blockchain_swarm, pubkey_bin, []),
                                         case lists:member(Addr, ConsensusAddrs) of
                                             false -> Acc;
                                             true ->
                                                 {ok, Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Node, blockchain_swarm, keys, []),
                                                 [{Addr, Pubkey, SigFun} | Acc]
                                         end
                                 end, [], Nodes)).

node_grpc_port(Node)->
    Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, []),
    SwarmTID = ct_rpc:call(Node, blockchain_swarm, tid, []),
    ListenAddrs = ct_rpc:call(Node, libp2p_swarm, listen_addrs, [Swarm]),
    [Addr | _] = ct_rpc:call(Node, libp2p_transport, sort_addrs, [SwarmTID, ListenAddrs]),
    [_, _, _IP, _, Port] = re:split(Addr, "/"),
    GrpcPort = list_to_integer(binary_to_list(Port)) + 1000,
    ct:pal("peer p2p port ~p, grpc port ~p", [Port, GrpcPort]),
    GrpcPort.
