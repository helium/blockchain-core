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

%% Setup --------------------------------------------------------

all() ->
    [
        submit_and_query_test,
        grpc_submit_and_query_test
    ].

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

    %% GenGwTxns = [blockchain_txn_gen_gateway_v1:new(Addr, hd(ConsensusAddrs), h3:from_geo({37.780586, -122.469470}, 13), 0)
    %%              || Addr <- Addrs],

    Txns = InitVars ++ GenPaymentTxns ++ [GenConsensusGroupTxn],
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

    Src = t_user:new(),

    [{src_user, Src},
     {nodes, Nodes},
     {consensus_members, ConsensusMembers}
     | InitConfig].

end_per_testcase(TestCase, Config) ->
    blockchain_ct_utils:end_per_testcase(TestCase, Config).

%% Cases --------------------------------------------------------

submit_and_query_test(Cfg) ->
    [N1 | _Nodes] = ?config(nodes, Cfg),

    Src = ?config(src_user, Cfg),
    Dst = t_user:new(),
    Txn = t_txn:pay(Src, Dst, 1000, 1),

    {ok, Key, Height} = ct_rpc:call(N1, blockchain_txn_mgr, grpc_submit, [Txn]), %% blockchain_txn_mgr:grpc_submit(Txn),
    ct:pal("Received ~p from txn submitted at height ~p", [Key, Height]),

    ?assert(is_integer(binary_to_integer(Key))),
    ?assertEqual(1, Height),

    %% Accept = {CG1, 2, 3, 4},
    %% Reject = {CG2, 2, fail},

    %% [{Key, Txn, {txn_data, Callbk, RecHt, _, _, Dials}}] = ets:lookup(txn_cache, Key),
    %% ets:insert(txn_cache, {Key, Txn, {txn_data, Callbk, RecHt, [Accept], [Reject], Dials}}),
    %% ets:insert(height_cache, {cur_height, 2}),

    %% {ok, pending, #{key := Key,
    %%                 recv_block_height := RecvHeight,
    %%                 height := CurHeight,
    %%                 acceptors := [Acc],
    %%                 rejectors := [Rej]}} = blockchain_txn_mgr:txn_status(Key),

    %% ct:pal("Accepted by : ~p, Rejected by : ~p", [Acc, Rej]),
    %% ?assertEqual(1, RecvHeight),
    %% ?assertEqual(2, CurHeight),
    %% ?assertEqual(CG1, element(1, Acc)),
    %% ?assertEqual(fail, element(3, Rej)).
    Result = ct_rpc:call(N1, blockchain_txn_mgr, txn_status, [Key]),
    ct:pal("Current Status ~p", [Result]),
    ct:fail("Boom").

grpc_submit_and_query_test(_) -> ok.
%% grpc_submit_and_query_test(Cfg) ->
%%     [_CG1 | _Group] = ?config(users_in_consensus, Cfg),
%%     Chain = ?config(chain, Cfg),
%%     Src = ?config(src_user, Cfg),
%%     Dst = t_user:new(),

%%     Amount = 1000,
%%     #blockchain_txn_payment_v1_pb{payer = SrcPub,
%%                                   payee = DstPub,
%%                                   amount = Amount,
%%                                   fee = Fee,
%%                                   nonce = Nonce,
%%                                   signature = Sig} = t_txn:pay(Src, Dst, Amount, 1),
%%     TxnMap = #{payer => SrcPub,
%%                payee => DstPub,
%%                amount => Amount,
%%                fee => Fee,
%%                nonce => Nonce,
%%                signature => Sig},

%%     ok = test_utils:wait_until(fun() -> {ok, 1} =:= blockchain:height(Chain) end),

%%     Config = application:get_env(grpcbox, server),
%%     ct:pal("GRPC BOX CONFIG ~p", [Config]),
%%     {ok, GrpcConn} = grpc_client:connect(tcp, "127.0.0.1", 10001),
%%     %% {ok, #{http_status := 200,
%%     %%        result := #txn_submit_resp_v1_pb{recv_height = Height, key = Key}}} =
%%     Result = grpc_client:unary(GrpcConn,
%%                           #{txn => #{txn => {payment, TxnMap}}},
%%                           'helium.transaction',
%%                           submit,
%%                           transaction_client_pb,
%%                           [{timeout, 1000}]),
%%     ct:pal("RESULT ~p", [Result]),
%%     %% ct:pal("Received ~p from txn submitted at height ~p", [Key, Height]),

%%     %% ?assert(is_integer(binary_to_integer(Key))),
%%     %% ?assertEqual(1, Height).
%%     ct:fail("Boom").

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
