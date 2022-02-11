-module(blockchain_dist_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-include("blockchain.hrl").

-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
         gossip_test/1
        ]).

%% common test callbacks

all() -> [
          gossip_test
         ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

%% ------------------------------------------------------------------
%% Configurations
%% ------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    InitConfig = blockchain_ct_utils:init_per_testcase(TestCase, Config0),
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
    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new(ConsensusAddrs, <<"proof">>, 1, 0),
    Txs = InitialVars ++ GenPaymentTxs ++ [GenConsensusGroupTx],
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
    [
        {consensus_memebers, ConsensusMembers}
        | InitConfig].

end_per_testcase(_TestCase, Config) ->
    blockchain_ct_utils:end_per_testcase(_TestCase, Config).

%% ------------------------------------------------------------------
%% Test Cases
%% ------------------------------------------------------------------

gossip_test(Config) ->
    Nodes = ?config(nodes, Config),
    ConsensusMembers = ?config(consensus_memebers, Config),

    %% let these two serve as dummys
    [FirstNode, SecondNode | _Rest] = Nodes,

    %% First node creates a payment transaction for the second node
    Payer = ct_rpc:call(FirstNode, blockchain_swarm, pubkey_bin, []),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(FirstNode, blockchain_swarm, keys, []),
    Recipient = ct_rpc:call(SecondNode, blockchain_swarm, pubkey_bin, []),
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 1),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    {ok, Block} = ct_rpc:call(FirstNode, test_utils, create_block, [ConsensusMembers, [SignedTx]]),
    ct:pal("Block: ~p", [Block]),

    PayerSwarm = ct_rpc:call(FirstNode, blockchain_swarm, swarm, []),
    GossipGroup = ct_rpc:call(FirstNode, libp2p_swarm, gossip_group, [PayerSwarm]),
    ct:pal("GossipGroup: ~p", [GossipGroup]),

    Chain = ct_rpc:call(FirstNode, blockchain_worker, blockchain, []),
    ct:pal("FirstNode Chain: ~p", [Chain]),
    Swarm = ct_rpc:call(FirstNode, blockchain_swarm, swarm, []),
    ct:pal("FirstNode Swarm: ~p", [Swarm]),
    N = length(Nodes),
    ct:pal("N: ~p", [N]),

    GossipGroup = ct_rpc:call(FirstNode, libp2p_swarm, gossip_group, [Swarm]),
    GossipData = ct_rpc:call(FirstNode, blockchain_gossip_handler, gossip_data_v1, [Swarm, Block]),

    CallRet = ct_rpc:call(FirstNode, libp2p_group_gossip, send, [GossipGroup, ?GOSSIP_PROTOCOL_V1, GossipData]),

    ct:pal("call return ~p", [CallRet]),

    ok = lists:foreach(fun(Node) ->
        ok = blockchain_ct_utils:wait_until(fun() ->
            C = ct_rpc:call(Node, blockchain_worker, blockchain, []),
            {ok, 2} == ct_rpc:call(Node, blockchain, height, [C])
        end, 10, timer:seconds(1))
    end, Nodes),

    Chain2 = ct_rpc:call(FirstNode, blockchain_worker, blockchain, []),
    ct:pal("FirstNode Chain2: ~p", [Chain2]),

    Heights = lists:foldl(fun(Node, Acc) ->
        C2 = ct_rpc:call(Node, blockchain_worker, blockchain, []),
        {ok, H} = ct_rpc:call(Node, blockchain, height, [C2]),
        [{Node, H} | Acc]
    end, [], Nodes),

    ct:comment("Heights: ~p", [Heights]),
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
