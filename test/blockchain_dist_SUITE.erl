-module(blockchain_dist_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-include("include/blockchain.hrl").

-export([
         init_per_suite/1
         ,end_per_suite/1
         ,init_per_testcase/2
         ,end_per_testcase/2
         ,all/0
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

init_per_testcase(_TestCase, Config0) ->
    blockchain_ct_utils:init_per_testcase(_TestCase, Config0).

end_per_testcase(_TestCase, Config) ->
    blockchain_ct_utils:end_per_testcase(_TestCase, Config).

gossip_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Balance = 5000,
    NumConsensusMembers = proplists:get_value(num_consensus_members, Config),

    %% accumulate the address of each node
    Addrs = lists:foldl(fun(Node, Acc) ->
                                Addr = ct_rpc:call(Node, blockchain_swarm, address, []),
                                [Addr | Acc]
                        end, [], Nodes),

    ConsensusAddrs = lists:sublist(lists:sort(Addrs), NumConsensusMembers),

    % Create genesis block
    GenPaymentTxs = [blockchain_txn_coinbase:new(Addr, Balance) || Addr <- Addrs],
    GenConsensusGroupTx = blockchain_txn_gen_consensus_group:new(ConsensusAddrs),
    Txs = GenPaymentTxs ++ [GenConsensusGroupTx],
    GenesisBlock = blockchain_block:new_genesis_block(Txs),

    %% tell each node to integrate the genesis block
    lists:foreach(fun(Node) ->
                          ok = ct_rpc:call(Node, blockchain_worker, integrate_genesis_block, [GenesisBlock])
                  end, Nodes),

    %% wait till each worker gets the gensis block
    ok = lists:foreach(fun(Node) ->
                               ok = blockchain_ct_utils:wait_until(fun() ->
                                                                           1 == ct_rpc:call(Node, blockchain_worker, height, [])
                                                                   end, 10, timer:seconds(6))
                       end, Nodes),

    %% FIXME: should do this for each test case presumably
    lists:foreach(fun(Node) ->
                          BlockHash = ct_rpc:call(Node, blockchain_block, hash_block, [GenesisBlock]),
                          HeadHash = ct_rpc:call(Node, blockchain_worker, head_hash, []),
                          HeadBlock = ct_rpc:call(Node, blockchain_worker, head_block, []),
                          GenesisHash = ct_rpc:call(Node, blockchain_worker, genesis_hash, []),
                          WorkerGenesisBlock = ct_rpc:call(Node, blockchain_worker, genesis_block, []),
                          Height = ct_rpc:call(Node, blockchain_worker, height, []),
                          ?assertEqual(BlockHash, HeadHash),
                          ?assertEqual(GenesisBlock, HeadBlock),
                          ?assertEqual(BlockHash, GenesisHash),
                          ?assertEqual(GenesisBlock, WorkerGenesisBlock),
                          ?assertEqual(1, Height)
                  end, Nodes),

    %% FIXME: move this elsewhere
    ConsensusMembers = lists:keysort(1, lists:foldl(fun(Node, Acc) ->
                                                            Addr = ct_rpc:call(Node, blockchain_swarm, address, []),
                                                            case lists:member(Addr, ConsensusAddrs) of
                                                                false -> Acc;
                                                                true ->
                                                                    {ok, Pubkey, SigFun} = ct_rpc:call(Node, blockchain_swarm, keys, []),
                                                                    [{Addr, Pubkey, SigFun} | Acc]
                                                            end
                                                    end, [], Nodes)),

    %% let these two serve as dummys
    [FirstNode, SecondNode | _Rest] = Nodes,

    %% First node creates a payment transaction for the second node
    Payer = ct_rpc:call(FirstNode, blockchain_swarm, address, []),
    {ok, _Pubkey, SigFun} = ct_rpc:call(FirstNode, blockchain_swarm, keys, []),
    Recipient = ct_rpc:call(SecondNode, blockchain_swarm, address, []),
    Tx = blockchain_txn_payment:new(Payer, Recipient, 2500, 1),
    SignedTx = blockchain_txn_payment:sign(Tx, SigFun),
    Block = ct_rpc:call(FirstNode, blockchain_util, create_block, [ConsensusMembers, [SignedTx]]),
    ct:pal("Block: ~p", [Block]),

    PayerSwarm = ct_rpc:call(FirstNode, blockchain_swarm, swarm, []),
    GossipGroup = ct_rpc:call(FirstNode, libp2p_swarm, gossip_group, [PayerSwarm]),
    ct:pal("GossipGroup: ~p", [GossipGroup]),

    ok = ct_rpc:call(FirstNode, blockchain_worker, add_block, [Block, self()]),

    ok = lists:foreach(fun(Node) ->
                               ok = blockchain_ct_utils:wait_until(fun() ->
                                                                           2 == ct_rpc:call(Node, blockchain_worker, height, [])
                                                                   end, 10, timer:seconds(6))
                       end, Nodes),
    ok.

