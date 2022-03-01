-module(blockchain_sync_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    basic/1
]).

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
    %%% thaere are no tests to run because the basic test doesn't
    %%% actually work at all.
    [
        basic
    ].

%%--------------------------------------------------------------------
%% TEST SUITE SETUP
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    blockchain_ct_utils:init_per_suite(Config).

%%--------------------------------------------------------------------
%% TEST SUITE TEARDOWN
%%--------------------------------------------------------------------
end_per_suite(Config) ->
    Config.


%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    {ok, SimSwarm} = libp2p_swarm:start(sync_SUITE_sim, [{libp2p_nat, [{enabled, false}]}]),
    ok = libp2p_swarm:listen(SimSwarm, "/ip4/0.0.0.0/tcp/0"),
    blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, [{swarm, SimSwarm}|Config]).

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, Config) ->
    SimSwarm = ?config(swarm, Config),
    libp2p_swarm:stop(SimSwarm),
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic(Config) ->
    BaseDir = ?config(base_dir, Config),
    SimDir = ?config(sim_dir, Config),
    SimSwarm = ?config(swarm, Config),

    Balance = 5000,
    BlocksN = 100,
    {ok, Sup, {PrivKey, PubKey}, _Opts} = test_utils:init(BaseDir),
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    % Simulate other chain with sync handler only
    _Chain = blockchain:new(SimDir, Genesis, undefined, undefined),

    % Add some blocks
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
                {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            _ = blockchain_gossip_handler:add_block(Block, Chain0,  blockchain_swarm:pubkey_bin(), blockchain_swarm:tid()),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

%%    ok = libp2p_swarm:add_stream_handler(
%%        SimSwarm
%%        ,?SYNC_PROTOCOL_V1
%%        ,{libp2p_framed_stream, server, [c, ?MODULE, ?SYNC_PROTOCOL_V1, Chain]}
%%    ),

    % This is just to connect the 2 swarms
    [ListenAddr|_] = libp2p_swarm:listen_addrs(blockchain_swarm:swarm()),
    {ok, _} = libp2p_swarm:connect(SimSwarm, ListenAddr),
    ok = test_utils:wait_until(fun() -> erlang:length(libp2p_peerbook:values(libp2p_swarm:peerbook(blockchain_swarm:swarm()))) > 1 end),

    % Simulate add block from other chain
    _ = blockchain_gossip_handler:add_block(LastBlock, Chain0, libp2p_swarm:pubkey_bin(SimSwarm), blockchain_swarm:tid()),

    ok = test_utils:wait_until(fun() ->{ok, BlocksN + 1} =:= blockchain:height(Chain0) end),
    ?assertEqual({ok, LastBlock}, blockchain:head_block(blockchain_worker:blockchain())),
    true = erlang:exit(Sup, normal),
    ok.
