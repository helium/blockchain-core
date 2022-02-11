-module(blockchain_fastforward_SUITE).

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
         v1/1, v2/1, basic/4
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
    [v1, v2].


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
    % Simulate other chain with fastforward handler only
    {ok, SimSwarm} = libp2p_swarm:start(fastforward_SUITE_sim, [{libp2p_nat, [{enabled, false}]},
                                                                {libp2p_peerbook,
                                                                 [{allow_rfc1918, true}]}

                                                               ]),
    ok = libp2p_swarm:listen(SimSwarm, "/ip4/0.0.0.0/tcp/0"),
    application:set_env(blockchain, peerbook_allow_rfc1918, true),
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

v1(Config) ->
    BaseDir = ?config(base_dir, Config),
    {ok, Sup, Keys, _Opts} = test_utils:init(BaseDir),
    try
        basic(v1, Sup, Keys, Config)
    catch C:E:S ->
            lager:info("v1 failure ~p:~p", [C, E]),
            lager:info("stacktrace ~p", [S]),
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> erlang:is_process_alive(Sup) == false end),
            erlang:C(E)
    end.

v2(Config) ->
    BaseDir = ?config(base_dir, Config),
    {ok, Sup, Keys, _Opts} = test_utils:init(BaseDir),
    lager:info("XXX sup ~p ~p", [Sup, erlang:is_process_alive(Sup)]),
    case Sup of
        P when is_pid(P) ->
            ok;
        {error, {already_started, P}} ->
            lager:info("XXX ~p ~p", [P, erlang:is_process_alive(P)])
    end,
    try
        basic(v2, Sup, Keys, Config)
    catch C:E:S ->
            lager:info("v2 failure ~p:~p", [C, E]),
            lager:info("stacktrace ~p", [S]),
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> erlang:is_process_alive(Sup) == false end),
            erlang:C(E)
    end.

basic(Version, Sup, {PrivKey, PubKey}, Config) ->
    SimDir = ?config(sim_dir, Config),
    SimSwarm = ?config(swarm, Config),

    Balance = 5000,
    BlocksN = 100,
    %% create a chain
    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, _} = test_utils:init_chain(Balance, {PrivKey, PubKey}),
    Chain0 = blockchain_worker:blockchain(),
    {ok, Genesis} = blockchain:genesis_block(Chain0),

    %% create a second chain
    {ok, Chain} = blockchain:new(SimDir, Genesis, undefined, undefined),

    % Add some blocks to the first chain
    Blocks = lists:reverse(lists:foldl(
        fun(_, Acc) ->
            {ok, Block} = test_utils:create_block(ConsensusMembers, []),
            %_ = blockchain_gossip_handler:add_block(blockchain_swarm:swarm(), Block, Chain0, length(ConsensusMembers), blockchain_swarm:pubkey_bin()),
            ok = blockchain:add_block(Block, Chain0),
            [Block|Acc]
        end,
        [],
        lists:seq(1, BlocksN)
    )),
    LastBlock = lists:last(Blocks),

    ok = libp2p_swarm:add_stream_handler(
           SimSwarm,
           ?FASTFORWARD_PROTOCOL_V1,
           {libp2p_framed_stream, server, [blockchain_fastforward_handler, ?MODULE, [?FASTFORWARD_PROTOCOL_V1, Chain]]}
    ),
    ok = libp2p_swarm:add_stream_handler(
           SimSwarm,
           ?FASTFORWARD_PROTOCOL_V2,
           {libp2p_framed_stream, server, [blockchain_fastforward_handler, ?MODULE, [?FASTFORWARD_PROTOCOL_V2, Chain]]}
    ),
    % This is just to connect the 2 swarms
    [ListenAddr|_] = libp2p_swarm:listen_addrs(blockchain_swarm:swarm()),
    {ok, _} = libp2p_swarm:connect(SimSwarm, ListenAddr),
    [ListenAddr2|_] = libp2p_swarm:listen_addrs(SimSwarm),
    ok = test_utils:wait_until(
           fun() ->
                   erlang:length(libp2p_peerbook:values(libp2p_swarm:peerbook(blockchain_swarm:swarm()))) > 1
           end),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    ?assertEqual({ok, 101}, blockchain:height(Chain0)),

    %% use fastforward to fastforward the peer
    ProtocolVersion =
        case Version of
            v1 -> ?FASTFORWARD_PROTOCOL_V1;
            v2 -> ?FASTFORWARD_PROTOCOL_V2
        end,
    case blockchain_fastforward_handler:dial(blockchain_swarm:tid(), Chain0, ListenAddr2, ProtocolVersion) of
        {ok, _Stream} ->
            ct:pal("got stream ~p~n", [_Stream]),
            ok
    end,

    ok = test_utils:wait_until(fun() ->{ok, BlocksN + 1} =:= blockchain:height(Chain) end, 10, 1000),
    ?assertEqual({ok, LastBlock}, blockchain:head_block(blockchain_worker:blockchain())),
    gen:stop(Sup),
    ok = test_utils:wait_until(fun() -> erlang:is_process_alive(Sup) == false end),
    ok.
