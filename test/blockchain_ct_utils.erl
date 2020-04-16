-module(blockchain_ct_utils).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("include/blockchain_vars.hrl").

-export([pmap/2,
         wait_until/1,
         wait_until/3,
         wait_until_height/2,
         wait_until_disconnected/2,
         start_node/3,
         partition_cluster/2,
         heal_cluster/2,
         connect/1,
         count/2,
         randname/1,
         get_config/2,
         random_n/2,
         init_per_testcase/2,
         end_per_testcase/2,
         create_vars/0, create_vars/1,
         raw_vars/1,
         init_base_dir_config/3
        ]).

pmap(F, L) ->
    Parent = self(),
    lists:foldl(
      fun(X, N) ->
              spawn_link(fun() ->
                                 Parent ! {pmap, N, F(X)}
                         end),
              N+1
      end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

wait_until(Fun) ->
    wait_until(Fun, 40, 100).
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

wait_until_offline(Node) ->
    wait_until(fun() ->
                       pang == net_adm:ping(Node)
               end, 60*2, 500).

wait_until_disconnected(Node1, Node2) ->
    wait_until(fun() ->
                       pang == rpc:call(Node1, net_adm, ping, [Node2])
               end, 60*2, 500).

wait_until_connected(Node1, Node2) ->
    wait_until(fun() ->
                       pong == rpc:call(Node1, net_adm, ping, [Node2])
               end, 60*2, 500).

start_node(Name, Config, Case) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [
                  {monitor_master, true},
                  {boot_timeout, 10},
                  {init_timeout, 10},
                  {startup_timeout, 10},
                  {startup_functions, [
                                       {code, set_path, [CodePath]}
                                      ]}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            ok = wait_until(fun() ->
                                    net_adm:ping(Node) == pong
                            end, 60, 500),
            Node;
        {error, already_started, Node} ->
            ct_slave:stop(Name),
            wait_until_offline(Node),
            start_node(Name, Config, Case);
        {error, started_not_connected, Node} ->
            connect(Node),
            ct_slave:stop(Name),
            wait_until_offline(Node),
            start_node(Name, Config, Case)
    end.

partition_cluster(ANodes, BNodes) ->
    pmap(fun({Node1, Node2}) ->
                 true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
                 true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
                 ok = wait_until_disconnected(Node1, Node2)
         end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    pmap(fun({Node1, Node2}) ->
                 true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
                 ok = wait_until_connected(Node1, Node2)
         end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

connect(Node) ->
    connect(Node, true).

connect(NodeStr, Auto) when is_list(NodeStr) ->
    connect(erlang:list_to_atom(lists:flatten(NodeStr)), Auto);
connect(Node, Auto) when is_atom(Node) ->
    connect(node(), Node, Auto).

connect(Node, Node, _) ->
    {error, self_join};
connect(_, Node, _Auto) ->
    attempt_connect(Node).

attempt_connect(Node) ->
    case net_kernel:connect_node(Node) of
        false ->
            {error, not_reachable};
        true ->
            {ok, connected}
    end.

count(_, []) -> 0;
count(X, [X|XS]) -> 1 + count(X, XS);
count(X, [_|XS]) -> count(X, XS).

randname(N) ->
    randname(N, []).

randname(0, Acc) ->
    Acc;
randname(N, Acc) ->
    randname(N - 1, [rand:uniform(26) + 96 | Acc]).

get_config(Arg, Default) ->
    case os:getenv(Arg, Default) of
        false -> Default;
        T when is_list(T) -> list_to_integer(T);
        T -> T
    end.

random_n(N, List) ->
    lists:sublist(shuffle(List), N).

shuffle(List) ->
    [x || {_,x} <- lists:sort([{rand:uniform(), N} || N <- List])].

init_per_testcase(TestCase, Config) ->

    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner-blockchain-" ++
                                        integer_to_list(erlang:system_time(nanosecond)) ++
                                        "@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _},_}} -> ok
    end,

    %% Node configuration, can be input from os env
    TotalNodes = get_config("T", 8),
    NumConsensusMembers = get_config("N", 7),
    SeedNodes = [],
    PeerCacheTimeout = 100,
    Port = get_config("PORT", 0),

    NodeNames = lists:map(fun(_M) -> list_to_atom(randname(5)) end, lists:seq(1, TotalNodes)),

    Nodes = pmap(fun(Node) ->
                         start_node(Node, Config, TestCase)
                 end, NodeNames),

    ConfigResult = pmap(fun(Node) ->
                                ct_rpc:call(Node, cover, start, []),
                                ct_rpc:call(Node, application, load, [lager]),
                                ct_rpc:call(Node, application, load, [blockchain]),
                                ct_rpc:call(Node, application, load, [libp2p]),
                                ct_rpc:call(Node, application, load, [erlang_stats]),
                                %% give each node its own log directory
                                LogRoot = "log/" ++ atom_to_list(TestCase) ++ "/" ++ atom_to_list(Node),
                                ct_rpc:call(Node, application, set_env, [lager, log_root, LogRoot]),
                                ct_rpc:call(Node, lager, set_loglevel, [{lager_file_backend, "log/console.log"}, debug]),

                                %% set blockchain configuration
                                #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
                                Key = {PubKey, libp2p_crypto:mk_sig_fun(PrivKey), libp2p_crypto:mk_ecdh_fun(PrivKey)},
                                BaseDir = "data_" ++ atom_to_list(TestCase) ++ "_" ++ atom_to_list(Node),
                                ct_rpc:call(Node, application, set_env, [blockchain, base_dir, BaseDir]),
                                ct_rpc:call(Node, application, set_env, [blockchain, num_consensus_members, NumConsensusMembers]),
                                ct_rpc:call(Node, application, set_env, [blockchain, port, Port]),
                                ct_rpc:call(Node, application, set_env, [blockchain, seed_nodes, SeedNodes]),
                                ct_rpc:call(Node, application, set_env, [blockchain, key, Key]),
                                ct_rpc:call(Node, application, set_env, [blockchain, peer_cache_timeout, PeerCacheTimeout]),
                                ct_rpc:call(Node, application, set_env, [blockchain, sc_client_handler, sc_client_test_handler]),
                                ct_rpc:call(Node, application, set_env, [blockchain, sc_packet_handler, sc_packet_test_handler]),

                                {ok, StartedApps} = ct_rpc:call(Node, application, ensure_all_started, [blockchain]),
                                ct:pal("Node: ~p, StartedApps: ~p", [Node, StartedApps])
                        end, Nodes),

    %% check that the config loaded correctly on each node
    true = lists:all(fun(Res) -> Res == ok end, ConfigResult),

    %% tell the rest of the miners to connect to the first miner
    [First | Rest] = Nodes,
    FirstSwarm = ct_rpc:call(First, blockchain_swarm, swarm, []),
    FirstListenAddr = hd(ct_rpc:call(First, libp2p_swarm, listen_addrs, [FirstSwarm])),
    ok = lists:foreach(fun(Node) ->
                               Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, []),
                               ct_rpc:call(Node, libp2p_swarm, connect, [Swarm, FirstListenAddr])
                       end, Rest),

    %% XXX: adding this relatively low sleep for gossip groups to get connected
    timer:sleep(timer:seconds(5)),

    %% also do the reverse just to ensure swarms are _properly_ connected
    [Head | Tail] = lists:reverse(Nodes),
    HeadSwarm = ct_rpc:call(Head, blockchain_swarm, swarm, []),
    HeadListenAddr = hd(ct_rpc:call(Head, libp2p_swarm, listen_addrs, [HeadSwarm])),
    ok = lists:foreach(fun(Node) ->
                               Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, []),
                               ct_rpc:call(Node, libp2p_swarm, connect, [Swarm, HeadListenAddr])
                       end, Tail),

    %% test that each node setup libp2p properly
    lists:foreach(fun(Node) ->
                          Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, []),
                          SwarmID = ct_rpc:call(Node, libp2p_swarm, network_id, [Swarm]),
                          Addr = ct_rpc:call(Node, blockchain_swarm, pubkey_bin, []),
                          Sessions = ct_rpc:call(Node, libp2p_swarm, sessions, [Swarm]),
                          GossipGroup = ct_rpc:call(Node, libp2p_swarm, gossip_group, [Swarm]),
                          wait_until(fun() ->
                                             ConnectedAddrs = ct_rpc:call(Node, libp2p_group_gossip,
                                                                          connected_addrs, [GossipGroup, all]),
                                             TotalNodes == length(ConnectedAddrs)
                                     end, 250, 20),
                          ConnectedAddrs = ct_rpc:call(Node, libp2p_group_gossip,
                                                       connected_addrs, [GossipGroup, all]),
                          ct:pal("Node: ~p~nAddr: ~p~nP2PAddr: ~p~nSessions : ~p~nGossipGroup:"
                                 " ~p~nConnectedAddrs: ~p~nSwarm:~p~nSwarmID: ~p",
                                 [Node,
                                  Addr,
                                  libp2p_crypto:pubkey_bin_to_p2p(Addr),
                                  Sessions,
                                  GossipGroup,
                                  ConnectedAddrs,
                                  Swarm,
                                  SwarmID
                                 ])
                  end, Nodes),

    [{nodes, Nodes}, {num_consensus_members, NumConsensusMembers} | Config].

end_per_testcase(_TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    pmap(fun(Node) -> ct_slave:stop(Node) end, Nodes),
    ok.

create_vars() ->
    create_vars(#{}).

create_vars(Vars) ->
    #{secret := Priv, public := Pub} =
        libp2p_crypto:generate_keys(ecc_compact),

    Vars1 = raw_vars(Vars),
    ct:pal("vars ~p", [Vars1]),

    BinPub = libp2p_crypto:pubkey_to_bin(Pub),

    Txn = blockchain_txn_vars_v1:new(Vars1, 2, #{master_key => BinPub}),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn),
    Txn1 = blockchain_txn_vars_v1:key_proof(Txn, Proof),
    {[Txn1], {master_key, {Priv, Pub}}}.


raw_vars(Vars) ->
    DefVars = #{
                ?chain_vars_version => 2,
                ?vars_commit_delay => 10,
                ?election_version => 2,
                ?election_restart_interval => 5,
                ?election_replacement_slope => 20,
                ?election_replacement_factor => 4,
                ?election_selection_pct => 70,
                ?election_removal_pct => 85,
                ?election_cluster_res => 8,
                ?block_version => v1,
                ?predicate_threshold => 0.85,
                ?num_consensus_members => 7,
                ?monthly_reward => 50000 * 1000000,
                ?securities_percent => 0.35,
                ?poc_challengees_percent => 0.19 + 0.16,
                ?poc_challengers_percent => 0.09 + 0.06,
                ?poc_witnesses_percent => 0.02 + 0.03,
                ?consensus_percent => 0.10,
                ?min_assert_h3_res => 12,
                ?max_staleness => 100000,
                ?alpha_decay => 0.007,
                ?beta_decay => 0.0005,
                ?block_time => 30000,
                ?election_interval => 30,
                ?poc_challenge_interval => 30,
                ?h3_exclusion_ring_dist => 2,
                ?h3_max_grid_distance => 13,
                ?h3_neighbor_res => 12,
                ?min_score => 0.15,
                ?reward_version => 1,
                ?allow_zero_amount => false
               },

    maps:merge(DefVars, Vars).


%%--------------------------------------------------------------------
%% @doc
%% generate a tmp directory based off priv_data to be used as a scratch by common tests
%% @end
%%-------------------------------------------------------------------
-spec init_base_dir_config(atom(), atom(), list()) -> {list(), list()}.
init_base_dir_config(Mod, TestCase, Config)->
    PrivDir = ?config(priv_dir, Config),
    TCName = erlang:atom_to_list(TestCase),
    BaseDir = PrivDir ++ "data/" ++ erlang:atom_to_list(Mod) ++ "_" ++ TCName,
    SimDir = BaseDir ++ "_sim",
    [
        {base_dir, BaseDir},
        {sim_dir, SimDir}
        | Config
    ].

wait_until_height(Node, Height) ->
    wait_until(fun() ->
                       C = ct_rpc:call(Node, blockchain_worker, blockchain, []),
                       {ok, Height} == ct_rpc:call(Node, blockchain, height, [C])
               end, 30, timer:seconds(1)).
