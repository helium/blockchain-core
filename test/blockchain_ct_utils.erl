-module(blockchain_ct_utils).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("include/blockchain_vars.hrl").
-include("include/blockchain.hrl").
-include("blockchain_ct_utils.hrl").

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
         init_per_suite/1,
         end_per_testcase/2,
         create_vars/0, create_vars/1,
         raw_vars/1,
         init_base_dir_config/3,
         join_packet/3,
         ledger/2,
         destroy_ledger/0,
         download_serialized_region/1
        ]).

-ifdef(EQC).
log(Format, Args) ->
    lager:debug(Format, Args).
-else.
log(Format, Args) ->
    ct:pal(Format, Args).
-endif.

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

init_per_suite(Config) ->
    application:ensure_all_started(ranch),
    application:set_env(lager, error_logger_flush_queue, false),
    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, debug),
    application:ensure_all_started(throttle),
    Config.

init_per_testcase(TestCase, Config) ->
    BaseDir = ?config(base_dir, Config),
    LogDir = ?config(log_dir, Config),
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
                                LogRoot = LogDir ++ "_" ++ atom_to_list(Node),
                                ct_rpc:call(Node, application, set_env, [lager, log_root, LogRoot]),
                                ct_rpc:call(Node, lager, set_loglevel, [{lager_file_backend, "log/console.log"}, debug]),

                                %% set blockchain configuration
                                #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
                                Key = {PubKey, libp2p_crypto:mk_sig_fun(PrivKey), libp2p_crypto:mk_ecdh_fun(PrivKey)},
                                BlockchainBaseDir = BaseDir ++ "_" ++ atom_to_list(Node),
                                ct_rpc:call(Node, application, set_env, [blockchain, enable_nat, false]),
                                ct_rpc:call(Node, application, set_env, [blockchain, base_dir, BlockchainBaseDir]),
                                ct_rpc:call(Node, application, set_env, [blockchain, num_consensus_members, NumConsensusMembers]),
                                ct_rpc:call(Node, application, set_env, [blockchain, port, Port]),
                                ct_rpc:call(Node, application, set_env, [blockchain, seed_nodes, SeedNodes]),
                                ct_rpc:call(Node, application, set_env, [blockchain, key, Key]),
                                ct_rpc:call(Node, application, set_env, [blockchain, peer_cache_timeout, PeerCacheTimeout]),
                                ct_rpc:call(Node, application, set_env, [blockchain, sc_client_handler, sc_client_test_handler]),
                                ct_rpc:call(Node, application, set_env, [blockchain, sc_packet_handler, sc_packet_test_handler]),

                                {ok, StartedApps} = ct_rpc:call(Node, application, ensure_all_started, [blockchain]),
                                log("Node: ~p, StartedApps: ~p", [Node, StartedApps])
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
                                     end, 50, 20),
                          ConnectedAddrs = ct_rpc:call(Node, libp2p_group_gossip,
                                                       connected_addrs, [GossipGroup, all]),
                          log("Node: ~p~nAddr: ~p~nP2PAddr: ~p~nSessions : ~p~nGossipGroup:"
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

end_per_testcase(TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    pmap(fun(Node) -> ct_slave:stop(Node) end, Nodes),
    case ?config(tc_status, Config) of
        ok ->
            %% test passed, we can cleanup
            cleanup_per_testcase(TestCase, Config);
        _ ->
            %% leave results alone for analysis
            ok
    end,
    {comment, done}.

cleanup_per_testcase(_TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    BaseDir = ?config(base_dir, Config),
    LogDir = ?config(log_dir, Config),
    lists:foreach(fun(Node) ->
                          LogRoot = LogDir ++ "_" ++ atom_to_list(Node),
                          Res = os:cmd("rm -rf " ++ LogRoot),
                          log("rm -rf ~p -> ~p", [LogRoot, Res]),
                          DataDir = BaseDir ++ "_" ++ atom_to_list(Node),
                          Res2 = os:cmd("rm -rf " ++ DataDir),
                          log("rm -rf ~p -> ~p", [DataDir, Res2]),
                          ok
                  end, Nodes).

create_vars() ->
    create_vars(#{}).

create_vars(Vars) ->
    #{secret := Priv, public := Pub} =
        libp2p_crypto:generate_keys(ecc_compact),

    Vars1 = raw_vars(Vars),
    log("vars ~p", [Vars1]),

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
                ?monthly_reward => ?bones(5000000),
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
                ?allow_zero_amount => false,
                ?poc_version => 8,
                ?poc_good_bucket_low => -132,
                ?poc_good_bucket_high => -80,
                ?poc_v5_target_prob_randomness_wt => 1.0,
                ?poc_v4_target_prob_edge_wt => 0.0,
                ?poc_v4_target_prob_score_wt => 0.0,
                ?poc_v4_prob_rssi_wt => 0.0,
                ?poc_v4_prob_time_wt => 0.0,
                ?poc_v4_randomness_wt => 0.5,
                ?poc_v4_prob_count_wt => 0.0,
                ?poc_centrality_wt => 0.5,
                ?poc_max_hop_cells => 2000,
                ?poc_path_limit => 7,
                ?poc_typo_fixes => true,
                ?poc_target_hex_parent_res => 5,
                ?witness_refresh_interval => 10,
                ?witness_refresh_rand_n => 100,
                ?max_open_sc => 2,
                ?min_expire_within => 10,
                ?max_xor_filter_size => 1024*100,
                ?max_xor_filter_num => 5,
                ?max_subnet_size => 65536,
                ?min_subnet_size => 8,
                ?max_subnet_num => 20,
                ?dc_payload_size => 24
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
    LogDir = PrivDir ++ "logs/" ++ erlang:atom_to_list(Mod) ++ "_" ++ TCName,
    SimDir = BaseDir ++ "_sim",
    application:set_env(blockchain, base_dir, BaseDir),
    [
        {base_dir, BaseDir},
        {sim_dir, SimDir},
        {log_dir, LogDir}
        | Config
    ].

wait_until_height(Node, Height) ->
    wait_until(fun() ->
                       C = ct_rpc:call(Node, blockchain_worker, blockchain, []),
                       {ok, Height} == ct_rpc:call(Node, blockchain, height, [C])
               end, 30, timer:seconds(1)).

join_packet(AppKey, DevNonce, RSSI) ->
    RoutingInfo = {devaddr, 1207959553},
    blockchain_helium_packet_v1:new(lorawan,
                                    join_payload(AppKey, DevNonce),
                                    1000,
                                    RSSI,
                                    923.3,
                                    <<"SF8BW125">>,
                                    0.0,
                                    RoutingInfo).

join_payload(AppKey, DevNonce) ->
    MType = ?JOIN_REQUEST,
    MHDRRFU = 0,
    Major = 0,
    AppEUI = reverse_bin(?APPEUI),
    DevEUI = reverse_bin(?DEVEUI),
    Payload0 = <<MType:3, MHDRRFU:3, Major:2, AppEUI:8/binary, DevEUI:8/binary, DevNonce:2/binary>>,
    MIC = crypto:macN(cmac, aes_128_cbc, AppKey, Payload0, 4),
    <<Payload0/binary, MIC:4/binary>>.

reverse_bin(Bin) -> reverse_bin(Bin, <<>>).
reverse_bin(<<>>, Acc) -> Acc;
reverse_bin(<<H:1/binary, Rest/binary>>, Acc) ->
    reverse_bin(Rest, <<H/binary, Acc/binary>>).

ledger(ExtraVars, S3URL) ->
    %% Ledger at height: 481929
    %% ActiveGateway Count: 8000
    {ok, Dir} = file:get_cwd(),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    %% Path to static ledger tar
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    %% Extract ledger tar if required
    ok = extract_ledger_tar(PrivDir, LedgerTar, S3URL),
    %% Get the ledger
    Ledger = blockchain_ledger_v1:new(PrivDir),
    %% Get current ledger vars
    LedgerVars = ledger_vars(Ledger),
    %% Ensure the ledger has the vars we're testing against
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    blockchain_ledger_v1:vars(maps:merge(LedgerVars, ExtraVars), [], Ledger1),
    %% If the hexes aren't on the ledger add them
    blockchain:bootstrap_hexes(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    Ledger.

extract_ledger_tar(PrivDir, LedgerTar, S3URL) ->
    case filelib:is_file(LedgerTar) of
        true ->
            %% if we have already unpacked it, no need to do it again
            LedgerDB = filename:join([PrivDir, "ledger.db"]),
            case filelib:is_dir(LedgerDB) of
                true ->
                    ok;
                false ->
                    %% ledger tar file present, extract
                    erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
            end;
        false ->
            %% ledger tar file not found, download & extract
            ok = ssl:start(),
            {ok, {{_, 200, "OK"}, _, Body}} = httpc:request(S3URL),
            ok = file:write_file(filename:join([PrivDir, "ledger.tar.gz"]), Body),
            erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
    end.

ledger_vars(Ledger) ->
    blockchain_utils:vars_binary_keys_to_atoms(maps:from_list(blockchain_ledger_v1:snapshot_vars(Ledger))).

destroy_ledger() ->
    {ok, Dir} = file:get_cwd(),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    LedgerDB = filename:join([PrivDir, "ledger.db"]),

    case filelib:is_file(LedgerTar) of
        true ->
            %% we found a ledger tarball, remove it
            file:delete(LedgerTar);
        false ->
            ok
    end,
    case filelib:is_dir(LedgerDB) of
        true ->
            %% we found a ledger.db, remove it
            file:del_dir(LedgerDB);
        false ->
            %% ledger.db dir not found, don't do anything
            ok
    end.

download_serialized_region(URL) ->
    %% Example URL: "https://github.com/JayKickliter/lorawan-h3-regions/blob/main/serialized/US915.res7.h3idx?raw=true"
    {ok, Dir} = file:get_cwd(),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    ok = ssl:start(),
    {ok, {{_, 200, "OK"}, _, Body}} = httpc:request(URL),
    FName = hd(string:tokens(hd(lists:reverse(string:tokens(URL, "/"))), "?")),
    FPath = filename:join([PrivDir, FName]),
    ok = file:write_file(FPath, Body),
    {ok, Data} = file:read_file(FPath),
    Data.

