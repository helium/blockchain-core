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
         download_serialized_region/1,
         find_connected_node_pair/1
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
    lists:sublist(blockchain_utils:shuffle(List), N).

init_per_suite(Config) ->
    application:ensure_all_started(ranch),
    application:set_env(lager, error_logger_flush_queue, false),
    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, debug),
    application:ensure_all_started(throttle),
    application:ensure_all_started(telemetry),
    Config.

init_per_testcase(TestCase, Config) ->
    BaseDir = ?config(base_dir, Config),
    LogDir = ?config(log_dir, Config),
    SCClientTransportHandler = ?config(sc_client_transport_handler, Config),
    application:set_env(blockchain, testing, true),

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
    PeerCacheTimeout = 10000,
    Port = get_config("PORT", 0),

    NodeNames = lists:map(fun(_M) -> list_to_atom(randname(5)) end, lists:seq(1, TotalNodes)),

    Nodes =
        pmap(
            fun(Node) ->
                 start_node(Node, Config, TestCase)
             end, NodeNames),

    pmap(
        fun(Node) ->
            LogRoot = LogDir ++ "_" ++ atom_to_list(Node),

            %% set blockchain configuration
            #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
            Key = {PubKey, libp2p_crypto:mk_sig_fun(PrivKey), libp2p_crypto:mk_ecdh_fun(PrivKey)},
            BlockchainBaseDir = BaseDir ++ "_" ++ atom_to_list(Node),

            ct_rpc:call(Node, cover, start, []),

            application_load(Node,
                [lager, blockchain, libp2p, erlang_stats, grpcbox]),

            application_set_env(Node, [
                % give each node its own log directory
                {lager, log_root, LogRoot},
                {blockchain, testing, true},
                {blockchain, enable_nat, false},
                {blockchain, base_dir, BlockchainBaseDir},
                {blockchain, num_consensus_members, NumConsensusMembers},
                {blockchain, port, Port},
                {blockchain, seed_nodes, SeedNodes},
                {blockchain, key, Key},
                {blockchain, peer_cache_timeout, PeerCacheTimeout},
                {blockchain, sc_client_handler, sc_client_test_handler},
                {blockchain, sc_packet_handler, sc_packet_test_handler},
                {blockchain, peerbook_update_interval, 200},
                {blockchain, peerbook_allow_rfc1918, true},
                {blockchain, max_inbound_connections, TotalNodes*2},
                {blockchain, outbound_gossip_connections, TotalNodes},
                {blockchain, listen_interface, "127.0.0.1"},
                {blockchain, sc_client_transport_handler, SCClientTransportHandler},
                {blockchain, sc_sup_type, testing}
            ]),

            {Node, {ok, StartedApps3}} = {
                Node,
                ct_rpc:call(Node,
                    application, ensure_all_started, [blockchain])},
            log("Node: ~p, StartedApps: ~p", [Node, StartedApps3]),

            ct_rpc:call(Node, lager, set_loglevel, [{lager_file_backend, "log/console.log"}, debug])

        end, Nodes),

    %% accumulate the listen addr of all the nodes
    Addrs = pmap(
              fun(Node) ->
                      Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, [], 2000),
                      [H|_] = ct_rpc:call(Node, libp2p_swarm, listen_addrs, [Swarm], 2000),
                      H
              end, Nodes),


    %% connect the nodes
    pmap(
      fun(Node) ->
              Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, [], 2000),
              lists:foreach(
                fun(A) ->
                        ct_rpc:call(Node, libp2p_swarm, connect, [Swarm, A], 2000)
                end, Addrs)
      end, Nodes),

    %% make sure each node is gossiping with a majority of its peers
    ok = wait_until(
             fun() ->
                     lists:all(
                       fun(Node) ->
                               try
                                   GossipPeers = ct_rpc:call(Node, blockchain_swarm, gossip_peers, [], 500),
                                   case length(GossipPeers) >= (length(Nodes) / 2) + 1 of
                                       true -> true;
                                       false ->
                                           ct:pal("~p is not connected to enough peers ~p", [Node, GossipPeers]),
                                           Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, [], 500),
                                           lists:foreach(
                                             fun(A) ->
                                                     CRes = ct_rpc:call(Node, libp2p_swarm, connect, [Swarm, A], 500),
                                                     ct:pal("Connecting ~p to ~p: ~p", [Node, A, CRes])
                                             end, Addrs),
                                           false
                                   end
                               catch _C:_E ->
                                       false
                               end
                       end, Nodes)
             end, 200, 150),

    %% to enable the tests to run over grpc we need to deterministically set the grpc listen addr
    %% with libp2p all the port data is in the peer entries
    %% in the real world we would run grpc over a known port
    %% but for the sake of the tests which run multiple nodes on a single instance
    %% we need to choose a random port for each node
    %% and the client needs to know which port was choosen
    %% so for the sake of the tests what we do here is get the libp2p port
    %% and run grpc on that value + 1000
    %% the client then just has to pull the libp2p peer data
    %% retrieve the libp2p port and derive the grpc port from that

    GRPCServerConfigFun = fun(PeerPort)->
         [#{grpc_opts => #{service_protos => [state_channel_pb],
                           services => #{'helium.state_channel' => blockchain_grpc_sc_server_handler}
                            },

            transport_opts => #{ssl => false},

            listen_opts => #{port => PeerPort,
                             ip => {0,0,0,0}},

            pool_opts => #{size => 2},

            server_opts => #{header_table_size => 4096,
                             enable_push => 1,
                             max_concurrent_streams => unlimited,
                             initial_window_size => 65535,
                             max_frame_size => 16384,
                             max_header_list_size => unlimited}}]
             end,

    ok = lists:foreach(fun(Node) ->
            Swarm = ct_rpc:call(Node, blockchain_swarm, swarm, []),
            TID = ct_rpc:call(Node, blockchain_swarm, tid, []),
            ListenAddrs = ct_rpc:call(Node, libp2p_swarm, listen_addrs, [Swarm]),
            [H | _ ] = _SortedAddrs = ct_rpc:call(Node, libp2p_transport, sort_addrs, [TID, ListenAddrs]),
            [_, _, _IP,_, Libp2pPort] = _Full = re:split(H, "/"),
             ThisPort = list_to_integer(binary_to_list(Libp2pPort)),
            ct_rpc:call(Node, application, set_env, [grpcbox, servers, GRPCServerConfigFun(ThisPort + 1000)]),
            ct_rpc:call(Node, application, ensure_all_started, [grpcbox])
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
                ?vars_commit_delay => 1,
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
                       {ok, NodeHeight} = ct_rpc:call(Node, blockchain, height, [C]),
                        ct:pal("node height ~p, waiting for height ~p", [NodeHeight, Height]),
                        NodeHeight == Height
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


-spec find_connected_node_pair([{node(), libp2p_crypto:pubkey_bin()}]) ->
    {node(), node()}.
find_connected_node_pair([{FirstNode, _} | NodeAddrList]) ->
    AddrToNodeMap =
        lists:foldl(
            fun({Node, Addr}, Acc) ->
                AddrStr = libp2p_crypto:pubkey_bin_to_p2p(Addr),
                Acc#{AddrStr => Node}
            end, #{}, NodeAddrList),
    [ConnectedNode | _] =
        addr_to_node(
            ct_rpc:call(FirstNode, blockchain_swarm, gossip_peers, [], 500),
            AddrToNodeMap),
    {FirstNode, ConnectedNode}.


-spec addr_to_node([string()], #{string() => node()}) -> [node()].
addr_to_node(Addrs, AddrToNodeMap) ->
    [maps:get(Addr, AddrToNodeMap, undefined) || Addr <- Addrs].


-spec application_load(node(), [atom()]) -> ok.
application_load(Node, Applications) ->
    lists:foreach(
        fun(Application) ->
            ct_rpc:call(Node, application, load, [Application])
        end, Applications).


-spec application_set_env(node(), [{atom(), atom(), term()}]) -> ok.
application_set_env(Node, Env) ->
    lists:foreach(
        fun({Application, Par, Val}) ->
            ok = ct_rpc:call(
                Node, application, set_env, [Application, Par, Val])
        end, Env).
