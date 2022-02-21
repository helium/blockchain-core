-module(t_chain).

-export_type([
    t/0,
    ct_cfg/0
]).

-export([
    start/1,
    start/2,
    stop/0,

    commit/3,
    commit_n_empty_blocks/3,
    get_active_gateways/1,
    get_balance/2
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("include/blockchain.hrl").
-include_lib("include/blockchain_vars.hrl").

-type t() ::
    blockchain:blockchain().

-type ct_cfg() ::
    [
          {blockchain_sup, pid()}
        | {chain, t()}
        | {master_key, {libp2p_crypto:privkey(), libp2p_crypto:pubkey()}}
        | {consensus_members, [t_user:t()]}
        | {atom(), term()}
    ].

-type optional_params() ::
    #{
        vars                       => map(),
        default_init_balance_hnt   => non_neg_integer(),
        default_init_balance_dc    => non_neg_integer(),
        default_init_balance_hst   => non_neg_integer(),
        num_init_users             => non_neg_integer(),

        user_master                => t_user:t(),
        user_app                   => t_user:t(),
        users_with_hnt             => [{t_user:t(), non_neg_integer()}],
        users_with_hst             => [{t_user:t(), non_neg_integer()}],
        users_with_dc              => [{t_user:t(), non_neg_integer()}],
        users_as_gateways          => [t_user:t()],
        users_in_consensus         => [t_user:t()]
    }.

-spec start(ct_cfg()) -> ct_cfg().
start(Cfg) ->
    start(Cfg, #{}).

-spec start(ct_cfg(), optional_params()) -> ct_cfg().
start(Cfg, Opts) when is_list(Cfg), is_map(Opts) ->
    DirPerSuite = ?config(priv_dir, Cfg),
    %% priv_dir, when called from:
    %% - init_per_suite, is per suite;
    %% - TestCase, is per test case.
    %% by not relying on it being either way and just creating our own unique
    %% string, we liberate the caller from worrying about the difference
    %% between calling this function from either location.
    DirUnique = filename:join(DirPerSuite, unique_string()),
    DirForData = filename:join(DirUnique, "data"),

    InitBalanceHNT = maps:get(default_init_balance_hnt, Opts, 5000),
    InitBalanceDC  = maps:get(default_init_balance_dc , Opts, 5000),
    InitBalanceHST = maps:get(default_init_balance_hst, Opts, 5000),
    NumInitUsers   = maps:get(num_init_users  , Opts, 8),
    ExtraVars      = maps:get(vars            , Opts, #{}),

    Vars = maps:merge(vars_default(), ExtraVars),
    NumInConsensus = maps:get(num_consensus_members, Vars),

    MasterUser = maps_get_or(user_master, Opts, fun() -> t_user:new() end),
    AppUser    = maps_get_or(user_app   , Opts, fun() -> t_user:new() end),

    UsersGetOrGenPlain =
        fun(K, N) ->
            Gen = fun() -> t_user:n_new(N) end,
            maps_get_or(K , Opts, Gen)
        end,
    UsersGetOrGenWithBalance =
        fun(K, N, DefaultBalance) ->
            Gen = fun() -> [{U, DefaultBalance} || U <- t_user:n_new(N)] end,
            maps_get_or(K , Opts, Gen)
        end,

    UsersWithHNT = UsersGetOrGenWithBalance(users_with_hnt, NumInitUsers, InitBalanceHNT),
    UsersWithHST = UsersGetOrGenWithBalance(users_with_hst, NumInitUsers, InitBalanceHST),
    UsersWithDC  = UsersGetOrGenWithBalance(users_with_dc , NumInitUsers, InitBalanceDC),

    UsersAsGateways  = UsersGetOrGenPlain(users_as_gateways , NumInitUsers),
    UsersInConsensus = UsersGetOrGenPlain(users_in_consensus, NumInConsensus),

    Addrs = fun (Us) -> [t_user:addr(U) || U <- Us] end,

    GenesisTxns =
        [genesis_txn_vars(Vars, t_user:key_pair(MasterUser))] ++
        genesis_txns_pay_hnt([{t_user:addr(U), B} || {U, B} <- UsersWithHNT]) ++
        genesis_txns_pay_dc( [{t_user:addr(U), B} || {U, B} <- UsersWithDC]) ++
        genesis_txns_pay_sec([{t_user:addr(U), B} || {U, B} <- UsersWithHST]) ++
        genesis_txns_consensus(Addrs(UsersAsGateways), election_version(ExtraVars)) ++
        [blockchain_txn_consensus_group_v1:new(Addrs(UsersInConsensus), <<"proof">>, 1, 0)],
    GenesisBlock = blockchain_block:new_genesis_block(GenesisTxns),

    blockchain_utils:teardown_var_cache(),

    ok = application:set_env(blockchain, base_dir, DirForData),
    ?assertMatch(
        {ok, _},
        blockchain_sup:start_link(
            [
                {key, t_user:key_triple(AppUser)},
                {base_dir, DirForData}
            ]
        )
    ),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),
    ?assertEqual(
        ok,
        blockchain_worker:integrate_genesis_block_synchronously(GenesisBlock)
    ),
    Chain = blockchain_worker:blockchain(),
    ?assert(Chain =/= undefined),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    {ok, HeadBlock} = blockchain:head_block(Chain),
    ?assertEqual(
        blockchain_block:hash_block(GenesisBlock),
        blockchain_block:hash_block(HeadBlock)
    ),
    ?assertEqual(
        {ok, GenesisBlock},
        blockchain:head_block(Chain)
    ),
    ?assertEqual(
        {ok, blockchain_block:hash_block(GenesisBlock)},
        blockchain:genesis_hash(Chain)
    ),
    ?assertEqual(
        {ok, GenesisBlock},
        blockchain:genesis_block(Chain)
    ),
    ?assertEqual(
        {ok, 1},
        blockchain:height(Chain)
    ),
    {ok, SwarmPubKey, _, _} = blockchain_swarm:keys(),
    ?assertEqual(
        libp2p_crypto:pubkey_to_bin(SwarmPubKey),
        t_user:addr(AppUser)
    ),
    %% TODO Assert other balances
    lists:foreach(
        fun ({User, Balance}) ->
            ?assertEqual(Balance, get_balance(Chain, User))
        end,
        UsersWithHNT
    ),
    [
        {chain, Chain},

        {users_in_consensus, UsersInConsensus},
        {users_with_hnt    , UsersWithHNT},
        {users_with_hst    , UsersWithHST},
        {users_with_dc     , UsersWithDC},
        {users_as_gateways , UsersAsGateways},

        %% TODO May not need the pair form eventually, as t_user is adopted.
        {master_key, t_user:key_pair(MasterUser)},

        %% XXX Some test cases still expect base_dir in config,
        %%     though it's questionable if they really need it.
        {base_dir, DirForData}
    |
        Cfg
    ].

-spec stop() -> ok.
stop() ->
    Sup = erlang:whereis(blockchain_sup),
    case erlang:is_pid(Sup) andalso erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    ok.

-spec commit_n_empty_blocks(t(), [t_user:t()], pos_integer()) ->
    ok | {error, _}.
commit_n_empty_blocks(Chain, ConsensusMembers, N) ->
    lists:foreach(
        fun (_) -> ok = commit(Chain, ConsensusMembers, []) end,
        lists:seq(1, N)
    ).

%% Make block and add it to chain.
-spec commit(t(), [t_user:t()], [t_txn:t()]) ->
    ok | {error, _}.
commit(Chain, ConsensusMembers, Txns0) ->
    Txns = lists:sort(fun blockchain_txn:sort/2, Txns0),
    case blockchain_txn:validate(Txns, Chain) of
        {_, []} ->
            Block = txns_to_block(Chain, ConsensusMembers, Txns),
            {ok, Height0} = blockchain:height(Chain),
            ok = blockchain:add_block(Block, Chain),
            {ok, Height1} = blockchain:height(Chain),
            ?assertEqual(1 + Height0, Height1),
            ok;
        {_, [_|_]=Invalid} ->
            ct:pal("Invalid transactions: ~p", [Invalid]),
            {error, {invalid_txns, Invalid}}
    end.

get_active_gateways(Chain) ->
    Ledger = blockchain:ledger(Chain),
    blockchain_ledger_v1:active_gateways(Ledger).

%% TODO t-last order
-spec get_balance(t(), t_user:t()) ->
    non_neg_integer().
get_balance(Chain, User) ->
    Addr = t_user:addr(User),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(Addr, Ledger) of
        {error, address_entry_not_found} ->
            0;
        {ok, Entry} ->
            blockchain_ledger_entry_v1:balance(Entry)
    end.

%% Internal ===================================================================

-spec maps_get_or(K, #{K => V}, fun(() -> V)) -> V.
maps_get_or(K, Map, F) ->
    opt_or(maps_get(K, Map), F).

-spec maps_get(K, #{K => V}) ->
    none | {some, V}.
maps_get(K, Map) ->
    case maps:find(K, Map) of
        error -> none;
        {ok, V} -> {some, V}
    end.

-spec opt_or(none | {some, A}, fun(() -> A)) -> A.
opt_or(none, F) -> F();
opt_or({some, X}, _) -> X.

unique_string() ->
    lists:flatten(io_lib:format("~b", [erlang:unique_integer([positive])])).

-spec txns_to_block(blockchain:blockchain(), [t_user:t()], [blockchain_txn:txn()]) ->
    blockchain_block:block().
txns_to_block(Chain, ConsensusMembers, Txns) ->
    {ok, HeadBlock} = blockchain:head_block(Chain),
    {ok, PrevHash} = blockchain:head_hash(Chain),
    Height = blockchain_block:height(HeadBlock) + 1,
    Time = blockchain_block:time(HeadBlock) + 1,
    ct:pal("making block with transactions: ~p", [Txns]),
    BlockParams =
        #{
            prev_hash => PrevHash,
            height => Height,
            transactions => Txns,
            signatures => [],
            time => Time,
            hbbft_round => 0,
            election_epoch => 1,
            epoch_start => 0,
            seen_votes => [],
            bba_completion => <<>>,
            poc_keys => []
        },
    Block0 = blockchain_block_v1:new(BlockParams),
    BinBlock = blockchain_block:serialize(Block0),
    Signatures = sign(ConsensusMembers, BinBlock),
    Block1 = blockchain_block:set_signatures(Block0, Signatures),
    ct:pal("made block: ~p", [Block1]),
    Block1.

-spec sign([t_user:t()], binary()) -> [{Addr :: binary(), Sig :: binary()}].
sign(ConsensusMembers, <<Bin/binary>>) ->
    [{t_user:addr(User), t_user:sign(Bin, User)} || User <- ConsensusMembers].

genesis_txn_vars(Vars, {MasterKeyPriv, MasterKeyPub}) ->
    Txn = blockchain_txn_vars_v1:new(Vars, 2, #{master_key => libp2p_crypto:pubkey_to_bin(MasterKeyPub)}),
    Proof = blockchain_txn_vars_v1:create_proof(MasterKeyPriv, Txn),
    blockchain_txn_vars_v1:key_proof(Txn, Proof).

-spec genesis_txns_pay_hnt([{libp2p_crypto:pubkey_bin(), non_neg_integer()}]) ->
    [blockchain_txn:txn()].
genesis_txns_pay_hnt(AddrBalancePairs) ->
    [blockchain_txn_coinbase_v1:new(A, B) || {A, B} <- AddrBalancePairs].

-spec genesis_txns_pay_dc([{libp2p_crypto:pubkey_bin(), non_neg_integer()}]) ->
    [blockchain_txn:txn()].
genesis_txns_pay_dc(AddrBalancePairs) ->
    [blockchain_txn_dc_coinbase_v1:new(A, B) || {A, B} <- AddrBalancePairs].

-spec genesis_txns_pay_sec([{libp2p_crypto:pubkey_bin(), non_neg_integer()}]) ->
    [blockchain_txn:txn()].
genesis_txns_pay_sec(AddrBalancePairs) ->
    [blockchain_txn_security_coinbase_v1:new(A, B) || {A, B} <- AddrBalancePairs].

-spec genesis_txns_consensus(
    [{libp2p_crypto:pubkey_bin(), non_neg_integer()}],
    gt_5 | lt_5_or_unspecified
) ->
    [blockchain_txn:txn()].
genesis_txns_consensus(AddressesOfGateways, ElectionVersion) ->
    Locations =
        [
            h3:from_geo({37.780586, -122.469470 + I/100}, 12)
        ||
            I <- lists:seq(1, length(AddressesOfGateways))
        ],
    case ElectionVersion of
        gt_5 ->
            [
                blockchain_txn_gen_validator_v1:new(Addr, Addr, ?bones(10000))
            ||
                Addr <- AddressesOfGateways
            ];
        lt_5_or_unspecified ->
            [
                blockchain_txn_gen_gateway_v1:new(Addr, Addr, Loc, 0)
            ||
                {Addr, Loc} <- lists:zip(AddressesOfGateways, Locations)
            ]
    end.

election_version(#{election_version := V}) when V >= 5 ->
    gt_5;
election_version(_) ->
    lt_5_or_unspecified.

vars_default() ->
    #{
        ?allow_zero_amount                => false,
        ?alpha_decay                      => 0.007,
        ?beta_decay                       => 0.0005,
        ?block_time                       => 30000,
        ?block_version                    => v1,
        ?chain_vars_version               => 2,
        ?consensus_percent                => 0.10,
        ?dc_payload_size                  => 24,
        ?election_cluster_res             => 8,
        ?election_interval                => 30,
        ?election_removal_pct             => 85,
        ?election_replacement_factor      => 4,
        ?election_replacement_slope       => 20,
        ?election_restart_interval        => 5,
        ?election_selection_pct           => 70,
        ?election_version                 => 2,
        ?h3_exclusion_ring_dist           => 2,
        ?h3_max_grid_distance             => 13,
        ?h3_neighbor_res                  => 12,
        ?max_open_sc                      => 2,
        ?max_staleness                    => 100000,
        ?max_subnet_num                   => 20,
        ?max_subnet_size                  => 65536,
        ?max_xor_filter_num               => 5,
        ?max_xor_filter_size              => 1024*100,
        ?min_assert_h3_res                => 12,
        ?min_expire_within                => 10,
        ?min_score                        => 0.15,
        ?min_subnet_size                  => 8,
        ?monthly_reward                   => ?bones(5000000),
        ?num_consensus_members            => 7,
        ?poc_centrality_wt                => 0.5,
        ?poc_challenge_interval           => 30,
        ?poc_challengees_percent          => 0.19 + 0.16,
        ?poc_challengers_percent          => 0.09 + 0.06,
        ?poc_good_bucket_high             => -80,
        ?poc_good_bucket_low              => -132,
        ?poc_max_hop_cells                => 2000,
        ?poc_path_limit                   => 7,
        ?poc_target_hex_parent_res        => 5,
        ?poc_typo_fixes                   => true,
        ?poc_v4_prob_count_wt             => 0.0,
        ?poc_v4_prob_rssi_wt              => 0.0,
        ?poc_v4_prob_time_wt              => 0.0,
        ?poc_v4_randomness_wt             => 0.5,
        ?poc_v4_target_prob_edge_wt       => 0.0,
        ?poc_v4_target_prob_score_wt      => 0.0,
        ?poc_v5_target_prob_randomness_wt => 1.0,
        ?poc_version                      => 8,
        ?poc_witnesses_percent            => 0.02 + 0.03,
        ?predicate_threshold              => 0.85,
        ?reward_version                   => 1,
        ?securities_percent               => 0.35,
        ?vars_commit_delay                => 10,
        ?witness_refresh_interval         => 10,
        ?witness_refresh_rand_n           => 100
    }.
