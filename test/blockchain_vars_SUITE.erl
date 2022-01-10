-module(blockchain_vars_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("blockchain_vars.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    version_change_test/1,
    master_key_test/1,
    cache_test/1,
    hook_test/1
]).

%% Setup ----------------------------------------------------------------------

all() ->
    [
        version_change_test,
        master_key_test,
        cache_test,
        hook_test
    ].

init_per_testcase(_TestCase, Cfg) ->
    Opts =
        #{
            vars =>
                #{
                    %% Setting vars_commit_delay to 1 is crucial,
                    %% otherwise var changes will not take effect.
                    ?vars_commit_delay => 1
                }
        },
    t_chain:start(Cfg, Opts).

end_per_testcase(_TestCase, _Cfg) ->
    t_chain:stop().

%% Cases ----------------------------------------------------------------------

version_change_test(Cfg) ->
    {Priv, _} = ?config(master_key, Cfg),
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    Key = garbage_value,

    %% enable vars v1
    %% (enabling them at init causes init failures, so we do it here, post init)
    ?assertMatch(
        ok,
        var_set(?chain_vars_version, 1, Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, 1}, var_get(?chain_vars_version, Chain)),

    %% check that vars v1 are working
    ?assertMatch(
        ok,
        var_set_legacy(Key, totes_goats_garb, Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, totes_goats_garb}, var_get(Key, Chain)),

    %% switch back to vars v2
    ?assertMatch(
        ok,
        var_set_legacy(?chain_vars_version, 2, Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, 2}, var_get(?chain_vars_version, Chain)),

    %% check that vars v2 are working
    ?assertMatch(
        ok,
        var_set(Key, goats_are_not_garb, Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, goats_are_not_garb}, var_get(Key, Chain)),

    %% ensure vars v1 are no longer accepted
    ?assertMatch(
        {error, {invalid_txns, [{_, bad_block_proof}]}},
        var_set_legacy(Key, goats_are_too_garb, Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, goats_are_not_garb}, var_get(Key, Chain)),

    ok.

master_key_test(Cfg) ->
    {Priv1, _Pub1} = ?config(master_key, Cfg),
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    Key = garbage_value,

    Val1 = totes_goats_garb,
    ?assertMatch(ok, var_set(Key, Val1, Priv1, ConsensusMembers, Chain)),
    ?assertEqual({ok, Val1}, var_get(Key, Chain)),

    %% bad master key
    #{secret := Priv2, public := Pub2} = libp2p_crypto:generate_keys(ecc_compact),
    BinPub2 = libp2p_crypto:pubkey_to_bin(Pub2),

    Txn2_0 =
        blockchain_txn_vars_v1:new(
            #{Key => goats_are_not_garb},
            nonce_next(Chain),
            #{master_key => BinPub2}
        ),
    Proof2 = blockchain_txn_vars_v1:create_proof(Priv1, Txn2_0),
    Txn2_1 = blockchain_txn_vars_v1:proof(Txn2_0, Proof2),


    KeyProof2 = blockchain_txn_vars_v1:create_proof(Priv2, Txn2_0),
    Txn2_2_Good = blockchain_txn_vars_v1:key_proof(Txn2_1, KeyProof2),

    Txn2_2_Bad = blockchain_txn_vars_v1:key_proof(Txn2_1, <<Proof2/binary, "corruption">>),

    ?assertMatch(
        {error, {invalid_txns, [{_, bad_master_key}]}},
        t_chain:commit(Chain, ConsensusMembers, [Txn2_2_Bad])
    ),

    %% good master key
    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [Txn2_2_Good])),

    %% make sure old master key is no longer working
    ?assertMatch(
        {error, {invalid_txns, [{_, bad_block_proof}]}},
        var_set(Key, goats_are_too_garb, Priv1, ConsensusMembers, Chain)
    ),

    %% double check that new master key works
    ?assertMatch(
        ok,
        var_set(Key, goats_always_win, Priv2, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, goats_always_win}, var_get(Key, Chain)),

    %% test all the multikey stuff

    %% first enable them
    ?assertMatch(
        ok,
        var_set(?use_multi_keys, true, Priv2, ConsensusMembers, Chain)
    ),

    #{secret := Priv3, public := Pub3} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := Priv4, public := Pub4} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := Priv5, public := Pub5} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := Priv6, public := Pub6} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := Priv7, public := Pub7} = libp2p_crypto:generate_keys(ecc_compact),
    BinPub3 = libp2p_crypto:pubkey_to_bin(Pub3),
    BinPub4 = libp2p_crypto:pubkey_to_bin(Pub4),
    BinPub5 = libp2p_crypto:pubkey_to_bin(Pub5),
    BinPub6 = libp2p_crypto:pubkey_to_bin(Pub6),
    BinPub7 = libp2p_crypto:pubkey_to_bin(Pub7),

    Txn7_0 = blockchain_txn_vars_v1:new(
               #{Key => goat_jokes_are_so_single_key}, nonce_next(Chain),
               #{multi_keys => [BinPub2, BinPub3, BinPub4, BinPub5, BinPub6]}),
    Proofs7 = [blockchain_txn_vars_v1:create_proof(P, Txn7_0)
               %% shuffle the proofs to make sure we no longer need
               %% them in the correct order
               || P <- blockchain_utils:shuffle([Priv2, Priv3, Priv4, Priv5, Priv6])],
    Txn7_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn7_0, Proofs7),
    Proof7 = blockchain_txn_vars_v1:create_proof(Priv2, Txn7_1),
    Txn7 = blockchain_txn_vars_v1:proof(Txn7_1, Proof7),
    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [Txn7])),

    %% try with only three keys (and succeed)
    ?assertMatch(
        ok,
        var_set(
            Key,
            but_what_now,
            [Priv2, Priv3, Priv6],
            ConsensusMembers,
            Chain
        )
    ),
    ?assertMatch({ok, but_what_now}, var_get(Key, Chain)),

    %% try with only two keys (and fail)
    NonceForTxn9 = nonce_next(Chain),
    Txn9 = mvars(#{Key => sheep_jokes}, NonceForTxn9, [Priv3, Priv6]),
    ?assertMatch(
        {error, {invalid_txns, [{_, insufficient_votes}]}},
        t_chain:commit(Chain, ConsensusMembers, [Txn9])
    ),

    %% try with two valid and one corrupted key proof (and fail again)
    Txn10_0 = blockchain_txn_vars_v1:new(#{Key => cmon}, nonce_next(Chain)),
    Proofs10_0 = [blockchain_txn_vars_v1:create_proof(P, Txn10_0)
                || P <- [Priv2, Priv3, Priv4]],
    [Proof10 | Rem] = Proofs10_0,
    Proof10Corrupted = <<Proof10/binary, "asdasdasdas">>,
    Txn10 = blockchain_txn_vars_v1:multi_proofs(Txn10_0, [Proof10Corrupted | Rem]),

    ?assertMatch(
        {error, {invalid_txns, [{_, insufficient_votes}]}},
        t_chain:commit(Chain, ConsensusMembers, [Txn10])
    ),

    %% TODO Why agin? We already tried Txn9
    ?assertMatch(
        {error, {invalid_txns, [{_, insufficient_votes}]}},
        t_chain:commit(Chain, ConsensusMembers, [Txn9])
    ),

    %% make sure that we safely ignore bad proofs and keys
    #{secret := Priv8, public := _Pub8} = libp2p_crypto:generate_keys(ecc_compact),

    Txn11a =
        mvars(
            #{Key => sheep_are_inherently_unfunny},
            NonceForTxn9,
            [Priv2, Priv3, Priv4, Priv5, Priv6, Priv8]
        ),
    ?assertMatch(
        {error, {invalid_txns, [{_, too_many_proofs}]}},
        t_chain:commit(Chain, ConsensusMembers, [Txn11a])
    ),

    ?assertMatch(
        ok,
        var_set(
            Key,
            sheep_are_inherently_unfunny,
            [Priv2, Priv3, Priv5, Priv6, Priv8],
            ConsensusMembers,
            Chain
        )
    ),
    ?assertMatch({ok, sheep_are_inherently_unfunny}, var_get(Key, Chain)),

    Txn12_0 =
        blockchain_txn_vars_v1:new(
            #{Key => so_true},
            nonce_next(Chain),
            #{multi_keys => [BinPub3, BinPub4, BinPub5, BinPub6, BinPub7]}
        ),
    Proofs12 = [blockchain_txn_vars_v1:create_proof(P, Txn12_0)
                %% shuffle the proofs to make sure we no longer need
                %% them in the correct order
                || P <- blockchain_utils:shuffle([Priv7])],
    Txn12_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn12_0, Proofs12),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn12_1)
               || P <- [Priv3, Priv4, Priv5]],
    Txn12 = blockchain_txn_vars_v1:multi_proofs(Txn12_1, Proofs),
    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [Txn12])),
    ?assertMatch({ok, so_true}, var_get(Key, Chain)),

    ?assertMatch(
        ok,
        var_set(
            Key,
            lets_all_hate_on_sheep,
            [Priv5, Priv6, Priv7],
            ConsensusMembers,
            Chain
        )
    ),
    ?assertMatch({ok, lets_all_hate_on_sheep}, var_get(Key, Chain)),
    ok.

cache_test(Config) ->
    Chain = ?config(chain, Config),
    BaseDir = ?config(base_dir, Config),
    Ledger = blockchain:ledger(Chain),

    %% XXX disabled for now because new e2qc has no stats
    %Hits0 = proplists:get_value(hits, blockchain_utils:var_cache_stats()),
    %{ok, EV} = blockchain:config(?election_version, Ledger),
    %Hits1 = proplists:get_value(hits, blockchain_utils:var_cache_stats()),
    %?assertEqual(1, Hits1 - Hits0),

    AuxLedger0 = blockchain_aux_ledger_v1:bootstrap(
        filename:join([BaseDir, "cache_test.db"]),
        Ledger
    ),
    AuxLedger = blockchain_ledger_v1:mode(aux, AuxLedger0),

    AuxEV = 100,
    AuxVars = #{election_version => AuxEV},

    %% This resets the cache
    ok = blockchain_aux_ledger_v1:set_vars(AuxVars, AuxLedger),
    %% This should be zero now
    %0 = proplists:get_value(hits, blockchain_utils:var_cache_stats()),

    %% Main ledger should have the same election_version
    {ok, EV} = blockchain:config(?election_version, Ledger),

    %% aux ledger should have election_version=100
    {ok, AuxEV} = blockchain:config(?election_version, AuxLedger),

    #{election_version := EV} = blockchain_utils:get_vars([?election_version], Ledger),
    #{election_version := AuxEV} = blockchain_utils:get_vars([?election_version], AuxLedger),

    %% There should be some hits to cache by now
    %Hits2 = proplists:get_value(hits, blockchain_utils:var_cache_stats()),
    %true = Hits2 > 0,

    ok.

hook_test(Cfg) ->
    meck:new(blockchain_txn_vars_v1, [passthrough]),
    {Priv, _} = ?config(master_key, Cfg),
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    Ledger = blockchain:ledger(Chain),
    SelfPid = self(),

    Key = garbage_value,

    %% enable vars v1
    %% (enabling them at init causes init failures, so we do it here, post init)
    ?assertMatch(
        ok,
        var_set(?chain_vars_version, 1, Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, 1}, var_get(?chain_vars_version, Chain)),

    %% at this point, ledger reports nonce=3
    ?assertEqual({ok, 3}, blockchain_ledger_v1:vars_nonce(Ledger)),

    meck:expect(blockchain_txn_vars_v1,
                var_hook,
                fun(garbage_value, reset_vars_nonce, _Ledger) ->
                        {ok, var_hook_invoked}
                end),

    %% we will use garbage_value:reset_vars_nonce and invoke var_hook
    ?assertMatch(
        ok,
        var_set_legacy(Key, reset_vars_nonce, Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({ok, reset_vars_nonce}, var_get(Key, Chain)),

    %% Checking whether the above meck expectation got met
    %% It's unclear why neither meck:called | meck:validate | meck:num_calls return
    %% the right answer, but this works
    MeckHist1 = meck:history(blockchain_txn_vars_v1),
    {SelfPid, _, {ok, var_hook_invoked}} = lists:keyfind({ok, var_hook_invoked}, 3, MeckHist1),

    meck:expect(blockchain_txn_vars_v1,
                unset_hook,
                fun(garbage_value, _Ledger) ->
                        {ok, unset_hook_invoked}
                end),

    %% next we will unset garbage_value and invoke the unset_hook
    ?assertMatch(
        ok,
        var_unset_legacy([Key], Priv, ConsensusMembers, Chain)
    ),
    ?assertEqual({error, not_found}, var_get(Key, Chain)),

    MeckHist2 = meck:history(blockchain_txn_vars_v1),
    {SelfPid, _, {ok, unset_hook_invoked}} = lists:keyfind({ok, unset_hook_invoked}, 3, MeckHist2),

    meck:unload(blockchain_txn_vars_v1),
    ok.


%% Helpers --------------------------------------------------------------------

-spec nonce_curr(blockchain:blockchain()) -> integer().
nonce_curr(Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, Nonce} = blockchain_ledger_v1:vars_nonce(Ledger),
    Nonce.

-spec nonce_next(blockchain:blockchain()) -> integer().
nonce_next(Chain) ->
    nonce_curr(Chain) + 1.

%% TODO refactor as t_txn
var_set(Key, Val, PrivKey, ConsensusMembers, Chain) ->
    Nonce = nonce_next(Chain),
    MakeTxn =
        case is_list(PrivKey) of
            true -> fun mvars/3;
            false -> fun vars/3
        end,
    Txn = MakeTxn(#{Key => Val}, Nonce, PrivKey),
    t_chain:commit(Chain, ConsensusMembers, [Txn]).

%% TODO refactor as t_txn
var_set_legacy(Key, Val, PrivKey, ConsensusMembers, Chain) ->
    Vars = #{Key => Val},
    Txn1 = blockchain_txn_vars_v1:new(Vars, nonce_next(Chain)),
    Proof = blockchain_txn_vars_v1:legacy_create_proof(PrivKey, Vars),
    Txn2 = blockchain_txn_vars_v1:proof(Txn1, Proof),
    t_chain:commit(Chain, ConsensusMembers, [Txn2]).

%% TODO refactor as t_txn
var_unset_legacy(Keys, PrivKey, ConsensusMembers, Chain) ->
    Vars = #{},
    Txn1 = blockchain_txn_vars_v1:new(Vars, nonce_next(Chain), #{unsets => Keys}),
    Proof = blockchain_txn_vars_v1:legacy_create_proof(PrivKey, Vars),
    Txn2 = blockchain_txn_vars_v1:proof(Txn1, Proof),
    t_chain:commit(Chain, ConsensusMembers, [Txn2]).


%% TODO refactor as t_chain
var_get(Key, Chain) ->
    Ledger = blockchain:ledger(Chain),
    blockchain:config(Key, Ledger).

vars(Map, Nonce, Priv) ->
    Txn0 = blockchain_txn_vars_v1:new(Map, Nonce),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn0),
    blockchain_txn_vars_v1:proof(Txn0, Proof).

mvars(Map, Nonce, Privs) ->
    Txn = blockchain_txn_vars_v1:new(Map, Nonce),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn) || P <- blockchain_utils:shuffle(Privs)],
    blockchain_txn_vars_v1:multi_proofs(Txn, Proofs).
