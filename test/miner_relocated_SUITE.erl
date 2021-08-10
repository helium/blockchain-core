-module(miner_relocated_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("blockchain_vars.hrl").

-export([
    all/0
]).

-export([
    master_key_test/1
]).

%% Setup ----------------------------------------------------------------------

all() ->
    [
        master_key_test
    ].

%% Cases ----------------------------------------------------------------------

master_key_test(Cfg0) ->
    Cfg1 = blockchain_ct_utils:init_base_dir_config(?MODULE, master_key_test, Cfg0),
    {ok, _Sup, Keys={_, _}, _Opts} = test_utils:init(?config(base_dir, Cfg1)),

    Key = garbage_value,
    Val0 = init_garbage,
    {
        ok,
        _GenesisMembers,
        _GenesisBlock,
        ConsensusMembers,
        {master_key, {Priv1, _Pub1}}
    } =
        test_utils:init_chain(
            5000,
            Keys,
            true,
            #{
                Key => Val0,

                %% Setting vars_commit_delay to 1 is crucial,
                %% otherwise var changes will not take effect.
                ?vars_commit_delay => 1
            }
        ),
    Chain = blockchain_worker:blockchain(),

    ?assertEqual({ok, Val0}, var_get(Key, Chain)),
    Val1 = totes_goats_garb,
    ?assertMatch(ok, var_set(Key, Val1, Priv1, ConsensusMembers)),
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
        block_add(Chain, ConsensusMembers, Txn2_2_Bad)
    ),

    %% good master key

    ?assertMatch(ok, block_add(Chain, ConsensusMembers, Txn2_2_Good)),

    %% make sure old master key is no longer working
    ?assertMatch(
        {error, {invalid_txns, [{_, bad_block_proof}]}},
        var_set(Key, goats_are_too_garb, Priv1, ConsensusMembers)
    ),

    %% double check that new master key works
    ?assertMatch(ok, var_set(Key, goats_always_win, Priv2, ConsensusMembers)),
    ?assertEqual({ok, goats_always_win}, var_get(Key, Chain)),

    %% test all the multikey stuff

    %% first enable them
    ?assertMatch(ok, var_set(?use_multi_keys, true, Priv2, ConsensusMembers)),

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
               || P <- shuffle([Priv2, Priv3, Priv4, Priv5, Priv6])],
    Txn7_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn7_0, Proofs7),
    Proof7 = blockchain_txn_vars_v1:create_proof(Priv2, Txn7_1),
    Txn7 = blockchain_txn_vars_v1:proof(Txn7_1, Proof7),
    ok = block_add(Chain, ConsensusMembers, Txn7),

    %% try with only three keys (and succeed)
    ?assertMatch(ok, var_set(Key, but_what_now, [Priv2, Priv3, Priv6], ConsensusMembers)),
    ?assertMatch({ok, but_what_now}, var_get(Key, Chain)),

    %% try with only two keys (and fail)
    NonceForTxn9 = nonce_next(Chain),
    Txn9 = mvars(#{Key => sheep_jokes}, NonceForTxn9, [Priv3, Priv6]),
    {error, {invalid_txns, [{_, insufficient_votes}]}} =
        block_add(Chain, ConsensusMembers, Txn9),

    %% FIXME Remove redundant add
    {error, {invalid_txns, [{_, insufficient_votes}]}} =
        block_add(Chain, ConsensusMembers, Txn9),

    %% try with two valid and one corrupted key proof (and fail again)
    Txn10_0 = blockchain_txn_vars_v1:new(#{Key => cmon}, nonce_next(Chain)),
    Proofs10_0 = [blockchain_txn_vars_v1:create_proof(P, Txn10_0)
                || P <- [Priv2, Priv3, Priv4]],
    [Proof10 | Rem] = Proofs10_0,
    Proof10Corrupted = <<Proof10/binary, "asdasdasdas">>,
    Txn10 = blockchain_txn_vars_v1:multi_proofs(Txn10_0, [Proof10Corrupted | Rem]),

    {error, {invalid_txns, [{_, insufficient_votes}]}} =
        block_add(Chain, ConsensusMembers, Txn10),

    {error, {invalid_txns, [{_, insufficient_votes}]}} =
        block_add(Chain, ConsensusMembers, Txn9),

    %% make sure that we safely ignore bad proofs and keys
    #{secret := Priv8, public := _Pub8} = libp2p_crypto:generate_keys(ecc_compact),

    Txn11a =
        mvars(
            #{Key => sheep_are_inherently_unfunny},
            NonceForTxn9,
            [Priv2, Priv3, Priv4, Priv5, Priv6, Priv8]
        ),
    {error, {invalid_txns, [{_, too_many_proofs}]}} =
        block_add(Chain, ConsensusMembers, Txn11a),

    ?assertMatch(ok, var_set(Key, sheep_are_inherently_unfunny, [Priv2, Priv3, Priv5, Priv6, Priv8], ConsensusMembers)),
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
                || P <- shuffle([Priv7])],
    Txn12_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn12_0, Proofs12),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn12_1)
               || P <- [Priv3, Priv4, Priv5]],
    Txn12 = blockchain_txn_vars_v1:multi_proofs(Txn12_1, Proofs),
    ok = block_add(Chain, ConsensusMembers, Txn12),
    ?assertMatch({ok, so_true}, var_get(Key, Chain)),

    ?assertMatch(ok, var_set(Key, lets_all_hate_on_sheep, [Priv5, Priv6, Priv7], ConsensusMembers)),
    ?assertMatch({ok, lets_all_hate_on_sheep}, var_get(Key, Chain)),
    ok.

%% Helpers --------------------------------------------------------------------

-spec nonce_curr(blockchain:blockchain()) -> integer().
nonce_curr(Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, Nonce} = blockchain_ledger_v1:vars_nonce(Ledger),
    Nonce.

-spec nonce_next() -> integer().
nonce_next() ->
    Chain = blockchain_worker:blockchain(),
    nonce_next(Chain).

-spec nonce_next(blockchain:blockchain()) -> integer().
nonce_next(Chain) ->
    nonce_curr(Chain) + 1.

var_set(Key, Val, PrivKey, ConsensusMembers) ->
    Nonce = nonce_next(),
    MakeTxn =
        case is_list(PrivKey) of
            true -> fun mvars/3;
            false -> fun vars/3
        end,
    Txn = MakeTxn(#{Key => Val}, Nonce, PrivKey),
    block_add(ConsensusMembers, Txn).

var_get(Key, Chain) ->
    Ledger = blockchain:ledger(Chain),
    blockchain:config(Key, Ledger).

block_add(ConsensusMembers, Txn) ->
    Chain = blockchain_worker:blockchain(),
    block_add(Chain, ConsensusMembers, Txn).

-spec block_add(blockchain:blockchain(), [{_, {_, _, _}}], _) ->
    ok | {error, _}.
block_add(Chain, ConsensusMembers, Txn) ->
    case test_utils:create_block(ConsensusMembers, [Txn]) of
        {ok, Block} ->
            {ok, Height0} = blockchain:height(Chain),
            ok = blockchain:add_block(Block, Chain),
            {ok, Height1} = blockchain:height(Chain),
            ?assertEqual(1 + Height0, Height1),
            ok;
        {error, _}=Err ->
            Err
    end.

vars(Map, Nonce, Priv) ->
    Txn0 = blockchain_txn_vars_v1:new(Map, Nonce),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn0),
    blockchain_txn_vars_v1:proof(Txn0, Proof).

mvars(Map, Nonce, Privs) ->
    Txn = blockchain_txn_vars_v1:new(Map, Nonce),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn) || P <- shuffle(Privs)],
    blockchain_txn_vars_v1:multi_proofs(Txn, Proofs).

shuffle(Xs) ->
    N = length(Xs),
    {_, S} = lists:unzip(lists:sort([{rand:uniform(N), X} || X <- Xs])),
    S.
