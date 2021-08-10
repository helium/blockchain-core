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

    {ok, _GenesisMembers, _GenesisBlock, ConsensusMembers, {master_key, {Priv, _Pub}}} =
        test_utils:init_chain(
            5000,
            Keys,
            true,
            #{
                %% vars_commit_delay is crucial,
                %% otherwise var changes will not take effect.
                ?vars_commit_delay => 1
            }
        ),

    Chain = blockchain_worker:blockchain(),

    Vars = #{garbage_value => totes_goats_garb},
    Txn1_0 = blockchain_txn_vars_v1:new(Vars, nonce_next()),
    Proof1 = blockchain_txn_vars_v1:create_proof(Priv, Txn1_0),
    Txn1_1 = blockchain_txn_vars_v1:proof(Txn1_0, Proof1),

    {ok, _} = block_add(Chain, ConsensusMembers, Txn1_1),
    {ok, totes_goats_garb} = blockchain:config(garbage_value, blockchain:ledger(Chain)),

    %% bad master key
    #{secret := Priv2, public := Pub2} =
        libp2p_crypto:generate_keys(ecc_compact),

    BinPub2 = libp2p_crypto:pubkey_to_bin(Pub2),

    Vars2 = #{garbage_value => goats_are_not_garb},
    Txn2_0 = blockchain_txn_vars_v1:new(Vars2, nonce_next(), #{master_key => BinPub2}),
    Proof2 = blockchain_txn_vars_v1:create_proof(Priv, Txn2_0),
    KeyProof2 = blockchain_txn_vars_v1:create_proof(Priv2, Txn2_0),
    KeyProof2Corrupted = <<Proof2/binary, "asdasdasdas">>,
    Txn2_1 = blockchain_txn_vars_v1:proof(Txn2_0, Proof2),
    Txn2_2c = blockchain_txn_vars_v1:key_proof(Txn2_1, KeyProof2Corrupted),

    %% and then confirm the transaction did not apply
    {error, {invalid_txns, [{_, bad_master_key}]}} =
        block_add(Chain, ConsensusMembers, Txn2_2c),

    %% good master key

    Txn2_2 = blockchain_txn_vars_v1:key_proof(Txn2_1, KeyProof2),
    {ok, _} = block_add(Chain, ConsensusMembers, Txn2_2),

    %% make sure old master key is no longer working

    Vars4 = #{garbage_value => goats_are_too_garb},
    Txn4_0 = blockchain_txn_vars_v1:new(Vars4, nonce_next()),
    Proof4 = blockchain_txn_vars_v1:create_proof(Priv, Txn4_0),
    Txn4_1 = blockchain_txn_vars_v1:proof(Txn4_0, Proof4),

    {error, {invalid_txns, [{_, bad_block_proof}]}} =
        block_add(Chain, ConsensusMembers, Txn4_1),

    %% double check that new master key works

    Vars5 = #{garbage_value => goats_always_win},
    Txn5_0 = blockchain_txn_vars_v1:new(Vars5, nonce_next()),
    Proof5 = blockchain_txn_vars_v1:create_proof(Priv2, Txn5_0),
    Txn5_1 = blockchain_txn_vars_v1:proof(Txn5_0, Proof5),

    {ok, _} = block_add(Chain, ConsensusMembers, Txn5_1),

    %% test all the multikey stuff

    %% first enable them
    Txn6 = vars(#{?use_multi_keys => true}, nonce_next(), Priv2),
    {ok, _} = block_add(Chain, ConsensusMembers, Txn6),

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
               #{garbage_value => goat_jokes_are_so_single_key}, nonce_next(),
               #{multi_keys => [BinPub2, BinPub3, BinPub4, BinPub5, BinPub6]}),
    Proofs7 = [blockchain_txn_vars_v1:create_proof(P, Txn7_0)
               %% shuffle the proofs to make sure we no longer need
               %% them in the correct order
               || P <- shuffle([Priv2, Priv3, Priv4, Priv5, Priv6])],
    Txn7_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn7_0, Proofs7),
    Proof7 = blockchain_txn_vars_v1:create_proof(Priv2, Txn7_1),
    Txn7 = blockchain_txn_vars_v1:proof(Txn7_1, Proof7),
    {ok, _} = block_add(Chain, ConsensusMembers, Txn7),

    %% try with only three keys (and succeed)
    ct:pal("submitting 8"),
    Txn8 = mvars(#{garbage_value => but_what_now}, nonce_next(), [Priv2, Priv3, Priv6]),
    {ok, _} = block_add(Chain, ConsensusMembers, Txn8),

    %% try with only two keys (and fail)
    NonceForTxn9 = nonce_next(),
    Txn9 = mvars(#{garbage_value => sheep_jokes}, NonceForTxn9, [Priv3, Priv6]),
    {error, {invalid_txns, [{_, insufficient_votes}]}} =
        block_add(Chain, ConsensusMembers, Txn9),

    %% FIXME Remove redundant add
    {error, {invalid_txns, [{_, insufficient_votes}]}} =
        block_add(Chain, ConsensusMembers, Txn9),

    %% try with two valid and one corrupted key proof (and fail again)
    Txn10_0 = blockchain_txn_vars_v1:new(#{garbage_value => cmon}, nonce_next()),
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

    Txn11a = mvars(#{garbage_value => sheep_are_inherently_unfunny}, NonceForTxn9,
                   [Priv2, Priv3, Priv4, Priv5, Priv6, Priv8]),
    {error, {invalid_txns, [{_, too_many_proofs}]}} =
        block_add(Chain, ConsensusMembers, Txn11a),

    Txn11b = mvars(#{garbage_value => sheep_are_inherently_unfunny}, NonceForTxn9,
                   [Priv2, Priv3, Priv5, Priv6, Priv8]),
    {ok, _} = block_add(Chain, ConsensusMembers, Txn11b),

    Txn12_0 = blockchain_txn_vars_v1:new(
                #{garbage_value => so_true}, nonce_next(),
                #{multi_keys => [BinPub3, BinPub4, BinPub5, BinPub6, BinPub7]}),
    Proofs12 = [blockchain_txn_vars_v1:create_proof(P, Txn12_0)
                %% shuffle the proofs to make sure we no longer need
                %% them in the correct order
                || P <- shuffle([Priv7])],
    Txn12_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn12_0, Proofs12),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn12_1)
               || P <- [Priv3, Priv4, Priv5]],
    Txn12 = blockchain_txn_vars_v1:multi_proofs(Txn12_1, Proofs),

    {ok, _} = block_add(Chain, ConsensusMembers, Txn12),

    Txn13 = mvars(#{garbage_value => lets_all_hate_on_sheep}, nonce_next(),
                  [Priv5, Priv6, Priv7]),
    {ok, _} = block_add(Chain, ConsensusMembers, Txn13),
    ok.

%% Helpers --------------------------------------------------------------------

-spec nonce_curr() -> integer().
nonce_curr() ->
    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    {ok, Nonce} = blockchain_ledger_v1:vars_nonce(Ledger),
    Nonce.

-spec nonce_next() -> integer().
nonce_next() ->
    nonce_curr() + 1.

block_add(Chain, ConsensusMembers, Txn) ->
    case test_utils:create_block(ConsensusMembers, [Txn]) of
        {ok, Block} ->
            {ok, Height0} = blockchain:height(Chain),
            ok = blockchain:add_block(Block, Chain),
            {ok, Height1} = blockchain:height(Chain),
            ?assertEqual(1 + Height0, Height1),
            {ok, Block};
        {error, _}=Err ->
            Err
    end.

vars(Map, Nonce, Priv) ->
    Txn0 = blockchain_txn_vars_v1:new(Map, Nonce),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn0),
    blockchain_txn_vars_v1:proof(Txn0, Proof).

mvars(Map, Nonce, Privs) ->
    Txn0 = blockchain_txn_vars_v1:new(Map, Nonce),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn0)
               || P <- Privs],
    blockchain_txn_vars_v1:multi_proofs(Txn0, Proofs).

shuffle(Xs) ->
    N = length(Xs),
    {_, S} = lists:unzip(lists:sort([{rand:uniform(N), X} || X <- Xs])),
    S.
