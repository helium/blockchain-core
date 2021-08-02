-module(miner_relocated_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("blockchain_vars.hrl").

-export([
    all/0
]).

-export([
    master_key_test/1
]).

all() ->
    [
        master_key_test
    ].

master_key_test(Config) ->
    %% get all the miners
    Miners = ?config(miners, Config),

    %% baseline: chain vars are working
    {Priv, _Pub} = ?config(master_key, Config),

    Vars = #{garbage_value => totes_goats_garb},
    Txn1_0 = blockchain_txn_vars_v1:new(Vars, 2),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn1_0),
    Txn1_1 = blockchain_txn_vars_v1:proof(Txn1_0, Proof),

    _ = [ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [Txn1_1]) || Miner <- Miners],
    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, totes_goats_garb),

    %% bad master key

    #{secret := Priv2, public := Pub2} =
        libp2p_crypto:generate_keys(ecc_compact),

    BinPub2 = libp2p_crypto:pubkey_to_bin(Pub2),

    Vars2 = #{garbage_value => goats_are_not_garb},
    Txn2_0 = blockchain_txn_vars_v1:new(Vars2, 3, #{master_key => BinPub2}),
    Proof2 = blockchain_txn_vars_v1:create_proof(Priv, Txn2_0),
    KeyProof2 = blockchain_txn_vars_v1:create_proof(Priv2, Txn2_0),
    KeyProof2Corrupted = <<Proof2/binary, "asdasdasdas">>,
    Txn2_1 = blockchain_txn_vars_v1:proof(Txn2_0, Proof2),
    Txn2_2c = blockchain_txn_vars_v1:key_proof(Txn2_1, KeyProof2Corrupted),

    _ = [ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [Txn2_2c]) || Miner <- Miners],

    %% and then confirm the transaction did not apply
    false = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, goats_are_not_garb, 10),

    %% good master key

    Txn2_2 = blockchain_txn_vars_v1:key_proof(Txn2_1, KeyProof2),
    _ = [ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [Txn2_2])
         || Miner <- Miners],

    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, goats_are_not_garb),

    %% make sure old master key is no longer working

    Vars4 = #{garbage_value => goats_are_too_garb},
    Txn4_0 = blockchain_txn_vars_v1:new(Vars4, 4),
    Proof4 = blockchain_txn_vars_v1:create_proof(Priv, Txn4_0),
    Txn4_1 = blockchain_txn_vars_v1:proof(Txn4_0, Proof4),

    _ = [ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [Txn4_1])
         || Miner <- Miners],

    false = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, goats_are_too_garb, 10),

    %% double check that new master key works

    Vars5 = #{garbage_value => goats_always_win},
    Txn5_0 = blockchain_txn_vars_v1:new(Vars5, 4),
    Proof5 = blockchain_txn_vars_v1:create_proof(Priv2, Txn5_0),
    Txn5_1 = blockchain_txn_vars_v1:proof(Txn5_0, Proof5),

    _ = [ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [Txn5_1])
         || Miner <- Miners],

    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, goats_always_win),

    %% test all the multikey stuff

    %% first enable them
    Txn6 = vars(#{?use_multi_keys => true}, 5, Priv2),
    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn6]) || M <- Miners],
    ok = miner_ct_utils:wait_for_chain_var_update(Miners, ?use_multi_keys, true),

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
               #{garbage_value => goat_jokes_are_so_single_key}, 6,
               #{multi_keys => [BinPub2, BinPub3, BinPub4, BinPub5, BinPub6]}),
    Proofs7 = [blockchain_txn_vars_v1:create_proof(P, Txn7_0)
               %% shuffle the proofs to make sure we no longer need
               %% them in the correct order
               || P <- miner_ct_utils:shuffle([Priv2, Priv3, Priv4, Priv5, Priv6])],
    Txn7_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn7_0, Proofs7),
    Proof7 = blockchain_txn_vars_v1:create_proof(Priv2, Txn7_1),
    Txn7 = blockchain_txn_vars_v1:proof(Txn7_1, Proof7),
    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn7]) || M <- Miners],
    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, goat_jokes_are_so_single_key),

    %% try with only three keys (and succeed)
    ct:pal("submitting 8"),
    Txn8 = mvars(#{garbage_value => but_what_now}, 7, [Priv2, Priv3, Priv6]),
    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn8]) || M <- Miners],
    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, but_what_now),

    %% try with only two keys (and fail)
    Txn9 = mvars(#{garbage_value => sheep_jokes}, 8, [Priv3, Priv6]),
    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn9]) || M <- Miners],

    _ = [ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [Txn9]) || Miner <- Miners],

    false = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, sheep_jokes, 10),

    %% try with two valid and one corrupted key proof (and fail again)
    Txn10_0 = blockchain_txn_vars_v1:new(#{garbage_value => cmon}, 8),
    Proofs10_0 = [blockchain_txn_vars_v1:create_proof(P, Txn10_0)
                || P <- [Priv2, Priv3, Priv4]],
    [Proof10 | Rem] = Proofs10_0,
    Proof10Corrupted = <<Proof10/binary, "asdasdasdas">>,
    Txn10 = blockchain_txn_vars_v1:multi_proofs(Txn10_0, [Proof10Corrupted | Rem]),

    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn10]) || M <- Miners],

    _ = [ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [Txn9]) || Miner <- Miners],

    false = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, cmon, 10),

    %% make sure that we safely ignore bad proofs and keys
    #{secret := Priv8, public := _Pub8} = libp2p_crypto:generate_keys(ecc_compact),

    Txn11a = mvars(#{garbage_value => sheep_are_inherently_unfunny}, 8,
                   [Priv2, Priv3, Priv4, Priv5, Priv6, Priv8]),
    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn11a]) || M <- Miners],
    false = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, sheep_are_inherently_unfunny, 10),

    Txn11b = mvars(#{garbage_value => sheep_are_inherently_unfunny}, 8,
                   [Priv2, Priv3, Priv5, Priv6, Priv8]),
    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn11b]) || M <- Miners],
    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, sheep_are_inherently_unfunny),

    Txn12_0 = blockchain_txn_vars_v1:new(
                #{garbage_value => so_true}, 9,
                #{multi_keys => [BinPub3, BinPub4, BinPub5, BinPub6, BinPub7]}),
    Proofs12 = [blockchain_txn_vars_v1:create_proof(P, Txn12_0)
                %% shuffle the proofs to make sure we no longer need
                %% them in the correct order
                || P <- miner_ct_utils:shuffle([Priv7])],
    Txn12_1 = blockchain_txn_vars_v1:multi_key_proofs(Txn12_0, Proofs12),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn12_1)
               || P <- [Priv3, Priv4, Priv5]],
    Txn12 = blockchain_txn_vars_v1:multi_proofs(Txn12_1, Proofs),

    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn12]) || M <- Miners],
    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, so_true),

    Txn13 = mvars(#{garbage_value => lets_all_hate_on_sheep}, 10,
                  [Priv5, Priv6, Priv7]),
    _ = [ok = ct_rpc:call(M, blockchain_worker, submit_txn, [Txn13]) || M <- Miners],
    ok = miner_ct_utils:wait_for_chain_var_update(Miners, garbage_value, lets_all_hate_on_sheep),

    ok.

%% Helpers --------------------------------------------------------------------
vars(Map, Nonce, Priv) ->
    Txn0 = blockchain_txn_vars_v1:new(Map, Nonce),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn0),
    blockchain_txn_vars_v1:proof(Txn0, Proof).

mvars(Map, Nonce, Privs) ->
    Txn0 = blockchain_txn_vars_v1:new(Map, Nonce),
    Proofs = [blockchain_txn_vars_v1:create_proof(P, Txn0)
               || P <- Privs],
    blockchain_txn_vars_v1:multi_proofs(Txn0, Proofs).
