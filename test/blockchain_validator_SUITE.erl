-module(blockchain_validator_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    stake_ok/1,
    stake_fail_not_enough_hnt/1,
    stake_fail_incorrect_stake/1,
    stake_fail_validator_already_exists/1,
    unstake_ok/1,
    unstake_fail_unstake_in_consensus/1,
    unstake_fail_not_owner/1,
    unstake_fail_invalid_stake_release_height/1,
    unstake_fail_already_cooldown/1,
    unstake_fail_already_unstaked/1,
    transfer_ok_in_account/1,
    transfer_ok/1,
    unstake_ok_at_same_height/1
]).

all() ->
    [
        stake_ok,
        stake_fail_not_enough_hnt,
        stake_fail_incorrect_stake,
        stake_fail_validator_already_exists,
        unstake_ok,
        unstake_fail_unstake_in_consensus,
        unstake_fail_not_owner,
        unstake_fail_invalid_stake_release_height,
        unstake_fail_already_cooldown,
        unstake_fail_already_unstaked,
        transfer_ok_in_account,
        transfer_ok,
        unstake_ok_at_same_height
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance =
        case TestCase of
            stake_fail_not_enough_hnt -> ?bones(5000);
            _ -> ?bones(15000)
        end,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars =
        case TestCase of
            _ ->
                #{
                    ?election_version => 5,
                    ?validator_version => 3,
                    ?validator_minimum_stake => ?bones(10000),
                    ?validator_liveness_grace_period => 10,
                    ?validator_liveness_interval => 5,
                    ?validator_key_check => true,
                    ?stake_withdrawal_cooldown => 10,
                    ?stake_withdrawal_max => 500,
                    ?dkg_penalty => 1.0,
                    ?penalty_history_limit => 100,
                    ?election_bba_penalty => 0.01,
                    ?election_seen_penalty => 0.03
                }
        end,

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    _ = lists:foreach(
        fun(Entry) ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            0 = blockchain_ledger_entry_v1:nonce(Entry)
        end,
        maps:values(Entries)
    ),

    [
        {balance, Balance},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        Keys
        | Config0
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------

end_per_testcase(_, Config) ->
    Sup = ?config(sup, Config),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    ok = test_utils:cleanup_tmp_dir(?config(base_dir, Config)),
    ok.

%%--------------------------------------------------------------------
%% STAKE
%%--------------------------------------------------------------------
stake_ok(Config) ->
    Chain = ?config(chain, Config),

    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = ?config(genesis_members, Config),

    %% make a validator
    [{StakePubkeyBin, {_StakePub, _StakePriv, _StakeSigFun}}] = test_utils:generate_keys(1),

    ct:pal("StakePubkeyBin: ~p~nOwnerPubkeyBin: ~p", [StakePubkeyBin, OwnerPubkeyBin]),

    Txn = blockchain_txn_stake_validator_v1:new(
        StakePubkeyBin,
        OwnerPubkeyBin,
        ?bones(10000),
        ?bones(5)
    ),
    SignedTxn = blockchain_txn_stake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedStakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_stake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_stake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ok;
        Error -> ct:fail("error: ~p", [Error])
    end.

stake_fail_not_enough_hnt(Config) ->
    Chain = ?config(chain, Config),

    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = ?config(genesis_members, Config),

    %% make a validator
    [{StakePubkeyBin, {_StakePub, _StakePriv, _StakeSigFun}}] = test_utils:generate_keys(1),

    ct:pal("StakePubkeyBin: ~p~nOwnerPubkeyBin: ~p", [StakePubkeyBin, OwnerPubkeyBin]),

    Txn = blockchain_txn_stake_validator_v1:new(
        StakePubkeyBin,
        OwnerPubkeyBin,
        ?bones(10000),
        ?bones(5)
    ),
    SignedTxn = blockchain_txn_stake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedStakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_stake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_stake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected an balance_too_low error", []);
        {error, {balance_too_low, _}} -> ok
    end.

stake_fail_incorrect_stake(Config) ->
    Chain = ?config(chain, Config),

    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = ?config(genesis_members, Config),

    %% make a validator
    [{StakePubkeyBin, {_StakePub, _StakePriv, _StakeSigFun}}] = test_utils:generate_keys(1),

    ct:pal("StakePubkeyBin: ~p~nOwnerPubkeyBin: ~p", [StakePubkeyBin, OwnerPubkeyBin]),

    Txn = blockchain_txn_stake_validator_v1:new(
        StakePubkeyBin,
        OwnerPubkeyBin,
        ?bones(9999),
        ?bones(5)
    ),
    SignedTxn = blockchain_txn_stake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedStakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_stake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_stake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected an incorrect_stake error", []);
        {error, {incorrect_stake, _}} -> ok
    end.

stake_fail_validator_already_exists(Config) ->
    Chain = ?config(chain, Config),

    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = ?config(genesis_members, Config),

    Txn = blockchain_txn_stake_validator_v1:new(
        OwnerPubkeyBin,
        OwnerPubkeyBin,
        ?bones(9999),
        ?bones(5)
    ),
    SignedTxn = blockchain_txn_stake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedStakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_stake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_stake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected an reused_miner_key error", []);
        {error, validator_already_exists} -> ok
    end.

unstake_ok(Config) ->
    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = NonConsensus,

    ct:pal("OwnerPubkeyBin: ~p", [OwnerPubkeyBin]),

    Txn = blockchain_txn_unstake_validator_v1:new(
        OwnerPubkeyBin,
        OwnerPubkeyBin,
        ?bones(10000),
        50,
        200
    ),
    SignedTxn = blockchain_txn_unstake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedUnstakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ok;
        Error -> ct:fail("error: ~p", [Error])
    end.

unstake_fail_unstake_in_consensus(Config) ->
    Chain = ?config(chain, Config),

    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = ?config(
        consensus_members,
        Config
    ),

    ct:pal("OwnerPubkeyBin: ~p", [OwnerPubkeyBin]),

    Txn = blockchain_txn_unstake_validator_v1:new(
        OwnerPubkeyBin,
        OwnerPubkeyBin,
        ?bones(10000),
        50,
        200
    ),
    SignedTxn = blockchain_txn_unstake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedUnstakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected cannot_unstake_while_in_consensus", []);
        {error, cannot_unstake_while_in_consensus} -> ok
    end.

unstake_fail_not_owner(Config) ->
    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, _OwnerSigFun}} | _] = NonConsensus,

    %% make a validator
    [{NotOwnerPubkeyBin, {_NotPub, _NotPriv, NotSigFun}}] = test_utils:generate_keys(1),

    Txn = blockchain_txn_unstake_validator_v1:new(
        OwnerPubkeyBin,
        NotOwnerPubkeyBin,
        ?bones(10000),
        50,
        200
    ),
    SignedTxn = blockchain_txn_unstake_validator_v1:sign(Txn, NotSigFun),
    ct:pal("SignedUnstakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected bad_owner", []);
        {error, {not_owner, _}} -> ok
    end.

unstake_fail_invalid_stake_release_height(Config) ->
    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = NonConsensus,

    Txn = blockchain_txn_unstake_validator_v1:new(OwnerPubkeyBin, OwnerPubkeyBin, ?bones(10000), 1, 123),
    SignedTxn = blockchain_txn_unstake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedUnstakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected invalid_stake_release_height", []);
        {error, {invalid_stake_release_height, _}} -> ok
    end.

unstake_fail_already_cooldown(Config) ->
    meck:new(blockchain_ledger_validator_v1, [passthrough]),
    meck:expect(blockchain_ledger_validator_v1, status, fun(_) -> cooldown end),

    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = NonConsensus,

    Txn = blockchain_txn_unstake_validator_v1:new(OwnerPubkeyBin, OwnerPubkeyBin, ?bones(10000), 1, 123),
    SignedTxn = blockchain_txn_unstake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedUnstakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected already_cooldown", []);
        {error, already_cooldown} -> ok
    end,
    meck:unload().

unstake_fail_already_unstaked(Config) ->
    meck:new(blockchain_ledger_validator_v1, [passthrough]),
    meck:expect(blockchain_ledger_validator_v1, status, fun(_) -> unstaked end),

    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = NonConsensus,

    Txn = blockchain_txn_unstake_validator_v1:new(OwnerPubkeyBin, OwnerPubkeyBin, ?bones(10000), 1, 123),
    SignedTxn = blockchain_txn_unstake_validator_v1:sign(Txn, OwnerSigFun),
    ct:pal("SignedUnstakeTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn, Chain) of
        ok -> ct:fail("got ok, expected already_unstaked", []);
        {error, already_unstaked} -> ok
    end,
    meck:unload().

transfer_ok_in_account(Config) ->
    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [{OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}} | _] = NonConsensus,

    %% make a new validator address
    [{XferPubkeyBin, {_XferPub, _XferPriv, _XferSigFun}}] = test_utils:generate_keys(1),
    ct:pal("OwnerPubkeyBin: ~p StakePubkeyBin: ~p", [OwnerPubkeyBin, XferPubkeyBin]),

    Txn = blockchain_txn_transfer_validator_stake_v1:new(
        OwnerPubkeyBin,
        XferPubkeyBin,
        OwnerPubkeyBin,
        ?bones(10000),
        ?bones(5)
    ),
    SignedTxn = blockchain_txn_transfer_validator_stake_v1:sign(Txn, OwnerSigFun),
    ct:pal("TransferTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_transfer_validator_stake_v1:is_valid_old_owner(SignedTxn)),
    case blockchain_txn_transfer_validator_stake_v1:is_valid(SignedTxn, Chain) of
        ok -> ok;
        Error -> ct:fail("error: ~p", [Error])
    end.

transfer_ok(Config) ->
    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [
        {OwnerPubkeyBin, {_OwnerPub, _OwnerPriv, OwnerSigFun}},
        {NewOwnerPubkeyBin, {_NewPub, _NewPriv, NewOwnerSigFun}}
        | _
    ] = NonConsensus,

    %% make a new validator address
    [{XferPubkeyBin, {_XferPub, _XferPriv, _XferSigFun}}] = test_utils:generate_keys(1),
    ct:pal("OwnerPubkeyBin: ~p StakePubkeyBin: ~p", [OwnerPubkeyBin, XferPubkeyBin]),

    Txn = blockchain_txn_transfer_validator_stake_v1:new(
        OwnerPubkeyBin,
        XferPubkeyBin,
        OwnerPubkeyBin,
        NewOwnerPubkeyBin,
        ?bones(10000),
        ?bones(50),
        ?bones(5)
    ),
    SignedTxn0 = blockchain_txn_transfer_validator_stake_v1:sign(Txn, OwnerSigFun),
    SignedTxn = blockchain_txn_transfer_validator_stake_v1:new_owner_sign(
        SignedTxn0,
        NewOwnerSigFun
    ),
    ct:pal("TransferTxn: ~p", [SignedTxn]),

    ?assert(blockchain_txn_transfer_validator_stake_v1:is_valid_old_owner(SignedTxn)),
    ?assert(blockchain_txn_transfer_validator_stake_v1:is_valid_new_owner(SignedTxn)),
    case blockchain_txn_transfer_validator_stake_v1:is_valid(SignedTxn, Chain) of
        ok -> ok;
        Error -> ct:fail("error: ~p", [Error])
    end.

unstake_ok_at_same_height(Config) ->
    Chain = ?config(chain, Config),

    %% can't unstake validators in consensus
    Genesis = ?config(genesis_members, Config),
    Consensus = ?config(consensus_members, Config),
    NonConsensus = Genesis -- Consensus,
    [{Owner1PubkeyBin, {_Owner1Pub, _Owner1Priv, Owner1SigFun}},
     {Owner2PubkeyBin, {_Owner2Pub, _Owner2Priv, Owner2SigFun}} | _] = NonConsensus,

    ct:pal("Owner1PubkeyBin: ~p", [Owner1PubkeyBin]),
    ct:pal("Owner2PubkeyBin: ~p", [Owner2PubkeyBin]),

    {ok, Height} = blockchain:height(Chain),
    ct:pal("height: ~p", [Height]),

    Txn1 = blockchain_txn_unstake_validator_v1:new(
        Owner1PubkeyBin,
        Owner1PubkeyBin,
        ?bones(10000),
        Height + 11,
        35000
    ),
    SignedTxn1 = blockchain_txn_unstake_validator_v1:sign(Txn1, Owner1SigFun),
    ct:pal("SignedUnstakeTxn1: ~p", [SignedTxn1]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn1)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn1, Chain) of
        ok -> ok;
        Error -> ct:fail("error: ~p", [Error])
    end,

    Txn2 = blockchain_txn_unstake_validator_v1:new(
        Owner2PubkeyBin,
        Owner2PubkeyBin,
        ?bones(10000),
        Height + 11,
        35000
    ),
    SignedTxn2 = blockchain_txn_unstake_validator_v1:sign(Txn2, Owner2SigFun),
    ct:pal("SignedUnstakeTxn2: ~p", [SignedTxn2]),

    ?assert(blockchain_txn_unstake_validator_v1:is_valid_owner(SignedTxn2)),
    case blockchain_txn_unstake_validator_v1:is_valid(SignedTxn2, Chain) of
        ok -> ok;
        Error1 -> ct:fail("error: ~p", [Error1])
    end,


    {ok, Block} = test_utils:create_block(Consensus, [SignedTxn1, SignedTxn2]),
    _ = blockchain:add_block(Block, Chain),

    _ = lists:map(
          fun(_) ->
                  {ok, B} = test_utils:create_block(Consensus, []),
                  _ = blockchain:add_block(B, Chain)
          end,
          lists:seq(1, 11)),

    ExpectedHeight = Height+12, % 11 + 1 for the unstake txn block
    {ok, ExpectedHeight} = blockchain:height(Chain),

    Ledger = blockchain:ledger(Chain),

    {ok, Val1} = blockchain_ledger_v1:get_validator(Owner1PubkeyBin, Ledger),
    ct:pal("Val1: ~p", [Val1]),
    ?assertEqual(unstaked, blockchain_ledger_validator_v1:status(Val1)),
    {ok, LedgerEntry1} = blockchain_ledger_v1:find_entry(Owner1PubkeyBin, Ledger),
    ?assertEqual(?bones(25000), blockchain_ledger_entry_v1:balance(LedgerEntry1)),

    {ok, Val2} = blockchain_ledger_v1:get_validator(Owner2PubkeyBin, Ledger),
    ct:pal("Val2: ~p", [Val2]),
    ?assertEqual(unstaked, blockchain_ledger_validator_v1:status(Val2)),
    {ok, LedgerEntry2} = blockchain_ledger_v1:find_entry(Owner2PubkeyBin, Ledger),
    ?assertEqual(?bones(25000), blockchain_ledger_entry_v1:balance(LedgerEntry2)),
    ok.
