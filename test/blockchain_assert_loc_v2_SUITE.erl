-module(blockchain_assert_loc_v2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1,
    bad_owner_sig_test/1,
    bad_payer_sig_test/1,
    zero_staking_fee_test/1,
    same_loc_diff_gain_test/1,
    same_loc_diff_elevation_test/1,
    invalid_gain_test/1,
    insufficient_assert_version_test/1,
    insufficient_assert_res_test/1,
    bad_nonce_test/1,
    min_antenna_gain_not_set_test/1,
    max_antenna_gain_not_set_test/1
]).

all() ->
    [
        basic_test,
        bad_owner_sig_test,
        bad_payer_sig_test,
        zero_staking_fee_test,
        same_loc_diff_gain_test,
        same_loc_diff_elevation_test,
        invalid_gain_test,
        insufficient_assert_version_test,
        insufficient_assert_res_test,
        bad_nonce_test,
        min_antenna_gain_not_set_test,
        max_antenna_gain_not_set_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars = extra_vars(TestCase),

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
        {ledger, Ledger},
        {keys, Keys}
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
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

basic_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, OwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, OwnerSigFun),

    %% This transaction should be valid
    ok = blockchain_txn_assert_location_v2:is_valid(STxn1, Chain),

    %% Check that the transaction propogates properly
    {ok, Block} = test_utils:create_block(ConsensusMembers, [STxn1]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    ok.

bad_owner_sig_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [
        {_NotOwner, {_NotOwnerPubkey, _NotOwnerPrivKey, NotOwnerSigFun}},
        {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}}
        | _
    ] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, NotOwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, OwnerSigFun),

    %% This transaction should be invalid
    {error, {bad_owner_signature, _}} = blockchain_txn_assert_location_v2:is_valid(STxn1, Chain),

    ok.

bad_payer_sig_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [
        {_NotPayer, {_NotPayerPubkey, _NotPayerPrivKey, NotPayerSigFun}},
        {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}}
        | _
    ] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, OwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, NotPayerSigFun),

    %% This transaction should be invalid
    {error, {bad_payer_signature, _}} = blockchain_txn_assert_location_v2:is_valid(STxn1, Chain),

    ok.

zero_staking_fee_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {Txn1, _Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),

    %% Zero-ing out the staking_fee
    ZeroStakingFee = 0,
    ZeroStakingFeeTxn = blockchain_txn_assert_location_v2:staking_fee(Txn1, ZeroStakingFee),
    ZeroStakingFeeSTxn0 = blockchain_txn_assert_location_v2:sign(ZeroStakingFeeTxn, OwnerSigFun),
    ZeroStakingFeeSTxn1 = blockchain_txn_assert_location_v2:sign_payer(
        ZeroStakingFeeSTxn0,
        OwnerSigFun
    ),

    %% This transaction should be invalid
    %% Reason: We have zero-ed the staking_fee but a new location was specified
    {error, {wrong_staking_fee, {assert_location_v2, _, _}}} = blockchain_txn_assert_location_v2:is_valid(
        ZeroStakingFeeSTxn1,
        Chain
    ),

    ok.

same_loc_diff_gain_test(Config) ->
    Ledger = ?config(ledger, Config),
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% assert that the default gain is set correctly
    {ok, GW} = blockchain_ledger_v1:find_gateway_info(GatewayPubkeyBin, Ledger),
    ?assertEqual(?DEFAULT_GAIN, blockchain_ledger_gateway_v2:gain(GW)),
    ExistingLocation = blockchain_ledger_gateway_v2:location(GW),

    %% We will not change the location of the gateway but will update the gain
    %% and supply 0 staking_fee
    ZeroStakingFee = 0,
    NewGain = 23,
    SameLocDiffGainTxn0 = blockchain_txn_assert_location_v2:new(
        GatewayPubkeyBin,
        Owner,
        Payer,
        ExistingLocation,
        1
    ),
    SameLocDiffGainFee = blockchain_txn_assert_location_v2:calculate_fee(
        SameLocDiffGainTxn0,
        Chain
    ),
    SameLocDiffGainTxn1 = blockchain_txn_assert_location_v2:fee(
        SameLocDiffGainTxn0,
        SameLocDiffGainFee
    ),
    SameLocDiffGainTxn2 = blockchain_txn_assert_location_v2:gain(SameLocDiffGainTxn1, NewGain),
    SameLocDiffGainTxn3 = blockchain_txn_assert_location_v2:staking_fee(
        SameLocDiffGainTxn2,
        ZeroStakingFee
    ),
    SameLocDiffGainSTxn0 = blockchain_txn_assert_location_v2:sign(SameLocDiffGainTxn3, OwnerSigFun),
    SameLocDiffGainSTxn1 = blockchain_txn_assert_location_v2:sign_payer(
        SameLocDiffGainSTxn0,
        OwnerSigFun
    ),

    %% This transaction should be valid
    ok = blockchain_txn_assert_location_v2:is_valid(SameLocDiffGainSTxn1, Chain),

    %% Check that the transaction propogates properly
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SameLocDiffGainSTxn1]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    %% Check that the new elevation reflects properly
    Ledger1 = blockchain:ledger(Chain),
    {ok, GW1} = blockchain_ledger_v1:find_gateway_info(GatewayPubkeyBin, Ledger1),
    ?assertEqual(NewGain, blockchain_ledger_gateway_v2:gain(GW1)),

    ok.

same_loc_diff_elevation_test(Config) ->
    Ledger = ?config(ledger, Config),
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    {ok, GW} = blockchain_ledger_v1:find_gateway_info(GatewayPubkeyBin, Ledger),
    ?assertEqual(?DEFAULT_ELEVATION, blockchain_ledger_gateway_v2:elevation(GW)),
    ExistingLocation = blockchain_ledger_gateway_v2:location(GW),

    %% We will not change the location of the gateway but will update the gain
    %% and supply 0 staking_fee
    ZeroStakingFee = 0,
    NewElevation = 5,
    SameLocDiffElevationTxn0 = blockchain_txn_assert_location_v2:new(
        GatewayPubkeyBin,
        Owner,
        Payer,
        ExistingLocation,
        1
    ),
    SameLocDiffElevationFee = blockchain_txn_assert_location_v2:calculate_fee(
        SameLocDiffElevationTxn0,
        Chain
    ),
    SameLocDiffElevationTxn1 = blockchain_txn_assert_location_v2:fee(
        SameLocDiffElevationTxn0,
        SameLocDiffElevationFee
    ),
    SameLocDiffElevationTxn2 = blockchain_txn_assert_location_v2:elevation(
        SameLocDiffElevationTxn1,
        NewElevation
    ),
    SameLocDiffElevationTxn3 = blockchain_txn_assert_location_v2:staking_fee(
        SameLocDiffElevationTxn2,
        ZeroStakingFee
    ),
    SameLocDiffElevationSTxn0 = blockchain_txn_assert_location_v2:sign(
        SameLocDiffElevationTxn3,
        OwnerSigFun
    ),
    SameLocDiffElevationSTxn1 = blockchain_txn_assert_location_v2:sign_payer(
        SameLocDiffElevationSTxn0,
        OwnerSigFun
    ),

    %% This transaction should be valid
    ok = blockchain_txn_assert_location_v2:is_valid(SameLocDiffElevationSTxn1, Chain),

    %% Check that the transaction propogates properly
    {ok, Block} = test_utils:create_block(ConsensusMembers, [SameLocDiffElevationSTxn1]),
    _ = blockchain_gossip_handler:add_block(Block, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, blockchain_block:hash_block(Block)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),
    ?assertEqual({ok, Block}, blockchain:get_block(2, Chain)),

    %% Check that the new elevation reflects properly
    Ledger1 = blockchain:ledger(Chain),
    {ok, GW1} = blockchain_ledger_v1:find_gateway_info(GatewayPubkeyBin, Ledger1),
    ?assertEqual(NewElevation, blockchain_ledger_gateway_v2:elevation(GW1)),

    ok.

invalid_gain_test(Config) ->
    Ledger = ?config(ledger, Config),
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% assert that the default gain is set correctly
    {ok, GW} = blockchain_ledger_v1:find_gateway_info(GatewayPubkeyBin, Ledger),
    ?assertEqual(?DEFAULT_GAIN, blockchain_ledger_gateway_v2:gain(GW)),
    ExistingLocation = blockchain_ledger_gateway_v2:location(GW),

    %% We will not change the location of the gateway and supply 0 staking_fee
    ZeroStakingFee = 0,
    %% NOTE: Supply a gain value outside of the allowed range (16 dbi = 160)
    NewGain = 160,

    SameLocDiffGainTxn0 = blockchain_txn_assert_location_v2:new(
        GatewayPubkeyBin,
        Owner,
        Payer,
        ExistingLocation,
        1
    ),
    SameLocDiffGainFee = blockchain_txn_assert_location_v2:calculate_fee(
        SameLocDiffGainTxn0,
        Chain
    ),
    SameLocDiffGainTxn1 = blockchain_txn_assert_location_v2:fee(
        SameLocDiffGainTxn0,
        SameLocDiffGainFee
    ),
    SameLocDiffGainTxn2 = blockchain_txn_assert_location_v2:gain(SameLocDiffGainTxn1, NewGain),
    SameLocDiffGainTxn3 = blockchain_txn_assert_location_v2:staking_fee(
        SameLocDiffGainTxn2,
        ZeroStakingFee
    ),
    SameLocDiffGainSTxn0 = blockchain_txn_assert_location_v2:sign(SameLocDiffGainTxn3, OwnerSigFun),
    SameLocDiffGainSTxn1 = blockchain_txn_assert_location_v2:sign_payer(
        SameLocDiffGainSTxn0,
        OwnerSigFun
    ),

    %% This transaction should be invalid
    {error, {invalid_assert_loc_txn_v2, {invalid_antenna_gain, 160, 10, 150}}} = blockchain_txn_assert_location_v2:is_valid(
        SameLocDiffGainSTxn1,
        Chain
    ),

    ok.

insufficient_assert_version_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, OwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, OwnerSigFun),

    {error, {invalid_assert_loc_txn_v2, insufficient_assert_loc_txn_version}} = blockchain_txn_assert_location_v2:is_valid(
        STxn1,
        Chain
    ),

    ok.

insufficient_assert_res_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = h3:parent(631252734740306943, 9),
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, OwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, OwnerSigFun),

    %% This transaction should be valid
    {error, {insufficient_assert_res, _}} = blockchain_txn_assert_location_v2:is_valid(
        STxn1,
        Chain
    ),

    ok.

bad_nonce_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Some random bad nonce
    BadNonce = 10,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, BadNonce, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, OwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, OwnerSigFun),

    %% This transaction should be valid
    {error, {bad_nonce, {assert_location_v2, BadNonce, 0}}} = blockchain_txn_assert_location_v2:is_valid(
        STxn1,
        Chain
    ),

    ok.

min_antenna_gain_not_set_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, OwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, OwnerSigFun),

    {error, {invalid_assert_loc_txn_v2, min_antenna_gain_not_set}} = blockchain_txn_assert_location_v2:is_valid(
        STxn1,
        Chain
    ),

    ok.

max_antenna_gain_not_set_test(Config) ->
    Chain = ?config(chain, Config),
    ConsensusMembers = ?config(consensus_members, Config),

    [_, {Owner, {_OwnerPubkey, _OwnerPrivKey, OwnerSigFun}} | _] = ConsensusMembers,

    %% NOTE: gateway pubkey bin = owner = payer for the tests
    GatewayPubkeyBin = Owner,
    Payer = Owner,

    %% Construct a simple assert_location_v2 transaction
    NewLoc = 631252734740306943,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, NewLoc, 1, Chain),
    STxn0 = blockchain_txn_assert_location_v2:sign(Txn2, OwnerSigFun),
    STxn1 = blockchain_txn_assert_location_v2:sign_payer(STxn0, OwnerSigFun),

    %% This transaction should be invalid
    {error, {invalid_assert_loc_txn_v2, max_antenna_gain_not_set}} = blockchain_txn_assert_location_v2:is_valid(
        STxn1,
        Chain
    ),

    ok.

%%--------------------------------------------------------------------
%% HELPERS
%%--------------------------------------------------------------------

base_assert_loc_v2_txn(GatewayPubkeyBin, Owner, Payer, Loc, Nonce, Chain) ->
    Txn0 = blockchain_txn_assert_location_v2:new(GatewayPubkeyBin, Owner, Payer, Loc, Nonce),
    Fee = blockchain_txn_assert_location_v2:calculate_fee(Txn0, Chain),
    SFee = blockchain_txn_assert_location_v2:calculate_staking_fee(Txn0, Chain),
    Txn1 = blockchain_txn_assert_location_v2:fee(Txn0, Fee),
    {Txn1, blockchain_txn_assert_location_v2:staking_fee(Txn1, SFee)}.

extra_vars(min_antenna_gain_not_set_test) ->
    #{
        txn_fees => true,
        staking_fee_txn_assert_location_v1 => 1000000,
        assert_loc_txn_version => 2,
        max_antenna_gain => 150
    };
extra_vars(max_antenna_gain_not_set_test) ->
    #{
        txn_fees => true,
        staking_fee_txn_assert_location_v1 => 1000000,
        assert_loc_txn_version => 2,
        min_antenna_gain => 10
    };
extra_vars(insufficient_assert_version_test) ->
    #{
        txn_fees => true,
        staking_fee_txn_assert_location_v1 => 1000000,
        assert_loc_txn_version => 1,
        min_antenna_gain => 10,
        max_antenna_gain => 150
    };
extra_vars(_) ->
    #{
        txn_fees => true,
        staking_fee_txn_assert_location_v1 => 1000000,
        assert_loc_txn_version => 2,
        min_antenna_gain => 10,
        max_antenna_gain => 150
    }.
