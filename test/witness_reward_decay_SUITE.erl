%%--------------------------------------------------------------------
%%
%% Test suite group runs through a witness reward scenarios with varying
%% values supplied for the witness decay rate and for the witness decay
%% exclusion count.
%%
%%--------------------------------------------------------------------

-module(witness_reward_decay_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("helium_proto/include/blockchain_block_v1_pb.hrl").
-include_lib("helium_proto/include/blockchain_txn_pb.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").
-include("blockchain_ct_utils.hrl").
-include("blockchain_vars.hrl").

-export([
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    no_vars_test/1,
    decay_rate_0_6_test/1,
    decay_rate_0_7_test/1,
    decay_rate_0_8_test/1,
    decay_rate_0_9_test/1,
    decay_rate_1_0_test/1
]).

all() ->
    [
        {group, no_vars},
        {group, with_decay}
    ].

no_vars_cases() ->
    [
        no_vars_test
    ].

decay_rate_cases() ->
    [
        decay_rate_0_6_test,
        decay_rate_0_7_test,
        decay_rate_0_8_test,
        decay_rate_0_9_test,
        decay_rate_1_0_test
    ].

groups() ->
    [
        {no_vars, [], no_vars_cases()},
        {with_decay, [], decay_rate_cases()}
    ].

%%--------------------------------------------------------------------
%% group setup
%%--------------------------------------------------------------------

init_per_group(Group, Config) ->
    ExtraVars =
        case Group of
            no_vars ->
                #{};
            with_decay ->
                #{?witness_reward_decay_exclusion => 4}
        end,

    [{extra_vars, maps:merge(#{?poc_version => 10}, ExtraVars)} | Config].

%%--------------------------------------------------------------------
%% group teardown
%%--------------------------------------------------------------------

end_per_group(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% suite setup
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, StorePid} = blockchain_test_reward_store:start(),
    blockchain_test_reward_store:insert(witness_shares, #{}),
    [{store_pid, StorePid} | Config].

%%--------------------------------------------------------------------
%% suite teardown
%%--------------------------------------------------------------------

end_per_suite(_Config) ->
    ExpectedShares = #{
                       0.6 => #{b => 4170712034, c => 751640587, d => 1503281174, e => 751640587, f => 751640587, g => 751640587},
                       0.7 => #{b => 4150224857, c => 755055116, d => 1510110233, e => 755055116, f => 755055116, g => 755055116},
                       0.8 => #{b => 4131526220, c => 758171556, d => 1516343112, e => 758171556, f => 758171556, g => 758171556},
                       0.9 => #{b => 4114473474, c => 761013680, d => 1522027360, e => 761013680, f => 761013680, g => 761013680},
                       1.0 => #{b => 4098932958, c => 763603766, d => 1527207533, e => 763603766, f => 763603766, g => 763603766},
                       undefined => #{b => 4340277778, c => 723379630, d => 1446759259, e => 723379630, f => 723379630, g => 723379630}
                      },
    WitnessShares = blockchain_test_reward_store:fetch(witness_shares),
    ?assertEqual(WitnessShares, ExpectedShares),
    ct:print("Witness shares: ~p", [WitnessShares]),
    blockchain_test_reward_store:stop(),
    ok.

%%--------------------------------------------------------------------
%% test case setup
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config0) ->
    blockchain_test_reward_store:insert(elem_witness_map, #{}),

    Config = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config0),
    Balance = 5000,
    BaseDir = ?config(base_dir, Config),
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),

    ExtraVars0 = ?config(extra_vars, Config),
    ExtraVars = maps:merge(ExtraVars0, decay_rate(TestCase)),

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

    ActiveGateways = blockchain_ledger_v1:active_gateways(Ledger),
    GatewayAddrs = lists:sort(maps:keys(ActiveGateways)),
    AllGws = [a, b, c, d, e, f, g, h, i, j, k],

    GatewayLetterToAddrMap = lists:foldl(
        fun({Letter, A}, Acc) ->
            maps:put(Letter, A, Acc)
        end,
        #{},
        lists:zip(AllGws, GatewayAddrs)
    ),
    {GatewayLetterToAddrMap, GatewayAddrToLetterMap} = lists:foldl(
        fun({Letter, A}, {L2A, A2L}) ->
            {maps:put(Letter, A, L2A), maps:put(A, Letter, A2L)}
        end,
        {#{}, #{}},
        lists:zip(AllGws, GatewayAddrs)
    ),

    Challenger = maps:get(a, GatewayLetterToAddrMap),
    {_, {_, _, ChallengerSigFun}} = lists:keyfind(Challenger, 1, GenesisMembers),

    Beaconer1 = maps:get(c, GatewayLetterToAddrMap),
    Rx1 = blockchain_poc_receipt_v1:new(Beaconer1, 1000, 10, <<"first_rx">>, p2p),
    ConstructedWitnesses1 = construct_witnesses([b, d], GatewayLetterToAddrMap),

    Beaconer2 = maps:get(d, GatewayLetterToAddrMap),
    Rx2 = blockchain_poc_receipt_v1:new(Beaconer2, 1001, 10, <<"second_rx">>, p2p),
    ConstructedWitnesses2 = construct_witnesses([b, c], GatewayLetterToAddrMap),

    Beaconer3 = maps:get(e, GatewayLetterToAddrMap),
    Rx3 = blockchain_poc_receipt_v1:new(Beaconer3, 1002, 10, <<"third_rx">>, p2p),
    ConstructedWitnesses3 = construct_witnesses([b, d], GatewayLetterToAddrMap),

    Beaconer4 = maps:get(f, GatewayLetterToAddrMap),
    Rx4 = blockchain_poc_receipt_v1:new(Beaconer4, 1003, 10, <<"fourth_rx">>, p2p),
    ConstructedWitnesses4 = construct_witnesses([b, e], GatewayLetterToAddrMap),

    Beaconer5 = maps:get(g, GatewayLetterToAddrMap),
    Rx5 = blockchain_poc_receipt_v1:new(Beaconer5, 1004, 10, <<"fifth_rx">>, p2p),
    ConstructedWitnesses5 = construct_witnesses([b, f], GatewayLetterToAddrMap),

    Beaconer6 = maps:get(h, GatewayLetterToAddrMap),
    Rx6 = blockchain_poc_receipt_v1:new(Beaconer6, 1005, 10, <<"sixth_rx">>, p2p),
    ConstructedWitnesses6 = construct_witnesses([b, g], GatewayLetterToAddrMap),

    meck:expect(blockchain_txn_poc_request_v1, is_valid, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, absorb, fun(_, _) -> ok end),
    meck:expect(blockchain_txn_poc_receipts_v1, get_channels, fun(_, _) ->
        {ok, lists:seq(1, 11)}
    end),

    ok = create_req_and_poc_blocks(
        Challenger,
        ChallengerSigFun,
        Beaconer1,
        Rx1,
        ConstructedWitnesses1,
        ConsensusMembers,
        Chain
    ),
    ok = create_req_and_poc_blocks(
        Challenger,
        ChallengerSigFun,
        Beaconer2,
        Rx2,
        ConstructedWitnesses2,
        ConsensusMembers,
        Chain
    ),
    ok = create_req_and_poc_blocks(
        Challenger,
        ChallengerSigFun,
        Beaconer3,
        Rx3,
        ConstructedWitnesses3,
        ConsensusMembers,
        Chain
    ),
    ok = create_req_and_poc_blocks(
        Challenger,
        ChallengerSigFun,
        Beaconer4,
        Rx4,
        ConstructedWitnesses4,
        ConsensusMembers,
        Chain
    ),
    ok = create_req_and_poc_blocks(
        Challenger,
        ChallengerSigFun,
        Beaconer5,
        Rx5,
        ConstructedWitnesses5,
        ConsensusMembers,
        Chain
    ),
    ok = create_req_and_poc_blocks(
        Challenger,
        ChallengerSigFun,
        Beaconer6,
        Rx6,
        ConstructedWitnesses6,
        ConsensusMembers,
        Chain
    ),

    meck:expect(
        blockchain_txn_poc_receipts_v1,
        valid_witnesses,
        fun(E, _, _) ->
            ElemMap = blockchain_test_reward_store:fetch(elem_witness_map),
            case lists:any(fun(Elem) -> E == Elem end, maps:keys(ElemMap)) of
                true ->
                    maps:get(E, ElemMap);
                false -> []
            end
        end
    ),

    {ok, Height} = blockchain:height(Chain),
    ?assertEqual({ok, 13}, blockchain:height(Chain)),

    {ok, RewardsMd} = blockchain_txn_rewards_v2:calculate_rewards_metadata(1, Height, Chain),
    WitnessRewards = format_results(maps:get(poc_witness, RewardsMd), GatewayAddrToLetterMap),

    [
        {balance, Balance},
        {base_dir, BaseDir},
        {chain, Chain},
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        {ledger, Ledger},
        {n, N},
        {opts, Opts},
        {privkey, PrivKey},
        {pubkey, PubKey},
        {sup, Sup},
        {swarm, Swarm},
        {witness_rewards, WitnessRewards},
        Keys
        | Config
    ].

%%--------------------------------------------------------------------
%% test case teardown
%%--------------------------------------------------------------------

end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% test cases
%%--------------------------------------------------------------------

no_vars_test(Config) ->
    Rewards = ?config(witness_rewards, Config),
    stash_witness_shares(undefined, Rewards),
    ok.

decay_rate_0_6_test(Config) ->
    Rewards = ?config(witness_rewards, Config),
    stash_witness_shares(0.6, Rewards),
    ok.

decay_rate_0_7_test(Config) ->
    Rewards = ?config(witness_rewards, Config),
    stash_witness_shares(0.7, Rewards),
    ok.

decay_rate_0_8_test(Config) ->
    Rewards = ?config(witness_rewards, Config),
    stash_witness_shares(0.8, Rewards),
    ok.

decay_rate_0_9_test(Config) ->
    Rewards = ?config(witness_rewards, Config),
    stash_witness_shares(0.9, Rewards),
    ok.

decay_rate_1_0_test(Config) ->
    Rewards = ?config(witness_rewards, Config),
    stash_witness_shares(1.0, Rewards),
    ok.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

stash_witness_shares(Key, Value) ->
    WitnessShares = blockchain_test_reward_store:fetch(witness_shares),
    WitnessShares0 = maps:merge(WitnessShares, #{Key => Value}),
    blockchain_test_reward_store:insert(witness_shares, WitnessShares0),
    ok.

decay_rate(no_vars_test) ->
    #{};
decay_rate(Case) ->
    Rate = maps:get(Case, #{
        decay_rate_0_6_test => 0.6,
        decay_rate_0_7_test => 0.7,
        decay_rate_0_8_test => 0.8,
        decay_rate_0_9_test => 0.9,
        decay_rate_1_0_test => 1.0
    }),
    #{?witness_reward_decay_rate => Rate}.

construct_witnesses(WitnessList, GatewayLetterToAddrMap) ->
    lists:foldl(
        fun(W, Acc) ->
            WitnessGw = maps:get(W, GatewayLetterToAddrMap),
            Witness = blockchain_poc_witness_v1:new(
                WitnessGw,
                1001,
                10,
                crypto:strong_rand_bytes(32),
                9.800000190734863,
                915.2000122070313,
                10,
                "data_rate"
            ),
            [Witness | Acc]
        end,
        [],
        WitnessList
    ).

create_req_and_poc_blocks(
    Challenger, ChallengerSigFun, Beaconer, Rx, Witnesses, ConsensusMembers, Chain
) ->
    Secret = crypto:strong_rand_bytes(32),
    OnionKeyHash = crypto:strong_rand_bytes(32),
    BlockHash = crypto:strong_rand_bytes(32),

    ReqTxn = blockchain_txn_poc_request_v1:new(Challenger, Secret, OnionKeyHash, BlockHash, 10),
    SignedReqTxn = blockchain_txn_poc_request_v1:sign(ReqTxn, ChallengerSigFun),

    Elem = blockchain_poc_path_element_v1:new(Beaconer, Rx, Witnesses),
    ElemWitnessMap0 = blockchain_test_reward_store:fetch(elem_witness_map),
    blockchain_test_reward_store:insert(elem_witness_map, ElemWitnessMap0#{Elem => Witnesses}),

    PocTxn = blockchain_txn_poc_receipts_v1:new(Challenger, Secret, OnionKeyHash, BlockHash, [Elem]),
    SignedPocTxn = blockchain_txn_poc_receipts_v1:sign(PocTxn, ChallengerSigFun),

    {ok, ReqBlock} = test_utils:create_block(ConsensusMembers, [SignedReqTxn], #{}, false),
    _ = blockchain_gossip_handler:add_block(ReqBlock, Chain, self(), blockchain_swarm:tid()),

    {ok, PocBlock} = test_utils:create_block(ConsensusMembers, [SignedPocTxn], #{}, false),
    _ = blockchain_gossip_handler:add_block(PocBlock, Chain, self(), blockchain_swarm:tid()),

    ok.

format_results(Witnesses, LetterLookupMap) ->
    maps:fold(
        fun({gateway, poc_witnesses, Address}, Val, AccIn) ->
            AccIn#{maps:get(Address, LetterLookupMap) => Val}
        end,
        #{},
        Witnesses
    ).
