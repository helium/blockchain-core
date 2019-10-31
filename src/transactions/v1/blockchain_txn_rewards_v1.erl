%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Rewards ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_rewards_v1).

-behavior(blockchain_txn).

-include("blockchain_vars.hrl").
-include("pb/blockchain_txn_rewards_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    start_epoch/1,
    end_epoch/1,
    rewards/1,
    sign/2,
    fee/1,
    is_valid/2,
    absorb/2,
    calculate_rewards/3
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% monthly_reward          50000 * 1000000  In bones
% securities_percent      0.35
% dc_percent              0.25 Unused for now so give to POC
% poc_challengees_percent 0.19 + 0.16
% poc_challengers_percent 0.09 + 0.06
% poc_witnesses_percent   0.02 + 0.03
% consensus_percent       0.10

-type txn_rewards() :: #blockchain_txn_rewards_v1_pb{}.
-export_type([txn_rewards/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(non_neg_integer(), non_neg_integer(), blockchain_txn_reward_v1:rewards()) -> txn_rewards().
new(Start, End, Rewards) ->
    SortedRewards = lists:sort(Rewards),
    #blockchain_txn_rewards_v1_pb{start_epoch=Start, end_epoch=End, rewards=SortedRewards}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_rewards()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_rewards_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec start_epoch(txn_rewards()) -> non_neg_integer().
start_epoch(#blockchain_txn_rewards_v1_pb{start_epoch=Start}) ->
    Start.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec end_epoch(txn_rewards()) -> non_neg_integer().
end_epoch(#blockchain_txn_rewards_v1_pb{end_epoch=End}) ->
    End.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rewards(txn_rewards()) -> blockchain_txn_reward_v1:rewards().
rewards(#blockchain_txn_rewards_v1_pb{rewards=Rewards}) ->
    Rewards.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_rewards(), libp2p_crypto:sig_fun()) -> txn_rewards().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_rewards()) -> 0.
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    case ?MODULE:calculate_rewards(Start, End, Chain) of
        {error, _Reason}=Error ->
            Error;
        {ok, CalRewards} ->
            TxnRewards = ?MODULE:rewards(Txn),
            CalRewardsHashes = lists:sort([blockchain_txn_reward_v1:hash(R)|| R <- CalRewards]),
            TxnRewardsHashes = lists:sort([blockchain_txn_reward_v1:hash(R)|| R <- TxnRewards]),
            case CalRewardsHashes == TxnRewardsHashes of
                false -> {error, invalid_rewards};
                true -> ok
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Rewards = ?MODULE:rewards(Txn),
    AccRewards = lists:foldl(
        fun(Reward, Acc) ->
            Account = blockchain_txn_reward_v1:account(Reward),
            Amount = blockchain_txn_reward_v1:amount(Reward),
            Total = maps:get(Account, Acc, 0),
            maps:put(Account, Total + Amount, Acc)
        end,
        #{},
        Rewards
    ),
    maps:fold(
        fun(Account, Amount, _) ->
            blockchain_ledger_v1:credit_account(Account, Amount, Ledger)
        end,
        ok,
        AccRewards
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec calculate_rewards(non_neg_integer(), non_neg_integer(),blockchain:blockchain()) ->
    {ok, blockchain_txn_reward_v1:rewards()} | {error, any()}.
calculate_rewards(Start, End, Chain) ->
    case get_txns_for_epoch(Start, End, Chain) of
        {error, _Reason}=Error ->
            Error;
        {ok, Transactions} ->
            Filtered = lists:filter(
                fun(Txn) -> blockchain_txn:type(Txn) == ?MODULE end,
                Transactions
            ),
            case Filtered of
                [] ->
                    {ok, Ledger} = blockchain:ledger_at(End, Chain),
                    Vars = get_reward_vars(Start, End, Ledger),
                    SecuritiesRewards = securities_rewards(Ledger, Vars),
                    POCChallengersRewards = poc_challengers_rewards(Transactions, Vars),
                    POCChallengeesRewards = poc_challengees_rewards(Transactions, Vars),
                    POCWitnessesRewards = poc_witnesses_rewards(Transactions, Vars),
                    % Forcing calculation of EpochReward to always be around ElectionInterval (30 blocks) so that there is less incentive to stay in the consensus group
                    ConsensusEpochReward = calculate_epoch_reward(1, Start, End, Ledger),
                    ConsensusRewards = consensus_members_rewards(Ledger, maps:put(epoch_reward, ConsensusEpochReward, Vars)),
                    Result = lists:foldl(
                               fun(Map, Acc0) ->
                                       maps:fold(
                                         fun({owner, Type, Owner}, Amount, Acc1) ->
                                                 Reward = blockchain_txn_reward_v1:new(Owner, undefined, Amount, Type),
                                                 [Reward|Acc1];
                                            ({gateway, Type, Gateway}, Amount, Acc1) ->
                                                 case get_gateway_owner(Gateway, Ledger) of
                                                     {error, _} ->
                                                         Acc1;
                                                     {ok, Owner} ->
                                                         Reward = blockchain_txn_reward_v1:new(Owner, Gateway, Amount, Type),
                                                         [Reward|Acc1]
                                                 end
                                         end,
                                         Acc0,
                                         Map
                                        )
                               end,
                               [],
                               [ConsensusRewards, SecuritiesRewards, POCChallengersRewards,
                                POCChallengeesRewards, POCWitnessesRewards]
                              ),
                    %% clean up ledger context
                    blockchain_ledger_v1:delete_context(Ledger),
                    {ok, Result};
                [_RewardTxn|_] ->
                    {error, already_existing_rewards}
            end
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_txns_for_epoch(non_neg_integer(), non_neg_integer(), blockchain:blockchain()) -> {ok, blockchain_txn:txns()}
                                                                                           | {error, any()}.
get_txns_for_epoch(Start, End, Chain) ->
    get_txns_for_epoch(Start, End, Chain, []).

-spec get_txns_for_epoch(non_neg_integer(), non_neg_integer(),
                         blockchain:blockchain(), blockchain_txn:txns()) -> {ok, blockchain_txn:txns()}
                                                                            | {error, any()}.
get_txns_for_epoch(Start, End, _Chain, Txns) when Start == End+1 ->
    {ok, Txns};
get_txns_for_epoch(Current, End, Chain, Txns) ->
    case blockchain:get_block(Current, Chain) of
        {error, _Reason}=Error ->
            lager:error("failed to get block ~p ~p", [_Reason, Current]),
            Error;
        {ok, Block} ->
            Transactions = blockchain_block:transactions(Block),
            get_txns_for_epoch(Current+1, End, Chain, Txns ++ Transactions)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_reward_vars(non_neg_integer(), non_neg_integer(), blockchain_ledger_v1:ledger()) -> map().
get_reward_vars(Start, End, Ledger) ->
    {ok, MonthlyReward} = blockchain:config(?monthly_reward, Ledger),
    {ok, SecuritiesPercent} = blockchain:config(?securities_percent, Ledger),
    {ok, PocChallengeesPercent} = blockchain:config(?poc_challengees_percent, Ledger),
    {ok, PocChallengersPercent} = blockchain:config(?poc_challengers_percent, Ledger),
    {ok, PocWitnessesPercent} = blockchain:config(?poc_witnesses_percent, Ledger),
    {ok, ConsensusPercent} = blockchain:config(?consensus_percent, Ledger),
    POCVersion = case blockchain:config(?poc_version, Ledger) of
        {ok, V} -> V;
        _ -> 1
    end,
    EpochReward = calculate_epoch_reward(Start, End, Ledger),
    #{
        monthly_reward => MonthlyReward,
        epoch_reward => EpochReward,
        securities_percent => SecuritiesPercent,
        poc_challengees_percent => PocChallengeesPercent,
        poc_challengers_percent => PocChallengersPercent,
        poc_witnesses_percent => PocWitnessesPercent,
        consensus_percent => ConsensusPercent,
        poc_version => POCVersion
    }.

-spec calculate_epoch_reward(non_neg_integer(), non_neg_integer(), blockchain_ledger_v1:ledger()) -> float().
calculate_epoch_reward(Start, End, Ledger) ->
    Version = case blockchain:config(?reward_version, Ledger) of
        {ok, V} -> V;
        _ -> 1
    end,
    calculate_epoch_reward(Version, Start, End, Ledger).

-spec calculate_epoch_reward(non_neg_integer(), non_neg_integer(), non_neg_integer(), blockchain_ledger_v1:ledger()) -> float().
calculate_epoch_reward(Version, Start, End, Ledger) ->
    {ok, ElectionInterval} = blockchain:config(?election_interval, Ledger),
    {ok, BlockTime0} = blockchain:config(?block_time, Ledger),
    {ok, MonthlyReward} = blockchain:config(?monthly_reward, Ledger),
    calculate_epoch_reward(Version, Start, End, BlockTime0, ElectionInterval, MonthlyReward).

-spec calculate_epoch_reward(non_neg_integer(), non_neg_integer(), non_neg_integer(),
                             non_neg_integer(), non_neg_integer(), non_neg_integer()) -> float().
calculate_epoch_reward(Version, Start, End, BlockTime0, _ElectionInterval, MonthlyReward) when Version >= 2 ->
    BlockTime1 = (BlockTime0/1000),
    % Convert to blocks per min
    BlockPerMin = 60/BlockTime1,
    % Convert to blocks per hour
    BlockPerHour = BlockPerMin*60,
    % Calculate election interval in blocks
    ElectionInterval = End - Start,
    ElectionPerHour = BlockPerHour/ElectionInterval,
    MonthlyReward/30/24/ElectionPerHour;
calculate_epoch_reward(_Version, _Start, _End, BlockTime0, ElectionInterval, MonthlyReward) ->
    BlockTime1 = (BlockTime0/1000),
    % Convert to blocks per min
    BlockPerMin = 60/BlockTime1,
    % Convert to blocks per hour
    BlockPerHour = BlockPerMin*60,
    % Calculate number of elections per hour
    ElectionPerHour = BlockPerHour/ElectionInterval,
    MonthlyReward/30/24/ElectionPerHour.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members_rewards(blockchain_ledger_v1:ledger(),
                                map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
consensus_members_rewards(Ledger, #{epoch_reward := EpochReward,
                                    consensus_percent := ConsensusPercent}) ->
    case blockchain_ledger_v1:consensus_members(Ledger) of
        {error, _Reason} ->
            lager:error("failed to get consensus_members ~p", [_Reason]),
            #{};
            % TODO: Should we error out here?
        {ok, ConsensusMembers} ->
            ConsensusReward = EpochReward * ConsensusPercent,
            Total = erlang:length(ConsensusMembers),
            lists:foldl(
                fun(Member, Acc) ->
                    PercentofReward = 100/Total/100,
                    Amount = erlang:round(PercentofReward*ConsensusReward),
                    maps:put({gateway, consensus, Member}, Amount, Acc)
                end,
                #{},
                ConsensusMembers
            )
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec securities_rewards(blockchain_ledger_v1:ledger(),
                         map()) -> #{{owner, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
securities_rewards(Ledger, #{epoch_reward := EpochReward,
                             securities_percent := SecuritiesPercent}) ->
    Securities = blockchain_ledger_v1:securities(Ledger),
    TotalSecurities = maps:fold(
        fun(_, Entry, Acc) ->
            Acc + blockchain_ledger_security_entry_v1:balance(Entry)
        end,
        0,
        Securities
    ),
    SecuritiesReward = EpochReward * SecuritiesPercent,
    maps:fold(
        fun(Key, Entry, Acc) ->
            Balance = blockchain_ledger_security_entry_v1:balance(Entry),
            PercentofReward = (Balance*100/TotalSecurities)/100,
            Amount = erlang:round(PercentofReward*SecuritiesReward),
            maps:put({owner, securities, Key}, Amount, Acc)
        end,
        #{},
        Securities
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec poc_challengers_rewards(blockchain_txn:txns(),
                              map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_challengers_rewards(Transactions, #{epoch_reward := EpochReward,
                                        poc_challengers_percent := PocChallengersPercent}) ->
    {Challengers, TotalChallenged} = lists:foldl(
        fun(Txn, {Map, Total}=Acc) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc;
                true ->
                    Challenger = blockchain_txn_poc_receipts_v1:challenger(Txn),
                    I = maps:get(Challenger, Map, 0),
                    {maps:put(Challenger, I+1, Map), Total+1}
            end
        end,
        {#{}, 0},
        Transactions
    ),
    ChallengersReward = EpochReward * PocChallengersPercent,
    maps:fold(
        fun(Challenger, Challenged, Acc) ->
            PercentofReward = (Challenged*100/TotalChallenged)/100,
            Amount = erlang:round(PercentofReward * ChallengersReward),
            maps:put({gateway, poc_challengers, Challenger}, Amount, Acc)
        end,
        #{},
        Challengers
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec poc_challengees_rewards(blockchain_txn:txns(),
                              map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_challengees_rewards(Transactions, #{epoch_reward := EpochReward,
                                        poc_challengees_percent := PocChallengeesPercent,
                                        poc_version := Version}) ->
    ChallengeesReward = EpochReward * PocChallengeesPercent,
    {Challengees, TotalChallenged} = lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    Path = blockchain_txn_poc_receipts_v1:path(Txn),
                    poc_challengees_rewards_(Version, Path, Acc0)
            end
        end,
        {#{}, 0},
        Transactions
    ),
    maps:fold(
        fun(Challengee, Challenged, Acc) ->
            PercentofReward = (Challenged*100/TotalChallenged)/100,
            % TODO: Not sure about the all round thing...
            Amount = erlang:round(PercentofReward*ChallengeesReward),
            maps:put({gateway, poc_challengees, Challengee}, Amount, Acc)
        end,
        #{},
        Challengees
    ).


poc_challengees_rewards_(_Version, [], Acc) ->
    Acc;
poc_challengees_rewards_(Version, [Elem|Path], {Map, Total}=Acc0) when Version >= 2 ->
    case blockchain_poc_path_element_v1:receipt(Elem) of
        undefined ->
            poc_challengees_rewards_(Version, Path, Acc0);
        Receipt ->
            Challengee = blockchain_poc_path_element_v1:challengee(Elem),
            I = maps:get(Challengee, Map, 0),
            case blockchain_poc_receipt_v1:origin(Receipt) of
                radio ->
                    Acc1 = {maps:put(Challengee, I+1, Map), Total+1},
                    poc_challengees_rewards_(Version, Path, Acc1);
                p2p ->
                    case 
                        blockchain_poc_path_element_v1:witnesses(Elem) /= [] orelse
                        blockchain_txn_poc_receipts_v1:check_path_continuation(Path)
                    of
                        false ->
                            poc_challengees_rewards_(Version, Path, Acc0);
                        true ->
                            Acc1 = {maps:put(Challengee, I+1, Map), Total+1},
                            poc_challengees_rewards_(Version, Path, Acc1)
                    end
            end
    end;
poc_challengees_rewards_(Version, [Elem|Path], {Map, Total}=Acc0) ->
    case blockchain_poc_path_element_v1:receipt(Elem) of
        undefined ->
            poc_challengees_rewards_(Version, Path, Acc0);
        _Receipt ->
            Challengee = blockchain_poc_path_element_v1:challengee(Elem),
            I = maps:get(Challengee, Map, 0),
            Acc1 =  {maps:put(Challengee, I+1, Map), Total+1},
            poc_challengees_rewards_(Version, Path, Acc1)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec poc_witnesses_rewards(blockchain_txn:txns(),
                            map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_witnesses_rewards(Transactions, #{epoch_reward := EpochReward,
                                      poc_witnesses_percent := PocWitnessesPercent}) ->
    {Witnesses, TotalWitnesses} = lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    lists:foldl(
                        fun(Elem, Acc1) ->
                            lists:foldl(
                                fun(WitnessRecord, {Map, Total}) ->
                                    Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                                    I = maps:get(Witness, Map, 0),
                                    {maps:put(Witness, I+1, Map), Total+1}
                                end,
                                Acc1,
                                blockchain_poc_path_element_v1:witnesses(Elem)
                            )
                        end,
                        Acc0,
                        blockchain_txn_poc_receipts_v1:path(Txn)
                    )
            end
        end,
        {#{}, 0},
        Transactions
    ),
    WitnessesReward = EpochReward * PocWitnessesPercent,
    maps:fold(
        fun(Witness, Witnessed, Acc) ->
            PercentofReward = (Witnessed*100/TotalWitnesses)/100,
            Amount = erlang:round(PercentofReward*WitnessesReward),
            maps:put({gateway, poc_witnesses, Witness}, Amount, Acc)
        end,
        #{},
        Witnesses
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_gateway_owner(libp2p_crypto:pubkey_bin(), blockchain_ledger_v1:ledger())-> {ok, libp2p_crypto:pubkey_bin()}
                                                                                     | {error, any()}.
get_gateway_owner(Address, Ledger) ->
    case blockchain_ledger_v1:find_gateway_info(Address, Ledger) of
        {error, _Reason}=Error ->
            lager:error("failed to get gateway owner for ~p: ~p", [Address, _Reason]),
            Error;
        {ok, GwInfo} ->
            {ok, blockchain_ledger_gateway_v2:owner_address(GwInfo)}
    end.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_rewards_v1_pb{start_epoch=1, end_epoch=30, rewards=[]},
    ?assertEqual(Tx, new(1, 30, [])).

start_epoch_test() ->
    Tx = new(1, 30, []),
    ?assertEqual(1, start_epoch(Tx)).

end_epoch_test() ->
    Tx = new(1, 30, []),
    ?assertEqual(30, end_epoch(Tx)).

rewards_test() ->
    Tx = new(1, 30, []),
    ?assertEqual([], rewards(Tx)).

consensus_members_rewards_test() ->
    BaseDir = test_utils:tmp_dir("consensus_members_rewards_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined),
    Ledger = blockchain:ledger(Chain),
    Vars = #{
        epoch_reward => 1000,
        consensus_percent => 0.10
    },
    Rewards = #{
        {gateway, consensus, <<"1">>} => 50,
        {gateway, consensus, <<"2">>} => 50
    },
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, consensus_members, fun(_) ->
        {ok, [O || {gateway, consensus, O} <- maps:keys(Rewards)]}
    end),
    ?assertEqual(Rewards, consensus_members_rewards(Ledger, Vars)),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1).


securities_rewards_test() ->
    BaseDir = test_utils:tmp_dir("securities_rewards_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined),
    Ledger = blockchain:ledger(Chain),
    Vars = #{
        epoch_reward => 1000,
        securities_percent => 0.35
    },
    Rewards = #{
        {owner, securities, <<"1">>} => 175,
        {owner, securities, <<"2">>} => 175
    },
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, securities, fun(_) ->
        #{
            <<"1">> => blockchain_ledger_security_entry_v1:new(0, 2500),
            <<"2">> => blockchain_ledger_security_entry_v1:new(0, 2500)
        }
    end),
    ?assertEqual(Rewards, securities_rewards(Ledger, Vars)),
    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1).

poc_challengers_rewards_test() ->
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"1">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"2">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"1">>, <<"Secret">>, <<"OnionKeyHash">>, [])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengers_percent => 0.09 + 0.06
    },
    Rewards = #{
        {gateway, poc_challengers, <<"1">>} => 100,
        {gateway, poc_challengers, <<"2">>} => 50
    },
    ?assertEqual(Rewards, poc_challengers_rewards(Txns, Vars)).

poc_challengees_rewards_version_1_test() ->
    Receipt1 = blockchain_poc_receipt_v1:new(<<"1">>, 1, 1, <<"data">>, p2p),
    Receipt2 = blockchain_poc_receipt_v1:new(<<"2">>, 1, 1, <<"data">>, radio),

    Elem1 = blockchain_poc_path_element_v1:new(<<"1">>, Receipt1, []),
    Elem2 = blockchain_poc_path_element_v1:new(<<"2">>, Receipt2, []),
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem1]),
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem2])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.19 + 0.16,
        poc_version => 1
    },
    Rewards = #{
        {gateway, poc_challengees, <<"1">>} => 175,
        {gateway, poc_challengees, <<"2">>} => 175
    },
    ?assertEqual(Rewards, poc_challengees_rewards(Txns, Vars)).

poc_challengees_rewards_version_2_test() ->
    ReceiptFor1 = blockchain_poc_receipt_v1:new(<<"1">>, 1, 1, <<"data">>, p2p),
    WitnessFor1 = blockchain_poc_witness_v1:new(<<"1">>, 1, 1, <<>>),
    ReceiptFor2 = blockchain_poc_receipt_v1:new(<<"2">>, 1, 1, <<"data">>, radio),
    WitnessFor2 = blockchain_poc_witness_v1:new(<<"2">>, 1, 1, <<>>),
    
    ElemFor1 = blockchain_poc_path_element_v1:new(<<"1">>, ReceiptFor1, []),
    ElemFor1WithWitness = blockchain_poc_path_element_v1:new(<<"1">>, ReceiptFor1, [WitnessFor1]),
    ElemFor2 = blockchain_poc_path_element_v1:new(<<"2">>, ReceiptFor2, []),
    ElemFor2WithWitness = blockchain_poc_path_element_v1:new(<<"2">>, ReceiptFor2, [WitnessFor2]),

    Txns = [
        %% No rewards here, Only receipt with no witness or subsequent receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemFor1]),
        %% Reward because of witness
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemFor1WithWitness]),
        %% Reward because of next elem has receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemFor1, ElemFor2]),
        %% Reward because of witness (adding to make reward 50/50)
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemFor2WithWitness])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.19 + 0.16,
        poc_version => 2
    },
    Rewards = #{
        {gateway, poc_challengees, <<"1">>} => 175,
        {gateway, poc_challengees, <<"2">>} => 175
    },
    ?assertEqual(Rewards, poc_challengees_rewards(Txns, Vars)).


poc_witnesses_rewards_test() ->
    Witness1 = blockchain_poc_witness_v1:new(<<"1">>, 1, 1, <<>>),
    Witness2 = blockchain_poc_witness_v1:new(<<"2">>, 1, 1, <<>>),
    Elem = blockchain_poc_path_element_v1:new(<<"Y">>, <<"Receipt not undefined">>, [Witness1, Witness2]),
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem]),
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_witnesses_percent => 0.02 + 0.03
    },
    Rewards = #{
        {gateway, poc_witnesses, <<"1">>} => 25,
        {gateway, poc_witnesses, <<"2">>} => 25
    },
    ?assertEqual(Rewards, poc_witnesses_rewards(Txns, Vars)).

-endif.
