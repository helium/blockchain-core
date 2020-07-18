%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Rewards ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_rewards_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_rewards_v1_pb.hrl").

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
    calculate_rewards/3,
    print/1,
    to_json/2
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
-spec calculate_rewards(non_neg_integer(), non_neg_integer(), blockchain:blockchain()) ->
    {ok, blockchain_txn_reward_v1:rewards()} | {error, any()}.
calculate_rewards(Start, End, Chain) ->
    {ok, Ledger} = blockchain:ledger_at(End, Chain),
    Vars = get_reward_vars(Start, End, Ledger),
    case get_rewards_for_epoch(Start, End, Chain, Vars, Ledger) of
        {error, _Reason}=Error ->
            Error;
        {ok, POCChallengersRewards, POCChallengeesRewards, POCWitnessesRewards, DCRewards} ->
            SecuritiesRewards = securities_rewards(Ledger, Vars),
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
                        POCChallengeesRewards, POCWitnessesRewards, DCRewards]
                      ),
            {ok, Result}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_rewards()) -> iodata().
print(undefined) -> <<"type=rewards undefined">>;
print(#blockchain_txn_rewards_v1_pb{start_epoch=Start, end_epoch=End,
                                    rewards=Rewards}) ->
    io_lib:format("type=rewards start_epoch=~p end_epoch=~p rewards=~p",
                  [Start, End, Rewards]).

-spec to_json(txn_rewards(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"rewards_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      start_epoch => start_epoch(Txn),
      end_epoch => end_epoch(Txn),
      rewards => [blockchain_txn_reward_v1:to_json(R, []) || R <- rewards(Txn)]
     }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_rewards_for_epoch(non_neg_integer(), non_neg_integer(),
                         blockchain:blockchain(), map(), blockchain_ledger_v1:ledger()) -> {ok, map(), map(), map(), map()}
                                                                            | {error, any()}.

get_rewards_for_epoch(Start, End, Chain, Vars, Ledger) ->
    get_rewards_for_epoch(Start, End, Chain, Vars, Ledger, #{}, #{}, #{}, #{}).

get_rewards_for_epoch(Start, End, _Chain, Vars, _Ledger, ChallengerRewards, ChallengeeRewards, WitnessRewards, DCRewards) when Start == End+1 ->
    {ok, normalize_challenger_rewards(ChallengerRewards, Vars),
     normalize_challengee_rewards(ChallengeeRewards, Vars),
     normalize_witness_rewards(WitnessRewards, Vars),
     normalize_dc_rewards(DCRewards, Vars)};
get_rewards_for_epoch(Current, End, Chain, Vars, Ledger, ChallengerRewards, ChallengeeRewards, WitnessRewards, DCRewards) ->
    case blockchain:get_block(Current, Chain) of
        {error, _Reason}=Error ->
            lager:error("failed to get block ~p ~p", [_Reason, Current]),
            Error;
        {ok, Block} ->
            Transactions = blockchain_block:transactions(Block),
            case lists:any(fun(Txn) -> blockchain_txn:type(Txn) == ?MODULE end, Transactions) of
                true ->
                    {error, already_existing_rewards};
                false ->
                    get_rewards_for_epoch(Current+1, End, Chain, Vars, Ledger,
                                          poc_challengers_rewards(Transactions, Vars, ChallengerRewards),
                                          poc_challengees_rewards(Transactions, Vars, Ledger, ChallengeeRewards),
                                          poc_witnesses_rewards(Transactions, Vars, Ledger, WitnessRewards),
                                          dc_rewards(Transactions, End, Vars, Ledger, DCRewards))
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_reward_vars(pos_integer(), pos_integer(), blockchain_ledger_v1:ledger()) -> map().
get_reward_vars(Start, End, Ledger) ->
    {ok, MonthlyReward} = blockchain:config(?monthly_reward, Ledger),
    {ok, SecuritiesPercent} = blockchain:config(?securities_percent, Ledger),
    {ok, PocChallengeesPercent} = blockchain:config(?poc_challengees_percent, Ledger),
    {ok, PocChallengersPercent} = blockchain:config(?poc_challengers_percent, Ledger),
    {ok, PocWitnessesPercent} = blockchain:config(?poc_witnesses_percent, Ledger),
    {ok, ConsensusPercent} = blockchain:config(?consensus_percent, Ledger),
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(Ledger),
    DCPercent = case blockchain:config(?dc_percent, Ledger) of
                    {ok, R1} ->
                        R1;
                    _ ->
                        0
                end,
    SCGrace = case blockchain:config(?sc_grace_blocks, Ledger) of
                  {ok, R2} ->
                      R2;
                  _ ->
                      0
              end,
    SCVersion = case blockchain:config(?sc_version, Ledger) of
                    {ok, R3} ->
                        R3;
                    _ ->
                        1
                end,
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
        consensus_members => ConsensusMembers,
        dc_percent => DCPercent,
        sc_grace_blocks => SCGrace,
        sc_version => SCVersion,
        poc_version => POCVersion
    }.

-spec calculate_epoch_reward(pos_integer(), pos_integer(), blockchain_ledger_v1:ledger()) -> float().
calculate_epoch_reward(Start, End, Ledger) ->
    Version = case blockchain:config(?reward_version, Ledger) of
        {ok, V} -> V;
        _ -> 1
    end,
    calculate_epoch_reward(Version, Start, End, Ledger).

-spec calculate_epoch_reward(pos_integer(), pos_integer(), pos_integer(), blockchain_ledger_v1:ledger()) -> float().
calculate_epoch_reward(Version, Start, End, Ledger) ->
    {ok, ElectionInterval} = blockchain:config(?election_interval, Ledger),
    {ok, BlockTime0} = blockchain:config(?block_time, Ledger),
    {ok, MonthlyReward} = blockchain:config(?monthly_reward, Ledger),
    calculate_epoch_reward(Version, Start, End, BlockTime0, ElectionInterval, MonthlyReward).

-spec calculate_epoch_reward(pos_integer(), pos_integer(), pos_integer(),
                             pos_integer(), pos_integer(), pos_integer()) -> float().
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
                              map(), map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_challengers_rewards(Transactions, #{poc_version := Version},
                       ExistingRewards) ->
    lists:foldl(
        fun(Txn, Map) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Map;
                true ->
                    Challenger = blockchain_txn_poc_receipts_v1:challenger(Txn),
                    I = maps:get(Challenger, Map, 0),
                    case blockchain_txn_poc_receipts_v1:check_path_continuation(
                           blockchain_txn_poc_receipts_v1:path(Txn)) of
                        true when is_integer(Version), Version > 4 ->
                            %% not an all gray path, full credit
                            maps:put(Challenger, I+2, Map);
                        _ ->
                            %% all gray path or v4 or earlier, only partial credit
                            %% to incentivize fixing your networking
                            maps:put(Challenger, I+1, Map)
                    end
            end
        end,
        ExistingRewards,
        Transactions
    ).

normalize_challenger_rewards(ChallengerRewards, #{epoch_reward := EpochReward,
                                        poc_challengers_percent := PocChallengersPercent}) ->
    TotalChallenged = lists:sum(maps:values(ChallengerRewards)),
    ChallengersReward = EpochReward * PocChallengersPercent,
    maps:fold(
        fun(Challenger, Challenged, Acc) ->
            PercentofReward = (Challenged*100/TotalChallenged)/100,
            Amount = erlang:round(PercentofReward * ChallengersReward),
            maps:put({gateway, poc_challengers, Challenger}, Amount, Acc)
        end,
        #{},
        ChallengerRewards
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec poc_challengees_rewards(Transactions :: blockchain_txn:txns(),
                              Vars :: map(),
                              Ledger :: blockchain_ledger_v1:ledger(), map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_challengees_rewards(Transactions,
                        #{poc_version := Version},
                        Ledger, ExistingRewards) ->
    lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    Path = blockchain_txn_poc_receipts_v1:path(Txn),
                    poc_challengees_rewards_(Version, Path, Ledger, true, Acc0)
            end
        end,
        ExistingRewards,
        Transactions
    ).

normalize_challengee_rewards(ChallengeeRewards, #{epoch_reward := EpochReward, poc_challengees_percent := PocChallengeesPercent}) ->
    TotalChallenged = lists:sum(maps:values(ChallengeeRewards)),
    ChallengeesReward = EpochReward * PocChallengeesPercent,
    maps:fold(
        fun(Challengee, Challenged, Acc) ->
            PercentofReward = (Challenged*100/TotalChallenged)/100,
            % TODO: Not sure about the all round thing...
            Amount = erlang:round(PercentofReward*ChallengeesReward),
            maps:put({gateway, poc_challengees, Challengee}, Amount, Acc)
        end,
        #{},
        ChallengeeRewards
    ).

poc_challengees_rewards_(_Version, [], _Ledger, _, Acc) ->
    Acc;
poc_challengees_rewards_(Version, [Elem|Path], Ledger, IsFirst, Acc0) when Version >= 2 ->
    %% check if there were any legitimate witnesses
    Witnesses = case Version of
                    V when is_integer(V), V > 4 ->
                        blockchain_txn_poc_receipts_v1:good_quality_witnesses(Elem, Ledger);
                    _ ->
                        blockchain_poc_path_element_v1:witnesses(Elem)
                end,
    Challengee = blockchain_poc_path_element_v1:challengee(Elem),
    I = maps:get(Challengee, Acc0, 0),
    case blockchain_poc_path_element_v1:receipt(Elem) of
        undefined ->
            Acc1 = case
                       Witnesses /= [] orelse
                       blockchain_txn_poc_receipts_v1:check_path_continuation(Path)
                   of
                       true when is_integer(Version), Version > 4, IsFirst == true ->
                           %% while we don't have a receipt for this node, we do know
                           %% there were witnesses or the path continued which means
                           %% the challengee transmitted
                           maps:put(Challengee, I+1, Acc0);
                       true when is_integer(Version), Version > 4, IsFirst == false ->
                           %% while we don't have a receipt for this node, we do know
                           %% there were witnesses or the path continued which means
                           %% the challengee transmitted
                           %% Additionally, we know this layer came in over radio so
                           %% there's an implicit rx as well
                           maps:put(Challengee, I+2, Acc0);
                       _ ->
                           Acc0
                   end,
            poc_challengees_rewards_(Version, Path, Ledger, false, Acc1);
        Receipt ->
            case blockchain_poc_receipt_v1:origin(Receipt) of
                radio ->
                    Acc1 = case
                               Witnesses /= [] orelse
                               blockchain_txn_poc_receipts_v1:check_path_continuation(Path)
                           of
                               true when is_integer(Version), Version > 4 ->
                                   %% this challengee both rx'd and tx'd over radio
                                   %% AND sent a receipt
                                   %% so give them 3 payouts
                                   maps:put(Challengee, I+3, Acc0);
                               false when is_integer(Version), Version > 4 ->
                                   %% this challengee rx'd and sent a receipt
                                   maps:put(Challengee, I+2, Acc0);
                               _ ->
                                   maps:put(Challengee, I+1, Acc0)
                           end,
                    poc_challengees_rewards_(Version, Path, Ledger, false, Acc1);
                p2p ->
                    %% if there are legitimate witnesses or the path continues
                    %% the challengee did their job
                    Acc1 = case
                               Witnesses /= [] orelse
                               blockchain_txn_poc_receipts_v1:check_path_continuation(Path)
                           of
                               false ->
                                   %% path did not continue, this is an 'all gray' path
                                   Acc0;
                               true when is_integer(Version), Version > 4 ->
                                   %% Sent a receipt and the path continued on
                                   maps:put(Challengee, I+2, Acc0);
                               true ->
                                   maps:put(Challengee, I+1, Acc0)
                           end,
                    poc_challengees_rewards_(Version, Path, Ledger, false, Acc1)
            end
    end;
poc_challengees_rewards_(Version, [Elem|Path], Ledger, _IsFirst, Acc0) ->
    case blockchain_poc_path_element_v1:receipt(Elem) of
        undefined ->
            poc_challengees_rewards_(Version, Path, Ledger, false, Acc0);
        _Receipt ->
            Challengee = blockchain_poc_path_element_v1:challengee(Elem),
            I = maps:get(Challengee, Acc0, 0),
            Acc1 =  maps:put(Challengee, I+1, Acc0),
            poc_challengees_rewards_(Version, Path, Ledger, false, Acc1)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec poc_witnesses_rewards(Transactions :: blockchain_txn:txns(),
                            Vars :: map(),
                            Ledger :: blockchain_ledger_v1:ledger(), map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_witnesses_rewards(Transactions,
                      #{poc_version := POCVersion},
                      Ledger, WitnessRewards) ->
    lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    case POCVersion of
                        V when is_integer(V), V > 4 ->
                            lists:foldl(
                              fun(Elem, Acc1) ->
                                      case blockchain_txn_poc_receipts_v1:good_quality_witnesses(Elem, Ledger) of
                                          [] ->
                                              Acc1;
                                          GoodQualityWitnesses ->
                                              lists:foldl(
                                                fun(WitnessRecord, Map) ->
                                                        Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                                                        I = maps:get(Witness, Map, 0),
                                                        maps:put(Witness, I+1, Map)
                                                end,
                                                Acc1,
                                                GoodQualityWitnesses
                                               )
                                      end
                              end,
                              Acc0,
                              blockchain_txn_poc_receipts_v1:path(Txn)
                             );
                        _ ->
                            lists:foldl(
                              fun(Elem, Acc1) ->
                                      lists:foldl(
                                        fun(WitnessRecord, Map) ->
                                                Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                                                I = maps:get(Witness, Map, 0),
                                                maps:put(Witness, I+1, Map)
                                        end,
                                        Acc1,
                                        blockchain_poc_path_element_v1:witnesses(Elem)
                                       )
                              end,
                              Acc0,
                              blockchain_txn_poc_receipts_v1:path(Txn)
                             )
                    end
            end
        end,
        WitnessRewards,
        Transactions
    ).

normalize_witness_rewards(WitnessRewards, #{epoch_reward := EpochReward,
                                            poc_witnesses_percent := PocWitnessesPercent}) ->
    TotalWitnesses = lists:sum(maps:values(WitnessRewards)),
    WitnessesReward = EpochReward * PocWitnessesPercent,
    maps:fold(
        fun(Witness, Witnessed, Acc) ->
            PercentofReward = (Witnessed*100/TotalWitnesses)/100,
            Amount = erlang:round(PercentofReward*WitnessesReward),
            maps:put({gateway, poc_witnesses, Witness}, Amount, Acc)
        end,
        #{},
        WitnessRewards
    ).

dc_rewards(Transactions, EndHeight, #{sc_grace_blocks := GraceBlocks, sc_version := 2}, Ledger, DCRewards) ->
    lists:foldl(
      fun(Txn, Acc) ->
              %% check the state channel's grace period ended in this epoch
            case blockchain_txn:type(Txn) == blockchain_txn_state_channel_close_v1 andalso
                 blockchain_txn_state_channel_close_v1:state_channel_expire_at(Txn) + GraceBlocks < EndHeight
            of
                true ->
                    SCID = blockchain_txn_state_channel_close_v1:state_channel_id(Txn),
                    case lists:member(SCID, maps:get(seen, DCRewards, [])) of
                        false ->
                            %% haven't seen this state channel yet, pull the final result from the ledger
                            {ok, SC} = blockchain_ledger_v1:find_state_channel(blockchain_txn_state_channel_close_v1:state_channel_id(Txn),
                                                                               blockchain_txn_state_channel_close_v1:state_channel_owner(Txn),
                                                                               Ledger),
                            %% check for a holdover v1 channel
                            case blockchain_ledger_state_channel_v2:is_v2(SC) of
                                true ->
                                    %% pull out the final version of the state channel
                                    FinalSC = blockchain_ledger_state_channel_v2:state_channel(SC),
                                    Summaries = blockchain_state_channel_v1:summaries(FinalSC),

                                    %% check the dispute status
                                    Bonus = case blockchain_ledger_state_channel_v2:close_state(SC) of
                                                 dispute ->
                                                    %% the owner of the state channel did a naughty thing, divide their overcommit between the participants
                                                     OverCommit = blockchain_ledger_state_channel_v2:amount(SC) -blockchain_ledger_state_channel_v2:original(SC),
                                                     OverCommit div length(Summaries);
                                                 _ ->
                                                     0
                                             end,

                                    lists:foldl(fun(Summary, Acc1) ->
                                                        Key = blockchain_state_channel_summary_v1:client_pubkeybin(Summary),
                                                        DCs = blockchain_state_channel_summary_v1:num_dcs(Summary) + Bonus,
                                                        maps:update_with(Key, fun(V) -> V + DCs end, DCs, Acc1)
                                                end, maps:update_with(seen, fun(Seen) -> [SCID|Seen] end, [SCID], Acc),
                                                Summaries);
                                false ->
                                    Acc
                            end;
                        true ->
                            Acc
                    end;
                false ->
                    %% check for transaction fees above the minimum and credit the overages to the consensus group
                    %% XXX this calculation may not be entirely correct if the fees change during the epoch. However,
                    %% since an epoch may stretch past the lagging ledger, we cannot know for certain what the fee
                    %% structure was at any point in this epoch so we make the reasonable conclusion to use the
                    %% conditions at the end of the epoch to calculate fee overages for the purposes of rewards.
                    Type = blockchain_txn:type(Txn),
                    try Type:calculate_fee(Txn, Ledger) - Type:fee(Txn) of
                        Overage when Overage > 0 ->
                            maps:update_with(overages, fun(Overages) -> Overages + Overage end, Overage, Acc);
                        _ ->
                            Acc
                    catch
                        _:_ ->
                            Acc
                    end
            end
      end,
      DCRewards,
      Transactions
     );
dc_rewards(_Txns, _EndHeight, _Vars, _Ledger, DCRewards) ->
    DCRewards.

normalize_dc_rewards(DCRewards0, #{epoch_reward := EpochReward,
                                   consensus_members := ConsensusMembers,
                                  dc_percent := DCPercent}) ->
    DCRewards1 = maps:remove(seen, DCRewards0),
    DCRewards = case maps:take(overages, DCRewards1) of
                    {OverageTotal, NewMap} ->
                        OveragePerMember = OverageTotal div length(ConsensusMembers),
                        lists:foldl(fun(Member, Acc) ->
                                            maps:update_with(Member, fun(Balance) -> Balance + OveragePerMember end, OveragePerMember, Acc)
                                    end, NewMap, ConsensusMembers);
                    error ->
                        %% no overages to account for
                        DCRewards1
                end,
    TotalDCs = lists:sum(maps:values(DCRewards)),
    DCReward = EpochReward * DCPercent,
    maps:fold(
        fun(Key, NumDCs, Acc) ->
            PercentofReward = (NumDCs*100/TotalDCs)/100,
            Amount = erlang:round(PercentofReward*DCReward),
            maps:put({gateway, data_credits, Key}, Amount, Acc)
        end,
        #{},
        DCRewards
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_gateway_owner(libp2p_crypto:pubkey_bin(), blockchain_ledger_v1:ledger())-> {ok, libp2p_crypto:pubkey_bin()}
                                                                                     | {error, any()}.
get_gateway_owner(Address, Ledger) ->
    case blockchain_gateway_cache:get(Address, Ledger) of
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
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
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
    meck:unload(blockchain_ledger_v1),
    test_utils:cleanup_tmp_dir(BaseDir).



securities_rewards_test() ->
    BaseDir = test_utils:tmp_dir("securities_rewards_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
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
    meck:unload(blockchain_ledger_v1),
    test_utils:cleanup_tmp_dir(BaseDir).

poc_challengers_rewards_1_test() ->
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"a">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"b">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"a">>, <<"Secret">>, <<"OnionKeyHash">>, [])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengers_percent => 0.09 + 0.06,
        poc_version => 5
    },
    Rewards = #{
        {gateway, poc_challengers, <<"a">>} => 100,
        {gateway, poc_challengers, <<"b">>} => 50
    },
    ?assertEqual(Rewards, normalize_challenger_rewards(poc_challengers_rewards(Txns, Vars, #{}), Vars)).

poc_challengers_rewards_2_test() ->
    ReceiptForA = blockchain_poc_receipt_v1:new(<<"a">>, 1, 1, <<"data">>, radio),
    ElemForA = blockchain_poc_path_element_v1:new(<<"a">>, ReceiptForA, []),

    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"a">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"b">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"c">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengers_percent => 0.09 + 0.06,
        poc_version => 5
    },
    Rewards = #{
        {gateway, poc_challengers, <<"a">>} => 38,
        {gateway, poc_challengers, <<"b">>} => 38,
        {gateway, poc_challengers, <<"c">>} => 75
    },
    ?assertEqual(Rewards, normalize_challenger_rewards(poc_challengers_rewards(Txns, Vars, #{}), Vars)).

poc_challengees_rewards_1_test() ->
    BaseDir = test_utils:tmp_dir("poc_challengees_rewards_1_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.19 + 0.16,
        poc_version => 5
    },

    LedgerVars = maps:put(?poc_version, 5, common_poc_vars()),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    One = 631179381270930431,
    Two = 631196173757531135,

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"a">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"a">>, One, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"b">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"b">>, Two, 1, Ledger1),

    ok = blockchain_ledger_v1:commit_context(Ledger1),

    ReceiptForA = blockchain_poc_receipt_v1:new(<<"a">>, 1, 1, <<"data">>, p2p),
    WitnessForA = blockchain_poc_witness_v1:new(<<"a">>, 1, 1, <<>>),
    ReceiptForB = blockchain_poc_receipt_v1:new(<<"b">>, 1, 1, <<"data">>, radio),
    WitnessForB = blockchain_poc_witness_v1:new(<<"b">>, 1, 1, <<>>),

    ElemForA = blockchain_poc_path_element_v1:new(<<"a">>, ReceiptForA, []),
    ElemForAWithWitness = blockchain_poc_path_element_v1:new(<<"a">>, ReceiptForA, [WitnessForA]),
    ElemForB = blockchain_poc_path_element_v1:new(<<"b">>, ReceiptForB, []),
    ElemForBWithWitness = blockchain_poc_path_element_v1:new(<<"b">>, ReceiptForB, [WitnessForB]),

    Txns = [
        %% No rewards here, Only receipt with no witness or subsequent receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA]),
        %% Reward because of witness
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForAWithWitness]),
        %% Reward because of next elem has receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA, ElemForB]),
        %% Reward because of witness (adding to make reward 50/50)
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForBWithWitness])
    ],
    %% NOTE: These have changed because A has the receipt over p2p and no good witness
    Rewards = #{
        {gateway, poc_challengees, <<"a">>} => 117,
        {gateway, poc_challengees, <<"b">>} => 233
    },
    ?assertEqual(Rewards, normalize_challengee_rewards(poc_challengees_rewards(Txns, Vars, Ledger, #{}), Vars)).

poc_challengees_rewards_2_test() ->
    BaseDir = test_utils:tmp_dir("poc_challengees_rewards_2_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.19 + 0.16,
        poc_version => 5
    },

    LedgerVars = maps:put(?poc_version, 5, common_poc_vars()),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    One = 631179381270930431,
    Two = 631196173757531135,

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"a">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"a">>, One, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"b">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"b">>, Two, 1, Ledger1),

    ok = blockchain_ledger_v1:commit_context(Ledger1),

    ReceiptForA = blockchain_poc_receipt_v1:new(<<"a">>, 1, -80, <<"data">>, radio),
    WitnessForA = blockchain_poc_witness_v1:new(<<"a">>, 1, -80, <<>>),
    ReceiptForB = blockchain_poc_receipt_v1:new(<<"b">>, 1, -70, <<"data">>, radio),
    WitnessForB = blockchain_poc_witness_v1:new(<<"b">>, 1, -70, <<>>),

    ElemForA = blockchain_poc_path_element_v1:new(<<"a">>, undefined, []),
    ElemForAWithWitness = blockchain_poc_path_element_v1:new(<<"a">>, ReceiptForA, [WitnessForA]),
    ElemForB = blockchain_poc_path_element_v1:new(<<"b">>, ReceiptForB, []),
    ElemForBWithWitness = blockchain_poc_path_element_v1:new(<<"b">>, ReceiptForB, [WitnessForB]),

    Txns = [
        %% No rewards here, Only receipt with no witness or subsequent receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForB, ElemForA]),
        %% Reward because of witness
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForAWithWitness]),
        %% Reward because of next elem has receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA, ElemForB]),
        %% Reward because of witness (adding to make reward 50/50)
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForBWithWitness])
    ],
    %% NOTE: Rewards are split 33-66%
    Rewards = #{
        {gateway, poc_challengees, <<"a">>} => 117,
        {gateway, poc_challengees, <<"b">>} => 233
    },
    ?assertEqual(Rewards, normalize_challengee_rewards(poc_challengees_rewards(Txns, Vars, Ledger, #{}), Vars)).

poc_challengees_rewards_3_test() ->
    BaseDir = test_utils:tmp_dir("poc_challengees_rewards_3_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.19 + 0.16,
        poc_version => 5
    },

    LedgerVars = maps:put(?poc_version, 5, common_poc_vars()),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    One = 631179381270930431,
    Two = 631196173757531135,
    Three = 631196173214364159,

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"a">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"a">>, One, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"b">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"b">>, Two, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"c">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"c">>, Three, 1, Ledger1),

    ok = blockchain_ledger_v1:commit_context(Ledger1),

    ReceiptForA = blockchain_poc_receipt_v1:new(<<"a">>, 1, -120, <<"data">>, radio),
    WitnessForA = blockchain_poc_witness_v1:new(<<"c">>, 1, -120, <<>>),
    ReceiptForB = blockchain_poc_receipt_v1:new(<<"b">>, 1, -70, <<"data">>, radio),
    WitnessForB = blockchain_poc_witness_v1:new(<<"c">>, 1, -120, <<>>),
    ReceiptForC = blockchain_poc_receipt_v1:new(<<"c">>, 1, -120, <<"data">>, radio),

    ElemForA = blockchain_poc_path_element_v1:new(<<"a">>, ReceiptForA, []),
    ElemForAWithWitness = blockchain_poc_path_element_v1:new(<<"a">>, ReceiptForA, [WitnessForA]),
    ElemForB = blockchain_poc_path_element_v1:new(<<"b">>, undefined, []),
    ElemForBWithWitness = blockchain_poc_path_element_v1:new(<<"b">>, ReceiptForB, [WitnessForB]),
    ElemForC = blockchain_poc_path_element_v1:new(<<"c">>, ReceiptForC, []),

    Txns = [
        %% No rewards here, Only receipt with no witness or subsequent receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForB, ElemForA]),  %% 1, 2
        %% Reward because of witness
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForAWithWitness]), %% 3
        %% Reward because of next elem has receipt
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA, ElemForB, ElemForC]), %% 3, 2, 2
        %% Reward because of witness (adding to make reward 50/50)
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForBWithWitness]) %% 3
    ],
    Rewards = #{
        %% a gets 8 shares
        {gateway, poc_challengees, <<"a">>} => 175,
        %% b gets 6 shares
        {gateway, poc_challengees, <<"b">>} => 131,
        %% c gets 2 shares
        {gateway, poc_challengees, <<"c">>} => 44
    },
    ?assertEqual(Rewards, normalize_challengee_rewards(poc_challengees_rewards(Txns, Vars, Ledger, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

poc_witnesses_rewards_test() ->
    BaseDir = test_utils:tmp_dir("poc_witnesses_rewards_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    EpochVars = #{
      epoch_reward => 1000,
      poc_witnesses_percent => 0.02 + 0.03,
      poc_version => 5
     },

    LedgerVars = maps:put(?poc_version, 5, common_poc_vars()),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    One = 631179381270930431,
    Two = 631196173757531135,
    Three = 631196173214364159,
    Four = 631179381325720575,
    Five = 631179377081096191,

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"a">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"a">>, One, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"b">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"b">>, Two, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"c">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"c">>, Three, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"d">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"d">>, Four, 1, Ledger1),

    ok = blockchain_ledger_v1:add_gateway(<<"o">>, <<"e">>, Ledger1),
    ok = blockchain_ledger_v1:add_gateway_location(<<"e">>, Five, 1, Ledger1),

    ok = blockchain_ledger_v1:commit_context(Ledger1),

    Witness1 = blockchain_poc_witness_v1:new(<<"a">>, 1, -80, <<>>),
    Witness2 = blockchain_poc_witness_v1:new(<<"b">>, 1, -80, <<>>),
    Elem = blockchain_poc_path_element_v1:new(<<"c">>, <<"Receipt not undefined">>, [Witness1, Witness2]),
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"d">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem]),
        blockchain_txn_poc_receipts_v1:new(<<"e">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem])
    ],

    Rewards = #{{gateway,poc_witnesses,<<"a">>} => 25,
                {gateway,poc_witnesses,<<"b">>} => 25},

    ?assertEqual(Rewards, normalize_witness_rewards(poc_witnesses_rewards(Txns, EpochVars, Ledger, #{}), EpochVars)),
    test_utils:cleanup_tmp_dir(BaseDir).

old_poc_challengers_rewards_test() ->
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"1">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"2">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"1">>, <<"Secret">>, <<"OnionKeyHash">>, [])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengers_percent => 0.09 + 0.06,
        poc_version => 2
    },
    Rewards = #{
        {gateway, poc_challengers, <<"1">>} => 100,
        {gateway, poc_challengers, <<"2">>} => 50
    },
    ?assertEqual(Rewards, normalize_challenger_rewards(poc_challengers_rewards(Txns, Vars, #{}), Vars)).

old_poc_challengees_rewards_version_1_test() ->
    BaseDir = test_utils:tmp_dir("old_poc_challengees_rewards_version_1_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    LedgerVars = common_poc_vars(),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),
    ok = blockchain_ledger_v1:commit_context(Ledger1),

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
    ?assertEqual(Rewards, normalize_challengee_rewards(poc_challengees_rewards(Txns, Vars, Ledger, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

old_poc_challengees_rewards_version_2_test() ->
    BaseDir = test_utils:tmp_dir("old_poc_challengees_rewards_version_2_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    LedgerVars = common_poc_vars(),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),
    ok = blockchain_ledger_v1:commit_context(Ledger1),

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
    ?assertEqual(Rewards, normalize_challengee_rewards(poc_challengees_rewards(Txns, Vars, Ledger, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

old_poc_witnesses_rewards_test() ->
    BaseDir = test_utils:tmp_dir("old_poc_witnesses_rewards_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    LedgerVars = common_poc_vars(),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    Witness1 = blockchain_poc_witness_v1:new(<<"1">>, 1, 1, <<>>),
    Witness2 = blockchain_poc_witness_v1:new(<<"2">>, 1, 1, <<>>),
    Elem = blockchain_poc_path_element_v1:new(<<"Y">>, <<"Receipt not undefined">>, [Witness1, Witness2]),
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem]),
        blockchain_txn_poc_receipts_v1:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem])
    ],
    EpochVars = #{
        epoch_reward => 1000,
        poc_witnesses_percent => 0.02 + 0.03,
        poc_version => 2
    },
    Rewards = #{
        {gateway, poc_witnesses, <<"1">>} => 25,
        {gateway, poc_witnesses, <<"2">>} => 25
    },
    ?assertEqual(Rewards, normalize_witness_rewards(poc_witnesses_rewards(Txns, EpochVars, Ledger, #{}), EpochVars)),
    test_utils:cleanup_tmp_dir(BaseDir).


dc_rewards_test() ->
    BaseDir = test_utils:tmp_dir("poc_challengees_rewards_2_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 1000,
        dc_percent => 0.3,
        sc_version => 2,
        sc_grace_blocks => 5,
        consensus_members => [<<"c">>, <<"d">>]
    },

    LedgerVars = maps:merge(#{?poc_version => 5, ?sc_version => 2, ?sc_grace_blocks => 5}, common_poc_vars()),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    {SC0, _} = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 100, <<"blockhash">>, 10),
    SC = blockchain_state_channel_v1:summaries([blockchain_state_channel_summary_v1:new(<<"a">>, 1, 1), blockchain_state_channel_summary_v1:new(<<"b">>, 2, 2)], SC0),

    ok = blockchain_ledger_v1:add_state_channel(<<"id">>, <<"owner">>, 10, 1, 100, 200, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),

    ok = blockchain_ledger_v1:close_state_channel(<<"owner">>, <<"owner">>, SC, <<"id">>, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),


    Txns = [
         blockchain_txn_state_channel_close_v1:new(SC, <<"owner">>)
    ],
    %% NOTE: Rewards are split 33-66%
    Rewards = #{
        {gateway, data_credits, <<"a">>} => 100,
        {gateway, data_credits, <<"b">>} => 200
    },
    ?assertEqual(Rewards, normalize_dc_rewards(dc_rewards(Txns, 100, Vars, Ledger1, #{}), Vars)).


to_json_test() ->
    Tx = #blockchain_txn_rewards_v1_pb{start_epoch=1, end_epoch=30, rewards=[]},
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, start_epoch, end_epoch, rewards])).


common_poc_vars() ->
    #{
        ?poc_v4_exclusion_cells => 10,
        ?poc_v4_parent_res => 11,
        ?poc_v4_prob_bad_rssi => 0.01,
        ?poc_v4_prob_count_wt => 0.3,
        ?poc_v4_prob_good_rssi => 1.0,
        ?poc_v4_prob_no_rssi => 0.5,
        ?poc_v4_prob_rssi_wt => 0.3,
        ?poc_v4_prob_time_wt => 0.3,
        ?poc_v4_randomness_wt => 0.1,
        ?poc_v4_target_challenge_age => 300,
        ?poc_v4_target_exclusion_cells => 6000,
        ?poc_v4_target_prob_edge_wt => 0.2,
        ?poc_v4_target_prob_score_wt => 0.8,
        ?poc_v4_target_score_curve => 5,
        ?poc_v5_target_prob_randomness_wt => 0.0
    }.

-endif.
