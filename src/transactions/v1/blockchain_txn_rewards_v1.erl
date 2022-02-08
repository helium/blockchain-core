%%-------------------------------------------------------------------
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
    fee_payer/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    calculate_rewards/3,
    print/1,
    json_type/0,
    to_json/2,
    legit_witnesses/6
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

-define(T, #blockchain_txn_rewards_v1_pb).

-type t() :: txn_rewards().

-type txn_rewards() :: ?T{}.

-export_type([t/0, txn_rewards/0]).

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

-spec fee_payer(txn_rewards(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_rewards(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
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

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

%%--------------------------------------------------------------------
%% @doc Absorb rewards in main ledger and/or aux ledger (if enabled)
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_rewards(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),

    case blockchain_ledger_v1:mode(Ledger) == aux of
        false ->
            %% only absorb in the main ledger
            absorb_(Txn, Ledger);
        true ->
            %% absorb in the aux ledger
            aux_absorb(Txn, Ledger, Chain)
    end.

-spec aux_absorb(Txn :: txn_rewards(),
                 AuxLedger :: blockchain_ledger_v1:ledger(),
                 Chain :: blockchain:blockchain()) -> ok | {error, any()}.
aux_absorb(Txn, AuxLedger, Chain) ->
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    %% NOTE: This is an aux ledger, we don't use rewards(txn) here, instead we calculate them manually
    %% and do 0 verification for absorption
    case calculate_rewards_(Start, End, AuxLedger, Chain) of
        {error, _}=E -> E;
        {ok, AuxRewards} ->
            TxnRewards = rewards(Txn),
            %% absorb the rewards attached to the txn (real)
            absorb_rewards(TxnRewards, AuxLedger),
            %% set auxiliary rewards in the aux ledger also
            lager:info("are aux rewards equal?: ~p", [lists:sort(TxnRewards) == lists:sort(AuxRewards)]),
            %% rewards appear in (End + 1) block
            blockchain_aux_ledger_v1:set_rewards(End + 1, TxnRewards, AuxRewards, AuxLedger)
    end.

-spec absorb_(Txn :: txn_rewards(), Ledger :: blockchain_ledger_v1:ledger()) -> ok.
absorb_(Txn, Ledger) ->
    Rewards = ?MODULE:rewards(Txn),
    absorb_rewards(Rewards, Ledger).

-spec absorb_rewards(Rewards :: blockchain_txn_reward_v1:rewards(),
                     Ledger :: blockchain_ledger_v1:ledger()) -> ok.
absorb_rewards(Rewards, Ledger) ->
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
    calculate_rewards_(Start, End, Ledger, Chain).

-spec calculate_rewards_(
        Start :: non_neg_integer(),
        End :: non_neg_integer(),
        Ledger :: blockchain_ledger_v1:ledger(),
        Chain :: blockchain:blockchain()) -> {error, any()} | {ok, blockchain_txn_reward_v1:rewards()}.
calculate_rewards_(Start, End, Ledger, Chain) ->
    Vars = get_reward_vars(Start, End, Ledger),
    %% Previously, if a state_channel closed in the grace blocks before an
    %% epoch ended, then it wouldn't ever get rewarded.
    {ok, PreviousGraceBlockDCRewards} = collect_dc_rewards_from_previous_epoch_grace(Start, End,
                                                                                     Chain, Vars,
                                                                                     Ledger),
    case get_rewards_for_epoch(Start, End, Chain, Vars, Ledger, PreviousGraceBlockDCRewards) of
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
                                         case blockchain_ledger_v1:find_gateway_owner(Gateway, Ledger) of
                                             {error, _} ->
                                                 Acc1;
                                             {ok, Owner} ->
                                                 Reward = blockchain_txn_reward_v1:new(Owner, Gateway, Amount, Type),
                                                 [Reward|Acc1]
                                         end;
                                    ({validator, Type, Validator}, Amount, Acc1) ->
                                         case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                                             {error, _} ->
                                                 Acc1;
                                             {ok, Val} ->
                                                 Owner = blockchain_ledger_validator_v1:owner_address(Val),
                                                 Reward = blockchain_txn_reward_v1:new(Owner, Validator, Amount, Type),
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
            %% we are only keeping hex density calculations memoized for a single
            %% rewards transaction calculation, then we discard that work and avoid
            %% cache invalidation issues.
            true = blockchain_hex:destroy_memoization(),
            blockchain_ledger_v1:delete_context(Ledger),
            {ok, Result}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_rewards()) -> iodata().
print(undefined) -> <<"type=rewards undefined">>;
print(#blockchain_txn_rewards_v1_pb{start_epoch=Start,
                                    end_epoch=End,
                                    rewards=Rewards}) ->
    PrintableRewards = [blockchain_txn_reward_v1:print(R) || R <- Rewards],
    io_lib:format("type=rewards start_epoch=~p end_epoch=~p rewards=~p",
                  [Start, End, PrintableRewards]).

json_type() ->
    <<"rewards_v1">>.

-spec to_json(txn_rewards(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      start_epoch => start_epoch(Txn),
      end_epoch => end_epoch(Txn),
      rewards => [blockchain_txn_reward_v1:to_json(R, []) || R <- rewards(Txn)]
     }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec get_rewards_for_epoch(non_neg_integer(), non_neg_integer(),
                            blockchain:blockchain(), map(),
                            blockchain_ledger_v1:ledger(), map()) ->
    {ok, map(), map(), map(), map()} | {error, any()}.
get_rewards_for_epoch(Start, End, Chain, Vars, Ledger, DCRewards) ->
    get_rewards_for_epoch(Start, End, Chain, Vars, Ledger, #{}, #{}, #{}, DCRewards).

get_rewards_for_epoch(Start, End, _Chain, Vars0, _Ledger, ChallengerRewards, ChallengeeRewards, WitnessRewards, DCRewards) when Start == End+1 ->
    {DCRemainder, NewDCRewards} = normalize_dc_rewards(DCRewards, Vars0),
    Vars = maps:put(dc_remainder, DCRemainder, Vars0),

    NormalizedWitnessRewards = normalize_witness_rewards(WitnessRewards, Vars),

    %% apply the DC remainder, if any to the other PoC categories pro rata
    {ok, normalize_challenger_rewards(ChallengerRewards, Vars),
     normalize_challengee_rewards(ChallengeeRewards, Vars),
     NormalizedWitnessRewards,
     NewDCRewards};
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
                    case blockchain_hex:var_map(Ledger) of
                        {error, _} ->
                            %% do the old thing
                            get_rewards_for_epoch(Current+1, End, Chain, Vars, Ledger,
                                                  poc_challengers_rewards(Transactions, Vars,
                                                                          ChallengerRewards),
                                                  poc_challengees_rewards(Transactions, Vars,
                                                                          Chain, Ledger,
                                                                          ChallengeeRewards, #{}),
                                                  poc_witnesses_rewards(Transactions, Vars,
                                                                        Chain, Ledger,
                                                                        WitnessRewards, #{}),
                                                  dc_rewards(Transactions, End, Vars,
                                                             Ledger, DCRewards));
                        {ok, VarMap} ->
                            WR = poc_witnesses_rewards(Transactions, Vars,
                                                       Chain, Ledger,
                                                       WitnessRewards, VarMap),

                            C0R = poc_challengers_rewards(Transactions, Vars,
                                                          ChallengerRewards),

                            DCR = dc_rewards(Transactions, End, Vars,
                                             Ledger, DCRewards),

                            CR = poc_challengees_rewards(Transactions, Vars,
                                                         Chain, Ledger,
                                                         ChallengeeRewards,
                                                         VarMap),

                            %% do the new thing
                            get_rewards_for_epoch(Current+1, End, Chain, Vars, Ledger,
                                                  C0R,
                                                  CR,
                                                  WR,
                                                  DCR)
                    end
            end
    end.

-spec get_reward_vars(pos_integer(), pos_integer(), blockchain_ledger_v1:ledger()) -> map().
get_reward_vars(Start, End, Ledger) ->
    {ok, MonthlyReward} = blockchain:config(?monthly_reward, Ledger),
    {ok, SecuritiesPercent} = blockchain:config(?securities_percent, Ledger),
    {ok, PocChallengeesPercent} = blockchain:config(?poc_challengees_percent, Ledger),
    {ok, PocChallengersPercent} = blockchain:config(?poc_challengers_percent, Ledger),
    {ok, PocWitnessesPercent} = blockchain:config(?poc_witnesses_percent, Ledger),
    {ok, ConsensusPercent} = blockchain:config(?consensus_percent, Ledger),
    {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(Ledger),
    {ok, OraclePrice} = blockchain_ledger_v1:current_oracle_price(Ledger),
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
    RewardVersion = case blockchain:config(?reward_version, Ledger) of
        {ok, R4} -> R4;
        _ -> 1
    end,

    WitnessRedundancy = case blockchain:config(?witness_redundancy, Ledger) of
                            {ok, WR} -> WR;
                            _ -> undefined
                        end,

    DecayRate = case blockchain:config(?poc_reward_decay_rate, Ledger) of
                    {ok, R} -> R;
                    _ -> undefined
                end,

    DensityTgtRes = case blockchain:config(?density_tgt_res, Ledger) of
                        {ok, D} -> D;
                        _ -> undefined
                    end,

    EpochReward = calculate_epoch_reward(Start, End, Ledger),
    #{
        monthly_reward => MonthlyReward,
        epoch_reward => EpochReward,
        oracle_price => OraclePrice,
        securities_percent => SecuritiesPercent,
        poc_challengees_percent => PocChallengeesPercent,
        poc_challengers_percent => PocChallengersPercent,
        poc_witnesses_percent => PocWitnessesPercent,
        consensus_percent => ConsensusPercent,
        consensus_members => ConsensusMembers,
        dc_percent => DCPercent,
        sc_grace_blocks => SCGrace,
        sc_version => SCVersion,
        poc_version => POCVersion,
        reward_version => RewardVersion,
        witness_redundancy => WitnessRedundancy,
        poc_reward_decay_rate => DecayRate,
        density_tgt_res => DensityTgtRes
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

-spec consensus_members_rewards(blockchain_ledger_v1:ledger(),
                                map()) -> #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
consensus_members_rewards(Ledger, #{epoch_reward := EpochReward,
                                    consensus_percent := ConsensusPercent}) ->
    GwOrVal =
        case blockchain:config(?election_version, Ledger) of
            {ok, N} when N >= 5 ->
                validator;
            _ ->
                gateway
        end,
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
                    maps:put({GwOrVal, consensus, Member}, Amount, Acc)
                end,
                #{},
                ConsensusMembers
            )
    end.

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
                                        poc_challengers_percent := PocChallengersPercent}=Vars) ->
    TotalChallenged = lists:sum(maps:values(ChallengerRewards)),
    ShareOfDCRemainder = share_of_dc_rewards(poc_challengers_percent, Vars),
    ChallengersReward = (EpochReward * PocChallengersPercent) + ShareOfDCRemainder,
    maps:fold(
        fun(Challenger, Challenged, Acc) ->
            PercentofReward = (Challenged*100/TotalChallenged)/100,
            Amount = erlang:round(PercentofReward * ChallengersReward),
            maps:put({gateway, poc_challengers, Challenger}, Amount, Acc)
        end,
        #{},
        ChallengerRewards
    ).

-spec poc_challengees_rewards(Transactions :: blockchain_txn:txns(),
                              Vars :: map(),
                              Chain :: blockchain:blockchain(),
                              Ledger :: blockchain_ledger_v1:ledger(),
                              ExistingRewards :: map(),
                              VarMap :: blockchain_hex:var_map()) ->
    #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_challengees_rewards(Transactions,
                        Vars,
                        Chain,
                        Ledger,
                        ExistingRewards,
                        VarMap) ->
    lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    Path = blockchain_txn_poc_receipts_v1:path(Txn),
                    poc_challengees_rewards_(Vars, Path, Path, Txn, Chain, Ledger, true, VarMap, Acc0)
            end
        end,
        ExistingRewards,
        Transactions
    ).

normalize_challengee_rewards(ChallengeeRewards, #{epoch_reward := EpochReward,
                                                  poc_challengees_percent := PocChallengeesPercent}=Vars) ->
    TotalChallenged = lists:sum(maps:values(ChallengeeRewards)),
    ShareOfDCRemainder = share_of_dc_rewards(poc_challengees_percent, Vars),
    ChallengeesReward = (EpochReward * PocChallengeesPercent) + ShareOfDCRemainder,
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

poc_challengees_rewards_(_Vars, [], _StaticPath, _Txn, _Chain, _Ledger, _, _, Acc) ->
    Acc;
poc_challengees_rewards_(#{poc_version := Version}=Vars,
                         [Elem|Path],
                         StaticPath,
                         Txn,
                         Chain,
                         Ledger,
                         IsFirst,
                         VarMap,
                         Acc0) when Version >= 2 ->
    WitnessRedundancy = maps:get(witness_redundancy, Vars, undefined),
    DecayRate = maps:get(poc_reward_decay_rate, Vars, undefined),
    DensityTgtRes = maps:get(density_tgt_res, Vars, undefined),
    %% check if there were any legitimate witnesses
    Witnesses = legit_witnesses(Txn, Chain, Ledger, Elem, StaticPath, Version),
    Challengee = blockchain_poc_path_element_v1:challengee(Elem),
    ChallengeeLoc = case blockchain_ledger_v1:find_gateway_location(Challengee, Ledger) of
                        {ok, CLoc} ->
                            CLoc;
                        _ ->
                            undefined
                    end,
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
                           case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, Witnesses) of
                               {error, _} ->
                                   %% Old behavior
                                   maps:put(Challengee, I+1, Acc0);
                               {ok, ToAdd} ->
                                   TxScale = maybe_calc_tx_scale(Challengee,
                                                                 DensityTgtRes,
                                                                 ChallengeeLoc,
                                                                 VarMap,
                                                                 Ledger),
                                   maps:put(Challengee, I+(ToAdd * TxScale), Acc0)
                           end;
                       true when is_integer(Version), Version > 4, IsFirst == false ->
                           %% while we don't have a receipt for this node, we do know
                           %% there were witnesses or the path continued which means
                           %% the challengee transmitted
                           %% Additionally, we know this layer came in over radio so
                           %% there's an implicit rx as well
                           case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, Witnesses) of
                               {error, _} ->
                                   %% Old behavior
                                   maps:put(Challengee, I+2, Acc0);
                               {ok, ToAdd} ->
                                   TxScale = maybe_calc_tx_scale(Challengee,
                                                                 DensityTgtRes,
                                                                 ChallengeeLoc,
                                                                 VarMap,
                                                                 Ledger),
                                   maps:put(Challengee, I+(ToAdd * TxScale), Acc0)
                           end;
                       _ ->
                           Acc0
                   end,
            poc_challengees_rewards_(Vars, Path, StaticPath, Txn, Chain, Ledger, false, VarMap, Acc1);
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
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, Witnesses) of
                                       {error, _} ->
                                           %% Old behavior
                                           maps:put(Challengee, I+3, Acc0);
                                       {ok, ToAdd} ->
                                           TxScale = maybe_calc_tx_scale(Challengee,
                                                                         DensityTgtRes,
                                                                         ChallengeeLoc,
                                                                         VarMap,
                                                                         Ledger),
                                           maps:put(Challengee, I+(ToAdd * TxScale), Acc0)
                                   end;
                               false when is_integer(Version), Version > 4 ->
                                   %% this challengee rx'd and sent a receipt
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, Witnesses) of
                                       {error, _} ->
                                           %% Old behavior
                                           maps:put(Challengee, I+2, Acc0);
                                       {ok, ToAdd} ->
                                           TxScale = maybe_calc_tx_scale(Challengee,
                                                                         DensityTgtRes,
                                                                         ChallengeeLoc,
                                                                         VarMap,
                                                                         Ledger),
                                           maps:put(Challengee, I+(ToAdd * TxScale), Acc0)
                                   end;
                               _ ->
                                   %% Old behavior
                                   maps:put(Challengee, I+1, Acc0)
                           end,
                    poc_challengees_rewards_(Vars, Path, StaticPath, Txn, Chain, Ledger, false, VarMap, Acc1);
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
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, Witnesses) of
                                       {error, _} ->
                                           %% Old behavior
                                           maps:put(Challengee, I+2, Acc0);
                                       {ok, ToAdd} ->
                                           TxScale = maybe_calc_tx_scale(Challengee,
                                                                         DensityTgtRes,
                                                                         ChallengeeLoc,
                                                                         VarMap,
                                                                         Ledger),
                                           maps:put(Challengee, I+(ToAdd * TxScale), Acc0)
                                   end;
                               true ->
                                   maps:put(Challengee, I+1, Acc0)
                           end,
                    poc_challengees_rewards_(Vars, Path, StaticPath, Txn, Chain, Ledger, false, VarMap, Acc1)
            end
    end;
poc_challengees_rewards_(Vars, [Elem|Path], StaticPath, Txn, Chain, Ledger, _IsFirst, VarMap, Acc0) ->
    case blockchain_poc_path_element_v1:receipt(Elem) of
        undefined ->
            poc_challengees_rewards_(Vars, Path, StaticPath, Txn, Chain, Ledger, false, VarMap, Acc0);
        _Receipt ->
            Challengee = blockchain_poc_path_element_v1:challengee(Elem),
            I = maps:get(Challengee, Acc0, 0),
            Acc1 =  maps:put(Challengee, I+1, Acc0),
            poc_challengees_rewards_(Vars, Path, StaticPath, Txn, Chain, Ledger, false, VarMap, Acc1)
    end.

-spec poc_challengee_reward_unit(WitnessRedundancy :: undefined | pos_integer(),
                                 DecayRate :: undefined | float(),
                                 Witnesses :: blockchain_poc_witness_v1:poc_witnesses()) -> {error, any()} | {ok, float()}.
poc_challengee_reward_unit(WitnessRedundancy, DecayRate, Witnesses) ->
    case {WitnessRedundancy, DecayRate} of
        {undefined, _} -> {error, witness_redundancy_undefined};
        {_, undefined} -> {error, poc_reward_decay_rate_undefined};
        {N, R} ->
            W = length(Witnesses),
            Unit = poc_reward_tx_unit(R, W, N),
            {ok, normalize_reward_unit(Unit)}
    end.

-spec normalize_reward_unit(Unit :: float()) -> float().
normalize_reward_unit(Unit) when Unit > 1.0 -> 1.0;
normalize_reward_unit(Unit) -> Unit.

-spec poc_witnesses_rewards(Transactions :: blockchain_txn:txns(),
                            Vars :: map(),
                            Chain :: blockchain:blockchain(),
                            Ledger :: blockchain_ledger_v1:ledger(),
                            WitnessRewards :: map(),
                            VarMap :: blockchain_hex:var_map()) ->
    #{{gateway, libp2p_crypto:pubkey_bin()} => non_neg_integer()}.
poc_witnesses_rewards(Transactions,
                      #{poc_version := POCVersion}=Vars,
                      Chain,
                      Ledger,
                      WitnessRewards,
                      VarMap) ->
    WitnessRedundancy = maps:get(witness_redundancy, Vars, undefined),
    DecayRate = maps:get(poc_reward_decay_rate, Vars, undefined),
    DensityTgtRes = maps:get(density_tgt_res, Vars, undefined),
    lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    case POCVersion of
                        V when is_integer(V), V >= 9 ->
                            try
                                %% Get channels without validation
                                {ok, Channels} = blockchain_txn_poc_receipts_v1:get_channels(Txn, Chain),
                                Path = blockchain_txn_poc_receipts_v1:path(Txn),

                                %% Do the new thing for witness filtering
                                Res = lists:foldl(
                                  fun(Elem, Acc1) ->
                                          ElemPos = blockchain_utils:index_of(Elem, Path),
                                          WitnessChannel = lists:nth(ElemPos, Channels),
                                          case blockchain_txn_poc_receipts_v1:valid_witnesses(Elem, WitnessChannel, Ledger) of
                                              [] ->
                                                  Acc1;
                                              ValidWitnesses ->
                                                  %% We found some valid witnesses, we only apply the witness_redundancy and decay_rate if BOTH
                                                  %% are set as chain variables, otherwise we default to the old behavior and set ToAdd=1
                                                  %%
                                                  %% If both witness_redundancy and decay_rate are set, we calculate a scaled rx unit (the value ToAdd)
                                                  %% This is determined using the formulae mentioned in hip15
                                                  ToAdd = case {WitnessRedundancy, DecayRate} of
                                                              {undefined, _} -> 1;
                                                              {_, undefined} -> 1;
                                                              {N, R} ->
                                                                  W = length(ValidWitnesses),
                                                                  U = poc_witness_reward_unit(R, W, N),
                                                                  U
                                                          end,

                                                  case DensityTgtRes of
                                                      undefined ->
                                                          %% old (HIP15)
                                                          lists:foldl(
                                                            fun(WitnessRecord, Acc2) ->
                                                                    Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                                                                    I = maps:get(Witness, Acc2, 0),
                                                                    maps:put(Witness, I+ToAdd, Acc2)
                                                            end,
                                                            Acc1,
                                                            ValidWitnesses
                                                           );
                                                      D ->
                                                          %% new (HIP17)
                                                          lists:foldl(
                                                            fun(WitnessRecord, Acc2) ->
                                                                    Challengee = blockchain_poc_path_element_v1:challengee(Elem),
                                                                    %% This must always be {ok, ...}
                                                                    {ok, ChallengeeGw} = blockchain_ledger_v1:find_gateway_info(Challengee, Ledger),
                                                                    %% Challengee must have a location
                                                                    ChallengeeLoc = blockchain_ledger_gateway_v2:location(ChallengeeGw),
                                                                    Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                                                                    %% The witnesses get scaled by the value of their transmitters
                                                                    RxScale = blockchain_utils:normalize_float(
                                                                                blockchain_hex:scale(ChallengeeLoc,
                                                                                                   VarMap,
                                                                                                   D,
                                                                                                   Ledger)),
                                                                    Value = blockchain_utils:normalize_float(ToAdd * RxScale),
                                                                    I = maps:get(Witness, Acc2, 0),
                                                                    maps:put(Witness, I+Value, Acc2)
                                                            end,
                                                            Acc1,
                                                            ValidWitnesses
                                                           )
                                                  end
                                          end
                                  end,
                                  Acc0,
                                  Path
                                 ),
                                Res
                            catch What:Why:ST ->
                                  lager:error("failed to calculate poc_witnesses_rewards, error ~p:~p:~p", [What, Why, ST]),
                                  Acc0
                            end;
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
                                            poc_witnesses_percent := PocWitnessesPercent}=Vars) ->
    TotalWitnesses = lists:sum(maps:values(WitnessRewards)),
    ShareOfDCRemainder = share_of_dc_rewards(poc_witnesses_percent, Vars),
    WitnessesReward = (EpochReward * PocWitnessesPercent) + ShareOfDCRemainder,
    maps:fold(
        fun(Witness, Witnessed, Acc) ->
            PercentofReward = (Witnessed*100/TotalWitnesses)/100,
            Amount = erlang:round(PercentofReward*WitnessesReward),
            maps:put({gateway, poc_witnesses, Witness}, Amount, Acc)
        end,
        #{},
        WitnessRewards
    ).

-spec collect_dc_rewards_from_previous_epoch_grace(non_neg_integer(), non_neg_integer(),
                                                   blockchain:blockchain(), map(),
                                                   blockchain_ledger_v1:ledger()) ->
    {ok, map()} | {error, any()}.
collect_dc_rewards_from_previous_epoch_grace(Start, End, Chain,
                                             #{sc_grace_blocks := Grace,
                                               reward_version := RV} = Vars,
                                             Ledger) when RV > 4 ->
    scan_grace_block(max(1, Start - Grace), Start, End, Vars, Chain, Ledger, #{});
collect_dc_rewards_from_previous_epoch_grace(_Start, _End, _Chain, _Vars, _Ledger) -> {ok, #{}}.

scan_grace_block(Current, Start, _End, _Vars, _Chain, _Ledger, Acc)
                                           when Current == Start + 1 -> {ok, Acc};
scan_grace_block(Current, Start, End, Vars, Chain, Ledger, Acc) ->
    case blockchain:get_block(Current, Chain) of
        {error, _Error} = Err ->
            lager:error("failed to get grace block ~p ~p", [_Error, Current]),
            Err;
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            scan_grace_block(Current+1, Start, End, Vars, Chain, Ledger,
                             dc_rewards(Txns, End, Vars, Ledger, Acc))
    end.

dc_rewards(Transactions, EndHeight, #{sc_grace_blocks := GraceBlocks, sc_version := 2} = Vars, Ledger, DCRewards) ->
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
                                    RewardVersion = maps:get(reward_version, Vars, 1),

                                    Summaries = case RewardVersion > 3 of
                                                    %% reward version 4 normalizes payouts
                                                    true -> blockchain_state_channel_v1:summaries(blockchain_state_channel_v1:normalize(FinalSC));
                                                    false -> blockchain_state_channel_v1:summaries(FinalSC)
                                                end,

                                    %% check the dispute status
                                    Bonus = case blockchain_ledger_state_channel_v2:close_state(SC) of
                                                %% Reward version 4 or higher just slashes overcommit
                                                 dispute when RewardVersion < 4 ->
                                                    %% the owner of the state channel did a naughty thing, divide their overcommit between the participants
                                                     OverCommit = blockchain_ledger_state_channel_v2:amount(SC) - blockchain_ledger_state_channel_v2:original(SC),
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
                                   dc_percent := DCPercent}=Vars) ->
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
    OraclePrice = maps:get(oracle_price, Vars, 0),
    RewardVersion = maps:get(reward_version, Vars, 1),
    TotalDCs = lists:sum(maps:values(DCRewards)),
    MaxDCReward = EpochReward * DCPercent,
    %% compute the price HNT equivalent of the DCs burned in this epoch
    DCReward = case OraclePrice == 0 orelse RewardVersion < 3 of
        true ->
            %% no oracle price, or rewards =< 2
            MaxDCReward;
        false ->
            {ok, DCInThisEpochAsHNT} = blockchain_ledger_v1:dc_to_hnt(TotalDCs, OraclePrice),
            case DCInThisEpochAsHNT >= MaxDCReward of
                true ->
                    %% we spent enough, just allocate it proportionally
                    MaxDCReward;
                false ->
                    %% we didn't spend enough this epoch, return the remainder to the pool
                    DCInThisEpochAsHNT
            end
    end,

    {round(MaxDCReward - DCReward),
     maps:fold(
       fun(Key, NumDCs, Acc) ->
               PercentofReward = (NumDCs*100/TotalDCs)/100,
               Amount = erlang:round(PercentofReward*DCReward),
               maps:put({gateway, data_credits, Key}, Amount, Acc)
       end,
       #{},
       DCRewards
      )}.

-spec poc_reward_tx_unit(R :: float(),
                         W :: pos_integer(),
                         N :: pos_integer()) -> float().
poc_reward_tx_unit(_R, W, N) when W =< N ->
    blockchain_utils:normalize_float(W / N);
poc_reward_tx_unit(R, W, N) ->
    NoNorm = 1 + (1 - math:pow(R, (W - N))),
    blockchain_utils:normalize_float(NoNorm).

-spec poc_witness_reward_unit(R :: float(),
                              W :: pos_integer(),
                              N :: pos_integer()) -> float().
poc_witness_reward_unit(_R, W, N) when W =< N ->
    1.0;
poc_witness_reward_unit(R, W, N) ->
    normalize_reward_unit(blockchain_utils:normalize_float((N - (1 - math:pow(R, (W - N))))/W)).

legit_witnesses(Txn, Chain, Ledger, Elem, StaticPath, Version) ->
    case Version of
        V when is_integer(V), V >= 9 ->
            try
                %% Get channels without validation
                {ok, Channels} = blockchain_txn_poc_receipts_v1:get_channels(Txn, Chain),
                ElemPos = blockchain_utils:index_of(Elem, StaticPath),
                WitnessChannel = lists:nth(ElemPos, Channels),
                ValidWitnesses = blockchain_txn_poc_receipts_v1:valid_witnesses(Elem, WitnessChannel, Ledger),
                %% lager:info("ValidWitnesses: ~p",
                           %% [[blockchain_utils:addr2name(blockchain_poc_witness_v1:gateway(W)) || W <- ValidWitnesses]]),
                ValidWitnesses
            catch What:Why:ST ->
                      lager:error("failed to calculate poc_challengees_rewards, error ~p:~p:~p", [What, Why, ST]),
                      []
            end;
        V when is_integer(V), V > 4 ->
            blockchain_txn_poc_receipts_v1:good_quality_witnesses(Elem, Ledger);
        _ ->
            blockchain_poc_path_element_v1:witnesses(Elem)
    end.

maybe_calc_tx_scale(_Challengee,
                    DensityTgtRes,
                    ChallengeeLoc,
                    VarMap,
                    Ledger) ->
    case {DensityTgtRes, ChallengeeLoc} of
        {undefined, _} -> 1.0;
        {_, undefined} -> 1.0;
        {D, Loc} ->
            TxScale = blockchain_hex:scale(Loc, VarMap, D, Ledger),
            %% lager:info("Challengee: ~p, TxScale: ~p",
                       %% [blockchain_utils:addr2name(Challengee), TxScale]),
            blockchain_utils:normalize_float(TxScale)
    end.

share_of_dc_rewards(_Key, #{dc_remainder := 0}) ->
    0;
share_of_dc_rewards(Key, Vars=#{dc_remainder := DCRemainder}) ->
    erlang:round(DCRemainder * ((maps:get(Key, Vars) / (maps:get(poc_challengers_percent, Vars) + maps:get(poc_challengees_percent, Vars) + maps:get(poc_witnesses_percent, Vars))))).


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

consensus_members_rewards_test() -> {
    timeout,
    30,
    fun() ->
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
        test_utils:cleanup_tmp_dir(BaseDir)
    end
    }.



securities_rewards_test() -> {
    timeout,
    30,
    fun() ->
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
        test_utils:cleanup_tmp_dir(BaseDir)
    end
    }.

poc_challengers_rewards_1_test() ->
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"a">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"b">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"a">>, <<"Secret">>, <<"OnionKeyHash">>, [])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengers_percent => 0.15,
        poc_witnesses_percent => 0.0,
        poc_challengees_percent => 0.0,
        dc_remainder => 0,
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
        poc_challengers_percent => 0.15,
        poc_witnesses_percent => 0.0,
        poc_challengees_percent => 0.0,
        dc_remainder => 0,

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
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.35,
        poc_witnesses_percent => 0.0,
        poc_challengers_percent => 0.0,
        dc_remainder => 0,
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
    ?assertEqual(Rewards, normalize_challengee_rewards(
                            poc_challengees_rewards(Txns, Vars, Chain, Ledger, #{}, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

poc_challengees_rewards_2_test() ->
    BaseDir = test_utils:tmp_dir("poc_challengees_rewards_2_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.35,
        poc_witnesses_percent => 0.0,
        poc_challengers_percent => 0.0,
        dc_remainder => 0,
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
    ?assertEqual(Rewards, normalize_challengee_rewards(
                            poc_challengees_rewards(Txns, Vars, Chain, Ledger, #{}, {}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

poc_challengees_rewards_3_test() ->
    BaseDir = test_utils:tmp_dir("poc_challengees_rewards_3_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 1000,
        poc_challengees_percent => 0.35,
        poc_witnesses_percent => 0.0,
        poc_challengers_percent => 0.0,
        dc_remainder => 0,
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
    ?assertEqual(Rewards, normalize_challengee_rewards(
                            poc_challengees_rewards(Txns, Vars, Chain, Ledger, #{}, {}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

poc_witnesses_rewards_test() ->
    BaseDir = test_utils:tmp_dir("poc_witnesses_rewards_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    EpochVars = #{
      epoch_reward => 1000,
      poc_witnesses_percent => 0.05,
      poc_challengees_percent => 0.0,
      poc_challengers_percent => 0.0,
      dc_remainder => 0,
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

    ?assertEqual(Rewards, normalize_witness_rewards(
                            poc_witnesses_rewards(Txns, EpochVars, Chain, Ledger, #{}, #{}), EpochVars)),
    test_utils:cleanup_tmp_dir(BaseDir).

old_poc_challengers_rewards_test() ->
    Txns = [
        blockchain_txn_poc_receipts_v1:new(<<"1">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"2">>, <<"Secret">>, <<"OnionKeyHash">>, []),
        blockchain_txn_poc_receipts_v1:new(<<"1">>, <<"Secret">>, <<"OnionKeyHash">>, [])
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengers_percent => 0.15,
        poc_challengees_percent => 0.0,
        poc_witnesses_percent => 0.0,
        dc_remainder => 0,
        poc_version => 2
    },
    Rewards = #{
        {gateway, poc_challengers, <<"1">>} => 100,
        {gateway, poc_challengers, <<"2">>} => 50
    },
    ?assertEqual(Rewards, normalize_challenger_rewards(poc_challengers_rewards(Txns, Vars, #{}), Vars)),
    ok.

old_poc_challengees_rewards_version_1_test() ->
    BaseDir = test_utils:tmp_dir("old_poc_challengees_rewards_version_1_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
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
        poc_challengees_percent => 0.35,
        poc_challengers_percent => 0.0,
        poc_witnesses_percent => 0.0,
        dc_remainder => 0,
        poc_version => 1
    },
    Rewards = #{
        {gateway, poc_challengees, <<"1">>} => 175,
        {gateway, poc_challengees, <<"2">>} => 175
    },
    ?assertEqual(Rewards, normalize_challengee_rewards(
                            poc_challengees_rewards(Txns, Vars, Chain, Ledger, #{}, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

old_poc_challengees_rewards_version_2_test() ->
    BaseDir = test_utils:tmp_dir("old_poc_challengees_rewards_version_2_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
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
        poc_challengers_percent => 0.0,
        poc_witnesses_percent => 0.0,
        dc_remainder => 0,
        poc_version => 2
    },
    Rewards = #{
        {gateway, poc_challengees, <<"1">>} => 175,
        {gateway, poc_challengees, <<"2">>} => 175
    },
    ?assertEqual(Rewards, normalize_challengee_rewards(
                            poc_challengees_rewards(Txns, Vars, Chain, Ledger, #{}, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

old_poc_witnesses_rewards_test() ->
    BaseDir = test_utils:tmp_dir("old_poc_witnesses_rewards_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
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
        poc_witnesses_percent => 0.05,
        poc_challengers_percent => 0.0,
        poc_challengees_percent => 0.0,
        dc_remainder => 0,
        poc_version => 2
    },
    Rewards = #{
        {gateway, poc_witnesses, <<"1">>} => 25,
        {gateway, poc_witnesses, <<"2">>} => 25
    },
    ?assertEqual(Rewards, normalize_witness_rewards(
                            poc_witnesses_rewards(Txns, EpochVars, Chain, Ledger, #{}, #{}), EpochVars)),
    test_utils:cleanup_tmp_dir(BaseDir).

dc_rewards_test() ->
    BaseDir = test_utils:tmp_dir("dc_rewards_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 100000,
        dc_percent => 0.3,
        consensus_percent => 0.06 + 0.025,
        poc_challengees_percent => 0.18,
        poc_challengers_percent => 0.0095,
        poc_witnesses_percent => 0.0855,
        securities_percent => 0.34,
        sc_version => 2,
        sc_grace_blocks => 5,
        reward_version => 2,
        oracle_price => 100000000, %% 1 dollar
        consensus_members => [<<"c">>, <<"d">>]
    },

    LedgerVars = maps:merge(#{?poc_version => 5, ?sc_version => 2, ?sc_grace_blocks => 5}, common_poc_vars()),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    {SC0, _} = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 100, <<"blockhash">>, 10),
    SC = blockchain_state_channel_v1:summaries([blockchain_state_channel_summary_v1:new(<<"a">>, 1, 1), blockchain_state_channel_summary_v1:new(<<"b">>, 2, 2)], SC0),

    ok = blockchain_ledger_v1:add_state_channel(<<"id">>, <<"owner">>, 10, 1, 100, 200, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),

    ok = blockchain_ledger_v1:close_state_channel(<<"owner">>, <<"owner">>, SC, <<"id">>, false, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),


    Txns = [
         blockchain_txn_state_channel_close_v1:new(SC, <<"owner">>)
    ],
    %% NOTE: Rewards are split 33-66%
    Rewards = #{
        {gateway, data_credits, <<"a">>} => trunc(100000 * 0.3 * (1/3)),
        {gateway, data_credits, <<"b">>} => trunc(100000 * 0.3 * (2/3))
    },
    ?assertEqual({0, Rewards}, normalize_dc_rewards(dc_rewards(Txns, 100, Vars, Ledger1, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

dc_rewards_v3_test() ->
    BaseDir = test_utils:tmp_dir("dc_rewards_v3_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 100000,
        dc_percent => 0.3,
        consensus_percent => 0.06 + 0.025,
        poc_challengees_percent => 0.18,
        poc_challengers_percent => 0.0095,
        poc_witnesses_percent => 0.0855,
        securities_percent => 0.34,
        sc_version => 2,
        sc_grace_blocks => 5,
        reward_version => 3,
        oracle_price => 100000000, %% 1 dollar
        consensus_members => [<<"c">>, <<"d">>]
    },

    LedgerVars = maps:merge(#{?poc_version => 5, ?sc_version => 2, ?sc_grace_blocks => 5}, common_poc_vars()),

    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    {SC0, _} = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 100, <<"blockhash">>, 10),
    SC = blockchain_state_channel_v1:summaries([blockchain_state_channel_summary_v1:new(<<"a">>, 1, 1), blockchain_state_channel_summary_v1:new(<<"b">>, 2, 2)], SC0),

    ok = blockchain_ledger_v1:add_state_channel(<<"id">>, <<"owner">>, 10, 1, 100, 200, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),

    ok = blockchain_ledger_v1:close_state_channel(<<"owner">>, <<"owner">>, SC, <<"id">>, false, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),


    Txns = [
         blockchain_txn_state_channel_close_v1:new(SC, <<"owner">>)
    ],
    {ok, DCsInEpochAsHNT} = blockchain_ledger_v1:dc_to_hnt(3, 100000000), %% 3 DCs burned at HNT price of 1 dollar
    %% NOTE: Rewards are split 33-66%
    Rewards = #{
        {gateway, data_credits, <<"a">>} => round(DCsInEpochAsHNT * (1/3)),
        {gateway, data_credits, <<"b">>} => round(DCsInEpochAsHNT * (2/3))
    },
    ?assertEqual({26999, Rewards}, normalize_dc_rewards(dc_rewards(Txns, 100, Vars, Ledger1, #{}), Vars)),
    test_utils:cleanup_tmp_dir(BaseDir).

dc_rewards_v3_spillover_test() ->
    BaseDir = test_utils:tmp_dir("dc_rewards_v3_spillover_test"),
    Block = blockchain_block:new_genesis_block([]),
    {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
    Ledger = blockchain:ledger(Chain),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),

    Vars = #{
        epoch_reward => 100000,
        dc_percent => 0.05,
        consensus_percent => 0.1,
        poc_challengees_percent => 0.20,
        poc_challengers_percent => 0.15,
        poc_witnesses_percent => 0.15,
        securities_percent => 0.35,
        sc_version => 2,
        sc_grace_blocks => 5,
        reward_version => 3,
        poc_version => 5,
        dc_remainder => 0,
        oracle_price => 100000000, %% 1 dollar
        consensus_members => [<<"c">>, <<"d">>]
    },

    LedgerVars = maps:merge(#{?poc_version => 5, ?sc_version => 2, ?sc_grace_blocks => 5}, common_poc_vars()),

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


    {SC0, _} = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 100, <<"blockhash">>, 10),
    SC = blockchain_state_channel_v1:summaries([blockchain_state_channel_summary_v1:new(<<"a">>, 1, 1), blockchain_state_channel_summary_v1:new(<<"b">>, 2, 2)], SC0),

    ok = blockchain_ledger_v1:add_state_channel(<<"id">>, <<"owner">>, 10, 1, 100, 200, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),

    ok = blockchain_ledger_v1:close_state_channel(<<"owner">>, <<"owner">>, SC, <<"id">>, false, Ledger1),

    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),

    ok = blockchain_ledger_v1:commit_context(Ledger1),

    Txns2 = [
         blockchain_txn_state_channel_close_v1:new(SC, <<"owner">>)
    ],
    AllTxns = Txns ++ Txns2,
    {ok, DCsInEpochAsHNT} = blockchain_ledger_v1:dc_to_hnt(3, 100000000), %% 3 DCs burned at HNT price of 1 dollar
    %% NOTE: Rewards are split 33-66%
    Rewards = #{
        {gateway, data_credits, <<"a">>} => round(DCsInEpochAsHNT * (1/3)),
        {gateway, data_credits, <<"b">>} => round(DCsInEpochAsHNT * (2/3))
    },

    {DCRemainder, DCRewards} = normalize_dc_rewards(dc_rewards(AllTxns, 100, Vars, Ledger, #{}), Vars),


    DCAward = trunc(maps:get(epoch_reward, Vars) * maps:get(dc_percent, Vars)),

    ?assertEqual({DCAward - DCsInEpochAsHNT, Rewards}, {DCRemainder, DCRewards}),
    NewVars = maps:put(dc_remainder, DCRemainder, Vars),

    %% compute the original rewards with no spillover
    ChallengerRewards = normalize_challenger_rewards(poc_challengers_rewards(AllTxns, Vars, #{}), Vars),
    ChallengeeRewards = normalize_challengee_rewards(
                          poc_challengees_rewards(AllTxns, Vars, Chain, Ledger, #{}, #{}), Vars),
    WitnessRewards = normalize_witness_rewards(
                       poc_witnesses_rewards(AllTxns, Vars, Chain, Ledger, #{}, #{}), Vars),

    ChallengersAward = trunc(maps:get(epoch_reward, Vars) * maps:get(poc_challengers_percent, Vars)),
    ?assertEqual(#{{gateway,poc_challengers,<<"X">>} =>  ChallengersAward}, ChallengerRewards), %% entire 15% allocation
    ChallengeesAward = trunc(maps:get(epoch_reward, Vars) * maps:get(poc_challengees_percent, Vars)),
    ?assertEqual(#{{gateway,poc_challengees,<<"a">>} => trunc(ChallengeesAward * 4/8), %% 4 of 8 shares of 20% allocation
                   {gateway,poc_challengees,<<"b">>} => trunc(ChallengeesAward * 3/8), %% 3 shares
                   {gateway,poc_challengees,<<"c">>} => trunc(ChallengeesAward * 1/8)}, %% 1 share
                 ChallengeeRewards),
    WitnessesAward = trunc(maps:get(epoch_reward, Vars) * maps:get(poc_witnesses_percent, Vars)),
    ?assertEqual(#{{gateway,poc_witnesses,<<"c">>} => WitnessesAward}, %% entire 15% allocation
                 WitnessRewards),


    %% apply the DC remainder, if any to the other PoC categories pro rata
    SpilloverChallengerRewards = normalize_challenger_rewards(poc_challengers_rewards(AllTxns, Vars, #{}), NewVars),
    SpilloverChallengeeRewards = normalize_challengee_rewards(
                                   poc_challengees_rewards(AllTxns, Vars, Chain, Ledger, #{}, #{}), NewVars),
    SpilloverWitnessRewards = normalize_witness_rewards(
                                poc_witnesses_rewards(AllTxns, Vars, Chain, Ledger, #{}, #{}), NewVars),

    ChallengerSpilloverAward = erlang:round(DCRemainder * ((maps:get(poc_challengers_percent, Vars) / (maps:get(poc_challengees_percent, Vars) +
                                                                                                       maps:get(poc_witnesses_percent, Vars) +
                                                                                                       maps:get(poc_challengers_percent, Vars))))),

    ?assertEqual(#{{gateway,poc_challengers,<<"X">>} =>  ChallengersAward + ChallengerSpilloverAward}, SpilloverChallengerRewards), %% entire 15% allocation
    ChallengeeSpilloverAward = erlang:round(DCRemainder * ((maps:get(poc_challengees_percent, Vars) / (maps:get(poc_challengees_percent, Vars) +
                                                                                                       maps:get(poc_witnesses_percent, Vars) +
                                                                                                       maps:get(poc_challengers_percent, Vars))))),
    ?assertEqual(#{{gateway,poc_challengees,<<"a">>} => trunc((ChallengeesAward + ChallengeeSpilloverAward) * 4/8), %% 4 of 8 shares of 20% allocation
                   {gateway,poc_challengees,<<"b">>} => trunc((ChallengeesAward + ChallengeeSpilloverAward) * 3/8), %% 3 shares
                   {gateway,poc_challengees,<<"c">>} => trunc((ChallengeesAward + ChallengeeSpilloverAward) * 1/8)}, %% 1 share
                 SpilloverChallengeeRewards),
    WitnessesSpilloverAward = erlang:round(DCRemainder * ((maps:get(poc_witnesses_percent, Vars) / (maps:get(poc_challengees_percent, Vars) +
                                                                                                       maps:get(poc_witnesses_percent, Vars) +
                                                                                                       maps:get(poc_challengers_percent, Vars))))),
    ?assertEqual(#{{gateway,poc_witnesses,<<"c">>} => WitnessesAward + WitnessesSpilloverAward}, %% entire 15% allocation
                 SpilloverWitnessRewards),
    test_utils:cleanup_tmp_dir(BaseDir).

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
