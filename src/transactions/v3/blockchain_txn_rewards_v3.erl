%%-------------------------------------------------------------------
%% @doc
%% This module implements rewards v3 which only reward for:
%% - consensus group membership
%% - security holders
%% - the remaining rewards go to an on-chain variable treasury key
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_rewards_v3).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_rewards_v3_pb.hrl").

-export([
    %% required funs
    new/3,
    hash/1,
    start_epoch/1,
    end_epoch/1,
    rewards/1,
    sign/2,
    fee/1,
    fee_payer/2,
    is_valid/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2,

    %% reward v3 setters
    new_reward/2,

    %% reward v3 getters
    reward_account/1,
    reward_amount/1,

    %% exposing potential useful other funs
    get_reward_vars/3,
    calculate_rewards/3,
    calculate_rewards_md/3
]).

-type txn_rewards_v3() :: #blockchain_txn_rewards_v3_pb{}.
-type reward_v3() :: #blockchain_txn_reward_v3_pb{}.
-type rewards() :: [reward_v3()].
-type rewards_map() :: #{libp2p_crypto:pubkey_bin() => number()}.
-type rewards_md() :: #{
    consensus_rewards => rewards_map(),
    securities_rewards => rewards_map(),
    treasury_rewards => rewards_map()
}.

-export_type([txn_rewards_v3/0, rewards_md/0]).

%% ------------------------------------------------------------------
%% Public API
%% ------------------------------------------------------------------

-spec new(non_neg_integer(), non_neg_integer(), rewards()) -> txn_rewards_v3().
new(Start, End, Rewards) ->
    SortedRewards = lists:sort(Rewards),
    #blockchain_txn_rewards_v3_pb{start_epoch = Start, end_epoch = End, rewards = SortedRewards}.

-spec hash(txn_rewards_v3() | reward_v3()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_rewards_v3_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

-spec start_epoch(txn_rewards_v3()) -> non_neg_integer().
start_epoch(#blockchain_txn_rewards_v3_pb{start_epoch = Start}) ->
    Start.

-spec end_epoch(txn_rewards_v3()) -> non_neg_integer().
end_epoch(#blockchain_txn_rewards_v3_pb{end_epoch = End}) ->
    End.

-spec rewards(txn_rewards_v3()) -> rewards().
rewards(#blockchain_txn_rewards_v3_pb{rewards = Rewards}) ->
    Rewards.

-spec reward_account(reward_v3()) -> binary().
reward_account(#blockchain_txn_reward_v3_pb{account = Account}) ->
    Account.

-spec reward_amount(reward_v3()) -> non_neg_integer().
reward_amount(#blockchain_txn_reward_v3_pb{amount = Amount}) ->
    Amount.

-spec sign(txn_rewards_v3(), libp2p_crypto:sig_fun()) -> txn_rewards_v3().
sign(Txn, _SigFun) ->
    Txn.

-spec fee(txn_rewards_v3()) -> 0.
fee(_Txn) ->
    0.

-spec fee_payer(txn_rewards_v3(), blockchain_ledger_v1:ledger()) ->
    libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec is_valid(txn_rewards_v3(), blockchain:blockchain()) ->
    ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    TxnRewards = ?MODULE:rewards(Txn),
    Ledger = blockchain:ledger(Chain),
    RewardVars = get_reward_vars(Start, End, Ledger),
    case maps:get(reward_version, RewardVars) of
        7 ->
            case calculate_rewards(Start, End, Chain) of
                {error, _Reason} = Error ->
                    Error;
                {ok, CalRewards} ->
                    CalRewardsHashes = [hash(R) || R <- CalRewards],
                    TxnRewardsHashes = [hash(R) || R <- TxnRewards],
                    case CalRewardsHashes == TxnRewardsHashes of
                        false -> {error, invalid_rewards_v3};
                        true -> ok
                    end
            end;
        V ->
            {error, {unexpected_reward_version, V}}
    end.

-spec absorb(txn_rewards_v3(), blockchain:blockchain()) ->
    ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:mode(Ledger) == aux of
        false ->
            %% only absorb in the main ledger
            absorb_(Txn, Ledger);
        true ->
            aux_absorb(Txn, Ledger, Chain)
    end.

-spec absorb_(Txn :: txn_rewards_v3(), Ledger :: blockchain_ledger_v1:ledger()) -> ok.
absorb_(Txn, Ledger) ->
    Rewards = ?MODULE:rewards(Txn),
    absorb_rewards(Rewards, Ledger).

-spec aux_absorb(
    Txn :: txn_rewards_v3(),
    AuxLedger :: blockchain_ledger_v1:ledger(),
    Chain :: blockchain:blockchain()
) -> ok | {error, any()}.
aux_absorb(Txn, AuxLedger, Chain) ->
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    %% NOTE: This is an aux ledger, we don't use rewards(txn) here, instead we calculate them manually
    %% and do 0 verification for absorption
    case calculate_rewards_(Start, End, AuxLedger, Chain, true) of
        {error, _} = E ->
            E;
        {ok, AuxRewards, AuxMD} ->
            TxnRewards = rewards(Txn),
            %% absorb the rewards attached to the txn (real)
            absorb_rewards(TxnRewards, AuxLedger),
            %% set auxiliary rewards in the aux ledger also
            lager:info("are aux rewards equal?: ~p", [
                lists:sort(TxnRewards) == lists:sort(AuxRewards)
            ]),
            %% rewards appear in (End + 1) block
            blockchain_aux_ledger_v1:set_rewards(End + 1, TxnRewards, AuxRewards, AuxLedger),
            case
                calculate_rewards_(
                    Start,
                    End,
                    blockchain_ledger_v1:mode(active, AuxLedger),
                    Chain,
                    true
                )
            of
                {error, _} = E ->
                    E;
                {ok, _, OrigMD} ->
                    blockchain_aux_ledger_v1:set_rewards_md(End + 1, OrigMD, AuxMD, AuxLedger)
            end
    end.

-spec absorb_rewards(
    Rewards :: rewards(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> ok.
absorb_rewards(Rewards, Ledger) ->
    lists:foreach(
        fun(#blockchain_txn_reward_v3_pb{account = Account, amount = Amount}) ->
            ok = blockchain_ledger_v1:credit_account(Account, Amount, Ledger)
        end,
        Rewards
    ).

-spec print(txn_rewards_v3()) -> iodata().
print(undefined) ->
    <<"type=rewards_v3 undefined">>;
print(#blockchain_txn_rewards_v3_pb{
    start_epoch = Start,
    end_epoch = End
}) ->
    io_lib:format(
        "type=rewards_v3 start_epoch=~p end_epoch=~p",
        [Start, End]
    ).

json_type() ->
    <<"rewards_v3">>.

-spec to_json(txn_rewards_v3(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    Rewards = lists:foldl(
        fun(#blockchain_txn_reward_v3_pb{account = Account, amount = Amount}, Acc) ->
            [
                #{
                    type => <<"reward_v3">>,
                    account => ?BIN_TO_B58(Account),
                    amount => Amount
                }
                | Acc
            ]
        end,
        [],
        ?MODULE:rewards(Txn)
    ),
    #{
        type => ?MODULE:json_type(),
        hash => ?BIN_TO_B64(hash(Txn)),
        start_epoch => start_epoch(Txn),
        end_epoch => end_epoch(Txn),
        rewards => Rewards
    }.

-spec new_reward(
    Account :: libp2p_crypto:pubkey_bin(),
    Amount :: non_neg_integer()
) -> reward_v3().
new_reward(Account, Amount) ->
    #blockchain_txn_reward_v3_pb{account = Account, amount = Amount}.

-spec get_reward_vars(
    Start :: pos_integer(),
    End :: pos_integer(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> map().
get_reward_vars(Start, End, Ledger) ->
    {ok, MonthlyReward} = blockchain:config(?monthly_reward, Ledger),
    {ok, SecuritiesPercent} = blockchain:config(?securities_percent, Ledger),
    {ok, ConsensusPercent} = blockchain:config(?consensus_percent, Ledger),
    {ok, ElectionInterval} = blockchain:config(?election_interval, Ledger),
    {ok, ElectionRestartInterval} = blockchain:config(?election_restart_interval, Ledger),
    {ok, BlockTime} = blockchain:config(?block_time, Ledger),
    {ok, RewardVersion} = blockchain:config(?reward_version, Ledger),
    {ok, NetEmissionsMaxRate} = blockchain:config(?net_emissions_max_rate, Ledger),
    {ok, HNTBurned} = blockchain_ledger_v1:hnt_burned(Ledger),
    {ok, NetOverage} = blockchain_ledger_v1:net_overage(Ledger),

    TreasuryPubkeyBin =
        case blockchain:config(?treasury_pubkey_bin, Ledger) of
            {ok, TPB} -> TPB;
            _ -> undefined
        end,

    Vars0 = #{
        monthly_reward => MonthlyReward,
        securities_percent => SecuritiesPercent,
        consensus_percent => ConsensusPercent,
        reward_version => RewardVersion,
        election_interval => ElectionInterval,
        election_restart_interval => ElectionRestartInterval,
        block_time => BlockTime,
        net_emissions_max_rate => NetEmissionsMaxRate,
        hnt_burned => HNTBurned,
        net_overage => NetOverage,
        treasury_pubkey_bin => TreasuryPubkeyBin
    },
    EpochReward = calculate_epoch_reward(Start, End, Vars0),
    maps:put(epoch_reward, EpochReward, Vars0).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec calculate_epoch_reward(
    Start :: pos_integer(),
    End :: pos_integer(),
    Vars :: map()
) -> float().
calculate_epoch_reward(Start, End, Vars) ->
    BlockTime0 = maps:get(block_time, Vars),
    MonthlyReward = maps:get(monthly_reward, Vars),
    BlockTime1 = (BlockTime0 / 1000),
    % Convert to blocks per min
    BlockPerMin = 60 / BlockTime1,
    % Convert to blocks per hour
    BlockPerHour = BlockPerMin * 60,
    % Calculate election interval in blocks

    % epoch is inclusive of start and end
    ElectionInterval = End - Start + 1,
    ElectionPerHour = BlockPerHour / ElectionInterval,
    Reward = MonthlyReward / 30 / 24 / ElectionPerHour,
    Extra = calculate_net_emissions_reward(Vars),
    Reward + Extra.

calculate_net_emissions_reward(Vars) ->
    case maps:get(net_emissions_enabled, Vars, false) of
        true ->
            %% initial proposed max 34.24
            NetEmissionsMaxRate = maps:get(net_emissions_max_rate, Vars),
            HNTBurned = maps:get(hnt_burned, Vars),
            NetOverage = maps:get(net_overage, Vars),
            min(NetEmissionsMaxRate, HNTBurned + NetOverage);
        false ->
            0
    end.

-spec calculate_rewards(
    Start :: non_neg_integer(),
    End :: non_neg_integer(),
    Chain :: blockchain:blockchain()
) ->
    {ok, rewards()} | {error, any()}.
%% @doc Calculate and return an ordered list (as ordered by lists:sort/1) of
%% rewards for use in a rewards_v3 transaction. Given how lists:sort/1 works,
%% ordering will depend on (binary) account information.
calculate_rewards(Start, End, Chain) ->
    {ok, Ledger} = blockchain:ledger_at(End, Chain),
    Result = calculate_rewards_(Start, End, Ledger, Chain, false),
    _ = blockchain_ledger_v1:delete_context(Ledger),
    Result.

-spec calculate_rewards_(
    Start :: non_neg_integer(),
    End :: non_neg_integer(),
    Ledger :: blockchain_ledger_v1:ledger(),
    Chain :: blockchain:blockchain(),
    ReturnMD :: boolean()
) -> {error, any()} | {ok, rewards()} | {ok, rewards(), rewards_md()}.
calculate_rewards_(Start, End, Ledger, Chain, ReturnMD) ->
    {ok, Results} = calculate_rewards_md(Start, End, blockchain:ledger(Ledger, Chain)),
    try
        case ReturnMD of
            false ->
                {ok, prepare_rewards_v3_txns(Results, Ledger)};
            true ->
                {ok, prepare_rewards_v3_txns(Results, Ledger), Results}
        end
    catch
        C:Error:Stack ->
            lager:error("Caught ~p; couldn't prepare rewards txn because: ~p~n~p", [C, Error, Stack]),
            Error
    end.

-spec calculate_rewards_md(
    Start :: non_neg_integer(),
    End :: non_neg_integer(),
    Chain :: blockchain:blockchain()
) ->
    {ok, Metadata :: rewards_md()} | {error, Error :: term()}.
calculate_rewards_md(Start, End, Chain) ->
    {ok, Ledger} = blockchain:ledger_at(End, Chain),
    Vars = get_reward_vars(Start, End, Ledger),
    Result = calculate_rewards_md_(Start, End, Ledger, Vars),
    _ = blockchain_ledger_v1:delete_context(Ledger),
    Result.

-spec calculate_rewards_md_(
    Start :: non_neg_integer(),
    End :: non_neg_integer(),
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map()
) ->
    {ok, rewards_md()} | {error, term()}.
calculate_rewards_md_(Start, End, Ledger, Vars) ->
    try
        ConsensusEpochReward = calculate_consensus_epoch_reward(Start, End, Vars),
        Vars1 = Vars#{consensus_epoch_reward => ConsensusEpochReward},
        Results = finalize_reward_calculations(Ledger, Vars1),
        {ok, Results}
    catch
        C:Error:Stack ->
            lager:error("Caught ~p; couldn't calculate rewards metadata because: ~p~n~p", [
                C,
                Error,
                Stack
            ]),
            Error
    end.

-spec prepare_rewards_v3_txns(
    Results :: rewards_md(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> rewards().
prepare_rewards_v3_txns(Results, Ledger) ->
    %% we are going to fold over a list of keys in the rewards map (Results)
    %% and generate a new map which has _all_ the owners and the sum of
    %% _all_ rewards types in a new map...
    AllRewards = lists:foldl(
        fun(RewardCategory, Rewards) ->
            R = maps:get(RewardCategory, Results),
            %% R is our map of rewards of the given type
            %% and now we are going to do a maps:fold/3
            %% over this reward category and either
            %% add the owner and amount for the first
            %% time or add an amount to an existing owner
            %% in the Rewards accumulator

            maps:fold(
                fun(Entry, Amt, Acc) ->
                    case Entry of
                        {owner, _Type, O} ->
                            maps:update_with(
                                O,
                                fun(Balance) -> Balance + Amt end,
                                Amt,
                                Acc
                            );
                        {validator, _Type, V} ->
                            case blockchain_ledger_v1:get_validator(V, Ledger) of
                                {error, _} ->
                                    Acc;
                                {ok, Val} ->
                                    Owner = blockchain_ledger_validator_v1:owner_address(Val),
                                    maps:update_with(
                                        Owner,
                                        fun(Balance) -> Balance + Amt end,
                                        Amt,
                                        Acc
                                    )
                            end
                        % Entry case
                    end
                % function
                end,
                Rewards,
                %% bound memory size no matter size of map
                maps:iterator(R)
            )
        end,
        #{},
        [
            consensus_rewards,
            securities_rewards,
            treasury_rewards
        ]
    ),

    %% now we are going to fold over all rewards and construct our
    %% transaction for the blockchain

    Rewards = maps:fold(
        fun
            (Owner, 0, Acc) ->
                lager:debug(
                    "Dropping reward for ~p because the amount is 0",
                    [?BIN_TO_B58(Owner)]
                ),
                Acc;
            (Owner, Amount, Acc) ->
                [new_reward(Owner, Amount) | Acc]
        end,
        [],
        %% again, bound memory no matter size of map
        maps:iterator(AllRewards)
    ),

    %% sort the rewards list before it gets returned so list ordering is deterministic
    %% (map keys can be enumerated in any arbitrary order)
    lists:sort(Rewards).

-spec calculate_consensus_epoch_reward(
    Start :: pos_integer(),
    End :: pos_integer(),
    Vars :: map()
) -> float().
calculate_consensus_epoch_reward(Start, End, Vars) ->
    #{
        block_time := BlockTime0,
        election_interval := ElectionInterval,
        election_restart_interval := ElectionRestartInterval,
        monthly_reward := MonthlyReward
    } = Vars,
    BlockTime1 = (BlockTime0 / 1000),
    % Convert to blocks per min
    BlockPerMin = 60 / BlockTime1,
    % Convert to blocks per month
    BlockPerMonth = BlockPerMin * 60 * 24 * 30,
    % Calculate epoch length in blocks, cap at election interval + grace period
    EpochLength = erlang:min(End - Start + 1, ElectionInterval + ElectionRestartInterval),
    Reward = (MonthlyReward / BlockPerMonth) * EpochLength,
    Extra = calculate_net_emissions_reward(Vars),
    Reward + Extra.

-spec securities_rewards(
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map()
) -> rewards_map().
securities_rewards(Ledger, #{
    epoch_reward := EpochReward,
    securities_percent := SecuritiesPercent
}) ->
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
            PercentofReward = (Balance * 100 / TotalSecurities) / 100,
            Amount = erlang:round(PercentofReward * SecuritiesReward),
            maps:put({owner, securities, Key}, Amount, Acc)
        end,
        #{},
        Securities
    ).

-spec consensus_members_rewards(
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map()
) -> rewards_map().
consensus_members_rewards(
    Ledger,
    #{
        consensus_epoch_reward := EpochReward,
        consensus_percent := ConsensusPercent
    }
) ->
    {ok, Members} = blockchain_ledger_v1:consensus_members(Ledger),
    Count = erlang:length(Members),
    ConsensusReward = EpochReward * ConsensusPercent,
    lists:foldl(
        fun(Member, Acc) ->
            PercentofReward = 100 / Count / 100,
            Amount = erlang:round(PercentofReward * ConsensusReward),
            maps:put({validator, consensus, Member}, Amount, Acc)
        end,
        #{},
        Members
    ).

-spec treasury_rewards(Vars :: map()) -> rewards_map().
treasury_rewards(#{
    consensus_epoch_reward := EpochReward,
    treasury_pubkey_bin := TreasuryPubkeyBin,
    consensus_percent := ConsensusPercent,
    securities_percent := SecuritiesPercent
}) ->
    TreasuryPercent = 1 - (ConsensusPercent + SecuritiesPercent),
    TreasuryReward = erlang:round(EpochReward * TreasuryPercent),
    %% Treasury gets the full percentage
    #{{owner, treasury, TreasuryPubkeyBin} => TreasuryReward}.

-spec finalize_reward_calculations(
    Ledger :: blockchain_ledger_v1:ledger(),
    Vars :: map()
) -> rewards_md().
finalize_reward_calculations(
    Ledger,
    Vars
) ->
    SecuritiesRewards = securities_rewards(Ledger, Vars),
    ConsensusRewards = consensus_members_rewards(Ledger, Vars),
    TreasuryRewards = treasury_rewards(Vars),
    #{
        consensus_rewards => ConsensusRewards,
        securities_rewards => SecuritiesRewards,
        treasury_rewards => TreasuryRewards
    }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.
