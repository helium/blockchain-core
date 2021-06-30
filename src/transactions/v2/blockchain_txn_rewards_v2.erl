%%-------------------------------------------------------------------
%% @doc
%% This module implements rewards v2 which track only an account and
%% an amount. The breakdowns of rewards by type and associated gateway
%% are not kept here.  This was done to streamline the increasing
%% size of rewards as the network grows.
%%
%% In the future, we will need to work on further ways to streamline
%% the size of this transaction. One proposal was to use the ledger
%% as an encoding dictionary and use ledger offsets to mark the
%% account instead of using the full sized account id.
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_rewards_v2).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_rewards_v2_pb.hrl").

-export([
    new/3,
    hash/1,
    start_epoch/1,
    end_epoch/1,
    rewards/1,

    %% reward v2 accessors
    reward_account/1,
    reward_amount/1,

    sign/2,
    fee/1,
    fee_payer/2,
    is_valid/2,
    absorb/2,
    calculate_rewards/3,
    calculate_rewards_metadata/3,
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([v1_to_v2/1]).
-endif.

-type reward_types() :: poc_challengers | poc_challengees | poc_witnesses | data_credits | consensus.

-record(rewards_shares, {
          type = undefined :: undefined | reward_types(),
          shares = 0 :: number() %% shares of a reward type
}).

-type rewards_shares() :: #rewards_shares{}.

-record(rewards_meta, {
          hnt_amount = 0 :: number(), %% actual HNT value (rolling accumulator)
          shares = [] :: [ rewards_shares() ] %% individual shares composing HNT
}).

-type rewards_meta() :: #rewards_meta{}.

%% rolling accumulator for _total_ dc rewards
-record(dc_shares, {
          seen = [] :: [blockchain_state_channel_v1:id()],
          total = 0 :: number()
}).

-type txn_rewards_v2() :: #blockchain_txn_rewards_v2_pb{}.
-type reward_v2() :: #blockchain_txn_reward_v2_pb{}.
-type rewards() :: [reward_v2()].
-type reward_vars() :: map().
-type total_shares() :: #{ poc_challengers => non_neg_integer(),
                           poc_challengees => non_neg_integer(),
                           poc_witnesses => non_neg_integer(),
                           data_credits => dc_shares_record(),
                           overages => non_neg_integer() }.
-type dc_shares_record() :: #dc_shares{}.

%% `shares_acc' is a rolling accumulator for the _total_ amount
%% of each reward type that generates share values. These values
%% are used in normalization calculations.
%%
%% The pubkey bin here is a _gateway owner_ address; we get rid
%% of the gateway address as soon as we can and will rely on
%% rocks caching to hopefully not hit disk for these lookups
%% too frequently
-type rewards_metadata() :: #{  shares_acc => total_shares(), libp2p_crypto:pubkey_bin() => rewards_meta()
                               }.

-export_type([txn_rewards_v2/0, rewards_metadata/0]).


%% ------------------------------------------------------------------
%% Public API
%% ------------------------------------------------------------------

-spec new(non_neg_integer(), non_neg_integer(), rewards()) -> txn_rewards_v2().
new(Start, End, Rewards) ->
    SortedRewards = lists:sort(Rewards),
    #blockchain_txn_rewards_v2_pb{start_epoch=Start, end_epoch=End, rewards=SortedRewards}.

-spec hash(txn_rewards_v2()|reward_v2()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_rewards_v2_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

-spec start_epoch(txn_rewards_v2()) -> non_neg_integer().
start_epoch(#blockchain_txn_rewards_v2_pb{start_epoch=Start}) ->
    Start.

-spec end_epoch(txn_rewards_v2()) -> non_neg_integer().
end_epoch(#blockchain_txn_rewards_v2_pb{end_epoch=End}) ->
    End.

-spec rewards(txn_rewards_v2()) -> rewards().
rewards(#blockchain_txn_rewards_v2_pb{rewards=Rewards}) ->
    Rewards.

-spec reward_account(reward_v2()) -> binary().
reward_account(#blockchain_txn_reward_v2_pb{account = Account}) ->
    Account.

-spec reward_amount(reward_v2()) -> non_neg_integer().
reward_amount(#blockchain_txn_reward_v2_pb{amount = Amount}) ->
    Amount.

-spec sign(txn_rewards_v2(), libp2p_crypto:sig_fun()) -> txn_rewards_v2().
sign(Txn, _SigFun) ->
    Txn.

-spec fee(txn_rewards_v2()) -> 0.
fee(_Txn) ->
    0.

-spec fee_payer(txn_rewards_v2(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec is_valid(txn_rewards_v2(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    TxnRewards = ?MODULE:rewards(Txn),
    %% TODO: REMOVE THIS ENTIRE CASE STATEMENT AT NEXT RESTART
    case TxnRewards of
        [] ->
            ok;
        _ ->
            case ?MODULE:calculate_rewards(Start, End, Chain) of
                {error, _Reason}=Error ->
                    Error;
                {ok, CalRewards} ->
                    CalRewardsHashes = [hash(R)|| R <- CalRewards],
                    TxnRewardsHashes = [hash(R)|| R <- TxnRewards],
                    case CalRewardsHashes == TxnRewardsHashes of
                        false -> {error, invalid_rewards_v2};
                        true -> ok
                    end
            end
    end.

-spec absorb(txn_rewards_v2(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),

    case blockchain_ledger_v1:mode(Ledger) == aux of
        false ->
            %% only absorb in the main ledger
            absorb_(Txn, Ledger);
        true ->
            aux_absorb(Txn, Ledger, Chain)
    end.

-spec absorb_(Txn :: txn_rewards_v2(), Ledger :: blockchain_ledger_v1:ledger()) -> ok.
absorb_(Txn, Ledger) ->
    Rewards = ?MODULE:rewards(Txn),
    absorb_rewards(Rewards, Ledger).

-spec absorb_rewards(Rewards :: rewards(),
                     Ledger :: blockchain_ledger_v1:ledger()) -> ok.
absorb_rewards(Rewards, Ledger) ->
    lists:foreach(
        fun(#blockchain_txn_reward_v2_pb{account=Account, amount=Amount}) ->
            ok = blockchain_ledger_v1:credit_account(Account, Amount, Ledger)
        end,
        Rewards
    ).

-spec aux_absorb(Txn :: txn_rewards_v2(),
                 AuxLedger :: blockchain_ledger_v1:ledger(),
                 Chain :: blockchain:blockchain()) -> ok | {error, any()}.
aux_absorb(Txn, AuxLedger, Chain) ->
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    %% NOTE: This is an aux ledger, we don't use rewards(txn) here, instead we calculate them manually
    %% and do 0 verification for absorption
    case calculate_rewards_(Start, End, Chain) of
        {error, _}=E -> E;
        {ok, AuxRewards} ->
            TxnRewards = rewards(Txn),
            %% absorb the rewards attached to the txn (real)
            absorb_rewards(TxnRewards, AuxLedger),
            %% set auxiliary rewards in the aux ledger also
            lager:info("are aux rewards equal?: ~p", [lists:sort(TxnRewards) == lists:sort(AuxRewards)]),
            %% rewards appear in (End + 1) block
            blockchain_ledger_v1:set_aux_rewards(End + 1, TxnRewards, AuxRewards, AuxLedger)
    end.


-spec calculate_rewards(non_neg_integer(), non_neg_integer(), blockchain:blockchain()) ->
    {ok, rewards()} | {error, any()}.
%% @doc Calculate and return an ordered list (as ordered by lists:sort/1) of
%% rewards for use in a rewards_v2 transaction. Given how lists:sort/1 works,
%% ordering will depend on (binary) account information.
calculate_rewards(Start, End, Chain) ->
    calculate_rewards_(Start, End, Chain).

-spec calculate_rewards_(
        Start :: non_neg_integer(),
        End :: non_neg_integer(),
        Chain :: blockchain:blockchain()) -> {error, any()} | {ok, rewards()}.
calculate_rewards_(Start, End, Chain) ->
    {ok, Results} = calculate_rewards_metadata(Start, End, Chain),
    try
        {ok, prepare_rewards_v2_txns(Results)}
    catch
        C:Error:Stack ->
            lager:error("Caught ~p; couldn't prepare rewards txn because: ~p~n~p", [C, Error, Stack]),
            Error
    end.

-spec calculate_rewards_metadata(
        Start :: non_neg_integer(),
        End :: non_neg_integer(),
        Chain :: blockchain:blockchain() ) ->
    {ok, Metadata :: rewards_metadata()} | {error, Error :: term()}.
%% @doc Calculate <i>only</i> rewards metadata (do not return v2 reward records
%% to the caller.)
calculate_rewards_metadata(Start, End, Chain) ->
    {ok, Ledger} = blockchain:ledger_at(End, Chain),
    Vars0 = get_reward_vars(Start, End, Ledger),
    VarMap = case blockchain_hex:var_map(Ledger) of
                 {error, _Reason} -> #{};
                 {ok, VM} -> VM
             end,

    Vars = Vars0#{ var_map => VarMap },

    %% Previously, if a state_channel closed in the grace blocks before an
    %% epoch ended, then it wouldn't ever get rewarded.
    %%
    %% This rewards data structure is a map, documented more fully by its
    %% type definition.
    {ok, RewardsInit} = collect_dc_rewards_from_previous_epoch_grace(Start, End,
                                                                     Chain, Vars,
                                                                     Ledger),

    try
        %% We only want to fold over the blocks and transaction in an epoch once,
        %% so we will do that top level work here. If we get a thrown error while
        %% we are folding, we will abort reward calculation.
        Results0 = fold_blocks_for_rewards(Start, End, Chain,
                                           Vars, Ledger, RewardsInit),

        %% Forcing calculation of the EpochReward amount for the CG to always
        %% be around ElectionInterval (30 blocks) so that there is less incentive
        %% to stay in the consensus group
        ConsensusEpochReward = calculate_epoch_reward(1, Start, End, Ledger),
        Vars1 = Vars#{ consensus_epoch_reward => ConsensusEpochReward },

        Results = finalize_reward_calculations(Results0, Ledger, Vars1),
        %% we are only keeping hex density calculations memoized for a single
        %% rewards transaction calculation, then we discard that work and avoid
        %% cache invalidation issues.
        true = blockchain_hex:destroy_memoization(),
        {ok, Results}
    catch
        C:Error:Stack ->
            lager:error("Caught ~p; couldn't calculate rewards metadata because: ~p~n~p", [C, Error, Stack]),
            Error
    end.

-spec print(txn_rewards_v2()) -> iodata().
print(undefined) -> <<"type=rewards_v2 undefined">>;
print(#blockchain_txn_rewards_v2_pb{start_epoch=Start,
                                    end_epoch=End,
                                    rewards=Rewards}) ->
    PrintableRewards = [ print_reward(R) || R <- Rewards],
    io_lib:format("type=rewards_v2 start_epoch=~p end_epoch=~p rewards=~p",
                  [Start, End, PrintableRewards]).

-spec to_json(txn_rewards_v2(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    Rewards = [ reward_to_json(R, []) || R <- rewards(Txn)],
    #{
      type => <<"rewards_v2">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      start_epoch => start_epoch(Txn),
      end_epoch => end_epoch(Txn),
      rewards => Rewards
    }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec print_reward( reward_v2() ) -> iodata().
print_reward(#blockchain_txn_reward_v2_pb{account = Account, amount = Amt}) ->
    io_lib:format("type=reward_v2 account=~p amount=~p",
                  [Account, Amt]).

-spec reward_to_json( Reward :: reward_v2(),
                      Opts :: blockchain_json:opts() ) -> blockchain_json:json_object().
reward_to_json(#blockchain_txn_reward_v2_pb{account = Account, amount = Amt}, _Opts) ->
    #{
      type => <<"reward_v2">>,
      account => ?BIN_TO_B58(Account),
      amount => Amt
     }.

-spec new_reward( Account :: libp2p_crypto:pubkey_bin(),
                  Amount :: number() ) -> reward_v2().
new_reward(Account, Amount) ->
    #blockchain_txn_reward_v2_pb{account=Account, amount=Amount}.

-spec fold_blocks_for_rewards( Current :: pos_integer(),
                               End :: pos_integer(),
                               Chain :: blockchain:blockchain(),
                               Vars :: reward_vars(),
                               Ledger :: blockchain_ledger_v1:ledger(),
                               Acc :: rewards_metadata() ) -> rewards_metadata().
fold_blocks_for_rewards(Current, End, _Chain, _Vars, _Ledger, Acc) when Current == End + 1 -> Acc;
fold_blocks_for_rewards(Current, End, Chain, Vars, Ledger, Acc) ->
    case blockchain:get_block(Current, Chain) of
        {error, _Reason} = Error -> throw(Error);
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            NewAcc = lists:foldl(fun(T, A) ->
                                         calculate_reward_for_txn(blockchain_txn:type(T), T, End,
                                                                  A, Chain, Ledger, Vars)
                                 end,
                                 Acc, Txns),
            fold_blocks_for_rewards(Current+1, End, Chain, Vars, Ledger, NewAcc)
    end.

-spec calculate_reward_for_txn( Type :: atom(),
                                Txn :: blockchain_txn:txn(),
                                End :: pos_integer(),
                                Acc :: rewards_metadata(),
                                Chain :: blockchain:blockchain(),
                                Ledger :: blockchain_ledger_v1:ledger(),
                                Vars :: reward_vars() ) -> rewards_metadata().
calculate_reward_for_txn(?MODULE, _Txn, _End, _Acc, _Chain,
                         _Ledger, _Vars) -> throw({error, already_existing_rewards_v2});
calculate_reward_for_txn(blockchain_txn_rewards_v1, _Txn, _End, _Acc, _Chain,
                         _Ledger, _Vars) -> throw({error, already_existing_rewards_v1});
calculate_reward_for_txn(blockchain_txn_poc_receipts_v1, Txn, _End, Acc,
                         Chain, Ledger, Vars) ->
    Acc0 = poc_challenger_reward(Txn, Acc, Ledger, Vars),
    Acc1 = calculate_poc_challengee_rewards(Txn, Acc0, Chain, Ledger, Vars),
    calculate_poc_witness_rewards(Txn, Acc1, Chain, Ledger, Vars);
calculate_reward_for_txn(blockchain_txn_state_channel_close_v1, Txn, End, Acc, Chain, Ledger, Vars) ->
    calculate_dc_rewards(Txn, End, Acc, Chain, Ledger, Vars);
calculate_reward_for_txn(Type, Txn, _End, Acc, _Chain, Ledger, _Vars) ->
    consider_overage(Type, Txn, Acc, Ledger).

-spec consider_overage( Type :: atom(),
                        Txn :: blockchain_txn:txn(),
                        RewardsMD :: rewards_metadata(),
                        Ledger :: blockchain_ledger_v1:ledger() ) -> rewards_metadata().
consider_overage(Type, Txn, #{ shares_acc := Shares } = RewardsMD, Ledger) ->
    %% calculate any fee paid in excess which we will distribute as bonus HNT
    %% to consensus members
    try
        Type:calculate_fee(Txn, Ledger) - Type:fee(Txn) of
        Overage when Overage > 0 ->
            NewShares = maps:update_with(overages,
                                         fun(Overages) -> Overages + Overage end,
                                         Overage, Shares),
            RewardsMD#{ shares_acc => NewShares };
        _ ->
            RewardsMD
    catch
        _:_ ->
            RewardsMD
    end.

-spec calculate_poc_challengee_rewards( Txn :: blockchain_txn:txn(),
                                        Acc :: rewards_metadata(),
                                        Chain :: blockchain:blockchain(),
                                        Ledger :: blockchain_ledger_v1:ledger(),
                                        Vars :: reward_vars() ) -> rewards_metadata().
calculate_poc_challengee_rewards(Txn, Acc, Chain, Ledger, #{ var_map := VarMap } = Vars) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    poc_challengees_rewards_(Vars, Path, Path, Txn, Chain, Ledger, true, VarMap, Acc).

-spec calculate_poc_witness_rewards( Txn :: blockchain_txn:txn(),
                                     Acc :: rewards_metadata(),
                                     Chain :: blockchain:blockchain(),
                                     Ledger :: blockchain_ledger_v1:ledger(),
                                     Vars :: reward_vars() ) -> rewards_metadata().
calculate_poc_witness_rewards(Txn, Acc, Chain, Ledger, Vars) ->
    poc_witness_reward(Txn, Acc, Chain, Ledger, Vars).

-spec calculate_dc_rewards( Txn :: blockchain_txn:txn(),
                            End :: pos_integer(),
                            Acc :: rewards_metadata(),
                            Chain :: blockchain:blockchain(),
                            Ledger :: blockchain_ledger_v1:ledger(),
                            Vars :: reward_vars() ) -> rewards_metadata().
calculate_dc_rewards(Txn, End, Acc, _Chain, Ledger, Vars) ->
    dc_reward(Txn, End, Acc, Ledger, Vars).

-spec finalize_reward_calculations( RewardsMD :: rewards_metadata(),
                                    Ledger :: blockchain_ledger_v1:ledger(),
                                    Vars :: reward_vars() ) -> rewards_metadata().
finalize_reward_calculations(#{shares_acc := Shares} = RewardsMD, Ledger, Vars) ->
    RewardsMD0 = securities_rewards(RewardsMD, Ledger, Vars),
    Overages = maps:get(overages, Shares, 0),
    RewardsMD1 = consensus_members_rewards(RewardsMD0, Ledger, Vars, Overages),

    {DCRemainder, DCReward} = calculate_dc_remainder(RewardsMD1, Vars),


    %% ok, we are now going to calculate the total reward amount for each
    %% reward type that uses shares - we do this here to calculate it
    %% once for all subsequent reward calculations for each entry in
    %% the rewards metadata rather than recalculating it over and over.
    Vars0 = lists:foldl(fun(T, VarMap) ->
                                {K, V} = calculate_total_reward_for_type(T, VarMap),
                                VarMap#{K => V}
                        end,
                        Vars#{dc_remainder => DCRemainder,
                              dc_reward => DCReward},
                        [poc_challengers, poc_challengees, poc_witnesses]),


    %% we need to map over gateway owners and then fold over the list
    %% of rewards shares and turn those shares into HNT payouts
    maps:map(fun(shares_acc, V) -> V;
                (_K, #rewards_meta{ shares = S } = RMD) ->
                      HNT = lists:foldl(fun(#rewards_shares{type = T,
                                                            shares = A}, Acc) ->
                                                Acc + normalize_shares(T, A, Shares, Vars0)
                                        end,
                                        0, S),
                      update_hnt(RMD, HNT)
             end,
             maps:iterator(RewardsMD)).

-spec calculate_total_reward_for_type(Type :: reward_types(),
                                      Vars :: reward_vars()
                                     ) -> { Key :: atom(), Value :: number() }.
%% @doc For each of challengers, challengees, and witnesses, calculate the entire
%% amount of HNT available in this epoch, then return a Key and a Value to store
%% in our rewards vars map.
calculate_total_reward_for_type(poc_challengers,
                                #{epoch_reward := EpochReward,
                                  poc_challengers_percent := PocChallengersPercent}=Vars) ->
    ShareOfDCRemainder = share_of_dc_rewards(poc_challengers_percent, Vars),
    {poc_challengers_reward, (EpochReward * PocChallengersPercent) + ShareOfDCRemainder};
calculate_total_reward_for_type(poc_challengees,
                                #{epoch_reward := EpochReward,
                                  poc_challengees_percent := PocChallengeesPercent}=Vars) ->
    ShareOfDCRemainder = share_of_dc_rewards(poc_challengees_percent, Vars),
    {poc_challengees_reward, (EpochReward * PocChallengeesPercent) + ShareOfDCRemainder};
calculate_total_reward_for_type(poc_witnesses,
                                #{epoch_reward := EpochReward,
                                  poc_witnesses_percent := PocWitnessesPercent}=Vars) ->
    ShareOfDCRemainder = share_of_dc_rewards(poc_witnesses_percent, Vars),
    {poc_witnesses_reward, (EpochReward * PocWitnessesPercent) + ShareOfDCRemainder}.

-spec normalize_shares( Type :: reward_types(),
                        Amount :: number(),
                        Totals :: total_shares(),
                        Vars :: reward_vars() ) -> HNT :: number().
%% @doc This function takes a reward type and a number of shares of that reward
%% type and converts it into an HNT payout.
normalize_shares(poc_challengers, Amount,
                 #{poc_challengers := TotalChallengerShares},
                 #{poc_challengers_reward := TotalChallengerReward}) ->
    shares_to_reward(Amount, TotalChallengerShares, TotalChallengerReward);
normalize_shares(poc_challengees, Amount,
                 #{poc_challengees := TotalChallengeeShares},
                 #{poc_challengees_reward := TotalChallengeeReward}) ->
    shares_to_reward(Amount, TotalChallengeeShares, TotalChallengeeReward);
normalize_shares(poc_witnesses, Amount,
                 #{poc_witnesses := TotalWitnessShares},
                 #{poc_witnesses_reward := TotalWitnessReward}) ->
    shares_to_reward(Amount, TotalWitnessShares, TotalWitnessReward);
normalize_shares(data_credits, Amount,
                 #{data_credits := #dc_shares{total=TotalDCs}},
                 #{dc_reward := DCReward}) ->
    shares_to_reward(Amount, TotalDCs, DCReward).

-spec shares_to_reward(Amount :: number(),
                       TotalShares :: number(),
                       TotalReward :: number() ) -> number().
%% @doc Calculate a reward based on the total number of shares for a reward type
%% and the total amount of HNT allocated for that reward in this epoch.
shares_to_reward(Amount, TotalShares, TotalReward) ->
    PercentofReward = (Amount*100/TotalShares)/100,
    erlang:round(PercentofReward * TotalReward).

-spec prepare_rewards_v2_txns( Results :: rewards_metadata() ) -> rewards().
prepare_rewards_v2_txns(Results0) ->
    %% fold over rewards and construct our transaction for the blockchain

    %% don't need the shares acculumator term any more
    Results = maps:remove(shares_acc, Results0),

    %% reminder that the map shape is
    %% #{ GwOwner => #rewards_meta{ hnt_amount = HNT }}
    Rewards = maps:fold(fun(Owner, #rewards_meta{hnt_amount = 0}, Acc) ->
                                lager:debug("Dropping reward for ~p because the amount is 0",
                                            [?BIN_TO_B58(Owner)]),
                                Acc;
                            (Owner, #rewards_meta{hnt_amount = A}, Acc) ->
                                [ new_reward(Owner, A) | Acc ]
                        end,
              [],
              maps:iterator(Results)),

    %% sort the rewards list before it gets returned so list ordering is deterministic
    %% (map keys can be enumerated in any arbitrary order)
    lists:sort(Rewards).

-spec get_reward_vars(pos_integer(), pos_integer(), blockchain_ledger_v1:ledger()) -> reward_vars().
get_reward_vars(Start, End, Ledger) ->
    {ok, MonthlyReward} = blockchain:config(?monthly_reward, Ledger),
    {ok, SecuritiesPercent} = blockchain:config(?securities_percent, Ledger),
    {ok, PocChallengeesPercent} = blockchain:config(?poc_challengees_percent, Ledger),
    {ok, PocChallengersPercent} = blockchain:config(?poc_challengers_percent, Ledger),
    {ok, PocWitnessesPercent} = blockchain:config(?poc_witnesses_percent, Ledger),
    {ok, ConsensusPercent} = blockchain:config(?consensus_percent, Ledger),
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

    HIP15TxRewardUnitCap = case blockchain:config(?hip15_tx_reward_unit_cap, Ledger) of
                          {ok, Val} -> Val;
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
        dc_percent => DCPercent,
        sc_grace_blocks => SCGrace,
        sc_version => SCVersion,
        poc_version => POCVersion,
        reward_version => RewardVersion,
        witness_redundancy => WitnessRedundancy,
        poc_reward_decay_rate => DecayRate,
        density_tgt_res => DensityTgtRes,
        hip15_tx_reward_unit_cap => HIP15TxRewardUnitCap
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

-spec consensus_members_rewards(rewards_metadata(),
                                blockchain_ledger_v1:ledger(),
                                reward_vars(),
                                non_neg_integer()) -> rewards_metadata().
consensus_members_rewards(RewardsMD, Ledger,
                          #{consensus_epoch_reward := EpochReward,
                            consensus_percent := ConsensusPercent}, OverageTotal) ->
    GwOrVal =
        case blockchain:config(?election_version, Ledger) of
            {ok, N} when N >= 5 ->
                validator;
            _ ->
                gateway
        end,
    {ok, Members} = blockchain_ledger_v1:consensus_members(Ledger),
    Count = erlang:length(Members),
    OveragePerMember = OverageTotal div Count,
    ConsensusReward = EpochReward * ConsensusPercent,
    lists:foldl(
      fun(Member, Acc) ->
              PercentofReward = 100/Count/100,
              Amount = erlang:round(PercentofReward*ConsensusReward),
              case get_owner_address(GwOrVal, Member, Ledger) of
                  {ok, Owner} ->
                      update_hnt(Owner, Amount + OveragePerMember, Acc);
                  {error, _Error} -> Acc
              end
      end,
      RewardsMD,
      Members).

-spec securities_rewards(rewards_metadata(),
                         blockchain_ledger_v1:ledger(),
                         reward_vars()) -> rewards_metadata().
securities_rewards(RewardsMD, Ledger, #{epoch_reward := EpochReward,
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
            update_hnt(Key, Amount, Acc)
        end,
        RewardsMD,
        Securities
    ).

-spec get_shares( Type :: reward_types(),
                  Gateway :: libp2p_crypto:pubkey_bin(), %% NOTE: _NOT_ the owner, raw gateway
                  RewardsMD :: rewards_metadata(),
                  Ledger :: blockchain_ledger_v1:ledger(),
                  Default :: non_neg_integer() ) -> CurrentShares :: number().
%% @doc This function looks up the gateway owner, looks for the given reward type in the
%% shares list of `#rewards_meta{}' and returns either the current value for that reward
%% type or the default value if there is no entry for the reward type.
get_shares(Type, Gateway, RewardsMD, Ledger, Default) ->
    case blockchain_ledger_v1:find_gateway_owner(Gateway, Ledger) of
        {ok, GwOwner} ->
            case maps:get(GwOwner, RewardsMD, Default) of
                %% this gateway owner is NOT in the rewards metadata (yet)
                Default -> Default;
                #rewards_meta{ shares = S } ->
                    %% ok, this owner IS in the metadata, so let's find
                    %% the share data for this rewards type
                    case find_reward_share_by_type(Type, S) of
                        %% did not find the type, so return default
                        false -> Default;
                        #rewards_shares{ shares = Shares } -> Shares
                    end
            end;
        {error, _Error} ->
            %% XXX what to do here? could not resolve this gateway owner
            %%
            %% I guess we return the default - probably should blow up or skip
            %% it or something
            Default
    end.

-spec find_reward_share_by_type(Type :: reward_types(),
                                S :: [rewards_shares()]) -> false | rewards_shares().
find_reward_share_by_type(Type, S) ->
    FilterFun = fun(#rewards_shares{type=T}) ->
                        T == Type
                end,
    %% XXX: Not sure if the filter can return a list of more than one element
    %% Blow up if it does?
    case lists:filter(FilterFun, S) of
        [] -> false;
        [R] -> R
    end.

-spec put_shares(Type :: reward_types(),
                 Gateway :: libp2p_crypto:pubkey_bin(), %% NOT the owner, raw gateway
                 RewardsMD :: rewards_metadata(),
                 Ledger :: blockchain_ledger_v1:ledger(),
                 Shares :: number() ) -> NewRewardsMD :: rewards_metadata().
%% @doc Resolves gateway owner, takes an <i>absolute</i> number of shares and updates the
%% shares metadata information for the given reward type. Also updates the _total_
%% number of shares for the given reward type.
put_shares(Type, Gateway, #{ shares_acc := TotalShares} = RewardsMD, Ledger, Shares) ->
    %% update the total number of shares first
    NewTotalShares = maps:update_with(Type,
                                      fun(Current) -> Current + Shares end,
                                      Shares,
                                      TotalShares),

    NewRewardsMD = case blockchain_ledger_v1:find_gateway_owner(Gateway, Ledger) of
                       {ok, GwOwner} ->
                           maps:update_with(GwOwner,
                                            fun(#rewards_meta{ shares = S }=RMD) ->
                                                    NewS = update_rewards_shares(Type, Shares, S),
                                                    RMD#rewards_meta{ shares = NewS }
                                            end,
                                            new_rewards_meta(Type, Shares),
                                            RewardsMD);
                       {error, _Error} -> RewardsMD
                   end,
    NewRewardsMD#{ shares_acc => NewTotalShares }.

-spec new_rewards_meta( Type :: reward_types(),
                        Shares :: number() ) -> rewards_meta().
new_rewards_meta(Type, Shares) ->
    #rewards_meta{ shares = [ new_rewards_share(Type, Shares) ] }.

-spec update_rewards_shares( Type :: reward_types(),
                             Shares :: number(),
                             SharesList :: [ rewards_shares() ] ) -> [ rewards_shares() ].
update_rewards_shares(Type, Shares, SharesList) ->
    case find_reward_share_by_type(Type, SharesList) of
        false -> [ new_rewards_share(Type, Shares) ];
        #rewards_shares{ shares = Current } = Rec ->
            case find_reward_share_by_type(Type, SharesList) of
                false -> Rec;
                Rec -> Rec#rewards_shares{ shares = Current + Shares }
            end
    end.

new_rewards_share(Type, Shares) -> #rewards_shares{ type = Type, shares = Shares }.

-spec poc_challenger_reward( Txn :: blockchain_txn:txn(),
                             Acc :: rewards_metadata(),
                             Ledger :: blockchain_ledger_v1:ledger(),
                             Vars :: reward_vars() ) -> rewards_metadata().
poc_challenger_reward(Txn, Acc, Ledger, #{poc_version := Version}) ->
    Challenger = blockchain_txn_poc_receipts_v1:challenger(Txn),
    I = get_shares(poc_challengers, Challenger, Acc, Ledger, 0),
    case blockchain_txn_poc_receipts_v1:check_path_continuation(
           blockchain_txn_poc_receipts_v1:path(Txn)) of
        true when is_integer(Version) andalso Version > 4 ->
            put_shares(poc_challengers, Challenger, Acc, Ledger, I+2);
        _ ->
            put_shares(poc_challengers, Challenger, Acc, Ledger, I+1)
    end.

-spec poc_challengees_rewards_( Vars :: reward_vars(),
                                Paths :: blockchain_poc_path_element_v1:poc_path(),
                                StaticPath :: blockchain_poc_path_element_v1:poc_path(),
                                Txn :: blockchain_poc_receipts_v1:txn_poc_receipts(),
                                Chain :: blockchain:blockchain(),
                                Ledger :: blockchain_ledger_v1:ledger(),
                                IsFirst :: boolean(),
                                VarMap :: blockchain_hex:var_map(),
                                Acc0 :: rewards_metadata()) -> rewards_metadata().
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
    HIP15TxRewardUnitCap = maps:get(hip15_tx_reward_unit_cap, Vars, undefined),
    %% check if there were any legitimate witnesses
    Witnesses = legit_witnesses(Txn, Chain, Ledger, Elem, StaticPath, Version),
    Challengee = blockchain_poc_path_element_v1:challengee(Elem),
    ChallengeeLoc = case blockchain_ledger_v1:find_gateway_location(Challengee, Ledger) of
                        {ok, CLoc} ->
                            CLoc;
                        _ ->
                            undefined
                    end,
    I = get_shares(poc_challengees, Challengee, Acc0, Ledger, 0),
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
                           case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
                               {error, _} ->
                                   %% Old behavior
                                   put_shares(poc_challengees, Challengee, Acc0, Ledger, I+1);
                               {ok, ToAdd} ->
                                   TxScale = maybe_calc_tx_scale(Challengee,
                                                                 DensityTgtRes,
                                                                 ChallengeeLoc,
                                                                 VarMap,
                                                                 Ledger),
                                   put_shares(poc_challengees, Challengee, Acc0, Ledger, I+(ToAdd * TxScale))
                           end;
                       true when is_integer(Version), Version > 4, IsFirst == false ->
                           %% while we don't have a receipt for this node, we do know
                           %% there were witnesses or the path continued which means
                           %% the challengee transmitted
                           %% Additionally, we know this layer came in over radio so
                           %% there's an implicit rx as well
                           case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
                               {error, _} ->
                                   %% Old behavior
                                   put_shares(poc_challengees, Challengee, Acc0, Ledger, I+2);
                               {ok, ToAdd} ->
                                   TxScale = maybe_calc_tx_scale(Challengee,
                                                                 DensityTgtRes,
                                                                 ChallengeeLoc,
                                                                 VarMap,
                                                                 Ledger),
                                   put_shares(poc_challengees, Challengee, Acc0, Ledger, I+(ToAdd * TxScale))
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
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
                                       {error, _} ->
                                           %% Old behavior
                                           put_shares(poc_challengees, Challengee, Acc0, Ledger, I+3);
                                       {ok, ToAdd} ->
                                           TxScale = maybe_calc_tx_scale(Challengee,
                                                                         DensityTgtRes,
                                                                         ChallengeeLoc,
                                                                         VarMap,
                                                                         Ledger),
                                           put_shares(poc_challengees, Challengee, Acc0, Ledger, I+(ToAdd * TxScale))
                                   end;
                               false when is_integer(Version), Version > 4 ->
                                   %% this challengee rx'd and sent a receipt
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
                                       {error, _} ->
                                           %% Old behavior
                                           put_shares(poc_challengees, Challengee, Acc0, Ledger, I+2);
                                       {ok, ToAdd} ->
                                           TxScale = maybe_calc_tx_scale(Challengee,
                                                                         DensityTgtRes,
                                                                         ChallengeeLoc,
                                                                         VarMap,
                                                                         Ledger),
                                           put_shares(poc_challengees, Challengee, Acc0, Ledger, I+(ToAdd * TxScale))
                                   end;
                               _ ->
                                   %% Old behavior
                                   put_shares(poc_challengees, Challengee, Acc0, Ledger, I+1)
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
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
                                       {error, _} ->
                                           %% Old behavior
                                           put_shares(poc_challengees, Challengee, Acc0, Ledger, I+2);
                                       {ok, ToAdd} ->
                                           TxScale = maybe_calc_tx_scale(Challengee,
                                                                         DensityTgtRes,
                                                                         ChallengeeLoc,
                                                                         VarMap,
                                                                         Ledger),
                                           put_shares(poc_challengees, Challengee, Acc0, Ledger, I+(ToAdd * TxScale))
                                   end;
                               true ->
                                   put_shares(poc_challengees, Challengee, Acc0, Ledger, I+1)
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
            I = get_shares(poc_challengees, Challengee, Acc0, Ledger, 0),
            Acc1 = put_shares(poc_challengees, Challengee, Acc0, Ledger, I+1),
            poc_challengees_rewards_(Vars, Path, StaticPath, Txn, Chain, Ledger, false, VarMap, Acc1)
    end.

-spec poc_challengee_reward_unit(WitnessRedundancy :: undefined | pos_integer(),
                                 DecayRate :: undefined | float(),
                                 HIP15TxRewardUnitCap :: undefined | float(),
                                 Witnesses :: blockchain_poc_witness_v1:poc_witnesses()) -> {error, any()} | {ok, float()}.
poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) ->
    case {WitnessRedundancy, DecayRate} of
        {undefined, _} -> {error, witness_redundancy_undefined};
        {_, undefined} -> {error, poc_reward_decay_rate_undefined};
        {N, R} ->
            W = length(Witnesses),
            Unit = poc_reward_tx_unit(R, W, N),
            NUnit = normalize_reward_unit(HIP15TxRewardUnitCap, Unit),
            {ok, NUnit}
    end.

-spec normalize_reward_unit(HIP15TxRewardUnitCap :: undefined | float(), Unit :: float()) -> float().
normalize_reward_unit(undefined, Unit) when Unit > 1.0 -> 1.0;
normalize_reward_unit(undefined, Unit) -> Unit;
normalize_reward_unit(HIP15TxRewardUnitCap, Unit) when Unit >= HIP15TxRewardUnitCap -> HIP15TxRewardUnitCap;
normalize_reward_unit(_TxRewardUnitCap, Unit) -> Unit.

-spec normalize_reward_unit(Unit :: float()) -> float().
normalize_reward_unit(Unit) when Unit > 1.0 -> 1.0;
normalize_reward_unit(Unit) -> Unit.

-spec poc_witness_reward( Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                          AccIn :: rewards_metadata(),
                          Chain :: blockchain:blockchain(),
                          Ledger :: blockchain_ledger_v1:ledger(),
                          Vars :: reward_vars() ) -> rewards_metadata().
poc_witness_reward(Txn, AccIn,
                   Chain, Ledger,
                   #{ poc_version := POCVersion,
                      var_map := VarMap } = Vars) when is_integer(POCVersion)
                                                       andalso POCVersion >= 9 ->

    WitnessRedundancy = maps:get(witness_redundancy, Vars, undefined),
    DecayRate = maps:get(poc_reward_decay_rate, Vars, undefined),
    DensityTgtRes = maps:get(density_tgt_res, Vars, undefined),

    try
        %% Get channels without validation
        {ok, Channels} = blockchain_txn_poc_receipts_v1:get_channels(Txn, Chain),
        Path = blockchain_txn_poc_receipts_v1:path(Txn),

        %% Do the new thing for witness filtering
        lists:foldl(
                fun(Elem, Acc1) ->
                        ElemPos = blockchain_utils:index_of(Elem, Path),
                        WitnessChannel = lists:nth(ElemPos, Channels),
                        case blockchain_txn_poc_receipts_v1:valid_witnesses(Elem,
                                                                            WitnessChannel,
                                                                            Ledger) of
                            [] -> Acc1;
                            ValidWitnesses ->
                                %% We found some valid witnesses, we only apply
                                %% the witness_redundancy and decay_rate if
                                %% BOTH are set as chain variables, otherwise
                                %% we default to the old behavior and set
                                %% ToAdd=1
                                %%
                                %% If both witness_redundancy and decay_rate
                                %% are set, we calculate a scaled rx unit (the
                                %% value ToAdd)
                                %%
                                %% This is determined using the formulae
                                %% mentioned in hip15
                                ToAdd = case {WitnessRedundancy, DecayRate} of
                                            {undefined, _} -> 1;
                                            {_, undefined} -> 1;
                                            {N, R} ->
                                                W = length(ValidWitnesses),
                                                poc_witness_reward_unit(R, W, N)
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
                                           ValidWitnesses);
                                     D ->
                                         %% new (HIP17)
                                         lists:foldl(
                                           fun(WitnessRecord, Acc2) ->
                                                   Challengee = blockchain_poc_path_element_v1:challengee(Elem),
                                                   %% This must always be {ok, ...}
                                                   %% Challengee must have a location
                                                   {ok, ChallengeeLoc} =
                                                      blockchain_ledger_v1:find_gateway_location(Challengee, Ledger),
                                                   Witness =
                                                      blockchain_poc_witness_v1:gateway(WitnessRecord),
                                                   %% The witnesses get scaled by the value of their transmitters
                                                   RxScale = blockchain_utils:normalize_float(
                                                                 blockchain_hex:scale(ChallengeeLoc,
                                                                                      VarMap,
                                                                                      D,
                                                                                      Ledger)),
                                                   Value = blockchain_utils:normalize_float(ToAdd * RxScale),
                                                   I = get_shares(poc_witnesses, Witness, Acc2, Ledger, 0),
                                                   put_shares(poc_witnesses, Witness, Acc2, Ledger, I+Value)
                                           end,
                                           Acc1,
                                           ValidWitnesses)
                                  end
                        end
                end,
                AccIn,
                Path)
    catch
        What:Why:ST ->
            lager:error("failed to calculate poc_witnesses_rewards, error ~p:~p:~p", [What, Why, ST]),
            AccIn
    end;
poc_witness_reward(Txn, AccIn, _Chain, Ledger,
                   #{ poc_version := POCVersion }) when is_integer(POCVersion)
                                                        andalso POCVersion > 4 ->
    lists:foldl(
      fun(Elem, A) ->
              case blockchain_txn_poc_receipts_v1:good_quality_witnesses(Elem, Ledger) of
                  [] ->
                      A;
                  GoodQualityWitnesses ->
                      lists:foldl(
                        fun(WitnessRecord, Map) ->
                                Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                                I = get_shares(poc_witnesses, Witness, Map, Ledger, 0),
                                put_shares(poc_witnesses, Witness, Map, Ledger, I+1)
                        end,
                        A,
                        GoodQualityWitnesses)
              end
      end,
      AccIn,
      blockchain_txn_poc_receipts_v1:path(Txn)
     );
poc_witness_reward(Txn, AccIn, _Chain, Ledger, _Vars) ->
    lists:foldl(
      fun(Elem, A) ->
              lists:foldl(
                fun(WitnessRecord, Map) ->
                        Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                        I = get_shares(poc_witnesses, Witness, Map, Ledger, 0),
                        put_shares(poc_witnesses, Witness, Map, Ledger, I+1)

                end,
                A,
                blockchain_poc_path_element_v1:witnesses(Elem))
      end,
      AccIn,
      blockchain_txn_poc_receipts_v1:path(Txn)).

-spec collect_dc_rewards_from_previous_epoch_grace(
        Start :: non_neg_integer(),
        End :: non_neg_integer(),
        Chain :: blockchain:blockchain(),
        Vars :: reward_vars(),
        Ledger :: blockchain_ledger_v1:ledger()) -> {ok, rewards_metadata()} | {error, any()}.
collect_dc_rewards_from_previous_epoch_grace(Start, End, Chain,
                                             #{sc_grace_blocks := Grace,
                                               reward_version := RV} = Vars,
                                             Ledger) when RV > 4 ->
    AccIn = #{ shares_acc => #{ data_credits => #dc_shares{} } },
    scan_grace_block(max(1, Start - Grace), Start, End,
                     Vars, Chain, Ledger, AccIn);
collect_dc_rewards_from_previous_epoch_grace(_Start, _End, _Chain, _Vars, _Ledger) -> {ok, #{}}.

-spec scan_grace_block( Current :: pos_integer(),
                        Start :: pos_integer(),
                        End :: pos_integer(),
                        Vars :: reward_vars(),
                        Chain :: blockchain:blockchain(),
                        Ledger :: blockchain_ledger_v1:ledger(),
                        Acc :: rewards_metadata() ) ->
    {ok, Meta :: rewards_metadata()} | {error, term()}.
scan_grace_block(Current, Start, _End, _Vars, _Chain, _Ledger, Acc)
                                           when Current == Start + 1 -> {ok, Acc};
scan_grace_block(Current, Start, End, Vars, Chain, Ledger, AccIn) ->
    case blockchain:get_block(Current, Chain) of
        {error, _Error} = Err ->
            lager:error("failed to get grace block ~p ~p", [_Error, Current]),
            Err;
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            NewAcc = lists:foldl(fun(T, A) ->
                                         case blockchain_txn:type(T) of
                                             blockchain_txn_state_channel_close_v1 ->
                                                 dc_reward(T, End, A, Ledger, Vars);
                                             _ -> A
                                         end
                                 end,
                                 AccIn,
                                 Txns),
            scan_grace_block(Current+1, Start, End, Vars, Chain, Ledger, NewAcc)
    end.

-spec dc_reward( Txn :: blockchain_txn_state_channel_close_v1:txn_state_channel_close(),
                 End :: pos_integer(),
                 AccIn :: rewards_metadata(),
                 Ledger :: blockchain_ledger_v1:ledger(),
                 Vars :: reward_vars() ) -> rewards_metadata().
dc_reward(Txn, End,
          #{ shares_acc := #{ data_credits := DCShares } } = AccIn,
          Ledger, #{ sc_grace_blocks := GraceBlocks,
                     sc_version := 2} = Vars) ->
    SeenList = DCShares#dc_shares.seen,
    case blockchain_txn_state_channel_close_v1:state_channel_expire_at(Txn) + GraceBlocks < End of
        true ->
            SCID = blockchain_txn_state_channel_close_v1:state_channel_id(Txn),
            case lists:member(SCID, SeenList) of
                false ->
                    %% haven't seen this state channel yet, pull the final result from the ledger
                    case blockchain_ledger_v1:find_state_channel(
                           blockchain_txn_state_channel_close_v1:state_channel_id(Txn),
                           blockchain_txn_state_channel_close_v1:state_channel_owner(Txn),
                           Ledger) of
                        {ok, SC} ->
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
                                                    %% the owner of the state channel
                                                    %% did a naughty thing, divide
                                                    %% their overcommit between the
                                                    %% participants
                                                    OverCommit = blockchain_ledger_state_channel_v2:amount(SC)
                                                        - blockchain_ledger_state_channel_v2:original(SC),
                                                    OverCommit div length(Summaries);
                                                _ ->
                                                    0
                                            end,

                                    lists:foldl(fun(Summary, A) ->
                                                        Key = blockchain_state_channel_summary_v1:client_pubkeybin(Summary),
                                                        DCs = blockchain_state_channel_summary_v1:num_dcs(Summary) + Bonus,
                                                        case blockchain_ledger_v1:find_gateway_owner(Key, Ledger) of
                                                            {ok, GWOwner} ->
                                                                update_shares(data_credits,
                                                                              DCs,
                                                                              GWOwner,
                                                                              update_total_shares(
                                                                                data_credits,
                                                                                DCs, A)
                                                                             );
                                                            {error, _Error} ->
                                                                %% we are updating the
                                                                %% total number of DCs to
                                                                %% preserve previous behavior.
                                                                %%
                                                                %% in the old code path
                                                                %% we collected _all_ DCs
                                                                %% even if later we could not
                                                                %% resolve an owner for their
                                                                %% gateway.
                                                                update_total_shares(
                                                                  data_credits,
                                                                  DCs, A)
                                                        end
                                                end,
                                                update_seen(SCID, AccIn),
                                                Summaries);
                                false ->
                                    %% this is a v1 SC; ignore
                                    AccIn
                            end;
                        {error, not_found} ->
                            ExpireAt = blockchain_txn_state_channel_close_v1:state_channel_expire_at(Txn),
                            lager:warning("missing scid ~p", [SCID]),
                            lager:warning("expire ~p + grace ~p > end ~p?", [ExpireAt, GraceBlocks, End]),
                            AccIn
                    end;
                true ->
                    %% we have already seen this SCID before; ignore
                    AccIn
            end;
        false ->
            %% SC did not close in _this_ epoch; skip it
            AccIn
    end.

-spec update_hnt(RewardsMeta :: rewards_meta(),
                 Amount :: number()) -> rewards_meta().
update_hnt(#rewards_meta{hnt_amount = Current} = RMeta, Amount) ->
    RMeta#rewards_meta{hnt_amount = Current + Amount}.

-spec update_hnt(RewardsMD :: rewards_metadata(),
                 Owner :: libp2p_crypto:pubkey_bin(),
                 Amount :: number() ) -> rewards_metadata().
update_hnt(RewardsMD, Owner, Amount) ->
    maps:update_with(Owner,
                     fun(RMD) -> update_hnt(Amount, RMD) end,
                     #rewards_meta{hnt_amount = Amount},
                     RewardsMD).

-spec update_seen(SCID :: blockchain_state_channel_v1:id(),
                  RewardsMD :: rewards_metadata() ) -> NewRewardsMD :: rewards_metadata().
%% @doc Given rewards_meta, update the seen state channel id field so we don't tabulate
%% a state channel we've seen before.
update_seen(Id, #{ shares_acc := Shares } = RewardsMD) ->
    NewShares = maps:update_with(data_credits,
                                 fun(#dc_shares{ seen = S } = DCShares) ->
                                         DCShares#dc_shares{ seen = [ Id | S ] }
                                 end,
                                 #dc_shares{ seen = [ Id ] },
                                 Shares),
    RewardsMD#{ shares_acc => NewShares }.

-spec update_total_shares( Type :: reward_types(),
                           Amount :: number(),
                           RewardsMD :: rewards_metadata() ) -> NewRewardsMD :: rewards_metadata().
update_total_shares(data_credits, Amount, #{ shares_acc := Shares } = RewardsMD) ->
    NewShares = maps:update_with(data_credits,
                                 fun(#dc_shares{total = T} = DCShares) ->
                                         DCShares#dc_shares{total = T + Amount}
                                 end,
                                 #dc_shares{total = Amount},
                                 Shares),
    RewardsMD#{ shares_acc => NewShares };
update_total_shares(Type, Amount, #{ shares_acc := Shares } = RewardsMD) ->
    NewShares = maps:update_with(Type,
                                 fun(Current) -> Current + Amount end,
                                 Amount, Shares),
    RewardsMD#{shares_acc => NewShares}.

-spec update_shares( Type :: reward_types(),
                     Shares :: number(),
                     Key :: libp2p_crypto:pubkey_bin(), %% should be a gateway owner
                     Rewards :: rewards_metadata() ) -> NewRewards :: rewards_metadata().
update_shares(Type, Shares, Key, RewardsMeta) ->
    maps:update_with(Key, fun(#rewards_meta{} = MD) ->
                                  update_shares_list(Type, Shares, MD)
                          end,
                     #rewards_meta{shares = [new_rewards_shares(Type, Shares)]},
                     RewardsMeta).

-spec update_shares_list( Type :: reward_types(),
                          Amount :: number(),
                          MDRecord :: rewards_meta() ) -> NewMDRecord :: rewards_meta().
update_shares_list(Type, Amount, #rewards_meta{shares = SharesList} = MDRecord) ->
    NewSharesList = case lists:keyfind(Type, #rewards_shares.type, SharesList) of
                        false ->
                            %% no record of this type exists, add it to our list
                            [ #rewards_shares{type = Type, shares = Amount} | SharesList ];
                        #rewards_shares{shares = S} = Rec ->
                            %% update existing record with new amount and put it back into our list
                            case find_reward_share_by_type(Type, SharesList) of
                                false -> Rec;
                                Rec -> Rec#rewards_shares{ shares = S + Amount }
                            end
                    end,
    MDRecord#rewards_meta{shares = NewSharesList}.

-spec new_rewards_shares( Type :: reward_types(),
                          Amount :: number() ) -> RewardsShares :: rewards_shares().
new_rewards_shares(Type, Amount) -> #rewards_shares{type = Type, shares = Amount}.

get_owner_address(validator, Addr, Ledger) ->
    case blockchain_ledger_v1:get_validator(Addr, Ledger) of
        {error, _} = Err -> Err;
        {ok, Val} ->
            Owner = blockchain_ledger_validator_v1:owner_address(Val),
            {ok, Owner}
    end;
get_owner_address(gateway, Addr, Ledger) ->
    blockchain_ledger_v1:find_gateway_owner(Addr, Ledger).

-spec calculate_dc_remainder(rewards_metadata(), reward_vars()) -> {number(), number()}.
calculate_dc_remainder(#{ shares_acc := #{ data_credits := #dc_shares{ total = TotalDCs } } },
                          #{epoch_reward := EpochReward,
                            dc_percent := DCPercent}=Vars) ->
    OraclePrice = maps:get(oracle_price, Vars, 0),
    RewardVersion = maps:get(reward_version, Vars, 1),
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

    {max(0, round(MaxDCReward - DCReward)), DCReward}.

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
    %% It's okay to call the previously broken normalize_reward_unit here because
    %% the value does not asympotically tend to 2.0, instead it tends to 0.0
    normalize_reward_unit(blockchain_utils:normalize_float((N - (1 - math:pow(R, (W - N))))/W)).

-spec legit_witnesses( Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                       Chain :: blockchain:blockchain(),
                       Ledger :: blockchain_ledger_v1:ledger(),
                       Elem :: blockchain_poc_path_element_v1:poc_element(),
                       StaticPath :: blockchain_poc_path_element_v1:poc_path(),
                       Version :: pos_integer()
                     ) -> [blockchain_txn_poc_witnesses_v1:poc_witness()].
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
    erlang:round(DCRemainder
                 * ((maps:get(Key, Vars) /
                     (maps:get(poc_challengers_percent, Vars)
                      + maps:get(poc_challengees_percent, Vars)
                      + maps:get(poc_witnesses_percent, Vars))))
                ).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% @doc Given a list of reward_v1 txns, return the equivalent reward_v2
%% list.
-spec v1_to_v2( RewardsV1 :: [blockchain_txn_reward_v1:rewards()] ) -> rewards().
v1_to_v2(RewardsV1) ->
    R = lists:foldl(fun(R, Acc) ->
                            Owner = blockchain_txn_reward_v1:account(R),
                            Amt = blockchain_txn_reward_v1:amount(R),
                            maps:update_with(Owner, fun(Balance) -> Balance + Amt end, Amt, Acc)
                    end,
                    #{},
                    RewardsV1),
    lists:sort(maps:fold(fun(_O, 0, Acc) -> Acc; %% drop any 0 amount reward, as in v2
                             (O, A, Acc) -> [ new_reward(O, A) | Acc ] end,
                         [],
                         R)).

new_test() ->
    Tx = #blockchain_txn_rewards_v2_pb{start_epoch=1, end_epoch=30, rewards=[]},
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
    ChallengerShares = lists:foldl(fun(T, Acc) -> poc_challenger_reward(T, Acc, Vars) end, #{}, Txns),
    ?assertEqual(Rewards, normalize_challenger_rewards(ChallengerShares, Vars)).

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
    ChallengeeShares = lists:foldl(fun(T, Acc) ->
                                           Path = blockchain_txn_poc_receipts_v1:path(T),
                                           poc_challengees_rewards_(Vars, Path, Path, T, Chain, Ledger, true, #{}, Acc)
                                   end,
                                   #{},
                                   Txns),
    ?assertEqual(Rewards, normalize_challengee_rewards(ChallengeeShares, Vars)),
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

    LedgerVars = maps:merge(common_poc_vars(), EpochVars),

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

    WitnessShares = lists:foldl(fun(T, Acc) -> poc_witness_reward(T, Acc, Chain, Ledger, EpochVars) end,
                                #{}, Txns),
    ?assertEqual(Rewards, normalize_witness_rewards(WitnessShares, EpochVars)),
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


    SCClose = blockchain_txn_state_channel_close_v1:new(SC, <<"owner">>),
    {ok, DCsInEpochAsHNT} = blockchain_ledger_v1:dc_to_hnt(3, 100000000), %% 3 DCs burned at HNT price of 1 dollar
    %% NOTE: Rewards are split 33-66%
    Rewards = #{
        {gateway, data_credits, <<"a">>} => round(DCsInEpochAsHNT * (1/3)),
        {gateway, data_credits, <<"b">>} => round(DCsInEpochAsHNT * (2/3))
    },
    DCShares = dc_reward(SCClose, 100, #{}, Ledger1, Vars),
    ?assertEqual({26999, Rewards}, normalize_dc_rewards(DCShares, Vars)),
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
        var_map => undefined,
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

    RewardSharesInit = #{
      dc_rewards => #{},
      poc_challenger => #{},
      poc_challengee => #{},
      poc_witness => #{}
     },

    NoSpillover = lists:foldl(fun(T, Acc) -> calculate_reward_for_txn(
                                               blockchain_txn:type(T), T, 100,
                                               Acc, Chain, Ledger, Vars)
                                    end,
                                    RewardSharesInit,
                                    AllTxns),
    DCShares = maps:get(dc_rewards, NoSpillover),
    {DCRemainder, DCRewards} = normalize_dc_rewards(DCShares, Vars),
    DCAward = trunc(maps:get(epoch_reward, Vars) * maps:get(dc_percent, Vars)),
    ?assertEqual({DCAward - DCsInEpochAsHNT, Rewards}, {DCRemainder, DCRewards}),

    NewVars = maps:put(dc_remainder, DCRemainder, Vars),

    Spillover = lists:foldl(fun(T, Acc) -> calculate_reward_for_txn(
                                               blockchain_txn:type(T), T, 100,
                                               Acc, Chain, Ledger, NewVars)
                                    end,
                                    RewardSharesInit,
                                    AllTxns),

    ChallengerShares = maps:get(poc_challenger, NoSpillover),
    ChallengeeShares = maps:get(poc_challengee, NoSpillover),
    WitnessShares = maps:get(poc_witness, NoSpillover),

    ChallengerRewards = normalize_challenger_rewards(ChallengerShares, Vars),
    ChallengeeRewards = normalize_challengee_rewards(ChallengeeShares, Vars),
    WitnessRewards = normalize_witness_rewards(WitnessShares, Vars),

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
    SpilloverChallengerShares = maps:get(poc_challenger, Spillover),
    SpilloverChallengeeShares = maps:get(poc_challengee, Spillover),
    SpilloverWitnessShares = maps:get(poc_witness, Spillover),

    SpilloverChallengerRewards = normalize_challenger_rewards(SpilloverChallengerShares, NewVars),
    SpilloverChallengeeRewards = normalize_challengee_rewards(SpilloverChallengeeShares, NewVars),
    SpilloverWitnessRewards = normalize_witness_rewards(SpilloverWitnessShares, NewVars),

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
    Tx = #blockchain_txn_rewards_v2_pb{start_epoch=1, end_epoch=30, rewards=[]},
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, start_epoch, end_epoch, rewards])).


fixed_normalize_reward_unit_test() ->
    Rewards = [0.1, 0.9, 1.0, 1.8, 2.5, 2000, 0],

    %% Expectation: reward should get capped at 2.0
    Correct = lists:foldl(
                fun(Reward, Acc) ->
                        %% Set cap=2.0
                        maps:put(Reward, normalize_reward_unit(2.0, Reward), Acc)
                end, #{}, Rewards),

    Incorrect = lists:foldl(
                  fun(Reward, Acc) ->
                          %% Set cap=undefined (old behavior)
                          maps:put(Reward, normalize_reward_unit(undefined, Reward), Acc)
                  end, #{}, Rewards),

    ?assertEqual(1.8, maps:get(1.8, Correct)),          %% 1.8 -> 1.8
    ?assertEqual(0.1, maps:get(0.1, Correct)),          %% 0.1 -> 0.1
    ?assertEqual(0.9, maps:get(0.9, Correct)),          %% 0.9 -> 0.9
    ?assertEqual(2.0, maps:get(2.5, Correct)),          %% 2.5 -> 2.0
    ?assertEqual(1.0, maps:get(2.5, Incorrect)),        %% 2.5 -> 1.0 (incorrect)
    ?assertEqual(1.0, maps:get(1.8, Incorrect)),        %% 1.8 -> 1.0 (incorrect)
    ?assertEqual(0.1, maps:get(0.1, Incorrect)),        %% 0.1 -> 0.1
    ?assertEqual(0.9, maps:get(0.9, Incorrect)),        %% 0.9 -> 0.9

    ok.

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
