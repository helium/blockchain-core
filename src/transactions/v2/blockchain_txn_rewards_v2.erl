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
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    calculate_rewards/3,
    calculate_rewards_metadata/3,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([v1_to_v2/1]).
-endif.

-define(T, #blockchain_txn_rewards_v2_pb).

-type t() :: txn_rewards_v2().
-type txn_rewards_v2() :: ?T{}.
-type reward_v2() :: #blockchain_txn_reward_v2_pb{}.
-type rewards() :: [reward_v2()].
-type reward_vars() :: map().
-type rewards_share_map() :: #{ libp2p_crypto:pubkey_bin() => non_neg_integer() }.
-type dc_rewards_share_map() :: #{ libp2p_crypto:pubkey_bin() => non_neg_integer(),
                                   seen => [ blockchain_state_channel_v1:id() ] }.
-type rewards_share_metadata() :: #{ poc_challenger => rewards_share_map(),
                                     poc_challengee => rewards_share_map(),
                                     poc_witness => rewards_share_map(),
                                     dc_rewards => dc_rewards_share_map(),
                                     overages => non_neg_integer() }.
-type owner_key() :: {owner, securities, libp2p_crypto:pubkey_bin()}.
-type reward_types() :: poc_challengers | poc_challengees | poc_witnesses | data_credits | consensus.
-type gateway_key() :: {gateway, reward_types(), libp2p_crypto:pubkey_bin()}.
-type rewards_map() :: #{ owner_key() | gateway_key() => number() }.
-type rewards_metadata() :: #{ poc_challenger => rewards_map(),
                               poc_challengee => rewards_map(),
                               poc_witness => rewards_map(),
                               dc_rewards => rewards_map(),
                               consensus_rewards => rewards_map(),
                               securities_rewards => rewards_map(),
                               overages => non_neg_integer() }.

-export_type([t/0, txn_rewards_v2/0, rewards_metadata/0]).

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

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

-spec absorb(txn_rewards_v2(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),

    case blockchain:config(?net_emissions_enabled, Ledger) of
        {ok, true} ->
            %% initial proposed max 34.24
            {ok, Max} = blockchain:config(?net_emissions_max_rate, Ledger),
            {ok, Burned} = blockchain_ledger_v1:hnt_burned(Ledger),
            {ok, Overage} = blockchain_ledger_v1:net_overage(Ledger),

            %% clear this since we have it already
            ok = blockchain_ledger_v1:clear_hnt_burned(Ledger),

            case Burned > Max of
                %% if burned > max, then add (burned - max) to overage
                true ->
                    Overage1 = Overage + (Burned - Max),
                    ok = blockchain_ledger_v1:net_overage(Overage1, Ledger);
                %% else we may have pulled from overage to the tune of
                %% max - burned
                 _ ->
                    %% here we pulled from overage up to max
                    case (Max - Burned) < Overage  of
                        %% emitted max, pulled from overage
                        true ->
                            Overage1 = Overage - (Max - Burned),
                            ok = blockchain_ledger_v1:net_overage(Overage1, Ledger);
                        %% not enough overage to emit up to max, 0 overage
                        _ ->
                            ok = blockchain_ledger_v1:net_overage(0, Ledger)
                    end
            end;
        _ ->
            ok
    end,

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
    case calculate_rewards_(Start, End, AuxLedger, Chain, true) of
        {error, _}=E -> E;
        {ok, AuxRewards, AuxMD} ->
            TxnRewards = rewards(Txn),
            %% absorb the rewards attached to the txn (real)
            absorb_rewards(TxnRewards, AuxLedger),
            %% set auxiliary rewards in the aux ledger also
            lager:info("are aux rewards equal?: ~p", [lists:sort(TxnRewards) == lists:sort(AuxRewards)]),
            %% rewards appear in (End + 1) block
            blockchain_aux_ledger_v1:set_rewards(End + 1, TxnRewards, AuxRewards, AuxLedger),
            case calculate_rewards_(Start, End, blockchain_ledger_v1:mode(active, AuxLedger), Chain, true) of
                {error, _}=E -> E;
                {ok, _, OrigMD} ->
                    blockchain_aux_ledger_v1:set_rewards_md(End + 1, OrigMD, AuxMD, AuxLedger)
            end
    end.


-spec calculate_rewards(non_neg_integer(), non_neg_integer(), blockchain:blockchain()) ->
    {ok, rewards()} | {error, any()}.
%% @doc Calculate and return an ordered list (as ordered by lists:sort/1) of
%% rewards for use in a rewards_v2 transaction. Given how lists:sort/1 works,
%% ordering will depend on (binary) account information.
calculate_rewards(Start, End, Chain) ->
    {ok, Ledger} = blockchain:ledger_at(End, Chain),
    calculate_rewards_(Start, End, Ledger, Chain, false).

-spec calculate_rewards_(
        Start :: non_neg_integer(),
        End :: non_neg_integer(),
        Ledger :: blockchain_ledger_v1:ledger(),
        Chain :: blockchain:blockchain(),
        ReturnMD :: boolean()
       ) -> {error, any()} | {ok, rewards()} | {ok, rewards(), rewards_metadata()}.
calculate_rewards_(Start, End, Ledger, Chain, ReturnMD) ->
    {ok, Results} = calculate_rewards_metadata(Start, End, blockchain:ledger(Ledger, Chain)),
    try
        case ReturnMD of
            false ->
                {ok, prepare_rewards_v2_txns(Results, Ledger)};
            true ->
                {ok, prepare_rewards_v2_txns(Results, Ledger), Results}
        end
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
%%
%% Keys that exist in the rewards metadata map include:
%% <ul>
%%    <li>poc_challenger</li>
%%    <li>poc_witness</li>
%%    <li>poc_challengee</li>
%%    <li>dc_rewards</li>
%%    <li>consensus_rewards</li>
%%    <li>securities_rewards</li>
%% </ul>
%%
%% Each of the keys is itself a map which has the shape of `#{ Entry => Amount }'
%% where Entry is defined as a tuple of `{gateway, reward_type, Gateway}' or
%% `{owner, reward_type, Owner}'
%%
%% There is an additional key `overages' which may or may not have an integer
%% value. It represents the amount of excess fees paid in the given epoch which
%% were used as bonus HNT rewards for the consensus members.
%% @end
calculate_rewards_metadata(Start, End, Chain) ->
    {ok, Ledger} = blockchain:ledger_at(End, Chain),
    Vars0 = get_reward_vars(Start, End, Ledger),
    VarMap = case blockchain_hex:var_map(Ledger) of
                 {error, _Reason} -> #{};
                 {ok, VM} -> VM
             end,

    RegionVars = blockchain_region_v1:get_all_region_bins(Ledger),

    Vars = Vars0#{ var_map => VarMap, region_vars => RegionVars},

    %% Previously, if a state_channel closed in the grace blocks before an
    %% epoch ended, then it wouldn't ever get rewarded.
    {ok, PreviousGraceBlockDCRewards} = collect_dc_rewards_from_previous_epoch_grace(Start, End,
                                                                                     Chain, Vars,
                                                                                     Ledger),

    %% Initialize our reward accumulator. We are going to build up a map which
    %% will be in the shape of
    %% #{ reward_type => #{ Entry => Amount } }
    %%
    %% where Entry is of the the shape
    %% {owner, reward_type, Owner} or
    %% {gateway, reward_type, Gateway}
    AccInit = #{ dc_rewards => PreviousGraceBlockDCRewards,
                 poc_challenger => #{},
                 poc_challengee => #{},
                 poc_witness => #{} },

    try
        %% We only want to fold over the blocks and transaction in an epoch once,
        %% so we will do that top level work here. If we get a thrown error while
        %% we are folding, we will abort reward calculation.
        PerfTab = ets:new(rwd_perf, [named_table]),
        Results0 = fold_blocks_for_rewards(Start, End, Chain,
                                           Vars, Ledger, AccInit),

        %% Prior to HIP 28 (reward_version <6), force EpochReward amount for the CG to always
        %% be around ElectionInterval (30 blocks) so that there is less incentive
        %% to stay in the consensus group. With HIP 28, relax that to be up to election_interval +
        %% election_retry_interval to allow for time for election to complete.
        ConsensusEpochReward =
            case maps:get(reward_version, Vars) of
               RewardVersion when RewardVersion >= 6 ->
                    calculate_consensus_epoch_reward(Start, End, Vars, Ledger);
                _ ->
                    calculate_epoch_reward(1, Start, End, Ledger)
            end,

        Vars1 = Vars#{ consensus_epoch_reward => ConsensusEpochReward },

        Results = finalize_reward_calculations(Results0, Ledger, Vars1),
        %% we are only keeping hex density calculations memoized for a single
        %% rewards transaction calculation, then we discard that work and avoid
        %% cache invalidation issues.
        case application:get_env(blockchain, destroy_memo, true) of
            true ->
                true = blockchain_hex:destroy_memoization();
            _ -> ok
        end,
        perf_report(PerfTab),
        ets:delete(PerfTab),
        {ok, Results}
    catch
        C:Error:Stack ->
            lager:error("Caught ~p; couldn't calculate rewards metadata because: ~p~n~p", [C, Error, Stack]),
            Error
    end.

perf(Tag, Time) ->
    catch ets:update_counter(rwd_perf, Tag, Time, {Tag, Time}).

perf_report(Tab) ->
    case application:get_env(blockchain, print_rewards_perf, false) of
        true ->
            Measurements = lists:reverse(lists:keysort(2, ets:tab2list(Tab))),
            lager:info("perf report:"),
            lists:foreach(
              fun({K, V}) ->
                      lager:info("txn ~p: ~pms", [K, V])
              end,
              Measurements);
        false ->
            ok
    end.

-spec print(txn_rewards_v2()) -> iodata().
print(undefined) -> <<"type=rewards_v2 undefined">>;
print(#blockchain_txn_rewards_v2_pb{start_epoch=Start,
                                    end_epoch=End}) ->
    io_lib:format("type=rewards_v2 start_epoch=~p end_epoch=~p",
                  [Start, End]).

json_type() ->
    <<"rewards_v2">>.

-spec to_json(txn_rewards_v2(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, Opts) ->
    RewardToJson =
        fun
            ({gateway, Type, G}, Amount, Ledger, Acc) ->
                case blockchain_ledger_v1:find_gateway_owner(G, Ledger) of
                    {error, _Error} ->
                        Acc;
                    {ok, GwOwner} ->
                        [#{account => ?BIN_TO_B58(GwOwner),
                           gateway => ?BIN_TO_B58(G),
                           amount => Amount,
                           type => Type} | Acc]
                end;
            ({validator, Type, V}, Amount, Ledger, Acc) ->
                case blockchain_ledger_v1:get_validator(V, Ledger) of
                    {error, _Error} ->
                        Acc;
                    {ok, Val} ->
                        Owner = blockchain_ledger_validator_v1:owner_address(Val),
                        [#{account => ?BIN_TO_B58(Owner),
                           gateway => ?BIN_TO_B58(V),
                           amount => Amount,
                           type => Type} | Acc]
                end;
            ({owner, Type, O}, Amount, _Ledger, Acc) ->
                [#{account => ?BIN_TO_B58(O),
                   gateway => undefined,
                   amount => Amount,
                   type => Type} | Acc]
        end,
    Rewards = case lists:keyfind(chain, 1, Opts) of
        {chain, Chain} ->
            Start = blockchain_txn_rewards_v2:start_epoch(Txn),
            End = ?MODULE:end_epoch(Txn),
            {ok, Ledger} = blockchain:ledger_at(End, Chain),
            {ok, Metadata} = case lists:keyfind(rewards_metadata, 1, Opts) of
                                {rewards_metadata, M} -> {ok, M};
                                _ -> ?MODULE:calculate_rewards_metadata(Start, End, Chain)
                            end,
            maps:fold(
                fun(overages, Amount, Acc) ->
                        [#{amount => Amount,
                           type => overages} | Acc];
                   (_RewardCategory, Rewards, Acc0) ->
                        maps:fold(
                        fun(Entry, Amount, Acc) ->
                            RewardToJson(Entry, Amount, Ledger, Acc)
                        end, Acc0, Rewards)
                end, [], Metadata);
        _ -> [ reward_to_json(R, []) || R <- rewards(Txn)]
    end,

    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      start_epoch => start_epoch(Txn),
      end_epoch => end_epoch(Txn),
      rewards => Rewards
    }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------


-spec reward_to_json( Reward :: reward_v2(),
                      Opts :: blockchain_json:opts() ) -> blockchain_json:json_object().
reward_to_json(#blockchain_txn_reward_v2_pb{account = Account, amount = Amt}, _Opts) ->
    #{
      type => <<"reward_v2">>,
      account => ?BIN_TO_B58(Account),
      amount => Amt
     }.

-spec new_reward( Account :: libp2p_crypto:pubkey_bin(),
                  Amount :: non_neg_integer() ) -> reward_v2().
new_reward(Account, Amount) ->
    #blockchain_txn_reward_v2_pb{account=Account, amount=Amount}.

-spec fold_blocks_for_rewards( Current :: pos_integer(),
                               End :: pos_integer(),
                               Chain :: blockchain:blockchain(),
                               Vars :: reward_vars(),
                               Ledger :: blockchain_ledger_v1:ledger(),
                               Acc :: rewards_share_metadata() ) -> rewards_share_metadata().
fold_blocks_for_rewards(Current, End, _Chain, _Vars, _Ledger, Acc) when Current == End + 1 -> Acc;
fold_blocks_for_rewards(910360, End, Chain, Vars, Ledger, Acc) ->
    fold_blocks_for_rewards(910361, End, Chain, Vars, Ledger, Acc);
fold_blocks_for_rewards(Current, End, Chain, Vars, Ledger, Acc) ->
    case blockchain:get_block(Current, Chain) of
        {error, _Reason} = Error -> throw(Error);
        {ok, Block} ->
            Txns = blockchain_block:transactions(Block),
            NewAcc = lists:foldl(fun(T, A) ->
                                         Type = blockchain_txn:type(T),
                                         Start = erlang:monotonic_time(microsecond),
                                         A1 = calculate_reward_for_txn(Type, T, End,
                                                                       A, Chain, Ledger, Vars),
                                         perf(Type, erlang:monotonic_time(microsecond) - Start),
                                         A1
                                 end,
                                 Acc, Txns),
            fold_blocks_for_rewards(Current+1, End, Chain, Vars, Ledger, NewAcc)
    end.

-spec calculate_reward_for_txn( Type :: atom(),
                                Txn :: blockchain_txn:txn(),
                                End :: pos_integer(),
                                Acc :: rewards_share_metadata(),
                                Chain :: blockchain:blockchain(),
                                Ledger :: blockchain_ledger_v1:ledger(),
                                Vars :: reward_vars() ) -> rewards_share_metadata().
calculate_reward_for_txn(?MODULE, _Txn, _End, _Acc, _Chain,
                         _Ledger, _Vars) -> throw({error, already_existing_rewards_v2});
calculate_reward_for_txn(blockchain_txn_rewards_v1, _Txn, _End, _Acc, _Chain,
                         _Ledger, _Vars) -> throw({error, already_existing_rewards_v1});
calculate_reward_for_txn(blockchain_txn_poc_receipts_v1 = T, Txn, _End,
                         #{ poc_challenger := Challenger } = Acc, Chain, Ledger, Vars) ->
    Start = erlang:monotonic_time(microsecond),
    Acc0 = poc_challenger_reward(Txn, Challenger, Vars),
    Start1 = erlang:monotonic_time(microsecond),
    perf({T, challenger}, Start1 - Start),
    Acc1 = calculate_poc_challengee_rewards(Txn, Acc#{ poc_challenger => Acc0 }, Chain, Ledger, Vars),
    Start2 = erlang:monotonic_time(microsecond),
    perf({T, challengee}, Start2 - Start1),
    Acc2 = calculate_poc_witness_rewards(Txn, Acc1, Chain, Ledger, Vars),
    WitnessTime = erlang:monotonic_time(microsecond) - Start2,
    perf({T, witnesses}, WitnessTime),
    Acc2;
calculate_reward_for_txn(blockchain_txn_state_channel_close_v1, Txn, End, Acc, Chain, Ledger, Vars) ->
    calculate_dc_rewards(Txn, End, Acc, Chain, Ledger, Vars);
calculate_reward_for_txn(Type, Txn, _End, Acc, _Chain, Ledger, _Vars) ->
    consider_overage(Type, Txn, Acc, Ledger).

-spec consider_overage( Type :: atom(),
                        Txn :: blockchain_txn:txn(),
                        Acc :: rewards_share_metadata(),
                        Ledger :: blockchain_ledger_v1:ledger() ) -> rewards_share_metadata().
consider_overage(Type, Txn, Acc, Ledger) ->
    %% calculate any fee paid in excess which we will distribute as bonus HNT
    %% to consensus members
    try
        Type:calculate_fee(Txn, Ledger) - Type:fee(Txn) of
        Overage when Overage > 0 ->
            maps:update_with(overages, fun(Overages) -> Overages + Overage end, Overage, Acc);
        _ ->
            Acc
    catch
        _:_ ->
            Acc
    end.

-spec calculate_poc_challengee_rewards( Txn :: blockchain_txn:txn(),
                                        Acc :: rewards_share_metadata(),
                                        Chain :: blockchain:blockchain(),
                                        Ledger :: blockchain_ledger_v1:ledger(),
                                        Vars :: reward_vars() ) -> rewards_share_metadata().
calculate_poc_challengee_rewards(Txn, #{ poc_challengee := ChallengeeMap } = Acc,
                                 Chain, Ledger, #{ var_map := VarMap } = Vars) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    NewCM = poc_challengees_rewards_(Vars, Path, Path, Txn, Chain, Ledger, true, VarMap, ChallengeeMap),
    Acc#{ poc_challengee => NewCM }.

-spec calculate_poc_witness_rewards( Txn :: blockchain_txn:txn(),
                                     Acc :: rewards_share_metadata(),
                                     Chain :: blockchain:blockchain(),
                                     Ledger :: blockchain_ledger_v1:ledger(),
                                     Vars :: reward_vars() ) -> rewards_share_metadata().
calculate_poc_witness_rewards(Txn, #{ poc_witness := WitnessMap } = Acc, Chain, Ledger, Vars) ->
    NewWM = poc_witness_reward(Txn, WitnessMap, Chain, Ledger, Vars),
    Acc#{ poc_witness => NewWM }.

-spec calculate_dc_rewards( Txn :: blockchain_txn:txn(),
                            End :: pos_integer(),
                            Acc :: rewards_share_metadata(),
                            Chain :: blockchain:blockchain(),
                            Ledger :: blockchain_ledger_v1:ledger(),
                            Vars :: reward_vars() ) -> rewards_share_metadata().
calculate_dc_rewards(Txn, End, #{ dc_rewards := DCRewardMap } = Acc, _Chain, Ledger, Vars) ->
    NewDCM = dc_reward(Txn, End, DCRewardMap, Ledger, Vars),
    Acc#{ dc_rewards => NewDCM }.

-spec finalize_reward_calculations( AccIn :: rewards_share_metadata(),
                                    Ledger :: blockchain_ledger_v1:ledger(),
                                    Vars :: reward_vars() ) -> rewards_metadata().
finalize_reward_calculations(#{ dc_rewards := DCShares,
                                poc_witness := WitnessShares,
                                poc_challenger := ChallengerShares,
                                poc_challengee := ChallengeeShares } = AccIn, Ledger, Vars) ->
    SecuritiesRewards = securities_rewards(Ledger, Vars),
    Overages = maps:get(overages, AccIn, 0),
    ConsensusRewards = consensus_members_rewards(Ledger, Vars, Overages),
    {DCRemainder, DCRewards} = normalize_dc_rewards(DCShares, Vars),
    Vars0 = maps:put(dc_remainder, DCRemainder, Vars),

    %% apply the DC remainder, if any to the other PoC categories pro rata
    %%
    %% these normalize functions take reward "shares" and convert them
    %% into HNT payouts

    #{ poc_witness => normalize_witness_rewards(WitnessShares, Vars0),
       poc_challenger => normalize_challenger_rewards(ChallengerShares, Vars0),
       poc_challengee => normalize_challengee_rewards(ChallengeeShares, Vars0),
       dc_rewards => DCRewards,
       consensus_rewards => ConsensusRewards,
       securities_rewards => SecuritiesRewards,
       overages => Overages }.

-spec prepare_rewards_v2_txns( Results :: rewards_metadata(),
                               Ledger :: blockchain_ledger_v1:ledger() ) -> rewards().
prepare_rewards_v2_txns(Results, Ledger) ->
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

              maps:fold(fun(Entry, Amt, Acc) ->
                                case Entry of
                                    {owner, _Type, O} ->
                                        maps:update_with(O,
                                                         fun(Balance) -> Balance + Amt end,
                                                         Amt,
                                                         Acc);
                                    {gateway, _Type, G} ->
                                        case blockchain_ledger_v1:find_gateway_owner(G, Ledger) of
                                            {error, _Error} -> Acc;
                                            {ok, GwOwner} ->
                                                maps:update_with(GwOwner,
                                                                 fun(Balance) -> Balance + Amt end,
                                                                 Amt,
                                                                 Acc)
                                        end; % gw case
                                    {validator, _Type, V} ->
                                        case blockchain_ledger_v1:get_validator(V, Ledger) of
                                            {error, _} -> Acc;
                                            {ok, Val} ->
                                                Owner = blockchain_ledger_validator_v1:owner_address(Val),
                                                maps:update_with(Owner,
                                                                 fun(Balance) -> Balance + Amt end,
                                                                 Amt,
                                                                 Acc)
                                         end
                                end % Entry case
                        end, % function
                        Rewards,
                        maps:iterator(R)) %% bound memory size no matter size of map
      end,
      #{},
      [poc_challenger, poc_challengee, poc_witness,
       dc_rewards, consensus_rewards, securities_rewards]),

    %% now we are going to fold over all rewards and construct our
    %% transaction for the blockchain

    Rewards = maps:fold(fun(Owner, 0, Acc) ->
                                lager:debug("Dropping reward for ~p because the amount is 0",
                                            [?BIN_TO_B58(Owner)]),
                                Acc;
                            (Owner, Amount, Acc) ->
                                [ new_reward(Owner, Amount) | Acc ]
                        end,
              [],
              maps:iterator(AllRewards)), %% again, bound memory no matter size of map

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
    SCDisputeStrategyVersion = case blockchain:config(?sc_dispute_strategy_version, Ledger) of
                                   {ok, SCDV} -> SCDV;
                                   _ -> 0
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

    {ok, ElectionInterval} = blockchain:config(?election_interval, Ledger),
    {ok, ElectionRestartInterval} = blockchain:config(?election_restart_interval, Ledger),
    {ok, BlockTime} = blockchain:config(?block_time, Ledger),

    WitnessRewardDecayRate = case blockchain:config(?witness_reward_decay_rate, Ledger) of
                                 {ok, Dec} -> Dec;
                                 _ -> undefined
                             end,

    WitnessRewardDecayExclusion =
        case blockchain:config(?witness_reward_decay_exclusion, Ledger) of
            {ok, Exc} -> Exc;
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
        sc_dispute_strategy_version => SCDisputeStrategyVersion,
        poc_version => POCVersion,
        reward_version => RewardVersion,
        witness_redundancy => WitnessRedundancy,
        poc_reward_decay_rate => DecayRate,
        density_tgt_res => DensityTgtRes,
        hip15_tx_reward_unit_cap => HIP15TxRewardUnitCap,
        election_interval => ElectionInterval,
        election_restart_interval => ElectionRestartInterval,
        block_time => BlockTime,
        witness_reward_decay_rate => WitnessRewardDecayRate,
        witness_reward_decay_exclusion => WitnessRewardDecayExclusion
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
    calculate_epoch_reward(Version, Start, End, BlockTime0,
                           ElectionInterval, MonthlyReward, Ledger).

calculate_net_emissions_reward(Ledger) ->
    case blockchain:config(?net_emissions_enabled, Ledger) of
        {ok, true} ->
            %% initial proposed max 34.24
            {ok, Max} = blockchain:config(?net_emissions_max_rate, Ledger),
            {ok, Burned} = blockchain_ledger_v1:hnt_burned(Ledger),
            {ok, Overage} = blockchain_ledger_v1:net_overage(Ledger),
            min(Max, Burned + Overage);
        _ ->
            0
    end.

-spec calculate_epoch_reward(pos_integer(), pos_integer(), pos_integer(),
                             pos_integer(), pos_integer(), pos_integer(),
                             blockchain_ledger_v1:ledger()) -> float().
calculate_epoch_reward(Version, Start, End, BlockTime0, _ElectionInterval, MonthlyReward, Ledger) when Version >= 6 ->
    BlockTime1 = (BlockTime0/1000),
    % Convert to blocks per min
    BlockPerMin = 60/BlockTime1,
    % Convert to blocks per hour
    BlockPerHour = BlockPerMin*60,
    % Calculate election interval in blocks
    ElectionInterval = End - Start + 1, % epoch is inclusive of start and end
    ElectionPerHour = BlockPerHour/ElectionInterval,
    Reward = MonthlyReward/30/24/ElectionPerHour,
    Extra = calculate_net_emissions_reward(Ledger),
    Reward + Extra;
calculate_epoch_reward(Version, Start, End, BlockTime0, _ElectionInterval, MonthlyReward, Ledger) when Version >= 2 ->
    BlockTime1 = (BlockTime0/1000),
    % Convert to blocks per min
    BlockPerMin = 60/BlockTime1,
    % Convert to blocks per hour
    BlockPerHour = BlockPerMin*60,
    % Calculate election interval in blocks
    ElectionInterval = End - Start,
    ElectionPerHour = BlockPerHour/ElectionInterval,
    Reward = MonthlyReward/30/24/ElectionPerHour,
    Extra = calculate_net_emissions_reward(Ledger),
    Reward + Extra;
calculate_epoch_reward(_Version, _Start, _End, BlockTime0, ElectionInterval, MonthlyReward, Ledger) ->
    BlockTime1 = (BlockTime0/1000),
    % Convert to blocks per min
    BlockPerMin = 60/BlockTime1,
    % Convert to blocks per hour
    BlockPerHour = BlockPerMin*60,
    % Calculate number of elections per hour
    ElectionPerHour = BlockPerHour/ElectionInterval,
    Reward = MonthlyReward/30/24/ElectionPerHour,
    Extra = calculate_net_emissions_reward(Ledger),
    Reward + Extra.



-spec calculate_consensus_epoch_reward(pos_integer(), pos_integer(),
                                       map(), blockchain_ledger_v1:ledger()) -> float().
calculate_consensus_epoch_reward(Start, End, Vars, Ledger) ->

    #{ block_time := BlockTime0,
       election_interval := ElectionInterval,
       election_restart_interval := ElectionRestartInterval,
       monthly_reward := MonthlyReward } = Vars,
    BlockTime1 = (BlockTime0/1000),
    % Convert to blocks per min
    BlockPerMin = 60/BlockTime1,
    % Convert to blocks per month
    BlockPerMonth = BlockPerMin*60*24*30,
    % Calculate epoch length in blocks, cap at election interval + grace period
    EpochLength = erlang:min(End - Start + 1, ElectionInterval + ElectionRestartInterval),
    Reward = (MonthlyReward/BlockPerMonth) * EpochLength,
    Extra = calculate_net_emissions_reward(Ledger),
    Reward + Extra.

-spec consensus_members_rewards(blockchain_ledger_v1:ledger(),
                                reward_vars(),
                                non_neg_integer()) -> rewards_map().
consensus_members_rewards(Ledger, #{consensus_epoch_reward := EpochReward,
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
              %% in transitional blocks and in the last reward block of v5 it's possible to still
              %% have gateways in who need to be rewarded, so make sure that everyone gets tagged
              %% correctly with the proper code path
              Actual =
                  case GwOrVal of
                      validator ->
                          case blockchain_ledger_v1:get_validator(Member, Ledger) of
                              {ok, _} -> validator;
                              {error, not_found} -> gateway
                          end;
                      gateway -> gateway
                  end,
              maps:put({Actual, consensus, Member}, Amount+OveragePerMember, Acc)
      end,
      #{},
      Members).

-spec securities_rewards(blockchain_ledger_v1:ledger(),
                         reward_vars()) -> rewards_map().
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

-spec poc_challenger_reward( Txn :: blockchain_txn:txn(),
                             Acc :: rewards_share_map(),
                             Vars :: reward_vars() ) -> rewards_share_map().
poc_challenger_reward(Txn, ChallengerRewards, #{poc_version := Version}) ->
    Challenger = blockchain_txn_poc_receipts_v1:challenger(Txn),
    I = maps:get(Challenger, ChallengerRewards, 0),
    case blockchain_txn_poc_receipts_v1:check_path_continuation(
           blockchain_txn_poc_receipts_v1:path(Txn)) of
        true when is_integer(Version) andalso Version > 4 ->
            maps:put(Challenger, I+2, ChallengerRewards);
        _ ->
            maps:put(Challenger, I+1, ChallengerRewards)
    end.

-spec normalize_challenger_rewards( ChallengerRewards :: rewards_share_map(),
                                    Vars :: reward_vars() ) -> rewards_map().
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

-spec normalize_challengee_rewards( ChallengeeRewards :: rewards_share_map(),
                                    Vars :: reward_vars() ) -> rewards_map().
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

-spec poc_challengees_rewards_( Vars :: reward_vars(),
                                Paths :: blockchain_poc_path_element_v1:poc_path(),
                                StaticPath :: blockchain_poc_path_element_v1:poc_path(),
                                Txn :: blockchain_poc_receipts_v1:txn_poc_receipts(),
                                Chain :: blockchain:blockchain(),
                                Ledger :: blockchain_ledger_v1:ledger(),
                                IsFirst :: boolean(),
                                VarMap :: blockchain_hex:var_map(),
                                Acc0 :: rewards_share_map() ) -> rewards_share_map().
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
    RegionVars = maps:get(region_vars, Vars), % explode on purpose
    WitnessRedundancy = maps:get(witness_redundancy, Vars, undefined),
    DecayRate = maps:get(poc_reward_decay_rate, Vars, undefined),
    DensityTgtRes = maps:get(density_tgt_res, Vars, undefined),
    HIP15TxRewardUnitCap = maps:get(hip15_tx_reward_unit_cap, Vars, undefined),
    %% check if there were any legitimate witnesses
    WitStart = erlang:monotonic_time(microsecond),
    Witnesses = legit_witnesses(Txn, Chain, Ledger, Elem, StaticPath, RegionVars, Version),
    WitEnd = erlang:monotonic_time(microsecond),
    perf({challengee, witness}, WitEnd - WitStart),
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
                           case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
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
                           case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
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
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
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
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
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
                                   case poc_challengee_reward_unit(WitnessRedundancy, DecayRate, HIP15TxRewardUnitCap, Witnesses) of
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
                          AccIn :: rewards_share_map(),
                          Chain :: blockchain:blockchain(),
                          Ledger :: blockchain_ledger_v1:ledger(),
                          Vars :: reward_vars() ) -> rewards_share_map().
poc_witness_reward(Txn, AccIn,
                   Chain, Ledger,
                   #{ poc_version := POCVersion,
                      var_map := VarMap } = Vars) when is_integer(POCVersion)
                                                       andalso POCVersion >= 9 ->

    WitnessRedundancy = maps:get(witness_redundancy, Vars, undefined),
    DecayRate = maps:get(poc_reward_decay_rate, Vars, undefined),
    DensityTgtRes = maps:get(density_tgt_res, Vars, undefined),
    RegionVars = maps:get(region_vars, Vars), % explode on purpose
    KeyHash = blockchain_txn_poc_receipts_v1:onion_key_hash(Txn),

    try
        %% Get channels without validation
        {ok, Channels} = blockchain_txn_poc_receipts_v1:get_channels(Txn, POCVersion, RegionVars, Chain),
        Path = blockchain_txn_poc_receipts_v1:path(Txn),

        %% Do the new thing for witness filtering
        lists:foldl(
                fun(Elem, Acc1) ->
                        ElemPos = blockchain_utils:index_of(Elem, Path),
                        WitnessChannel = lists:nth(ElemPos, Channels),
                        WitStart = erlang:monotonic_time(microsecond),
                        ElemHash = erlang:phash2(Elem),
                        ValidWitnesses =
                            case get({KeyHash, ElemHash}) of
                                undefined ->
                                    VW = blockchain_txn_poc_receipts_v1:valid_witnesses(Elem, WitnessChannel,
                                                                                        RegionVars, Ledger),
                                    put({KeyHash, ElemHash}, VW),
                                    VW;
                                VW -> VW
                            end,
                        case ValidWitnesses of
                            [] -> Acc1;
                            [_|_] ->
                                %% lager:info("witness witness ~p", [erlang:phash2(ValidWitnesses)]),
                                WitEnd = erlang:monotonic_time(microsecond),
                                perf({witnesses, witness}, WitEnd - WitStart),
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
                                                   {C, I} = maps:get(Witness, Acc2, {0, 0}),
                                                   maps:put(Witness, {C+1, I+(ToAdd * witness_decay(C, Vars))}, Acc2)
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
                                                   {C, I} = maps:get(Witness, Acc2, {0, 0}),
                                                   maps:put(Witness, {C+1, I+(Value * witness_decay(C, Vars))}, Acc2)
                                           end,
                                           Acc1,
                                           ValidWitnesses)
                                  end
                        end
                end,
                AccIn,
                Path)
    catch
        throw:{error, {unknown_region, Region}}:_ST ->
            lager:error("Reported unknown_region: ~p", [Region]),
            AccIn;
        What:Why:ST ->
            lager:error("failed to calculate poc_witness_rewards, error ~p:~p:~p", [What, Why, ST]),
            AccIn
    end;
poc_witness_reward(Txn, AccIn, _Chain, Ledger,
                   #{ poc_version := POCVersion } = Vars) when is_integer(POCVersion)
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
                                {C, I} = maps:get(Witness, Map, {0, 0}),
                                maps:put(Witness, {C+1, I+(1 * witness_decay(C, Vars))}, Map)
                        end,
                        A,
                        GoodQualityWitnesses)
              end
      end,
      AccIn,
      blockchain_txn_poc_receipts_v1:path(Txn)
     );
poc_witness_reward(Txn, AccIn, _Chain, _Ledger, Vars) ->
    lists:foldl(
      fun(Elem, A) ->
              lists:foldl(
                fun(WitnessRecord, Map) ->
                        Witness = blockchain_poc_witness_v1:gateway(WitnessRecord),
                        {C, I} = maps:get(Witness, Map, {0, 0}),
                        maps:put(Witness, {C+1, I+(1 * witness_decay(C, Vars))}, Map)
                end,
                A,
                blockchain_poc_path_element_v1:witnesses(Elem))
      end,
      AccIn,
      blockchain_txn_poc_receipts_v1:path(Txn)).

-spec normalize_witness_rewards( WitnessRewards :: rewards_share_map(),
                                 Vars :: reward_vars() ) -> rewards_map().
normalize_witness_rewards(WitnessRewards, #{epoch_reward := EpochReward,
                                            poc_witnesses_percent := PocWitnessesPercent}=Vars) ->
    TotalWitnesses = lists:sum(element(2, lists:unzip(maps:values(WitnessRewards)))),
    ShareOfDCRemainder = share_of_dc_rewards(poc_witnesses_percent, Vars),
    WitnessesReward = (EpochReward * PocWitnessesPercent) + ShareOfDCRemainder,
    maps:fold(
        fun(Witness, {_Count, Witnessed}, Acc) ->
            PercentofReward = (Witnessed*100/TotalWitnesses)/100,
            Amount = erlang:round(PercentofReward*WitnessesReward),
            maps:put({gateway, poc_witnesses, Witness}, Amount, Acc)
        end,
        #{},
        WitnessRewards
    ).

-spec collect_dc_rewards_from_previous_epoch_grace(
        Start :: non_neg_integer(),
        End :: non_neg_integer(),
        Chain :: blockchain:blockchain(),
        Vars :: reward_vars(),
        Ledger :: blockchain_ledger_v1:ledger()) -> {ok, dc_rewards_share_map()} | {error, any()}.
collect_dc_rewards_from_previous_epoch_grace(Start, End, Chain,
                                             #{sc_grace_blocks := Grace,
                                               reward_version := RV} = Vars,
                                             Ledger) when RV > 4 ->
    scan_grace_block(max(1, Start - Grace), Start, End, Vars, Chain, Ledger, #{});
collect_dc_rewards_from_previous_epoch_grace(_Start, _End, _Chain, _Vars, _Ledger) -> {ok, #{}}.

-spec scan_grace_block( Current :: pos_integer(),
                        Start :: pos_integer(),
                        End :: pos_integer(),
                        Vars :: reward_vars(),
                        Chain :: blockchain:blockchain(),
                        Ledger :: blockchain_ledger_v1:ledger(),
                        Acc :: dc_rewards_share_map() ) -> {ok, dc_rewards_share_map()} | {error, term()}.
scan_grace_block(Current, Start, _End, _Vars, _Chain, _Ledger, Acc)
                                           when Current == Start + 1 -> {ok, Acc};
scan_grace_block(Current, Start, End, Vars, Chain, Ledger, Acc) ->
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
                                 Acc,
                                 Txns),
            scan_grace_block(Current+1, Start, End, Vars, Chain, Ledger, NewAcc)
    end.

-spec dc_reward( Txn :: blockchain_txn_state_channel_close_v1:txn_state_channel_close(),
                 End :: pos_integer(),
                 AccIn :: dc_rewards_share_map(),
                 Ledger :: blockchain_ledger_v1:ledger(),
                 Vars :: reward_vars() ) -> dc_rewards_share_map().
dc_reward(Txn, End, AccIn, Ledger, #{ sc_grace_blocks := GraceBlocks,
                                      sc_version := 2} = Vars) ->
    case blockchain_txn_state_channel_close_v1:state_channel_expire_at(Txn) + GraceBlocks < End of
        true ->
            SCID = blockchain_txn_state_channel_close_v1:state_channel_id(Txn),
            case lists:member(SCID, maps:get(seen, AccIn, [])) of
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
                                    CloseState = blockchain_ledger_state_channel_v2:close_state(SC),
                                    SCDisputeStrategy = maps:get(?sc_dispute_strategy_version, Vars, 0),
                                    {Summaries, Bonus} =
                                        case {SCDisputeStrategy, CloseState} of
                                            {Ver, dispute} when Ver >= 1 ->
                                                %% When sc_dispute_strategy_version is 1 we want to zero out as much as possible.
                                                %% No Bonuses, no summaries. All slashed.
                                                {[], 0};
                                            {_, _} ->

                                                InnerSummaries = case RewardVersion > 3 of
                                                                     %% reward version 4 normalizes payouts
                                                                     true -> blockchain_state_channel_v1:summaries(blockchain_state_channel_v1:normalize(FinalSC));
                                                                     false -> blockchain_state_channel_v1:summaries(FinalSC)
                                                                 end,

                                                %% check the dispute status
                                                InnerBonus = case blockchain_ledger_state_channel_v2:close_state(SC) of
                                                                 %% Reward version 4 or higher just slashes overcommit
                                                                 dispute when RewardVersion < 4 ->
                                                                     %% the owner of the state channel
                                                                     %% did a naughty thing, divide
                                                                     %% their overcommit between the
                                                                     %% participants

                                                                     OverCommit = blockchain_ledger_state_channel_v2:amount(SC)
                                                                         - blockchain_ledger_state_channel_v2:original(SC),
                                                                     OverCommit div length(InnerSummaries);
                                                                 _ ->
                                                                     0
                                                             end,
                                                {InnerSummaries, InnerBonus}
                                        end,

                                    lists:foldl(fun(Summary, A) ->
                                                        Key = blockchain_state_channel_summary_v1:client_pubkeybin(Summary),
                                                        DCs = blockchain_state_channel_summary_v1:num_dcs(Summary) + Bonus,
                                                        maps:update_with(Key,
                                                                         fun(V) -> V + DCs end,
                                                                         DCs,
                                                                         A)
                                                end,
                                                maps:update_with(seen,
                                                                 fun(Seen) -> [SCID|Seen] end,
                                                                 [SCID],
                                                                 AccIn),
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

-spec normalize_dc_rewards( DCRewards0 :: dc_rewards_share_map(),
                            Vars :: reward_vars() ) -> {non_neg_integer(), rewards_map()}.
normalize_dc_rewards(DCRewards0, #{epoch_reward := EpochReward,
                                   dc_percent := DCPercent}=Vars) ->
    DCRewards = maps:remove(seen, DCRewards0),
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

    {max(0, round(MaxDCReward - DCReward)),
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
    %% It's okay to call the previously broken normalize_reward_unit here because
    %% the value does not asympotically tend to 2.0, instead it tends to 0.0
    normalize_reward_unit(blockchain_utils:normalize_float((N - (1 - math:pow(R, (W - N))))/W)).

-spec legit_witnesses( Txn :: blockchain_txn_poc_receipts_v1:txn_poc_receipts(),
                       Chain :: blockchain:blockchain(),
                       Ledger :: blockchain_ledger_v1:ledger(),
                       Elem :: blockchain_poc_path_element_v1:poc_element(),
                       StaticPath :: blockchain_poc_path_element_v1:poc_path(),
                       RegionVars :: {ok, [{atom(), binary() | {error, any()}}]} | {error, any()},
                       Version :: pos_integer()
                     ) -> [blockchain_txn_poc_witnesses_v1:poc_witness()].
legit_witnesses(Txn, Chain, Ledger, Elem, StaticPath, RegionVars, Version) ->
    case Version of
        V when is_integer(V), V >= 9 ->
            try
                %% Get channels without validation
                {ok, Channels} = blockchain_txn_poc_receipts_v1:get_channels(Txn, Version, RegionVars, Chain),
                ElemPos = blockchain_utils:index_of(Elem, StaticPath),
                WitnessChannel = lists:nth(ElemPos, Channels),
                KeyHash = blockchain_txn_poc_receipts_v1:onion_key_hash(Txn),
                ElemHash = erlang:phash2(Elem),
                ValidWitnesses =
                    case get({KeyHash, ElemHash}) of
                        undefined ->
                            VW = blockchain_txn_poc_receipts_v1:valid_witnesses(Elem, WitnessChannel, RegionVars, Ledger),
                            put({KeyHash, ElemHash}, VW),
                            VW;
                        VW -> VW
                    end,
                ValidWitnesses
            catch
                throw:{error, {unknown_region, Region}}:_ST ->
                    lager:error("Reported unknown_region: ~p", [Region]),
                    [];
                What:Why:ST ->
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

witness_decay(Count, Vars) ->
    case maps:find(witness_reward_decay_rate, Vars) of
        {ok, undefined} ->
            1;
        {ok, DecayRate} ->
            Exclusion = case maps:find(witness_reward_decay_exclusion, Vars) of
                            {ok, undefined} -> 0;
                            {ok, ExclusionValue} -> ExclusionValue
                        end,
            case Count < Exclusion of
                true ->
                    1;
                false ->
                    Scale = math:exp((Count - Exclusion) * -1 * DecayRate),
                    lager:debug("scaling witness reward by ~p", [Scale]),
                    Scale
            end;
        _ ->
            1
    end.

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
        poc_version => 5,
        region_vars => []
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

dc_rewards_sc_dispute_strategy_test() ->
    BaseDir = test_utils:tmp_dir("dc_rewards_dispute_sc_test"),
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
        reward_version => 4,
        oracle_price => 100000000, %% 1 dollar
        consensus_members => [<<"c">>, <<"d">>],
        %% This is the important part of what's being tested here.
        sc_dispute_strategy_version => 1
    },

    LedgerVars = maps:merge(#{?poc_version => 5, ?sc_version => 2, ?sc_grace_blocks => 5}, common_poc_vars()),
    ok = blockchain_ledger_v1:vars(LedgerVars, [], Ledger1),

    {SC0, _} = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 100, <<"blockhash">>, 10),
    SCValid = blockchain_state_channel_v1:summaries([blockchain_state_channel_summary_v1:new(<<"a">>, 1, 1), blockchain_state_channel_summary_v1:new(<<"b">>, 2, 2)], SC0),
    SCDispute = blockchain_state_channel_v1:summaries([blockchain_state_channel_summary_v1:new(<<"a">>, 2, 2), blockchain_state_channel_summary_v1:new(<<"b">>, 3, 3)], SC0),

    ok = blockchain_ledger_v1:add_state_channel(<<"id">>, <<"owner">>, 10, 1, 100, 200, Ledger1),
    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),

    ok = blockchain_ledger_v1:close_state_channel(<<"owner">>, <<"owner">>, SCValid, <<"id">>, false, Ledger1),
    {ok, _} = blockchain_ledger_v1:find_state_channel(<<"id">>, <<"owner">>, Ledger1),

    ok = blockchain_ledger_v1:close_state_channel(<<"owner">>, <<"a">>, SCDispute, <<"id">>, true, Ledger1),

    SCClose = blockchain_txn_state_channel_close_v1:new(SCValid, <<"owner">>),
    {ok, _DCsInEpochAsHNT} = blockchain_ledger_v1:dc_to_hnt(3, 100000000), %% 3 DCs burned at HNT price of 1 dollar

    DCShares = dc_reward(SCClose, 100, #{}, Ledger1, Vars),

    %% We only care that no rewards are generated when sc_dispute_strategy_version is active.
    {Res, M} = normalize_dc_rewards(DCShares, Vars),
    ?assertEqual(#{}, M, "no summaries in rewards map"),
    ?assertEqual(30000, Res),
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
        sc_dispute_strategy_version => 0,
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
        region_vars => [],
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


hip28_calc_test() ->
    {timeout, 30000,
     fun() ->
             meck:new(blockchain_ledger_v1, [passthrough]),
             meck:expect(blockchain_ledger_v1, hnt_burned,
                         fun(_Ledger) ->
                                 0
                         end),
             meck:expect(blockchain_ledger_v1, net_overage,
                         fun(_Ledger) ->
                                 0
                         end),
             meck:expect(blockchain_ledger_v1, config,
                         fun(_, _Ledger) ->
                                 0
                         end),

                                                % set test vars such that rewards are 1 per block
             Vars = #{ block_time => 60000,
                       election_interval => 30,
                       election_restart_interval => 5,
                       monthly_reward => 43200,
                       reward_version => 6 },
             ?assertEqual(30.0, calculate_consensus_epoch_reward(1, 30, Vars, ledger)),
             ?assertEqual(35.0, calculate_consensus_epoch_reward(1, 50, Vars, ledger)),
             meck:unload(blockchain_ledger_v1),
             ok
     end}.

consensus_epoch_reward_test() ->
    {timeout, 30000,
     fun() ->
             meck:new(blockchain_ledger_v1, [passthrough]),
             meck:expect(blockchain_ledger_v1, hnt_burned,
                         fun(_Ledger) ->
                                 0
                         end),
             meck:expect(blockchain_ledger_v1, net_overage,
                         fun(_Ledger) ->
                                 0
                         end),
             meck:expect(blockchain_ledger_v1, config,
                         fun(_, _Ledger) ->
                                 0
                         end),

             %% using test values such that reward is 1 per block
             %% should always return the election interval as the answer
             ?assertEqual(30.0,calculate_epoch_reward(1, 1, 25, 60000, 30, 43200, ledger)),

             %% more than 30 blocks should return 30
             ?assertEqual(30.0,calculate_epoch_reward(1, 1, 50, 60000, 30, 43200, ledger)),
             meck:unload(blockchain_ledger_v1)
     end}.

-endif.
