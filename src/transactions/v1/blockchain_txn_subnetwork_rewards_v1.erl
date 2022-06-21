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
-module(blockchain_txn_subnetwork_rewards_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_subnetwork_rewards_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    start_epoch/1,
    end_epoch/1,
    rewards/1,
    reward_server_signature/1,

    %% reward accessors
    reward_account/1,
    reward_amount/1,
    new_reward/2,

    sign/2,
    fee/1,
    fee_payer/2,
    is_valid/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_subnetwork_rewards_v1() :: #blockchain_txn_subnetwork_rewards_v1_pb{}.
-type subnetwork_reward_v1() :: #blockchain_txn_subnetwork_reward_v1_pb{}.
-type rewards() :: [subnetwork_reward_v1()].

-export_type([txn_subnetwork_rewards_v1/0]).

%% ------------------------------------------------------------------
%% Public API
%% ------------------------------------------------------------------

-spec new(blockchain_token_v1:type(), non_neg_integer(), non_neg_integer(), rewards()) ->
    txn_subnetwork_rewards_v1().
new(Type, Start, End, Rewards) ->
    SortedRewards = lists:sort(Rewards),
    #blockchain_txn_subnetwork_rewards_v1_pb{
        token_type = Type,
        start_epoch = Start,
        end_epoch = End,
        rewards = SortedRewards
    }.

-spec hash(txn_subnetwork_rewards_v1()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_subnetwork_rewards_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

-spec token_type(txn_subnetwork_rewards_v1()) -> blockchain_token_v1:type().
token_type(#blockchain_txn_subnetwork_rewards_v1_pb{token_type = Type}) ->
    Type.

-spec start_epoch(txn_subnetwork_rewards_v1()) -> non_neg_integer().
start_epoch(#blockchain_txn_subnetwork_rewards_v1_pb{start_epoch = Start}) ->
    Start.

-spec end_epoch(txn_subnetwork_rewards_v1()) -> non_neg_integer().
end_epoch(#blockchain_txn_subnetwork_rewards_v1_pb{end_epoch = End}) ->
    End.

-spec rewards(txn_subnetwork_rewards_v1()) -> rewards().
rewards(#blockchain_txn_subnetwork_rewards_v1_pb{rewards = Rewards}) ->
    Rewards.

-spec reward_account(subnetwork_reward_v1()) -> binary().
reward_account(#blockchain_txn_subnetwork_reward_v1_pb{account = Account}) ->
    Account.

-spec reward_amount(subnetwork_reward_v1()) -> non_neg_integer().
reward_amount(#blockchain_txn_subnetwork_reward_v1_pb{amount = Amount}) ->
    Amount.

-spec new_reward(
    Account :: libp2p_crypto:pubkey_bin(),
    Amount :: non_neg_integer()
) -> subnetwork_reward_v1().
new_reward(Account, Amount) ->
    #blockchain_txn_subnetwork_reward_v1_pb{account = Account, amount = Amount}.

reward_server_signature(#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature = RSS}) ->
    RSS.

-spec sign(txn_subnetwork_rewards_v1(), libp2p_crypto:sig_fun()) -> txn_subnetwork_rewards_v1().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature = <<>>},
    EncodedTxn = blockchain_txn_subnetwork_rewards_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature = SigFun(EncodedTxn)}.

-spec fee(txn_subnetwork_rewards_v1()) -> 0.
fee(_Txn) ->
    0.

-spec fee_payer(txn_subnetwork_rewards_v1(), blockchain_ledger_v1:ledger()) ->
    libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec is_valid(txn_subnetwork_rewards_v1(), blockchain:blockchain()) ->
    ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    Rewards = ?MODULE:rewards(Txn),
    TokenType = token_type(Txn),
    Signature = reward_server_signature(Txn),
    %% make sure that the signature is correct
    %% make sure that the rewards are less than the amount stored
    {ok, Subnet} = blockchain_ledger_v1:find_subnetwork_v1(TokenType, Ledger),
    TotalRewards = lists:foldl(
        fun(Reward, Acc) ->
            Acc + reward_amount(Reward)
        end,
        0,
        Rewards
    ),
    Tokens = blockchain_ledger_subnetwork_v1:token_treasury(Subnet),
    LastRewardedBlock = blockchain_ledger_subnetwork_v1:last_rewarded_block(Subnet),
    try
        %% this needs to somehow limit the mint here?  but if there is only premine I don't
        %% understand how we do that.
        case TotalRewards =< blockchain_ledger_subnetwork_v1:token_treasury(Subnet) of
            true -> ok;
            false -> throw({insufficient_tokens_to_fulfil_rewards, Tokens, TotalRewards})
        end,
        BaseTxn = Txn#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature = <<>>},
        Artifact = blockchain_txn_subnetwork_rewards_v1_pb:encode_msg(BaseTxn),
        case
            lists:any(
                fun(Key) ->
                    libp2p_crypto:verify(Artifact, Signature, libp2p_crypto:bin_to_pubkey(Key))
                end,
                blockchain_ledger_subnetwork_v1:reward_server_keys(Subnet)
            )
        of
            true -> ok;
            false -> throw(invalid_signature)
        end,
        case End > Start andalso Start > LastRewardedBlock of
            true -> ok;
            false -> throw({invalid_reward_range, Start, End, LastRewardedBlock})
        end
    catch
        throw:Err ->
            {error, Err}
    end.

-spec absorb(txn_subnetwork_rewards_v1(), blockchain:blockchain()) ->
    ok | {error, atom()} | {error, {atom(), any()}}.
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
                    case (Max - Burned) < Overage of
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

    %% these rewards are the same no matter the ledger
    absorb_rewards(rewards(Txn), Ledger),
    TokenType = token_type(Txn),
    {ok, Subnet} = blockchain_ledger_v1:find_subnetwork_v1(TokenType, Ledger),
    Subnet1 = blockchain_ledger_subnetwork_v1:last_rewarded_block(Subnet, end_epoch(Txn)),
    ok = blockchain_ledger_v1:update_subnetwork(Subnet1, Ledger).

-spec absorb_rewards(
    Rewards :: rewards(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> ok.
absorb_rewards(Rewards, Ledger) ->
    lists:foreach(
        fun(#blockchain_txn_subnetwork_reward_v1_pb{account = Account, amount = Amount}) ->
            ok = blockchain_ledger_v1:credit_account(Account, Amount, Ledger)
        end,
        Rewards
    ).

-spec print(txn_subnetwork_rewards_v1()) -> iodata().
print(undefined) ->
    <<"type=rewards_v2 undefined">>;
print(#blockchain_txn_subnetwork_rewards_v1_pb{
    start_epoch = Start,
    end_epoch = End
}) ->
    io_lib:format(
        "type=rewards_v2 start_epoch=~p end_epoch=~p",
        [Start, End]
    ).

json_type() ->
    <<"subnetwork_rewards_v1">>.

-spec to_json(txn_subnetwork_rewards_v1(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, Opts) ->
    RewardToJson =
        fun
            ({gateway, Type, G}, Amount, Ledger, Acc) ->
                case blockchain_ledger_v1:find_gateway_owner(G, Ledger) of
                    {error, _Error} ->
                        Acc;
                    {ok, GwOwner} ->
                        [
                            #{
                                account => ?BIN_TO_B58(GwOwner),
                                gateway => ?BIN_TO_B58(G),
                                amount => Amount,
                                type => Type
                            }
                            | Acc
                        ]
                end;
            ({validator, Type, V}, Amount, Ledger, Acc) ->
                case blockchain_ledger_v1:get_validator(V, Ledger) of
                    {error, _Error} ->
                        Acc;
                    {ok, Val} ->
                        Owner = blockchain_ledger_validator_v1:owner_address(Val),
                        [
                            #{
                                account => ?BIN_TO_B58(Owner),
                                gateway => ?BIN_TO_B58(V),
                                amount => Amount,
                                type => Type
                            }
                            | Acc
                        ]
                end;
            ({owner, Type, O}, Amount, _Ledger, Acc) ->
                [
                    #{
                        account => ?BIN_TO_B58(O),
                        gateway => undefined,
                        amount => Amount,
                        type => Type
                    }
                    | Acc
                ]
        end,
    Rewards =
        case lists:keyfind(chain, 1, Opts) of
            {chain, Chain} ->
                Start = start_epoch(Txn),
                End = end_epoch(Txn),
                {ok, Ledger} = blockchain:ledger_at(End, Chain),
                {ok, Metadata} =
                    case lists:keyfind(rewards_metadata, 1, Opts) of
                        {rewards_metadata, M} -> {ok, M};
                        _ -> ?MODULE:calculate_rewards_metadata(Start, End, Chain)
                    end,
                maps:fold(
                    fun
                        (overages, Amount, Acc) ->
                            [
                                #{
                                    amount => Amount,
                                    type => overages
                                }
                                | Acc
                            ];
                        (_RewardCategory, Rewards, Acc0) ->
                            maps:fold(
                                fun(Entry, Amount, Acc) ->
                                    RewardToJson(Entry, Amount, Ledger, Acc)
                                end,
                                Acc0,
                                Rewards
                            )
                    end,
                    [],
                    Metadata
                );
            _ ->
                [reward_to_json(R, []) || R <- rewards(Txn)]
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

-spec reward_to_json(
    Reward :: subnetwork_reward_v1(),
    Opts :: blockchain_json:opts()
) -> blockchain_json:json_object().
reward_to_json(#blockchain_txn_subnetwork_reward_v1_pb{account = Account, amount = Amt}, _Opts) ->
    #{
        type => <<"subnetwork_reward_v1">>,
        account => ?BIN_TO_B58(Account),
        amount => Amt
    }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-endif.
