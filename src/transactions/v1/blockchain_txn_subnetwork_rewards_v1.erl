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

-spec new(blockchain_token_v1:type(), non_neg_integer(), non_neg_integer(), rewards()) -> txn_subnetwork_rewards_v1().
new(Type, Start, End, Rewards) ->
    SortedRewards = lists:sort(Rewards),
    #blockchain_txn_subnetwork_rewards_v1_pb{
       token_type = Type,
       start_epoch=Start,
       end_epoch=End,
       rewards=SortedRewards}.

-spec hash(txn_subnetwork_rewards_v1()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_subnetwork_rewards_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

-spec token_type(txn_subnetwork_rewards_v1()) -> blockchain_token_v1:type().
token_type(#blockchain_txn_subnetwork_rewards_v1_pb{token_type = Type}) ->
    Type.

-spec start_epoch(txn_subnetwork_rewards_v1()) -> non_neg_integer().
start_epoch(#blockchain_txn_subnetwork_rewards_v1_pb{start_epoch=Start}) ->
    Start.

-spec end_epoch(txn_subnetwork_rewards_v1()) -> non_neg_integer().
end_epoch(#blockchain_txn_subnetwork_rewards_v1_pb{end_epoch=End}) ->
    End.

-spec rewards(txn_subnetwork_rewards_v1()) -> rewards().
rewards(#blockchain_txn_subnetwork_rewards_v1_pb{rewards=Rewards}) ->
    Rewards.

-spec reward_account(subnetwork_reward_v1()) -> binary().
reward_account(#blockchain_txn_subnetwork_reward_v1_pb{account = Account}) ->
    Account.

-spec reward_amount(subnetwork_reward_v1()) -> non_neg_integer().
reward_amount(#blockchain_txn_subnetwork_reward_v1_pb{amount = Amount}) ->
    Amount.

-spec new_reward( Account :: libp2p_crypto:pubkey_bin(),
                  Amount :: non_neg_integer() ) -> subnetwork_reward_v1().
new_reward(Account, Amount) ->
    #blockchain_txn_subnetwork_reward_v1_pb{account=Account, amount=Amount}.

reward_server_signature(#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature = RSS}) ->
    RSS.

-spec sign(txn_subnetwork_rewards_v1(), libp2p_crypto:sig_fun()) -> txn_subnetwork_rewards_v1().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature = <<>>},
    EncodedTxn = blockchain_txn_subnetwork_rewards_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature=SigFun(EncodedTxn)}.

-spec fee(txn_subnetwork_rewards_v1()) -> 0.
fee(_Txn) ->
    0.

-spec fee_payer(txn_subnetwork_rewards_v1(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec is_valid(txn_subnetwork_rewards_v1(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Start = ?MODULE:start_epoch(Txn),
    End = ?MODULE:end_epoch(Txn),
    %% Rewards = ?MODULE:rewards(Txn),
    TokenType = token_type(Txn),
    Signature = reward_server_signature(Txn),
    %% make sure that the signature is correct
    %% make sure that the rewards are less than the amount stored
    {ok, Subnet} = blockchain_ledger_v1:find_subnetwork_v1(TokenType, Ledger),
    %% TotalRewards = lists:foldl(
    %%                  fun(Reward, Acc) ->
    %%                          Acc + reward_amount(Reward)
    %%                  end,
    %%                  0,
    %%                  Rewards),
    %% Tokens = blockchain_ledger_subnetwork_v1:token_amount(Subnet),
    LastRewardedBlock = blockchain_ledger_subnetwork_v1:last_rewarded_block(Subnet),
    try
        %% this needs to somehow limit the mint here?  but if there is only premine I don't
        %% understand how we do that.

        %% case TotalRewards =< blockchain_ledger_subnetwork_v1:token_amount(Subnet) of
        %%     true -> ok;
        %%     false -> throw({insufficient_tokens_to_fulfil_rewards, Tokens, TotalRewards})
        %% end
        BaseTxn = Txn#blockchain_txn_subnetwork_rewards_v1_pb{reward_server_signature = <<>>},
        Artifact = blockchain_txn_subnetwork_rewards_v1_pb:encode_msg(BaseTxn),
        case lists:any(
               fun(Key) ->
                       libp2p_crypto:verify(Artifact, Signature, libp2p_crypto:bin_to_pubkey(Key))
               end, blockchain_ledger_subnetwork_v1:reward_server_keys(Subnet)) of
            true -> ok;
            false -> throw(invalid_signature)
        end,
        case End > Start andalso Start > LastRewardedBlock of
            true -> ok;
            false -> throw({invalid_reward_range, Start, End, LastRewardedBlock})
        end
    catch throw:Err ->
            {error, Err}
    end.

-spec absorb(txn_subnetwork_rewards_v1(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
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

    %% these rewards are the same no matter the ledger
    absorb_rewards(rewards(Txn), Ledger),
    TokenType = token_type(Txn),
    {ok, Subnet} = blockchain_ledger_v1:find_subnetwork_v1(TokenType, Ledger),
    Subnet1 = blockchain_ledger_subnetwork_v1:last_rewarded_block(Subnet, end_epoch(Txn)),
    ok = blockchain_ledger_v1:update_subnetwork(Subnet1, Ledger).

-spec absorb_rewards(Rewards :: rewards(),
                     Ledger :: blockchain_ledger_v1:ledger()) -> ok.
absorb_rewards(Rewards, Ledger) ->
    lists:foreach(
        fun(#blockchain_txn_subnetwork_reward_v1_pb{account=Account, amount=Amount}) ->
            ok = blockchain_ledger_v1:credit_account(Account, Amount, Ledger)
        end,
        Rewards
    ).

-spec print(txn_subnetwork_rewards_v1()) -> iodata().
print(undefined) -> <<"type=rewards_v2 undefined">>;
print(#blockchain_txn_subnetwork_rewards_v1_pb{start_epoch=Start,
                                               end_epoch=End}) ->
    io_lib:format("type=rewards_v2 start_epoch=~p end_epoch=~p",
                  [Start, End]).

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
            Start = start_epoch(Txn),
            End = end_epoch(Txn),
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


-spec reward_to_json( Reward :: subnetwork_reward_v1(),
                      Opts :: blockchain_json:opts() ) -> blockchain_json:json_object().
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
        blockchain_txn_poc_receipts_v2:new(<<"a">>, <<"Secret">>, <<"OnionKeyHash">>, [], <<"BlockHash">>),
        blockchain_txn_poc_receipts_v2:new(<<"b">>, <<"Secret">>, <<"OnionKeyHash">>, [], <<"BlockHash">>),
        blockchain_txn_poc_receipts_v2:new(<<"c">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA], <<"BlockHash">>)
    ],
    Vars = #{
        epoch_reward => 1000,
        poc_challengers_percent => 0.15,
        poc_witnesses_percent => 0.0,
        poc_challengees_percent => 0.0,
        dc_remainder => 0,
        poc_version => 5,
        poc_challenger_type => validator
    },
    Rewards = #{
        {validator, poc_challengers, <<"a">>} => 38,
        {validator, poc_challengers, <<"b">>} => 38,
        {validator, poc_challengers, <<"c">>} => 75
    },
    ChallengerShares = lists:foldl(fun(T, Acc) -> poc_challenger_reward(T, Acc, Vars) end, #{}, Txns),
    ?assertEqual(Rewards, normalize_challenger_rewards(ChallengerShares, Vars)),

    AltVars = Vars#{ poc_challenger_type => gateway },
    AltRewards = #{
        {gateway, poc_challengers, <<"a">>} => 38,
        {gateway, poc_challengers, <<"b">>} => 38,
        {gateway, poc_challengers, <<"c">>} => 75
    },
    ?assertEqual(AltRewards, normalize_challenger_rewards(ChallengerShares, AltVars)).

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
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForB, ElemForA], <<"BlockHash">>),  %% 1, 2
        %% Reward because of witness
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForAWithWitness], <<"BlockHash">>), %% 3
        %% Reward because of next elem has receipt
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA, ElemForB, ElemForC], <<"BlockHash">>), %% 3, 2, 2
        %% Reward because of witness (adding to make reward 50/50)
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForBWithWitness], <<"BlockHash">>) %% 3
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
                                           Path = blockchain_txn_poc_receipts_v2:path(T),
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
        blockchain_txn_poc_receipts_v2:new(<<"d">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem], <<"BlockHash">>),
        blockchain_txn_poc_receipts_v2:new(<<"e">>, <<"Secret">>, <<"OnionKeyHash">>, [Elem, Elem], <<"BlockHash">>)
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
        poc_challenger_type => validator,
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
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForB, ElemForA], <<"BlockHash">>),  %% 1, 2
        %% Reward because of witness
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForAWithWitness], <<"BlockHash">>), %% 3
        %% Reward because of next elem has receipt
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForA, ElemForB, ElemForC], <<"BlockHash">>), %% 3, 2, 2
        %% Reward because of witness (adding to make reward 50/50)
        blockchain_txn_poc_receipts_v2:new(<<"X">>, <<"Secret">>, <<"OnionKeyHash">>, [ElemForBWithWitness], <<"BlockHash">>) %% 3
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
    ?assertEqual(#{{validator,poc_challengers,<<"X">>} =>  ChallengersAward}, ChallengerRewards), %% entire 15% allocation
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

    ChallengerSpilloverAward = 0,

    ?assertEqual(#{{validator,poc_challengers,<<"X">>} =>  ChallengersAward + ChallengerSpilloverAward}, SpilloverChallengerRewards), %% entire 15% allocation
    ChallengeeSpilloverAward = erlang:round(DCRemainder * ((maps:get(poc_challengees_percent, Vars) / (maps:get(poc_challengees_percent, Vars) +
                                                                                                       maps:get(poc_witnesses_percent, Vars))))),
    ?assertEqual(#{{gateway,poc_challengees,<<"a">>} => trunc((ChallengeesAward + ChallengeeSpilloverAward) * 4/8), %% 4 of 8 shares of 20% allocation
                   {gateway,poc_challengees,<<"b">>} => trunc((ChallengeesAward + ChallengeeSpilloverAward) * 3/8), %% 3 shares
                   {gateway,poc_challengees,<<"c">>} => round((ChallengeesAward + ChallengeeSpilloverAward) * 1/8)}, %% 1 share
                 SpilloverChallengeeRewards),
    WitnessesSpilloverAward = erlang:round(DCRemainder * ((maps:get(poc_witnesses_percent, Vars) / (maps:get(poc_challengees_percent, Vars) +
                                                                                                       maps:get(poc_witnesses_percent, Vars))))),
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
