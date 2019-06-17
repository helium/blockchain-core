%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Epoch Rewards ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_epoch_rewards_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_epoch_rewards_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    start_of_epoch/1,
    end_of_epoch/1,
    signature/1,
    fee/1,
    sign/2,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% TODO: Make this chain vars
-define(MONTHLY_REWARD, 50000 * 1000000). % In bones 
-define(EPOCH_REWARD, ?MONTHLY_REWARD/30/24/2).
-define(SECURITIES_PERCENT, 0.35).
-define(DC_PERCENT, 0.25). % Unused for now so give to POC
-define(POC_CHALLENGEES_PERCENT, 0.19 + 0.16).
-define(POC_CHALLENGERS_PERCENT, 0.09 + 0.06).
-define(POC_WITNESSES_PERCENT, 0.02 + 0.03).
-define(CONSENSUS_PERCENT, 0.10).

-type txn_epoch_rewards() :: #blockchain_txn_epoch_rewards_v1_pb{}.
-export_type([txn_epoch_rewards/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(non_neg_integer(), non_neg_integer()) -> txn_epoch_rewards().
new(Start, End) ->
    #blockchain_txn_epoch_rewards_v1_pb{start_of_epoch=Start, end_of_epoch=End}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_epoch_rewards()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_epoch_rewards_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_epoch_rewards_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec start_of_epoch(txn_epoch_rewards()) -> non_neg_integer().
start_of_epoch(Txn) ->
    Txn#blockchain_txn_epoch_rewards_v1_pb.start_of_epoch.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec end_of_epoch(txn_epoch_rewards()) -> non_neg_integer().
end_of_epoch(Txn) ->
    Txn#blockchain_txn_epoch_rewards_v1_pb.end_of_epoch.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_epoch_rewards()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_epoch_rewards_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_epoch_rewards()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_epoch_rewards(), libp2p_crypto:sig_fun()) -> txn_epoch_rewards().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_epoch_rewards_v1_pb:encode_msg(Txn#blockchain_txn_epoch_rewards_v1_pb{signature = <<>>}),
    Txn#blockchain_txn_epoch_rewards_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_epoch_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Start = ?MODULE:start_of_epoch(Txn),
    End = ?MODULE:end_of_epoch(Txn),
    validate_epoch(Start, End, Chain).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_epoch_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Start = ?MODULE:start_of_epoch(Txn),
    End = ?MODULE:end_of_epoch(Txn),
    lager:info("calculating reward start at ~p ending at ~p", [Start, End]),
    Transactions = get_txns_for_epoch(Start, End, Chain),
    ok = consensus_members_rewards(Txn, Chain),
    ok = securities_rewards(Chain),
    ok = poc_challengers_rewards(Transactions, Chain),
    ok = poc_challengees_rewards(Transactions, Chain),
    ok = poc_witnesses_rewards(Transactions, Chain),
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
validate_epoch(Start, End, Chain) ->
    % TODO: Grab this from chain vars
    validate_epoch(Start, End, Chain, 30).
    
validate_epoch(Start, End, _Chain, Size) when End-Start =< Size ->
    ok;
validate_epoch(_Start, _end, _Chain, _Size) ->
    {error, epoch_size}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_txns_for_epoch(Start, End, Chain) ->
    get_txns_for_epoch(Start, End, Chain, []).
    
get_txns_for_epoch(Start, Start, _Chain, Txns) ->
    Txns;
get_txns_for_epoch(Start, Current, Chain, Txns) ->
    case blockchain:get_block(Current, Chain) of
        {error, _Reason} ->
            lager:error("failed to get block ~p ~p", [_Reason, Current]),
            % TODO: Should we error out here?
            Txns;
        {ok, Block} ->
            PrevHash = blockchain_block:prev_hash(Block),
            Transactions = blockchain_block:transactions(Block),
            get_txns_for_epoch(Start, PrevHash, Chain, Txns ++ Transactions)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
consensus_members_rewards(_Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:consensus_members(Ledger) of
        {error, _Reason} ->
            lager:error("failed to get consensus_members ~p", [_Reason]);
            % TODO: Should we error out here?
        {ok, ConsensusMembers} ->
            ConsensusReward = ?EPOCH_REWARD * ?CONSENSUS_PERCENT,
            Total = erlang:length(ConsensusMembers),
            lists:foreach(
                fun(Member) ->
                    PercentofReward = 100/Total/100,
                    Amount = erlang:round(PercentofReward*ConsensusReward),
                    blockchain_ledger_v1:credit_account(Member, Amount, Ledger)
                end,
                ConsensusMembers
            ),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
securities_rewards(Blockchain) ->
    Ledger = blockchain:ledger(Blockchain),
    SecuritiesReward = ?EPOCH_REWARD * ?SECURITIES_PERCENT,
    Securities = blockchain_ledger_v1:securities(Ledger),
    TotalSecurities = maps:fold(
        fun(_, Entry, Acc) ->
            Acc + blockchain_ledger_security_entry_v1:balance(Entry)
        end,
        0,
        Securities
    ),
    maps:fold(
        fun(Key, Entry, _Acc) ->
            Balance = blockchain_ledger_security_entry_v1:balance(Entry),
            PercentofReward = (Balance*100/TotalSecurities)/100,
            Amount = erlang:round(PercentofReward*SecuritiesReward),
            blockchain_ledger_v1:credit_account(Key, Amount, Ledger)
        end,
        ok,
        Securities
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
poc_challengers_rewards(Transactions, Chain) ->
    ChallengersReward = ?EPOCH_REWARD * ?POC_CHALLENGERS_PERCENT,
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
    Ledger = blockchain:ledger(Chain),
    maps:fold(
        fun(Challenger, Challenged, _Acc) ->
            PercentofReward = (Challenged*100/TotalChallenged)/100,
            Amount = erlang:round(PercentofReward * ChallengersReward),
            blockchain_ledger_v1:credit_account(Challenger, Amount, Ledger)
        end,
        ok,
        Challengers
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
poc_challengees_rewards(Transactions, Chain) ->
    ChallengeesReward = ?EPOCH_REWARD * ?POC_CHALLENGEES_PERCENT,
    {Challengees, TotalChallenged} = lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    Path = blockchain_txn_poc_receipts_v1:path(Txn),
                    lists:foldl(
                        fun(Elem, {Map, Total}=Acc1) ->
                            case blockchain_poc_path_element_v1:receipt(Elem) =/= undefined of
                                false ->
                                    Acc1;
                                true ->
                                    Challengee = blockchain_poc_path_element_v1:challengee(Elem),
                                    I = maps:get(Challengee, Map, 0),
                                    {maps:put(Challengee, I+1, Map), Total+1}
                            end
                        end,
                        Acc0,
                        Path
                    )
            end
        end,
        {#{}, 0},
        Transactions
    ),
    Ledger = blockchain:ledger(Chain),
    maps:fold(
        fun(Challengee, Challenged, _Acc) ->
            PercentofReward = (Challenged*100/TotalChallenged)/100,
            % TODO: Not sure about the all round thing...
            Amount = erlang:round(PercentofReward*ChallengeesReward),
            blockchain_ledger_v1:credit_account(Challengee, Amount, Ledger)
        end,
        ok,
        Challengees
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
poc_witnesses_rewards(Transactions, Chain) ->
    WitnessesReward = ?EPOCH_REWARD * ?POC_WITNESSES_PERCENT,
    {Witnesses, TotalWitnesses} = lists:foldl(
        fun(Txn, Acc0) ->
            case blockchain_txn:type(Txn) == blockchain_txn_poc_receipts_v1 of
                false ->
                    Acc0;
                true ->
                    lists:foldl(
                        fun(Elem, Acc1) ->
                            lists:foldl(
                                fun(Witness, {Map, Total}) ->
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
    Ledger = blockchain:ledger(Chain),
    maps:fold(
        fun(Witness, Witnessed, _Acc) ->
            PercentofReward = (Witnessed*100/TotalWitnesses)/100,
            Amount = erlang:round(PercentofReward*WitnessesReward),
            blockchain_ledger_v1:credit_account(Witness, Amount, Ledger)
        end,
        ok,
        Witnesses
    ),
    ok.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_epoch_rewards_v1_pb{start_of_epoch= <<"start_of_epoch">>, end_of_epoch= <<"end_of_epoch">>},
    ?assertEqual(Tx, new(<<"start_of_epoch">>, <<"end_of_epoch">>)).

start_of_epoch_test() ->
    Tx = new(<<"start_of_epoch">>, <<"end_of_epoch">>),
    ?assertEqual(<<"start_of_epoch">>, start_of_epoch(Tx)).

end_of_epoch_test() ->
    Tx = new(<<"start_of_epoch">>, <<"end_of_epoch">>),
    ?assertEqual(<<"end_of_epoch">>, end_of_epoch(Tx)).

-endif.
