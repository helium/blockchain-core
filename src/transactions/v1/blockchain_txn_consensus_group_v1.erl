%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Consensus Group ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_consensus_group_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_consensus_group_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    sign/2,
    members/1,
    proof/1,
    height/1,
    delay/1,
    fee/1,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% monthly_reward          50000 * 1000000  In bones 
% securities_percent      0.35
% dc_percent              0.25 Unused for now so give to POC
% poc_challengees_percen  0.19 + 0.16
% poc_challengers_percen  0.09 + 0.06
% poc_witnesses_percent   0.02 + 0.03
% consensus_percent       0.10

-type txn_consensus_group() :: #blockchain_txn_consensus_group_v1_pb{}.
-export_type([txn_consensus_group/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new([libp2p_crypto:pubkey_bin()], binary(), pos_integer(), non_neg_integer()) -> txn_consensus_group().
new(_Members, _Proof, 0, _Delay) ->
    error(blowupyay);
new(Members, Proof, Height, Delay) ->
    #blockchain_txn_consensus_group_v1_pb{members = Members,
                                          proof = Proof,
                                          height = Height,
                                          delay = Delay}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_consensus_group()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_consensus_group_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_consensus_group(), libp2p_crypto:sig_fun()) -> txn_consensus_group().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec members(txn_consensus_group()) -> [libp2p_crypto:pubkey_bin()].
members(Txn) ->
    Txn#blockchain_txn_consensus_group_v1_pb.members.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec proof(txn_consensus_group()) -> binary().
proof(Txn) ->
    Txn#blockchain_txn_consensus_group_v1_pb.proof.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec height(txn_consensus_group()) -> pos_integer().
height(Txn) ->
    Txn#blockchain_txn_consensus_group_v1_pb.height.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delay(txn_consensus_group()) -> non_neg_integer().
delay(Txn) ->
    Txn#blockchain_txn_consensus_group_v1_pb.delay.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_consensus_group()) -> 0.
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_consensus_group(), blockchain:blockchain()) -> ok.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Members = ?MODULE:members(Txn),
    Delay = ?MODULE:delay(Txn),
    Proof0 = ?MODULE:proof(Txn),
    case Members of
        [] -> {error, no_members};
        _ ->
            case blockchain_ledger_v1:election_height(Ledger) of
                {error, not_found} ->
                    ok;
                {ok, Height} ->
                    TxnHeight = ?MODULE:height(Txn),
                    case TxnHeight > Height of
                        true ->
                            Proof = binary_to_term(Proof0),
                            EffectiveHeight = TxnHeight + Delay,
                            {ok, OldLedger} = blockchain:ledger_at(EffectiveHeight, Chain),
                            {ok, Block} = blockchain:get_block(EffectiveHeight, Chain),
                            Hash = blockchain_block:hash_block(Block),
                            verify_proof(Proof, Members, Hash, OldLedger);
                        _ ->
                            {error, {duplicate_group, ?MODULE:height(Txn), Height}}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_consensus_group(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Members = ?MODULE:members(Txn),
    Height = ?MODULE:height(Txn),
    blockchain_ledger_v1:consensus_members(Members, Ledger),
    blockchain_ledger_v1:election_height(Height, Ledger),
    case Height - 30 < 0 of
        true ->
            ok;
        false ->
            Start = Height - 30,
            End = Height,
            Transactions = get_txns_for_epoch(Start, End, Chain),
            Vars = get_reward_vars(Chain),
            ok = consensus_members_rewards(Txn, Chain, Vars),
            ok = securities_rewards(Chain, Vars),
            ok = poc_challengers_rewards(Transactions, Chain, Vars),
            ok = poc_challengees_rewards(Transactions, Chain, Vars),
            ok = poc_witnesses_rewards(Transactions, Chain, Vars)
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
verify_proof(Proof, Members, Hash, OldLedger) ->
    %% verify that the list is the proper list
    L = length(Members),
    HashMembers = blockchain_election:new_group(OldLedger, Hash, L),
    Artifact = term_to_binary(Members),
    case HashMembers of
        Members ->
            %% verify all the signatures
            %% verify that the signatories are all in the members list
            case lists:all(fun({Addr, Sig}) ->
                                   lists:member(Addr, Members) andalso
                                       libp2p_crypto:verify(Artifact, Sig,
                                                            libp2p_crypto:bin_to_pubkey(Addr))

                           end, Proof) andalso
                lists:all(fun(M) ->
                                  lists:keymember(M, 1, Proof)
                          end, Members) of
                true ->
                    ok;
                _ ->
                    {error, group_verification_failed}
            end;
        _ ->
            lager:info("groups didn't match: ~ntxn ~p ~nhash ~p", [Members, HashMembers]),
            {error, group_mismatch}
    end.

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
get_reward_vars(Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, MonthlyReward} = blockchain:config(monthly_reward, Ledger),
    {ok, SecuritiesPercent} = blockchain:config(securities_percent, Ledger),
    {ok, PocChallengeesPercent} = blockchain:config(poc_challengees_percent, Ledger),
    {ok, PocChallengersPercent} = blockchain:config(poc_challengers_percent, Ledger),
    {ok, PocWitnessesPercent} = blockchain:config(poc_witnesses_percent, Ledger),
    {ok, ConsensusPercent} = blockchain:config(consensus_percent, Ledger),
    #{
        monthly_reward => MonthlyReward,
        epoch_reward => MonthlyReward/30/24/2,
        securities_percent => SecuritiesPercent,
        poc_challengees_percent => PocChallengeesPercent,
        poc_challengers_percent => PocChallengersPercent,
        poc_witnesses_percent => PocWitnessesPercent,
        consensus_percent => ConsensusPercent
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
consensus_members_rewards(_Txn, Chain, #{epoch_reward := EpochReward,
                                         consensus_percent := ConsensusPercent}) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:consensus_members(Ledger) of
        {error, _Reason} ->
            lager:error("failed to get consensus_members ~p", [_Reason]);
            % TODO: Should we error out here?
        {ok, ConsensusMembers} ->
            ConsensusReward = EpochReward * ConsensusPercent,
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
securities_rewards(Chain, #{epoch_reward := EpochReward,
                            securities_percent := SecuritiesPercent}) ->
    Ledger = blockchain:ledger(Chain),
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
poc_challengers_rewards(Transactions, Chain, #{epoch_reward := EpochReward,
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
    Ledger = blockchain:ledger(Chain),
    ChallengersReward = EpochReward * PocChallengersPercent,
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
poc_challengees_rewards(Transactions, Chain, #{epoch_reward := EpochReward,
                                               poc_challengees_percent := PocChallengeesPercent}) ->
    ChallengeesReward = EpochReward * PocChallengeesPercent,
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
poc_witnesses_rewards(Transactions, Chain, #{epoch_reward := EpochReward,
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
    WitnessesReward = EpochReward * PocWitnessesPercent,
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
    Tx = #blockchain_txn_consensus_group_v1_pb{members = [<<"1">>],
                                               proof = <<"proof">>,
                                               height = 1,
                                               delay = 0},
    ?assertEqual(Tx, new([<<"1">>], <<"proof">>, 1, 0)).

members_test() ->
    Tx = new([<<"1">>], <<"proof">>, 1, 0),
    ?assertEqual([<<"1">>], members(Tx)).

-endif.
