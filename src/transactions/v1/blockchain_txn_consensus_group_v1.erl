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
    try
        %% TODO: check and make sure that Members is the right size
        case Members of
            [] ->
                throw({error, no_members});
            _ ->
                ok
        end,
        TxnHeight = ?MODULE:height(Txn),
        case blockchain:height(Chain) of
            %% no chain, genesis block
            {error, not_found} ->
                ok;
            {ok, CurrHeight} ->
                {ok, CurrBlock} = blockchain:get_block(CurrHeight, Chain),
                {_, LastElectionHeight} = blockchain_block_v1:election_info(CurrBlock),
                {ok, ElectionInterval} = blockchain:config(election_interval, Ledger),
                %% The next election should be at least ElectionInterval blocks past the last election
                %% This check prevents elections ahead of schedule
                case TxnHeight >= LastElectionHeight + ElectionInterval  of
                    true ->
                        Proof = binary_to_term(Proof0),
                        EffectiveHeight = TxnHeight + Delay,
                        {ok, OldLedger} = blockchain:ledger_at(EffectiveHeight, Chain),
                        {ok, Block} = blockchain:get_block(EffectiveHeight, Chain),
                        {ok, RestartInterval} = blockchain:config(election_restart_interval, Ledger),
                        %% The next election should occur within RestartInterval blocks of when the election started
                        case CurrHeight > LastElectionHeight + ElectionInterval + Delay + RestartInterval of
                            true ->
                                throw({error, {txn_too_old, CurrHeight, LastElectionHeight + ElectionInterval + Delay + RestartInterval}});
                            _ ->
                                ok
                        end,
                        {ok, N} = blockchain:config(num_consensus_members, Ledger),
                        case length(Members) == N of
                            true -> ok;
                            _ -> throw({error, {wrong_members_size, N, length(Members)}})
                        end,
                        Hash = blockchain_block:hash_block(Block),
                        case verify_proof(Proof, Members, Hash, Delay, OldLedger) of
                            ok -> ok;
                            {error, _} = VerifyErr -> throw(VerifyErr)
                        end;
                    _ ->
                        throw({error, {election_too_early, TxnHeight, LastElectionHeight}})
                end,
                case blockchain_ledger_v1:election_height(Ledger) of
                    %% no chain, genesis block
                    {error, not_found} ->
                        ok;
                    {ok, BaseHeight} when TxnHeight > BaseHeight ->
                        ok;
                    _ ->
                        throw({error, {duplicate_group, ?MODULE:height(Txn), LastElectionHeight}})
                end
        end
    catch throw:E ->
            E
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
    {ok, Epoch} = blockchain_ledger_v1:election_epoch(Ledger),
    ok = blockchain_ledger_v1:election_epoch(Epoch + 1, Ledger),
    blockchain_ledger_v1:consensus_members(Members, Ledger),
    blockchain_ledger_v1:election_height(Height, Ledger).


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
verify_proof(Proof, Members, Hash, Delay, OldLedger) ->
    %% verify that the list is the proper list
    L = length(Members),
    HashMembers = blockchain_election:new_group(OldLedger, Hash, L, Delay),
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
