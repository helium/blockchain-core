%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Consensus Group ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_consensus_group_failure_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_txn_consensus_group_failure_v1_pb.hrl").

-export([new/3, hash/1, failed_members/1, members/1, height/1, delay/1, signatures/1, fee/1, sign/2, verify_signature/3, set_signatures/2, is_valid/2, absorb/2, print/1, to_json/2]).

-type txn_consensus_group_failure() :: #blockchain_txn_consensus_group_failure_v1_pb{}.
-export_type([txn_consensus_group_failure/0]).

-spec new([libp2p_crypto:pubkey_bin()], pos_integer(), non_neg_integer()) -> txn_consensus_group_failure().
new(FailedMembers, Height, Delay) ->
    #blockchain_txn_consensus_group_failure_v1_pb{failed_members = FailedMembers,
                                                  height = Height,
                                                  delay = Delay}.

-spec hash(txn_consensus_group_failure()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_consensus_group_failure_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

-spec failed_members(txn_consensus_group_failure()) -> [libp2p_crypto:pubkey_bin()].
failed_members(Txn) ->
    Txn#blockchain_txn_consensus_group_failure_v1_pb.failed_members.

-spec members(txn_consensus_group_failure()) -> [libp2p_crypto:pubkey_bin()].
members(Txn) ->
    Txn#blockchain_txn_consensus_group_failure_v1_pb.members.

-spec height(txn_consensus_group_failure()) -> pos_integer().
height(Txn) ->
    Txn#blockchain_txn_consensus_group_failure_v1_pb.height.

-spec delay(txn_consensus_group_failure()) -> non_neg_integer().
delay(Txn) ->
    Txn#blockchain_txn_consensus_group_failure_v1_pb.delay.

-spec signatures(txn_consensus_group_failure()) -> [binary()].
signatures(Txn) ->
    Txn#blockchain_txn_consensus_group_failure_v1_pb.signatures.

-spec fee(txn_consensus_group_failure()) -> 0.
fee(_Txn) ->
    0.

-spec sign(txn_consensus_group_failure(), libp2p_crypto:sig_fun()) -> binary().
sign(Txn, SigFun) ->
    Artifact = blockchain_txn_consensus_group_failure_v1_pb:encode_msg(Txn#blockchain_txn_consensus_group_failure_v1_pb{members=[], signatures=[]}),
    SigFun(Artifact).

-spec verify_signature(txn_consensus_group_failure(), libp2p_crypto:pubkey_bin(), binary()) -> boolean().
verify_signature(Txn, Address, Signature) ->
    Artifact = blockchain_txn_consensus_group_failure_v1_pb:encode_msg(Txn#blockchain_txn_consensus_group_failure_v1_pb{members=[], signatures=[]}),
    libp2p_crypto:verify(Artifact, Signature,
                         libp2p_crypto:bin_to_pubkey(Address)).

-spec set_signatures(txn_consensus_group_failure(), [{libp2p_crypto:pubkey_bin(), binary()}]) -> txn_consensus_group_failure().
set_signatures(Txn, AddrsAndSignatures) ->
    {Members, Signatures} = lists:unzip(AddrsAndSignatures),
    Txn#blockchain_txn_consensus_group_failure_v1_pb{members=Members, signatures=Signatures}.

-spec is_valid(txn_consensus_group_failure(), blockchain:blockchain()) -> {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    FailedMembers = ?MODULE:failed_members(Txn),
    Delay = ?MODULE:delay(Txn),
    try
        case FailedMembers of
            [] ->
                throw({error, no_members});
            _ ->
                ok
        end,
        TxnHeight = ?MODULE:height(Txn),
        case blockchain_ledger_v1:current_height(Ledger) of
            %% no chain, genesis block
            {ok, 0} ->
                throw({error, invalid_in_genesis_block});
            {ok, CurrHeight} ->
                {ok, CurrBlock} = blockchain:get_block(CurrHeight, Chain),

                case blockchain_ledger_v1:election_height(Ledger) of
                    %% no chain, genesis block
                    {error, not_found} ->
                        throw({error, invalid_in_genesis_block});
                    {ok, BaseHeight} when TxnHeight > BaseHeight ->
                        ok;
                    {ok, BaseHeight} ->
                        throw({error, {duplicate_group, {?MODULE:height(Txn), BaseHeight}}})
                end,
                %% TODO check for replays here
                %% TODO sanity check these validity criteria
                {_, LastElectionHeight} = blockchain_block_v1:election_info(CurrBlock),
                {ok, ElectionInterval} = blockchain:config(?election_interval, Ledger),
                %% The next election should be at least ElectionInterval blocks past the last election
                %% This check prevents elections ahead of schedule
                case TxnHeight >= LastElectionHeight + ElectionInterval of
                    true ->
                        EffectiveHeight = LastElectionHeight + ElectionInterval + Delay,
                        {ok, Block} = blockchain:get_block(EffectiveHeight, Chain),
                        {ok, RestartInterval} = blockchain:config(?election_restart_interval, Ledger),
                        %% The next election should occur within RestartInterval blocks of when the election started
                        NextRestart = LastElectionHeight + ElectionInterval + Delay + RestartInterval,
                        case CurrHeight > NextRestart of
                            true ->
                                throw({error, {txn_too_old, {CurrHeight, NextRestart}}});
                            _ ->
                                ok
                        end,
                        Hash = blockchain_block:hash_block(Block),
                        {ok, OldLedger} = blockchain:ledger_at(EffectiveHeight, Chain),
                        case verify_proof(Txn, Hash, OldLedger) of
                            ok -> ok;
                            {error, _} = VerifyErr -> throw(VerifyErr)
                        end;
                    _ ->
                        throw({error, {election_too_early, {TxnHeight,
                                       LastElectionHeight + ElectionInterval}}})
                end
        end
    catch throw:E ->
            E
    end.

verify_proof(Txn, Hash, OldLedger) ->
    %% verify that the list is the proper list
    {ok, L} = blockchain:config(?num_consensus_members, OldLedger),
    Members = blockchain_election:new_group(OldLedger, Hash, L, delay(Txn)),
    %% clean up ledger context
    blockchain_ledger_v1:delete_context(OldLedger),

    F = floor((length(Members) - 1) / 3),
    Artifact = blockchain_txn_consensus_group_failure_v1_pb:encode_msg(Txn#blockchain_txn_consensus_group_failure_v1_pb{members=[], signatures=[]}),

    %% verify all the signatures
    %% verify that the signatories are all in the members list
    case lists:all(fun({Addr, Sig}) ->
                           lists:member(Addr, Members) andalso
                           libp2p_crypto:verify(Artifact, Sig,
                                                libp2p_crypto:bin_to_pubkey(Addr))

                   end, lists:zip(members(Txn), signatures(Txn))) andalso
         %% check all the failed members are in the group
         lists:all(fun(Addr) -> lists:member(Addr, Members) end, failed_members(Txn)) andalso
         %% check the members and the failed members have no overlap
         not lists:any(fun(Addr) -> lists:member(Addr, members(Txn)) end, failed_members(Txn)) andalso
         %% verify we have enough signatures
         length(members(Txn)) >= (3*F)+1 of
        true ->
            ok;
        _ ->
            {error, group_verification_failed}
    end.

-spec absorb(txn_consensus_group_failure(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    FailedMembers = ?MODULE:failed_members(Txn),
    Delay = ?MODULE:delay(Txn),
    Height = ?MODULE:height(Txn),

    %% TODO: recheck replay protection here for races

    try
        lists:foreach(
          fun(M) ->
                  case blockchain_ledger_v1:get_validator(M, Ledger) of
                      {ok, V} ->
                          V1 = blockchain_ledger_validator_v1:add_recent_failure(V, Height, Delay, Ledger),
                          blockchain_ledger_v1:update_validator(M, V1, Ledger);
                      GetErr ->
                          throw({bad_validator, GetErr})
                  end
          end, FailedMembers)
    catch _:Err ->
            {error, Err}
    end,
    ok.

-spec print(txn_consensus_group_failure()) -> iodata().
print(undefined) -> <<"type=group_failure, undefined">>;
print(#blockchain_txn_consensus_group_failure_v1_pb{height = Height,
                                            delay = Delay,
                                            failed_members = FailedMembers,
                                            members = Members}) ->
    io_lib:format("type=group_failure height=~p delay=~p members=~p failed_members=~p",
                  [Height,
                   Delay,
                   lists:map(fun blockchain_utils:addr2name/1, Members),
                   lists:map(fun blockchain_utils:addr2name/1, FailedMembers)]).

-spec to_json(txn_consensus_group_failure(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"consensus_group_failure_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      members => [?BIN_TO_B58(M) || M <- members(Txn)],
      failed_members => [?BIN_TO_B58(M) || M <- members(Txn)],
      signatures => [?BIN_TO_B64(S) || S <- signatures(Txn)],
      height => height(Txn),
      delay => delay(Txn)
     }.


