%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Consensus Group ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_consensus_group_failure_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_txn_consensus_group_failure_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    failed_members/1,
    members/1,
    height/1,
    delay/1,
    signatures/1,
    fee/1,
    fee_payer/2,
    sign/2,
    verify_signature/3,
    set_signatures/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-define(T, #blockchain_txn_consensus_group_failure_v1_pb).

-type t() :: txn_consensus_group_failure().

-type txn_consensus_group_failure() :: ?T{}.

-export_type([t/0, txn_consensus_group_failure/0]).

-spec new([libp2p_crypto:pubkey_bin()], pos_integer(), non_neg_integer()) ->
    txn_consensus_group_failure().
new(FailedMembers, Height, Delay) ->
    #blockchain_txn_consensus_group_failure_v1_pb{
        failed_members = FailedMembers,
        height = Height,
        delay = Delay
    }.

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

-spec fee_payer(txn_consensus_group_failure(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec sign(txn_consensus_group_failure(), libp2p_crypto:sig_fun()) -> binary().
sign(Txn, SigFun) ->
    Artifact = blockchain_txn_consensus_group_failure_v1_pb:encode_msg(
        Txn#blockchain_txn_consensus_group_failure_v1_pb{members = [], signatures = []}
    ),
    SigFun(Artifact).

-spec verify_signature(txn_consensus_group_failure(), libp2p_crypto:pubkey_bin(), binary()) ->
    boolean().
verify_signature(Txn, Address, Signature) ->
    Artifact = blockchain_txn_consensus_group_failure_v1_pb:encode_msg(
        Txn#blockchain_txn_consensus_group_failure_v1_pb{members = [], signatures = []}
    ),
    libp2p_crypto:verify(
        Artifact,
        Signature,
        libp2p_crypto:bin_to_pubkey(Address)
    ).

-spec set_signatures(txn_consensus_group_failure(), [{libp2p_crypto:pubkey_bin(), binary()}]) ->
    txn_consensus_group_failure().
set_signatures(Txn, AddrsAndSignatures) ->
    {Members, Signatures} = lists:unzip(AddrsAndSignatures),
    Txn#blockchain_txn_consensus_group_failure_v1_pb{members = Members, signatures = Signatures}.

-spec is_valid(txn_consensus_group_failure(), blockchain:blockchain()) ->
    {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    FailedMembers = ?MODULE:failed_members(Txn),
    Delay = ?MODULE:delay(Txn),
    TxnHeight = ?MODULE:height(Txn),
    ReportHeight = TxnHeight + Delay,
    {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),
    #{
        election_height := ElectionHeight,
        start_height := StartHeight,
        election_delay := ElectionDelay
    } = blockchain_election:election_info(CurrHeight, Ledger),

    try
        case blockchain_ledger_v1:config(?election_version, Ledger) of
            {ok, N} when N >= 5 -> ok;
            _ -> throw(no_validators)
        end,
        case FailedMembers of
            [] -> throw(no_members);
            _ -> ok
        end,
        case CurrHeight of
            %% no chain, genesis block
            0 -> throw(invalid_in_genesis_block);
            _ -> ok
        end,

        %% is the proof reasonable?
        {ok, OldLedger} = blockchain:ledger_at(ReportHeight, Chain),
        {ok, #block_info_v2{hash = Hash}} = blockchain:get_block_info(ReportHeight, Chain),
        case verify_proof(Txn, Hash, OldLedger) of
            ok -> ok;
            {error, VerifyErr} -> throw(VerifyErr)
        end,

        %% has there already been a report about this dkg
        lists:foreach(
          fun(M) ->
                  case blockchain_ledger_v1:get_validator(M, Ledger) of
                      {ok, V} ->
                          Failures = blockchain_ledger_validator_v1:penalties(V),
                          case lists:any(fun(Penalty) ->
                                                 Type = blockchain_ledger_validator_v1:penalty_type(Penalty),
                                                 Ht = blockchain_ledger_validator_v1:penalty_height(Penalty),
                                                 Type == dkg andalso
                                                     Ht == TxnHeight + Delay
                                         end, Failures) of
                              true ->
                                  throw(already_absorbed);
                              false ->
                                  ok
                          end;
                      GetErr ->
                          throw({bad_validator, GetErr})
                  end
          end, FailedMembers),

        {ok, ElectionInterval} = blockchain:config(?election_interval, OldLedger),
        {ok, ElectionRestartInterval} = blockchain:config(?election_restart_interval, OldLedger),
        %% clean up ledger context
        blockchain_ledger_v1:delete_context(OldLedger),
        case Delay rem ElectionRestartInterval of
            0 -> ok;
            _ -> throw(bad_restart_height)
        end,
        case StartHeight of
            %% before a successful election
            BaseHeight when TxnHeight > ElectionHeight ->
                %% need more here?  I'm not sure what else to check, already checked delay
                case TxnHeight == BaseHeight + ElectionInterval of
                    true -> ok;
                    _ -> throw({bad_election_height, TxnHeight, BaseHeight + ElectionInterval})
                end,
                ok;
            %% after a successful election
            _BaseHeight when TxnHeight == ElectionHeight ->
                case Delay == ElectionDelay of
                    true -> throw(successful_election);
                    _ -> ok
                end;
            %% too far, we've elected since
            _ ->
                throw(too_old)
        end
    catch
        throw:E ->
            {error, E}
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

verify_proof(Txn, Hash, OldLedger) ->
    %% verify that the list is the proper list
    {ok, L} = blockchain:config(?num_consensus_members, OldLedger),
    Members = blockchain_election:new_group(OldLedger, Hash, L, delay(Txn)),
    F = floor((length(Members) - 1) / 3),
    Artifact = blockchain_txn_consensus_group_failure_v1_pb:encode_msg(
        Txn#blockchain_txn_consensus_group_failure_v1_pb{members = [], signatures = []}
    ),

    %% verify all the signatures
    %% verify that the signatories are all in the members list
    case
        lists:all(
            fun({Addr, Sig}) ->
                lists:member(Addr, Members) andalso
                    libp2p_crypto:verify(
                        Artifact,
                        Sig,
                        libp2p_crypto:bin_to_pubkey(Addr)
                    )
            end,
            lists:zip(members(Txn), signatures(Txn))
        ) andalso
            %% check all the failed members are in the group
            lists:all(fun(Addr) -> lists:member(Addr, Members) end, failed_members(Txn)) andalso
            %% check the members and the failed members have no overlap
            not lists:any(fun(Addr) -> lists:member(Addr, members(Txn)) end, failed_members(Txn)) andalso
            %% verify we have enough signatures
            length(members(Txn)) >= (2 * F) + 1
    of
        true ->
            ok;
        _ ->
            {error, group_verification_failed}
    end.

-spec absorb(txn_consensus_group_failure(), blockchain:blockchain()) ->
    ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    FailedMembers = ?MODULE:failed_members(Txn),
    Delay = ?MODULE:delay(Txn),
    Height = ?MODULE:height(Txn),
    {ok, Limit} = blockchain_ledger_v1:config(?penalty_history_limit, Ledger),
    {ok, Penalty} = blockchain_ledger_v1:config(?dkg_penalty, Ledger),

    try
        lists:foreach(
          fun(M) ->
                  case blockchain_ledger_v1:get_validator(M, Ledger) of
                      {ok, V} ->
                          V1 = blockchain_ledger_validator_v1:add_penalty(V, Height+Delay, dkg, Penalty, Limit),
                          blockchain_ledger_v1:update_validator(M, V1, Ledger);
                      GetErr ->
                          throw({bad_validator, GetErr})
                  end
          end, FailedMembers)
    catch _:Err ->
            {error, Err}
    end.

-spec print(txn_consensus_group_failure()) -> iodata().
print(undefined) ->
    <<"type=group_failure, undefined">>;
print(#blockchain_txn_consensus_group_failure_v1_pb{
    height = Height,
    delay = Delay,
    failed_members = FailedMembers,
    members = Members
}) ->
    io_lib:format(
        "type=group_failure height=~p delay=~p members=~p failed_members=~p",
        [
            Height,
            Delay,
            lists:map(fun blockchain_utils:addr2name/1, Members),
            lists:map(fun blockchain_utils:addr2name/1, FailedMembers)
        ]
    ).

json_type() ->
    <<"consensus_group_failure_v1">>.

-spec to_json(txn_consensus_group_failure(), blockchain_json:opts()) ->
    blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
        type => ?MODULE:json_type(),
        hash => ?BIN_TO_B64(hash(Txn)),
        members => [?BIN_TO_B58(M) || M <- members(Txn)],
        failed_members => [?BIN_TO_B58(M) || M <- failed_members(Txn)],
        signatures => [?BIN_TO_B64(S) || S <- signatures(Txn)],
        height => height(Txn),
        delay => delay(Txn)
    }.
