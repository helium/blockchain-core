%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Consensus Group ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_consensus_group_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain.hrl").

-include_lib("helium_proto/include/blockchain_txn_consensus_group_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    sign/2,
    members/1,
    proof/1,
    height/1,
    delay/1,
    fee/1,
    fee_payer/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_consensus_group_v1_pb).

-type t() :: txn_consensus_group().

-type txn_consensus_group() :: ?T{}.

-export_type([t/0, txn_consensus_group/0]).

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

-spec fee_payer(txn_consensus_group(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_consensus_group(), blockchain:blockchain()) -> {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Members = ?MODULE:members(Txn),
    Delay = ?MODULE:delay(Txn),
    Proof0 = ?MODULE:proof(Txn),
    try
        case Members of
            [] ->
                throw({error, no_members});
            _ ->
                ok
        end,
        TxnHeight = ?MODULE:height(Txn),
        case blockchain_ledger_v1:current_height(Ledger) of
            %% no chain, genesis block
            {ok, 0} ->
                ok;
            {ok, CurrHeight} ->
                {ok, #block_info_v2{election_info={_, LastElectionHeight}}} = blockchain:get_block_info(CurrHeight, Chain),

                case blockchain_ledger_v1:election_height(Ledger) of
                    %% no chain, genesis block
                    {error, not_found} ->
                        ok;
                    {ok, BaseHeight} when TxnHeight > BaseHeight ->
                        ok;
                    {ok, BaseHeight} ->
                        throw({error, {duplicate_group, {?MODULE:height(Txn), BaseHeight}}})
                end,
                {ok, ElectionInterval} = blockchain:config(?election_interval, Ledger),
                %% The next election should be at least ElectionInterval blocks past the last election
                %% This check prevents elections ahead of schedule
                case TxnHeight >= LastElectionHeight + ElectionInterval of
                    true ->
                        Proof = binary_to_term(Proof0),
                        EffectiveHeight = LastElectionHeight + ElectionInterval + Delay,
                        {ok, Block} = blockchain:get_block(EffectiveHeight, Chain),
                        {ok, RestartInterval} = blockchain:config(?election_restart_interval, Ledger),
                        IntervalRange =
                            case blockchain:config(?election_restart_interval_range, Ledger) of
                                {ok, IR} -> IR;
                                _ -> 1
                            end,
                        %% The next election should occur within RestartInterval blocks of when the election started
                        NextRestart = LastElectionHeight + ElectionInterval + Delay +
                            (RestartInterval * IntervalRange),
                        case CurrHeight > NextRestart of
                            true ->
                                throw({error, {txn_too_old, {CurrHeight, NextRestart}}});
                            _ ->
                                ok
                        end,
                        {ok, N} = blockchain:config(?num_consensus_members, Ledger),
                        case length(Members) == N of
                            true -> ok;
                            _ -> throw({error, {wrong_members_size, {N, length(Members)}}})
                        end,
                        %% if we're on validators make sure that everyone is staked
                        case blockchain_ledger_v1:config(?election_version, Ledger) of
                            {ok, N} when N >= 5 ->
                                case lists:all(fun(M) ->
                                                       {ok, V} = blockchain_ledger_v1:get_validator(M, Ledger),
                                                       blockchain_ledger_validator_v1:status(V) == staked end,
                                               Members) of
                                    true -> ok;
                                    false -> throw({error, not_all_validators_staked})
                                end;
                            _ -> ok
                        end,
                        Hash = blockchain_block:hash_block(Block),
                        {ok, OldLedger} = blockchain:ledger_at(EffectiveHeight, Chain),
                        case verify_proof(Proof, Members, Hash, Delay, OldLedger) of
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

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_consensus_group(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Height = ?MODULE:height(Txn),
    Ledger = blockchain:ledger(Chain),
    Members = ?MODULE:members(Txn),
    {Gen, Check} =
        case blockchain_ledger_v1:election_height(Ledger) of
            %% no chain, genesis block
            {error, not_found} ->
                {true, ok};
            {ok, BaseHeight} when Height > BaseHeight ->
                {false, ok};
            {ok, BaseHeight} ->
                {false, {error, {duplicate_group, {?MODULE:height(Txn), BaseHeight}}}}
        end,
    case Check of
        ok ->
            case blockchain_ledger_v1:config(?election_version, Ledger) of
                {ok, N} when N >= 5 andalso Gen == false ->
                    {ok, PenaltyLimit} = blockchain_ledger_v1:config(?penalty_history_limit, Ledger),
                    {ok, TenurePenalty} = blockchain_ledger_v1:config(?tenure_penalty, Ledger),
                    {ok, OldMembers0} = blockchain_ledger_v1:consensus_members(Ledger),
                    {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),

                    OldMembers = lists:filter(fun(X) -> is_validator(X, Ledger) end, OldMembers0),
                    EpochPenalties =
                        case OldMembers == OldMembers0 of
                            %% no gateways to mess up the adjustment
                            true ->
                                blockchain_election:validator_penalties(OldMembers, Ledger);
                            false -> #{}
                        end,

                    %% apply tenure penalties to new members at the start of the round
                    lists:foreach(
                      fun(M) ->
                              {ok, V} = blockchain_ledger_v1:get_validator(M, Ledger),
                              V1 = blockchain_ledger_validator_v1:add_penalty(V, CurrHeight,
                                                                              tenure,
                                                                              TenurePenalty,
                                                                              PenaltyLimit),
                              blockchain_ledger_v1:update_validator(M, V1, Ledger)
                      end,
                      Members),
                    %% persist performance penalties for all validators in the last epoch
                    lists:foreach(
                      fun(M) ->
                              {ok, V} = blockchain_ledger_v1:get_validator(M, Ledger),
                              V1 = case maps:get(M, EpochPenalties, none) of
                                       none -> V;
                                       0.0 -> V;
                                       Penalty ->
                                           blockchain_ledger_validator_v1:add_penalty(V,
                                                                                      CurrHeight,
                                                                                      performance,
                                                                                      Penalty,
                                                                                      PenaltyLimit)
                                   end,
                              blockchain_ledger_v1:update_validator(M, V1, Ledger)
                      end,
                      OldMembers);
                _ -> ok
            end,
            {ok, Epoch} = blockchain_ledger_v1:election_epoch(Ledger),
            ok = blockchain_ledger_v1:election_epoch(Epoch + 1, Ledger),
            ok = blockchain_ledger_v1:consensus_members(Members, Ledger),
            ok = blockchain_ledger_v1:election_height(Height, Ledger);
        {error, _} = Err ->
            Err
    end.

is_validator(Addr, Ledger) ->
    case blockchain_ledger_v1:get_validator(Addr, Ledger) of
        {ok, _V} -> true;
        _ -> false
    end.

-spec print(txn_consensus_group()) -> iodata().
print(undefined) -> <<"type=group, undefined">>;
print(#blockchain_txn_consensus_group_v1_pb{height = Height,
                                            delay = Delay,
                                            members = Members,
                                            proof = Proof}) ->
    io_lib:format("type=group height=~p delay=~p members=~p proof_hash=~p",
                  [Height,
                   Delay,
                   lists:map(fun blockchain_utils:addr2name/1, Members),
                   erlang:phash2(Proof)]).

json_type() ->
    <<"consensus_group_v1">>.

-spec to_json(txn_consensus_group(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      members => [?BIN_TO_B58(M) || M <- members(Txn)],
      proof => ?BIN_TO_B64(proof(Txn)),
      height => height(Txn),
      delay => delay(Txn)
     }.

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
    %% clean up ledger context
    blockchain_ledger_v1:delete_context(OldLedger),
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
            lager:info("groups didn't match: ~p ~p ~ntxn ~p ~nhash ~p",
                       [length(Members), length(HashMembers),
                        lists:map(fun blockchain_utils:addr2name/1, Members),
                        lists:map(fun blockchain_utils:addr2name/1, HashMembers)]),
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

to_json_test() ->
    Tx = new([<<"1">>], <<"proof">>, 1, 0),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, members, proof, height, delay])).

-endif.
