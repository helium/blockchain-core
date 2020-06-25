%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Chain Vars ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_vars_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").

-include_lib("helium_proto/include/blockchain_txn_vars_v1_pb.hrl").
-include("blockchain_vars.hrl").

-export([
         new/2, new/3,
         hash/1,
         fee/1,
         is_valid/2,
         master_key/1,
         key_proof/1, key_proof/2,
         proof/1, proof/2,
         vars/1,
         decoded_vars/1,
         version_predicate/1,
         unsets/1,
         cancels/1,
         nonce/1,
         absorb/2,
         rescue_absorb/2,
         sign/2,
         print/1,
         to_json/2
        ]).

%% helper API
-export([
         create_proof/2,
         legacy_create_proof/2,
         maybe_absorb/3,
         delayed_absorb/2
        ]).

-ifdef(TEST).
-include_lib("stdlib/include/assert.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(exceptions, [garbage_value]).
-define(allowed_predicate_funs, [test_version, version]).

-else.

-define(exceptions, []).
-define(allowed_predicate_funs, [version]).

-endif.

-ifdef(TEST).
-define(min_snap_interval, 1).
-else.
-define(min_snap_interval, 4*60).
-endif.

-type txn_vars() :: #blockchain_txn_vars_v1_pb{}.
-export_type([txn_vars/0]).

%% message var_v1 {
%%     string type = 1;
%%     bytes value = 2;
%% }

%% message txn_vars_v1 {
%%     map<string, var> vars = 1;
%%     uint32 version_predicate = 2;
%%     bytes proof = 3;
%%     bytes master_key = 4;
%%     bytes key_proof = 5;
%%     repeated bytes cancels = 6;
%%     repeated bytes unsets = 7;
%%     uint32 nonce = 8;
%% }


-spec new(#{}, integer()) -> txn_vars().
new(Vars, Nonce) ->
    new(Vars, Nonce, #{}).

-spec new(#{atom() => any()}, integer(), #{atom() => any()}) -> txn_vars().
new(Vars, Nonce, Optional) ->
    VersionP = maps:get(version_predicate, Optional, 0),
    MasterKey = maps:get(master_key, Optional, <<>>),
    KeyProof = maps:get(key_proof, Optional, <<>>),
    Unsets = maps:get(unsets, Optional, []),
    Cancels = maps:get(cancels, Optional, []),
    %% note that string inputs are normalized on creation, which has
    %% an effect on proof encoding :/

    #blockchain_txn_vars_v1_pb{vars = lists:sort(encode_vars(Vars)),
                               version_predicate = VersionP,
                               master_key = MasterKey,
                               key_proof = KeyProof,
                               unsets = encode_unsets(Unsets),
                               cancels = Cancels,
                               nonce = Nonce}.


encode_vars(Vars) ->
    V = maps:to_list(Vars),
    lists:map(fun({K, Val}) ->
                      to_var(atom_to_list(K), Val)
              end,V).

encode_unsets(Unsets) ->
    lists:map(fun(U) -> atom_to_binary(U, utf8) end, Unsets).

to_var(Name, V) when is_list(V) orelse is_binary(V) ->
    #blockchain_var_v1_pb{name = Name, type = "string", value = iolist_to_binary(V)};
to_var(Name, V) when is_integer(V) ->
    #blockchain_var_v1_pb{name = Name, type = "int", value = integer_to_binary(V)};
to_var(Name, V) when is_float(V) ->
    #blockchain_var_v1_pb{name = Name, type = "float", value = float_to_binary(V)};
to_var(Name, V) when is_atom(V) ->
    #blockchain_var_v1_pb{name = Name, type = "atom", value = atom_to_binary(V, utf8)};
to_var(_Name, _V) ->
    error(bad_var_type).

decode_vars(PBList) ->
    lists:foldl(
      fun(V, Acc) ->
              {K, V1} = from_var(V),
              K1 = list_to_atom(K),
              Acc#{K1 => V1}
      end,
      #{},
      PBList).

from_var(#blockchain_var_v1_pb{name = Name, type = "string", value = V}) ->
    {Name, V};
from_var(#blockchain_var_v1_pb{name = Name, type = "int", value = V}) ->
    {Name, binary_to_integer(V)};
from_var(#blockchain_var_v1_pb{name = Name, type = "float", value = V}) ->
    {Name, binary_to_float(V)};
from_var(#blockchain_var_v1_pb{name = Name, type = "atom", value = V}) ->
    {Name, binary_to_atom(V, utf8)};
from_var(_V) ->
    error(bad_var_type).

-spec hash(txn_vars()) -> blockchain_txn:hash().
hash(Txn) ->
    Vars = vars(Txn),
    Vars1 = lists:sort(Vars),
    EncodedTxn = blockchain_txn_vars_v1_pb:encode_msg(Txn#blockchain_txn_vars_v1_pb{vars = Vars1}),
    crypto:hash(sha256, EncodedTxn).

-spec sign(txn_vars(), libp2p_crypto:sig_fun()) -> txn_vars().
sign(Txn, _SigFun) ->
    Txn.

-spec fee(txn_vars()) -> non_neg_integer().
fee(_Txn) ->
    0.

master_key(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.master_key.

key_proof(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.key_proof.

key_proof(Txn, Proof) ->
    Txn#blockchain_txn_vars_v1_pb{key_proof = Proof}.

proof(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.proof.

proof(Txn, Proof) ->
    Txn#blockchain_txn_vars_v1_pb{proof = Proof}.

unset_proofs(Txn) ->
    Txn#blockchain_txn_vars_v1_pb{proof = <<>>,
                                  key_proof = <<>>}.

vars(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.vars.

decoded_vars(Txn) ->
    decode_vars(Txn#blockchain_txn_vars_v1_pb.vars).

version_predicate(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.version_predicate.

unsets(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.unsets.

cancels(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.cancels.

nonce(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.nonce.

-spec is_valid(txn_vars(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Gen =
        case blockchain_ledger_v1:current_height(Ledger) of
            {ok, 0} ->
                true;
            _ ->
                false
        end,
    Vars = decode_vars(vars(Txn)),
    Version =
        case blockchain:config(?chain_vars_version, Ledger) of
            {ok, 2} ->
                2;
            {error, not_found} when Gen == true ->
                %% if this isn't on the ledger, allow the contents of
                %% the genesis block to choose the validation type
                maps:get(?chain_vars_version, Vars, 1);
            _ ->
                1
        end,
    case Version of
        2 ->
            Artifact = create_artifact(Txn),
            lager:debug("validating vars ~p artifact ~p", [Vars, Artifact]),
            try
                Nonce = nonce(Txn),
                case blockchain_ledger_v1:vars_nonce(Ledger) of
                    {ok, LedgerNonce} when Nonce == (LedgerNonce + 1) ->
                        ok;
                    {error, not_found} when Gen == true ->
                        ok;
                    {error, not_found} ->
                        throw({error, missing_ledger_nonce});
                    {ok, LedgerNonce} ->
                        throw({error, bad_nonce, {exp, (LedgerNonce + 1), {got, Nonce}}})
                end,

                %% here we can accept a valid master key
                case master_key(Txn) of
                    <<>> when Gen == true ->
                        throw({error, genesis_requires_master_key});
                    <<>> ->
                        ok;
                    Key ->
                        KeyProof =
                            case key_proof(Txn) of
                                <<>> ->
                                    throw({error, no_master_key_proof}),
                                    <<>>;
                                P ->
                                    P
                            end,
                        case verify_key(Artifact, Key, KeyProof) of
                            true ->
                                ok;
                            _ ->
                                throw({error, bad_master_key})
                        end
                end,
                case Gen of
                    true ->
                        %% genesis block requires master key and has already
                        %% validated the proof if it has made it here.
                        ok;
                    _ ->
                        {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
                        case verify_key(Artifact, MasterKey, proof(Txn)) of
                            true ->
                                ok;
                            _ ->
                                throw({error, bad_block_proof})
                        end
                end,
                %% NB: validation errors MUST throw
                maps:map(fun validate_var/2, Vars),
                lists:foreach(
                  fun(VarName) ->
                          case blockchain:config(VarName, Ledger) of % ignore this one using "?"
                              {ok, _} -> ok;
                              {error, not_found} -> throw({error, {unset_var_not_set, VarName}})
                          end
                  end,
                  decode_unsets(unsets(Txn))),

                %% TODO: validate that a cancelled transaction is actually on
                %% the chain

                %% TODO: figure out how to validate that predicate functions
                %% actually exist when set, without breaking applications like
                %% the router and the API that only want to validate vars.

                ok
            catch throw:Ret ->
                    lager:error("invalid chain var transaction: ~p reason ~p", [Txn, Ret]),
                    Ret
            end;
        1 ->
            legacy_is_valid(Txn, Chain)
    end.

-spec legacy_is_valid(txn_vars(), blockchain:blockchain()) -> ok | {error, any()}.
legacy_is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Vars = decode_vars(vars(Txn)),
    Artifact = term_to_binary(Vars, [{compressed, 9}]),
    lager:debug("validating vars ~p artifact ~p", [Vars, Artifact]),
    try
        Gen =
            case blockchain_ledger_v1:current_height(Ledger) of
                {ok, 0} ->
                    true;
                _ ->
                    false
            end,

        Nonce = nonce(Txn),
        case blockchain_ledger_v1:vars_nonce(Ledger) of
            {ok, LedgerNonce} when Nonce > LedgerNonce ->
                ok;
            {error, not_found} when Gen == true ->
                ok;
            _ ->
                throw({error, bad_nonce})
        end,

        %% here we can accept a valid master key
        case master_key(Txn) of
            <<>> when Gen == true ->
                throw({error, genesis_requires_master_key});
            <<>> ->
                ok;
            Key ->
                KeyProof =
                    case key_proof(Txn) of
                        <<>> ->
                            throw({error, no_master_key_proof}),
                            <<>>;
                        P ->
                            P
                    end,
                case verify_key(Artifact, Key, KeyProof) of
                    true ->
                        ok;
                    _ ->
                        throw({error, bad_master_key})
                end
        end,
        case Gen of
            true ->
                %% genesis block requires master key and has already
                %% validated the proof if it has made it here.
                ok;
            _ ->
                {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
                case verify_key(Artifact, MasterKey, proof(Txn)) of
                    true ->
                        ok;
                    _ ->
                        throw({error, bad_block_proof})
                end
        end,
        lists:foreach(
          fun(VarName) ->
                  case blockchain:config(VarName, Ledger) of % ignore this one using "?"
                      {ok, _} -> ok;
                      {error, not_found} -> throw({error, {unset_var_not_set, VarName}})
                  end
          end,
          decode_unsets(unsets(Txn))),

        %% TODO: validate that a cancelled transaction is actually on
        %% the chain

        %% TODO: figure out how to validate that predicate functions
        %% actually exist when set, without breaking applications like
        %% the router and the API that only want to validate vars.

        ok
    catch throw:Ret ->
            lager:error("invalid chain var transaction: ~p reason ~p", [Txn, Ret]),
            Ret
    end.

%% TODO: we need a generalized hook here for when chain vars change
%% and invalidate something in the ledger, to enable stuff to stay consistent
-spec absorb(txn_vars(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),

    %% we absorb the nonce here to prevent replays of this txn, even
    %% if we cannot absorb the full txn right now.
    ok = blockchain_ledger_v1:vars_nonce(nonce(Txn), Ledger),

    case maybe_absorb(Txn, Ledger, Chain) of
        true ->
            ok;
        false ->
            ok = blockchain_ledger_v1:save_threshold_txn(Txn, Ledger)
    end.

%% in a rescue situation, we apply vars immediately.
rescue_absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    delayed_absorb(Txn, Ledger).

maybe_absorb(Txn, Ledger, _Chain) ->
    case blockchain_ledger_v1:current_height(Ledger) of
        %% genesis block is never delayed
        {ok, 0} ->
            delayed_absorb(Txn, Ledger),
            true;
        _ ->
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            {ok, Delay} = blockchain:config(?vars_commit_delay, Ledger),
            Effective = Delay + Height,
            case version_predicate(Txn) of
                0 ->
                    ok = blockchain_ledger_v1:delay_vars(Effective, Txn, Ledger),
                    true;
                V ->
                    {ok, Members} = blockchain_ledger_v1:consensus_members(Ledger),
                    %% TODO: combine these checks for efficiency?
                    case check_members(Members, V, Ledger) of
                        true ->
                            {ok, Threshold} = blockchain:config(?predicate_threshold, Ledger),
                            Versions = blockchain_ledger_v1:gateway_versions(Ledger),
                            case sum_higher(V, Versions) of
                                Pct when Pct >= Threshold andalso Delay =:= 0 ->
                                    delayed_absorb(Txn, Ledger),
                                    true;
                                Pct when Pct >= Threshold ->
                                    ok = blockchain_ledger_v1:delay_vars(Effective, Txn, Ledger),
                                    true;
                                _ ->
                                    false
                            end;
                        _ ->
                            false
                    end
            end
    end.

check_members(Members, Target, Ledger) ->
    lists:all(fun(M) ->
                      case blockchain_ledger_v1:find_gateway_info(M, Ledger) of
                          {ok, Gw} ->
                              V = blockchain_ledger_gateway_v2:version(Gw),
                              V >= Target;
                          _ -> false
                      end
              end,
              Members).

delayed_absorb(Txn, Ledger) ->
    Vars = decode_vars(vars(Txn)),
    Unsets = decode_unsets(unsets(Txn)),
    ok = blockchain_ledger_v1:vars(Vars, Unsets, Ledger),
    case master_key(Txn) of
        <<>> ->
            ok;
        Key ->
            ok = blockchain_ledger_v1:master_key(Key, Ledger)
    end.

sum_higher(Target, Proplist) ->
    sum_higher(Target, Proplist, 0).

sum_higher(_Target, [], Sum) ->
    Sum;
sum_higher(Target, [{Vers, Pct}| T], Sum) ->
    case Vers >= Target of
        true ->
            sum_higher(Target, T, Sum + Pct);
        false ->
            sum_higher(Target, T, Sum)
    end.

decode_unsets(Unsets) ->
    lists:map(fun(U) -> binary_to_atom(U, utf8) end, Unsets).

-spec print(txn_vars()) -> iodata().
print(undefined) -> <<"type=vars undefined">>;
print(#blockchain_txn_vars_v1_pb{vars = Vars, version_predicate = VersionP,
                                 master_key = MasterKey, key_proof = KeyProof,
                                 unsets = Unsets, cancels = Cancels,
                                 nonce = Nonce}) ->
    io_lib:format("type=vars vars=~p version_predicate=~p master_key=~p key_proof=~p unsets=~p cancels=~p nonce=~p",
                  [Vars, VersionP, MasterKey, KeyProof, Unsets, Cancels, Nonce]).

-spec to_json(txn_vars(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"vars_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      vars => maps:map(fun(_, V) when is_binary(V) ->
                               case lists:all(fun(C) -> C >= 32 andalso  C =< 127 end, binary_to_list(V)) of
                                   true ->
                                       V;
                                   false ->
                                       base64:encode(V)
                               end;
                          (_, V) -> V
                       end, decoded_vars(Txn)),
      version_predicate => version_predicate(Txn),
      proof => ?BIN_TO_B64(proof(Txn)),
      master_key => ?MAYBE_B58(master_key(Txn)),
      key_proof => ?BIN_TO_B64(key_proof(Txn)),
      cancels => cancels(Txn),
      unsets => unsets(Txn),
      nonce => nonce(Txn)
     }.

%%%
%%% Helper API
%%%

create_proof(Key, Txn) ->
    B = create_artifact(Txn),
    SignFun = libp2p_crypto:mk_sig_fun(Key),
    SignFun(B).

legacy_create_proof(Key, Vars) ->
    B = term_to_binary(Vars, [{compressed, 9}]),
    SignFun = libp2p_crypto:mk_sig_fun(Key),
    SignFun(B).

%%% helper functions

create_artifact(Txn) ->
    Txn1 = unset_proofs(Txn),
    blockchain_txn_vars_v1_pb:encode_msg(Txn1).

verify_key(_Artifact, _Key, <<>>) ->
    throw({error, no_proof});
verify_key(Artifact, Key, Proof) ->
    libp2p_crypto:verify(Artifact, Proof, libp2p_crypto:bin_to_pubkey(Key)).

validate_int(Value, Name, Min, Max, InfOK) ->
    case is_integer(Value) of
        false when InfOK == true andalso Value == infinity ->
            ok;
        false ->
            throw({error, {list_to_atom("non_integral_" ++ Name), Value}});
        true ->
            case Value >= Min andalso Value =< Max of
                false ->
                    throw({error, {list_to_atom(Name ++ "_out_of_range"), Value}});
                _ -> ok
            end
    end.

validate_float(Value, Name, Min, Max) ->
    case is_float(Value) of
        false when Value == infinity ->
            ok;
        false ->
            throw({error, {list_to_atom("non_float_" ++ Name), Value}});
        true ->
            case Value >= Min andalso Value =< Max of
                false ->
                    throw({error, {list_to_atom(Name ++ "_out_of_range"), Value}});
                _ -> ok
            end
    end.

validate_oracle_public_keys_format(Str) when is_binary(Str) ->
    PubKeys = blockchain_utils:vars_keys_to_list(Str),
    validate_oracle_keys(PubKeys).

validate_oracle_keys([]) -> ok;
validate_oracle_keys([H|T]) ->
    try
        _ = libp2p_crypto:bin_to_pubkey(H),
        validate_oracle_keys(T)
    catch
        _C:_E:_St ->
            throw({error, {invalid_oracle_pubkey, H}})
    end.

validate_staking_keys_format(Str) when is_binary(Str) ->
    PubKeys = blockchain_utils:vars_keys_to_list(Str),
    validate_staking_keys(PubKeys).

validate_staking_keys([]) -> ok;
validate_staking_keys([H|T]) ->
    try
        _ = libp2p_crypto:bin_to_pubkey(H),
        validate_staking_keys(T)
    catch
        _C:_E:_St ->
            throw({error, {invalid_staking_pubkey, H}})
    end.

%% ALL VALIDATION ERRORS MUST THROW ERROR TUPLES
%%
%% election vars
validate_var(?election_version, Value) ->
    case Value of
        undefined -> ok;
        2 -> ok;
        3 -> ok;
        _ ->
            throw({error, {invalid_election_version, Value}})
    end;
validate_var(?election_selection_pct, Value) ->
    validate_int(Value, "election_selection_pct", 1, 99, false);
validate_var(?election_removal_pct, Value) ->
    validate_int(Value, "election_removal_pct", 1, 99, false);
validate_var(?election_cluster_res, Value) ->
    validate_int(Value, "election_cluster_res", 0, 15, false);
validate_var(?election_replacement_factor, Value) ->
    validate_int(Value, "election_replacement_factor", 1, 100, false);
validate_var(?election_replacement_slope, Value) ->
    validate_int(Value, "election_replacement_slope", 1, 100, false);
validate_var(?election_interval, Value) ->
    validate_int(Value, "election_interval", 5, 100, true);
validate_var(?election_restart_interval, Value) ->
    validate_int(Value, "election_restart_interval", 5, 100, false);
validate_var(?election_bba_penalty, Value) ->
    validate_float(Value, "election_bba_penalty", 0.001, 0.5);
validate_var(?election_seen_penalty, Value) ->
    validate_float(Value, "election_seen_penalty", 0.001, 0.5);

%% ledger vars
validate_var(?var_gw_inactivity_threshold, Value) ->
    validate_int(Value, "var_gw_inactivity_threshold", 15, 2880, false);
validate_var(?witness_refresh_interval, Value) ->
    validate_int(Value, "witness_refresh_interval", 1, 20000, false);
validate_var(?witness_refresh_rand_n, Value) ->
    validate_int(Value, "witness_refresh_rand_n", 50, 1000, false);

%% meta vars
validate_var(?vars_commit_delay, Value) ->
    validate_int(Value, "vars_commit_delay", 1, 60, false);
validate_var(?chain_vars_version, Value) ->
    case Value of
        1 -> ok;
        2 -> ok;
        _ ->
            throw({error, {chain_vars_version, Value}})
    end;
validate_var(?predicate_threshold, Value) ->
    validate_float(Value, "predicate_threshold", 0.0, 1.0);
validate_var(?predicate_callback_mod, Value) ->
    case Value of
        miner ->
            ok;
        _ ->
            throw({error, {predicate_callback_mod, Value}})
    end;
validate_var(?predicate_callback_fun, Value) ->
    case lists:member(Value, ?allowed_predicate_funs) of
        true ->
            ok;
        _ ->
            throw({error, {predicate_callback_fun, Value}})
    end;

%% miner vars
validate_var(?num_consensus_members, Value) ->
    validate_int(Value, "num_consensus_members", 4, 100, false),
    case (Value - 1) rem 3 == 0 of
        true ->
            ok;
        false ->
            throw({error, {num_consensus_members_not_3fplus1, Value}})
    end;
validate_var(?block_time, Value) ->
    validate_int(Value, "block_time", 2, timer:minutes(10), false);
validate_var(?batch_size, Value) ->
    validate_int(Value, "batch_size", 10, 10000, false);
validate_var(?block_version, Value) ->
    case Value of
        v1 ->
            ok;
        _ ->
            throw({error, {invalid_block_version, Value}})
    end;
validate_var(?dkg_curve, Value) ->
    case Value of
        'SS512' ->
            ok;
        _ ->
            throw({error, {invalid_dkg_curve, Value}})
    end;

%% burn vars
validate_var(?token_burn_exchange_rate, Value) ->
    case is_integer(Value) andalso Value >= 0 of
        true ->
            ok;
        _ ->
            throw({error, {invalid_token_burn_exchange_rate, Value}})
    end;

%% poc related vars
validate_var(?h3_exclusion_ring_dist, Value) ->
    validate_int(Value, "h3_exclusion_ring_dist", 1, 10, false);
validate_var(?h3_max_grid_distance, Value) ->
    validate_int(Value, "h3_max_grid_distance", 1, 500, false);
validate_var(?h3_neighbor_res, Value) ->
    validate_int(Value, "h3_neighbor_res", 0, 15, false);
validate_var(?min_score, Value) ->
    validate_float(Value, "min_score", 0.0, 0.3);
validate_var(?min_assert_h3_res, Value) ->
    validate_int(Value, "min_assert_h3_res", 0, 15, false);
validate_var(?poc_challenge_interval, Value) ->
    validate_int(Value, "poc_challenge_interval", 10, 1440, false);
validate_var(?poc_version, Value) ->
    case Value of
        N when is_integer(N), N >= 1,  N =< 8 ->
            ok;
        _ ->
            throw({error, {invalid_poc_version, Value}})
    end;
validate_var(?poc_challenge_sync_interval, Value) ->
    validate_int(Value, "poc_challenge_sync_interval", 10, 1440, false);
validate_var(?poc_path_limit, undefined) ->
    ok;
validate_var(?poc_path_limit, Value) ->
    validate_int(Value, "poc_path_limit", 3, 10, false);
validate_var(?poc_witness_consideration_limit, Value) ->
    validate_int(Value, "poc_witness_consideration_limit", 10, 100, false);
validate_var(?poc_v4_exclusion_cells, Value) ->
    validate_int(Value, "poc_v4_exclusion_cells", 8, 12, false);
validate_var(?poc_v4_parent_res, Value) ->
    validate_int(Value, "poc_v4_parent_res", 8, 11, false);
validate_var(?poc_v4_prob_rssi_wt, Value) ->
    validate_float(Value, "poc_v4_prob_rssi_wt", 0.0, 1.0);
validate_var(?poc_v4_prob_time_wt, Value) ->
    validate_float(Value, "poc_v4_prob_time_wt", 0.0, 1.0);
validate_var(?poc_v4_prob_count_wt, Value) ->
    validate_float(Value, "poc_v4_prob_count_wt", 0.0, 1.0);
validate_var(?poc_v4_prob_no_rssi, Value) ->
    validate_float(Value, "poc_v4_prob_no_rssi", 0.0, 1.0);
validate_var(?poc_v4_prob_good_rssi, Value) ->
    validate_float(Value, "poc_v4_prob_good_rssi", 0.0, 1.0);
validate_var(?poc_v4_prob_bad_rssi, Value) ->
    validate_float(Value, "poc_v4_prob_bad_rssi", 0.0, 1.0);
validate_var(?poc_v4_target_score_curve, Value) ->
    validate_int(Value, "poc_v4_target_score_curve", 3, 7, false);
validate_var(?poc_v4_target_exclusion_cells, Value) ->
    validate_int(Value, "poc_v4_target_exclusion_cells", 6000, 40000, false);
validate_var(?poc_v4_target_challenge_age, Value) ->
    validate_int(Value, "poc_v4_target_challenge_age", 30, 1000, false);
validate_var(?poc_v4_target_prob_score_wt, Value) ->
    validate_float(Value, "poc_v4_target_prob_score_wt", 0.0, 1.0);
validate_var(?poc_v4_target_prob_edge_wt, Value) ->
    validate_float(Value, "poc_v4_target_prob_edge_wt", 0.0, 1.0);
validate_var(?poc_v4_randomness_wt, Value) ->
    validate_float(Value, "poc_v4_randomness_wt", 0.0, 1.0);
validate_var(?poc_v5_target_prob_randomness_wt, Value) ->
    validate_float(Value, "poc_v5_target_prob_randomness_wt", 0.0, 1.0);
validate_var(?poc_typo_fixes, Value) ->
    case Value of
        true -> ok;
        false -> ok;
        _ -> throw({error, {invalid_poc_typo_fixes, Value}})
    end;
validate_var(?poc_target_hex_parent_res, Value) ->
    validate_int(Value, "poc_target_hex_parent_res", 3, 7, false);
validate_var(?poc_good_bucket_low, Value) ->
    validate_int(Value, "poc_good_bucket_low", -150, -90, false);
validate_var(?poc_good_bucket_high, Value) ->
    validate_int(Value, "poc_good_bucket_high", -100, -70, false);
validate_var(?poc_centrality_wt, Value) ->
    validate_float(Value, "poc_centrality_wt", 0.0, 1.0);
validate_var(?poc_max_hop_cells, Value) ->
    validate_int(Value, "poc_max_hop_cells", 100, 4000, false);

%% score vars
validate_var(?alpha_decay, Value) ->
    validate_float(Value, "alpha_decay", 0.0, 0.1);
validate_var(?beta_decay, Value) ->
    validate_float(Value, "beta_decay", 0.0, 0.1);
validate_var(?max_staleness, Value) ->
    validate_int(Value, "max_staleness", 1000, 1000000, false);

%% reward vars
validate_var(?monthly_reward, Value) ->
    validate_int(Value, "monthly_reward", 1000 * 1000000, 10000000 * 1000000, false);
validate_var(?securities_percent, Value) ->
    validate_float(Value, "securities_percent", 0.0, 1.0);
validate_var(?consensus_percent, Value) ->
    validate_float(Value, "consensus_percent", 0.0, 1.0);
validate_var(?poc_challengees_percent, Value) ->
    validate_float(Value, "poc_challengees_percent", 0.0, 1.0);
validate_var(?poc_witnesses_percent, Value) ->
    validate_float(Value, "poc_witnesses_percent", 0.0, 1.0);
validate_var(?poc_challengers_percent, Value) ->
    validate_float(Value, "poc_challengers_percent", 0.0, 1.0);
validate_var(?dc_percent, Value) ->
    validate_float(Value, "dc_percent", 0.0, 1.0);
validate_var(?reward_version, Value) ->
    case Value of
        N when is_integer(N), N >= 1,  N =< 2 ->
            ok;
        _ ->
            throw({error, {invalid_reward_version, Value}})
    end;

%% bundle vars
validate_var(?max_bundle_size, Value) ->
    validate_int(Value, "max_bundle_size", 5, 100, false);

%% txn payment_v2 vars
validate_var(?max_payments, Value) ->
    validate_int(Value, "max_payments", 5, 50, false);

validate_var(?deprecate_payment_v1, Value) ->
    case Value of
        true -> ok;
        false -> ok;
        _ -> throw({error, {invalid_deprecate_payment_v1, Value}})
    end;

validate_var(?allow_zero_amount, Value) ->
    case Value of
        true -> ok;
        false -> ok;
        _ -> throw({error, {invalid_allow_zero_amount, Value}})
    end;

%% state channel vars
%% XXX: what are some reasonable limits here?
validate_var(?min_expire_within, Value) ->
    validate_int(Value, "min_expire_within", 9, 20, false);
validate_var(?max_open_sc, Value) ->
    validate_int(Value, "max_open_sc", 1, 3, false);
validate_var(?max_xor_filter_size, Value) ->
    validate_int(Value, "max_xor_filter_size", 1024, 1024*100, false);
validate_var(?max_xor_filter_num, Value) ->
    validate_int(Value, "max_xor_filter_num", 3, 6, false);
validate_var(?max_subnet_size, Value) ->
    validate_int(Value, "max_subnet_size", 8, 65536, false);
validate_var(?min_subnet_size, Value) ->
    validate_int(Value, "min_subnet_size", 8, 65536, false);
validate_var(?max_subnet_num, Value) ->
    validate_int(Value, "max_subnet_num", 1, 20, false);
validate_var(?sc_grace_blocks, Value) ->
    validate_int(Value, "sc_grace_blocks", 1, 100, false);
validate_var(?dc_payload_size, Value) ->
    validate_int(Value, "dc_payload_size", 1, 32, false);
validate_var(?sc_version, Value) ->
    validate_int(Value, "sc_version", 1, 10, false);
validate_var(?sc_overcommit, Value) ->
    validate_int(Value, "sc_overcommit", 1, 10, false); %% XXX should this be a float?

%% txn snapshot vars
validate_var(?snapshot_version, Value) ->
    case Value of
        N when is_integer(N), N == 1 ->
            ok;
        _ ->
            throw({error, {invalid_snapshot_version, Value}})
    end;
%% this is a copy of the lines at the top for informational value
%% -ifdef(TEST).
%% -define(min_snap_interval, 1).
%% -else.
%% -define(min_snap_interval, 4 * 60).
%% -endif.
validate_var(?snapshot_interval, Value) -> % half day to two weeks
    validate_int(Value, "snapshot_interval", ?min_snap_interval, 20160, false);

validate_var(?price_oracle_public_keys, Value) ->
    validate_oracle_public_keys_format(Value);

validate_var(?price_oracle_price_scan_delay, Value) ->
    %% Allowed: 0 seconds to 86400 seconds (1 day)
    validate_int(Value, "price_oracle_price_scan_delay", 0, 86400, false);

validate_var(?price_oracle_price_scan_max, Value) ->
    %% Allowed: 0 seconds to 604800 seconds (7 days)
    validate_int(Value, "price_oracle_price_scan_max", 0, 604800, false);

validate_var(?price_oracle_refresh_interval, Value) ->
    %% How many blocks to skip between price refreshes
    validate_int(Value, "price_oracle_refresh_interval", 0, 5000, false);

validate_var(?price_oracle_height_delta, Value) ->
    %% How many blocks to allow between when a txn is
    %% submitted and when it is committed to the ledger
    validate_int(Value, "price_oracle_height_delta", 0, 500, false);

%% txn fee related vars
validate_var(?txn_fees, Value) ->
    case Value of
        true -> ok;
        false -> ok;
        _ -> throw({error, {invalid_txn_fees, Value}})
    end;

validate_var(?staking_keys, Value) ->
    validate_staking_keys_format(Value);

%% txn fee vars below are in DC
validate_var(?staking_fee_txn_oui_v1, Value) ->
    %% the staking fee price for an OUI, in DC
    validate_int(Value, "staking_fee_txn_oui_v1", 0, 1000 * ?USD_TO_DC, false);

validate_var(?staking_fee_txn_oui_v1_per_address, Value) ->
    %% the staking fee price for each OUI address, in DC
    validate_int(Value, "staking_fee_txn_oui_v1_per_address", 0, 1000 * ?USD_TO_DC, false);

validate_var(?staking_fee_txn_add_gateway_v1, Value) ->
    %% the staking fee price for an add gateway txn, in DC
    validate_int(Value, "staking_fee_txn_add_gateway_v1", 0, 1000 * ?USD_TO_DC, false);

validate_var(?staking_fee_txn_assert_location_v1, Value) ->
    %% the staking fee price for an assert location txn, in DC
    validate_int(Value, "staking_fee_txn_assert_location_v1", 0, 1000 * ?USD_TO_DC, false);

validate_var(?txn_fee_multiplier, Value) ->
    %% a multiplier applied to txn fee, in DC
    validate_int(Value, "txn_fee_multiplier", 1, 65536, false);


validate_var(Var, Value) ->
    %% something we don't understand, crash
    invalid_var(Var, Value).

-ifdef(TEST).
invalid_var(Var, Value) ->
    case lists:member(Var, ?exceptions) of % test only
        true ->
            ok;
        _ ->
            throw({error, {unknown_var, Var, Value}})
    end.
-else.
invalid_var(Var, Value) ->
    throw({error, {unknown_var, Var, Value}}).
-endif.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

key_test() ->
    #{secret := Priv, public := Pub} =
        libp2p_crypto:generate_keys(ecc_compact),
    BPub = libp2p_crypto:pubkey_to_bin(Pub),
    Vars = #{a => 1,
             b => 2000,
             c => 2.5,
             d => <<"have to include a string">>},
    %% run vars through encode decode cycle
    Vars1 = encode_vars(Vars),
    Vars2 = decode_vars(Vars1),
    ?assertEqual(Vars, Vars2),

    Txn = blockchain_txn_vars_v1:new(Vars, 1, #{master_key => BPub}),
    Proof = create_proof(Priv, Txn),

    B = create_artifact(Txn),
    ?assert(verify_key(B, BPub, Proof)),

    ok.

legacy_key_test() ->
    #{secret := Priv, public := Pub} =
        libp2p_crypto:generate_keys(ecc_compact),
    BPub = libp2p_crypto:pubkey_to_bin(Pub),
    Vars = #{a => 1,
             b => 2000,
             c => 2.5,
             d => <<"have to include a string">>},
    Proof = legacy_create_proof(Priv, Vars),
    %% run vars through encode decode cycle
    Vars1 = encode_vars(Vars),
    Vars2 = decode_vars(Vars1),
    B = term_to_binary(Vars2, [{compressed, 9}]),
    ?assert(verify_key(B, BPub, Proof)).

to_json_test() ->
    #{secret := _Priv, public := Pub} =
        libp2p_crypto:generate_keys(ecc_compact),
    BPub = libp2p_crypto:pubkey_to_bin(Pub),
    Vars = #{a => 1,
             b => 2000,
             c => 2.5,
             d => <<"have to include a string">>,
             %% we didn't add a binary type but accidentally put binary vars in
             f => <<"f is for ffffff\0">>},

    Txn = blockchain_txn_vars_v1:new(Vars, 1, #{master_key => BPub}),
    Json = to_json(Txn, []),

    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, vars, version_predicate, proof, master_key, key_proof, cancels, unsets, nonce])),
    ?assertEqual(<<"f is for ffffff\0">>, base64:decode(maps:get(f, maps:get(vars, Json)))).


-endif.
