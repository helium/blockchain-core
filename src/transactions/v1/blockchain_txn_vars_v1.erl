%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Chain Vars ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_vars_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_vars_v1_pb.hrl").
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
         version_predicate/1,
         unsets/1,
         cancels/1,
         nonce/1,
         absorb/2,
         sign/2
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
    case blockchain:config(?chain_vars_version, Ledger) of
        {ok, 2} ->
            Vars = decode_vars(vars(Txn)),
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
        {error, not_found} ->
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

maybe_absorb(Txn, Ledger, Chain) ->
    %% the sorting order makes sure that this txn will be sorted after
    %% any epoch change so this will result in deterministic delay
    case blockchain_ledger_v1:current_height(Ledger) of
        %% genesis block is never delayed
        {ok, 0} ->
            delayed_absorb(Txn, Ledger),
            true;
        _ ->
            {ok, Height} = blockchain:height(Chain),
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
                              V = blockchain_ledger_gateway_v1:version(Gw),
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

%% ALL VALIDATION ERRORS MUST THROW ERROR TUPLES
validate_var(?election_interval, Value) ->
    case is_integer(Value) of
        false ->
            throw({error, non_integral_election_interval});
        _ -> ok
    end,
    case Value > 10 andalso Value < 100 of
        false ->
            throw({error, election_interval_out_of_range});
        _ -> ok
    end;
validate_var(?block_time, Value) ->
    case is_integer(Value) of
        false ->
            throw({error, non_integral_block_time});
        _ -> ok
    end,
    case Value >= 1 andalso Value < timer:minutes(10) of
        false ->
            throw({error, block_time_out_of_range});
        _ -> ok
    end;
validate_var(Var, Value) ->
    lager:debug("checking ~p ~p", [Var, Value]),
    %% TODO: hang individual var validations here
    ok.

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

-endif.
