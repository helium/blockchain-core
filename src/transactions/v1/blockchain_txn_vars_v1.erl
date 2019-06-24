%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Chain Vars ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_vars_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_vars_v1_pb.hrl").

-export([
         new/2, new/3,
         hash/1,
         fee/1,
         is_valid/2,
         master_key/1,
         key_proof/1,
         proof/1,
         vars/1,
         version_predicate/1,
         unsets/1,
         cancels/1,
         absorb/2,
         sign/2
]).

%% helper API
-export([
         create_proof/2,
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
%% }


-spec new(#{}, binary()) -> txn_vars().
new(Vars, Proof) ->
    new(Vars, Proof, #{}).

-spec new(#{atom() => any()}, binary(), #{atom() => any()}) -> txn_vars().
new(Vars, Proof, Optional) ->
    VersionP = maps:get(version_predicate, Optional, 0),
    MasterKey = maps:get(master_key, Optional, <<>>),
    KeyProof = maps:get(key_proof, Optional, <<>>),
    Unsets = maps:get(unsets, Optional, []),
    Cancels = maps:get(cancels, Optional, []),
    %% note that string inputs are normalized on creation, which has
    %% an effect on proof encoding :/
    #blockchain_txn_vars_v1_pb{vars = encode_vars(Vars),
                               proof = Proof,
                               version_predicate = VersionP,
                               master_key = MasterKey,
                               key_proof = KeyProof,
                               unsets = encode_unsets(Unsets),
                               cancels = Cancels}.

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

proof(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.proof.

vars(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.vars.

version_predicate(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.version_predicate.

unsets(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.unsets.

cancels(Txn) ->
    Txn#blockchain_txn_vars_v1_pb.cancels.

-spec is_valid(txn_vars(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
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
                  case blockchain:config(VarName, Ledger) of
                      {ok, _} -> ok;
                      {error, not_found} -> throw({error, {unset_var_not_set, VarName}})
                  end
          end,
          decode_unsets(unsets(Txn))),

        %% TODO: validate that a cancelled transaction is actually on
        %% the chain

        case Gen of
            false ->
                %% this is broken for non-miner users of the
                %% blockchain application, comment out till I can
                %% rework it later.


                %% try
                %%     {ok, Mod} = blockchain:config(predicate_callback_mod, Ledger),
                %%     {ok, Fun} = blockchain:config(predicate_callback_fun, Ledger),
                %%     Curr = Mod:Fun(),
                %%     case version_predicate(Txn) of
                %%         %% not required, commit.
                %%         0 ->
                %%             ok;
                %%         V when V < (Curr + 1) ->
                %%             throw({error, predicate_too_low});
                %%         _ -> ok
                %%     end
                %% catch throw:E ->
                %%         %% rethrow validation errors
                %%         throw(E);
                %%       _:_ ->
                %%         %% assume anything else is a bad function specification
                %%         throw({error, bad_predicate_fun})
                %% end;
                ok;
            _ -> ok
        end
    catch throw:Ret ->
            lager:error("invalid chain var transaction: ~p reason ~p", [Txn, Ret]),
            false
    end.

-spec absorb(txn_vars(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),

    %% the sorting order makes sure that this txn will be sorted after
    %% any epoch change so this will result in deterministic delay
    case blockchain_ledger_v1:current_height(Ledger) of
        %% genesis block is never delayed
        {ok, 0} ->
            delayed_absorb(Txn, Ledger);
        _ ->
            {ok, Epoch} = blockchain_ledger_v1:election_epoch(Ledger),
            %% So we'll be in block Current + 1
            %% {ok, Current} = blockchain:height(Chain),
            {ok, Delay} = blockchain:config(vars_commit_delay, Ledger),
            Effective = Delay + Epoch,
            ok = blockchain_ledger_v1:delay_vars(Effective, Txn, Ledger)
            %% case version_predicate(Txn) of
            %%     0 ->
            %%         lager:info("delaying var application until ~p", [Effective]),
            %%         ok = blockchain_ledger_v1:delay_vars(Effective, Txn, Ledger);
            %%     V ->
            %%         {ok, Mod} = blockchain:config(predicate_callback_mod, Ledger),
            %%         {ok, Fun} = blockchain:config(predicate_callback_fun, Ledger),
            %%         Curr = Mod:Fun(),
            %%         case V >= Curr of
            %%             true ->
            %%                 lager:info("delaying var application until ~p", [Effective]),
            %%                 ok = blockchain_ledger_v1:delay_vars(Effective, Txn, Ledger);
            %%             false ->
            %%                 %% TODO: this requires more work on the threshold side.
            %%                 ok
            %%         end
            %% end
    end.

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

decode_unsets(Unsets) ->
    lists:map(fun(U) -> binary_to_atom(U, utf8) end, Unsets).

%%%
%%% Helper API
%%%

create_proof(Key, Vars) ->
    B = term_to_binary(Vars, [{compressed, 9}]),
    SignFun = libp2p_crypto:mk_sig_fun(Key),
    SignFun(B).

%%% helper functions

verify_key(_Artifact, _Key, <<>>) ->
    throw({error, no_proof});
verify_key(Artifact, Key, Proof) ->
    libp2p_crypto:verify(Artifact, Proof, libp2p_crypto:bin_to_pubkey(Key)).

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
    Proof = create_proof(Priv, Vars),
    %% run vars through encode decode cycle
    Vars1 = encode_vars(Vars),
    Vars2 = decode_vars(Vars1),
    B = term_to_binary(Vars2, [{compressed, 9}]),
    ?assert(verify_key(B, BPub, Proof)).

-endif.
