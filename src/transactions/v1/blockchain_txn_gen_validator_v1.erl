%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_validator_v1).

-behavior(blockchain_txn).
-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_gen_validator_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    sign/2,
    address/1,
    owner/1,
    stake/1,
    description/1,
    fee/1,
    is_valid/2,
    absorb/2,
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_genesis_validator() :: #blockchain_txn_gen_validator_v1_pb{}.
-export_type([txn_genesis_validator/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(Address :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Stake :: pos_integer(),
          Description :: binary()) -> txn_genesis_validator().
new(Address, Owner, Stake, Description) ->
    #blockchain_txn_gen_validator_v1_pb{addr = Address,
                                        owner = Owner,
                                        stake = Stake,
                                        description = Description}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_genesis_validator()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_gen_validator_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_genesis_validator(), libp2p_crypto:sig_fun()) -> txn_genesis_validator().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(txn_genesis_validator()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_gen_validator_v1_pb.addr.

-spec owner(txn_genesis_validator()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_gen_validator_v1_pb.owner.

-spec stake(txn_genesis_validator()) -> pos_integer().
stake(Txn) ->
    Txn#blockchain_txn_gen_validator_v1_pb.stake.

-spec description(txn_genesis_validator()) -> string().
description(Txn) ->
    Txn#blockchain_txn_gen_validator_v1_pb.description.

-spec fee(txn_genesis_validator()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% This transaction should only be absorbed when it is in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_genesis_validator(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(_Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, 0} ->
            ok;
        _ ->
            {error, not_in_genesis_block}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_genesis_validator(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Address = ?MODULE:address(Txn),
    Owner = ?MODULE:owner(Txn),
    Stake = ?MODULE:stake(Txn),
    Description = ?MODULE:description(Txn),
    blockchain_ledger_v1:add_validator(Address,
                                       Owner,
                                       Stake,
                                       Description,
                                       Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_genesis_validator()) -> iodata().
print(undefined) -> <<"type=genesis_validator, undefined">>;
print(#blockchain_txn_gen_validator_v1_pb{
         addr = Address, owner = Owner,
         stake = Stake, description = Description}) ->
    io_lib:format("type=genesis_validator Address=~p, owner=~p, stake=~p, description=~s",
                  [?TO_ANIMAL_NAME(Address), ?TO_B58(Owner), Stake, Description]).


-spec to_json(txn_genesis_validator(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"gen_validator_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      address => ?BIN_TO_B58(address(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      stake => stake(Txn),
      description => description(Txn)
     }.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_gen_validator_v1_pb{addr = <<"0">>,
                                             owner = <<"1">>,
                                             stake = 10000,
                                             description = <<"meat popsicle">>},
    ?assertEqual(Tx, new(<<"0">>, <<"1">>, 10000, <<"meat popsicle">>)).

validator_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, <<"meat popsicle">>),
    ?assertEqual(<<"0">>, address(Tx)).

owner_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, <<"meat popsicle">>),
    ?assertEqual(<<"1">>, owner(Tx)).

stake_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, <<"meat popsicle">>),
    ?assertEqual(1000, stake(Tx)).

description_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, <<"meat popsicle">>),
    ?assertEqual(<<"meat popsicle">>, description(Tx)).

json_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, <<"meat popsicle">>),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, validator, owner, location, nonce])).


-endif.
