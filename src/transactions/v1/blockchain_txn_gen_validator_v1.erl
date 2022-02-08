%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Validator ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_validator_v1).

-behavior(blockchain_txn).
-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_gen_validator_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    sign/2,
    address/1,
    owner/1,
    stake/1,
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_gen_validator_v1_pb).

-type t() :: txn_genesis_validator().

-type txn_genesis_validator() :: ?T{}.

-export_type([t/0, txn_genesis_validator/0]).

-spec new(Address :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Stake :: pos_integer()) -> txn_genesis_validator().
new(Address, Owner, Stake) ->
    #blockchain_txn_gen_validator_v1_pb{address = Address,
                                        owner = Owner,
                                        stake = Stake}.


-spec hash(txn_genesis_validator()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_gen_validator_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

-spec sign(txn_genesis_validator(), libp2p_crypto:sig_fun()) -> txn_genesis_validator().
sign(Txn, _SigFun) ->
    Txn.

-spec address(txn_genesis_validator()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_gen_validator_v1_pb.address.

-spec owner(txn_genesis_validator()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_gen_validator_v1_pb.owner.

-spec stake(txn_genesis_validator()) -> pos_integer().
stake(Txn) ->
    Txn#blockchain_txn_gen_validator_v1_pb.stake.

-spec fee(txn_genesis_validator()) -> non_neg_integer().
fee(_Txn) ->
    0.

-spec fee_payer(txn_genesis_validator(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

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

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

-spec absorb(txn_genesis_validator(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Address = ?MODULE:address(Txn),
    Owner = ?MODULE:owner(Txn),
    Stake = ?MODULE:stake(Txn),
    blockchain_ledger_v1:add_validator(Address,
                                       Owner,
                                       Stake,
                                       Ledger).

-spec print(txn_genesis_validator()) -> iodata().
print(undefined) -> <<"type=genesis_validator, undefined">>;
print(#blockchain_txn_gen_validator_v1_pb{
         address = Address, owner = Owner,
         stake = Stake}) ->
    io_lib:format("type=genesis_validator Address=~p, owner=~p, stake=~p",
                  [?TO_ANIMAL_NAME(Address), ?TO_B58(Owner), Stake]).

json_type() ->
    <<"gen_validator_v1">>.

-spec to_json(txn_genesis_validator(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      address => ?BIN_TO_B58(address(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      stake => stake(Txn)
     }.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_gen_validator_v1_pb{address = <<"0">>,
                                             owner = <<"1">>,
                                             stake = 10000},
    ?assertEqual(Tx, new(<<"0">>, <<"1">>, 10000)).

validator_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000),
    ?assertEqual(<<"0">>, address(Tx)).

owner_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000),
    ?assertEqual(<<"1">>, owner(Tx)).

stake_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000),
    ?assertEqual(1000, stake(Tx)).

json_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_gen_validator_v1_pb))).

-endif.
