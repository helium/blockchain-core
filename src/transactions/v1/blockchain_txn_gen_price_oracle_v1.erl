%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Price Oracle ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_price_oracle_v1).

-behavior(blockchain_txn).
-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_gen_price_oracle_v1_pb.hrl").

-export([
    new/1,
    hash/1,
    sign/2,
    price/1,
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

-define(T, #blockchain_txn_gen_price_oracle_v1_pb).

-type t() :: txn_genesis_price_oracle().

-type txn_genesis_price_oracle() :: ?T{}.

-export_type([t/0, txn_genesis_price_oracle/0]).

%%--------------------------------------------------------------------
%% @doc
%% Create a new genesis price oracle transaction
%% @end
%%--------------------------------------------------------------------
-spec new(Price :: pos_integer()) -> txn_genesis_price_oracle().
new(Price) ->
    #blockchain_txn_gen_price_oracle_v1_pb{price=Price}.

%%--------------------------------------------------------------------
%% @doc
%% Return the sha256 hash of this transaction
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_genesis_price_oracle()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_gen_price_oracle_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% Sign this transaction. (This is a no-op for this transaction
%% type. It's only valid at genesis block)
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_genesis_price_oracle(), libp2p_crypto:sig_fun()) -> txn_genesis_price_oracle().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% Return the price of this transaction
%% @end
%%--------------------------------------------------------------------
-spec price(txn_genesis_price_oracle()) -> non_neg_integer().
price(Txn) ->
    Txn#blockchain_txn_gen_price_oracle_v1_pb.price.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_genesis_price_oracle()) -> non_neg_integer().
fee(_Txn) ->
    0.

-spec fee_payer(txn_genesis_price_oracle(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% This transaction should only be absorbed when it's in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_genesis_price_oracle(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Price = price(Txn),
    case {blockchain_ledger_v1:current_height(Ledger), Price > 0} of
        {{ok, 0}, true} ->
            ok;
        {{ok, 0}, false} ->
            {error, invalid_oracle_price};
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_genesis_price_oracle(), blockchain:blockchain()) -> ok | {error, not_in_genesis_block}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Price = ?MODULE:price(Txn),
    blockchain_ledger_v1:load_oracle_price(Price, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_genesis_price_oracle()) -> iodata().
print(undefined) -> <<"type=genesis_price_oracle, undefined">>;
print(#blockchain_txn_gen_price_oracle_v1_pb{price=P}) ->
    io_lib:format("type=genesis_price_oracle price=~p", [P]).

json_type() ->
    <<"gen_price_oracle_v1">>.

-spec to_json(txn_genesis_price_oracle(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      price => price(Txn)
     }.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_gen_price_oracle_v1_pb{price = 10},
    ?assertEqual(Tx, new(10)).

price_test() ->
    Tx = new(10),
    ?assertEqual(10, price(Tx)).

json_test() ->
    Tx = new(10),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, price])).


-endif.
