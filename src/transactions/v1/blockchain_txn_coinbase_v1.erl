%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Coinbase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_coinbase_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_coinbase_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    payee/1,
    amount/1,
    fee/1,
    fee_payer/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    sign/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_coinbase_v1_pb).

-type txn_coinbase() :: ?T{}.

-type t() :: txn_coinbase().

-export_type([t/0, txn_coinbase/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer()) -> txn_coinbase().
new(Payee, Amount) ->
    #blockchain_txn_coinbase_v1_pb{payee=Payee, amount=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_coinbase()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_coinbase_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_coinbase(), libp2p_crypto:sig_fun()) -> txn_coinbase().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_coinbase()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_coinbase_v1_pb.payee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_coinbase()) -> non_neg_integer().
amount(Txn) ->
    Txn#blockchain_txn_coinbase_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_coinbase()) -> non_neg_integer().
fee(_Txn) ->
    0.

-spec fee_payer(txn_coinbase(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% This transaction is only allowed in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_coinbase(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, 0} ->
            Amount = ?MODULE:amount(Txn),
            case Amount > 0 of
                true ->
                    ok;
                false ->
                    {error, zero_or_negative_amount}
            end;
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
-spec absorb(txn_coinbase(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payee = ?MODULE:payee(Txn),
    Amount = ?MODULE:amount(Txn),
    blockchain_ledger_v1:credit_account(Payee, Amount, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_coinbase()) -> [iodata()].
print(undefined) ->
    <<"type=coinbase, undefined">>;
print(#blockchain_txn_coinbase_v1_pb{payee=Payee, amount=Amount}) ->
    io_lib:format("txn_coinbase: payee: ~p, amount: ~p",
                  [?TO_B58(Payee), Amount]).

json_type() ->
    <<"coinbase_v1">>.

-spec to_json(txn_coinbase(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      payee => ?BIN_TO_B58(payee(Txn)),
      amount=> amount(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_coinbase_v1_pb{payee= <<"payee">>, amount=666},
    ?assertEqual(Tx, new(<<"payee">>, 666)).

payee_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(<<"payee">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payee">>, 666),
    ?assertEqual(666, amount(Tx)).

to_json_test() ->
    Tx = #blockchain_txn_coinbase_v1_pb{payee= <<"payee">>, amount=666},
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, payee, amount])).

-endif.
