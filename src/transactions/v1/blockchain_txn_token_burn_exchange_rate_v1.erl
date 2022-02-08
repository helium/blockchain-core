%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Token Burn Exchange Rate ==
%% This is a placeholder, Token Burn Exchange Rate is a chain var for now whne ready it will be a transaction again.
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_token_burn_exchange_rate_v1).

%%
%% TODO: if this txn type has never been used we should remove this module and associated code
%%

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include_lib("helium_proto/include/blockchain_txn_token_burn_exchange_rate_v1_pb.hrl").

-export([
    new/1,
    hash/1,
    rate/1,
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

-define(T, #blockchain_txn_token_burn_exchange_rate_v1_pb).

-type t() :: txn_token_burn_exchange_rate().

-type txn_token_burn_exchange_rate() :: ?T{}.

-export_type([t/0, txn_token_burn_exchange_rate/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(non_neg_integer()) -> txn_token_burn_exchange_rate().
new(Amount) ->
    #blockchain_txn_token_burn_exchange_rate_v1_pb{rate=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_token_burn_exchange_rate()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_token_burn_exchange_rate_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_token_burn_exchange_rate(), libp2p_crypto:sig_fun()) -> txn_token_burn_exchange_rate().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rate(txn_token_burn_exchange_rate()) -> non_neg_integer().
rate(Txn) ->
    Txn#blockchain_txn_token_burn_exchange_rate_v1_pb.rate.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_token_burn_exchange_rate()) -> non_neg_integer().
fee(_Txn) ->
    0.

-spec fee_payer(txn_token_burn_exchange_rate(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% This transaction is only allowed in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_token_burn_exchange_rate(), blockchain:blockchain()) -> {error, atom()} | {error, {atom(), any()}}.
is_valid(_Txn, _Chain) ->
    {error, not_implemented}.  % TODO Huh? What is going on here?
    % Amount = ?MODULE:rate(Txn),
    % case Amount > 0 of
    %     true ->
    %         ok;
    %     false ->
    %         {error, zero_or_negative_rate}
    % end.

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
-spec absorb(txn_token_burn_exchange_rate(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Rate = ?MODULE:rate(Txn),
    blockchain_ledger_v1:token_burn_exchange_rate(Rate, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_token_burn_exchange_rate()) -> iodata().
print(undefined) -> <<"type=burn_exchange_rate undefined">>;
print(#blockchain_txn_token_burn_exchange_rate_v1_pb{rate=Amount}) ->
    io_lib:format("type=burn_exchange_rate rate=~p", [Amount]).

json_type() ->
    <<"token_burn_exchange_rate_v1">>.

-spec to_json(txn_token_burn_exchange_rate(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      rate => rate(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_token_burn_exchange_rate_v1_pb{rate=666},
    ?assertEqual(Tx, new(666)).

rate_test() ->
    Tx = new(666),
    ?assertEqual(666, rate(Tx)).

to_json_test() ->
    Tx = new(666),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, rate])).

-endif.
