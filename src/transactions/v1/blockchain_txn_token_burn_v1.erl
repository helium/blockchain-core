%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_token_burn_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include_lib("helium_proto/include/blockchain_txn_token_burn_v1_pb.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").

-export([
    new/3, new/4,
    hash/1,
    payer/1,
    payee/1,
    amount/1,
    nonce/1,
    fee/1, fee/2,
    fee_payer/2,
    memo/1, memo/2,
    calculate_fee/2, calculate_fee/5,
    signature/1,
    sign/2,
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

-define(T, #blockchain_txn_token_burn_v1_pb).

-type t() :: txn_token_burn().

-type txn_token_burn() :: ?T{}.

-export_type([t/0, txn_token_burn/0]).

-spec new(libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer()) -> txn_token_burn().
new(Payer, Amount, Nonce) ->
    #blockchain_txn_token_burn_v1_pb{
        payer=Payer,
        payee=Payer,
        amount=Amount,
        nonce=Nonce,
        fee=?LEGACY_TXN_FEE,
        signature = <<>>
    }.

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer()) -> txn_token_burn().
new(Payer, Payee, Amount, Nonce) ->
    #blockchain_txn_token_burn_v1_pb{
        payer=Payer,
        payee=Payee,
        amount=Amount,
        nonce=Nonce,
        fee=?LEGACY_TXN_FEE,
        signature = <<>>
    }.

-spec hash(txn_token_burn()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_token_burn_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec payer(txn_token_burn()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.payer.

-spec payee(txn_token_burn()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.payee.

-spec amount(txn_token_burn()) -> pos_integer().
amount(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.amount.

-spec nonce(txn_token_burn()) -> pos_integer().
nonce(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.nonce.

-spec fee(txn_token_burn()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.fee.

-spec fee(txn_token_burn(), non_neg_integer()) -> txn_token_burn().
fee(Txn, Fee) ->
    Txn#blockchain_txn_token_burn_v1_pb{fee=Fee}.

-spec fee_payer(txn_token_burn(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    payer(Txn).

-spec memo(txn_token_burn()) -> non_neg_integer().
memo(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.memo.

-spec memo(txn_token_burn(), non_neg_integer()) -> txn_token_burn().
memo(Txn, Memo) ->
    Txn#blockchain_txn_token_burn_v1_pb{memo=Memo}.

-spec signature(txn_token_burn()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.signature.

 %%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_token_burn(), libp2p_crypto:sig_fun()) -> txn_token_burn().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_token_burn_v1_pb{signature=SigFun(EncodedTxn)}.


%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_token_burn(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_token_burn(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_token_burn_v1_pb{fee=0, signature= <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).


-spec is_valid(txn_token_burn(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_token_burn_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(BaseTxn),
    case blockchain_txn:validate_fields([{{payee, ?MODULE:payee(Txn)}, {address, libp2p}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                false ->
                    {error, bad_signature};
                true ->
                    case blockchain_ledger_v1:current_oracle_price_list(Ledger) of
                        {ok, []} ->
                            %% no oracle price exists
                            {error, no_oracle_prices};
                        _ ->
                            case blockchain_ledger_v1:find_entry(Payer, Ledger) of
                                {error, _}=Error0 ->
                                    Error0;
                                {ok, Entry} ->
                                    TxnNonce = ?MODULE:nonce(Txn),
                                    LedgerNonce = blockchain_ledger_entry_v1:nonce(Entry),
                                    case TxnNonce =:= LedgerNonce + 1 of
                                        false ->
                                            {error, {bad_nonce, {token_burn, TxnNonce, LedgerNonce}}};
                                        true ->
                                        HNTAmount = ?MODULE:amount(Txn),
                                        case blockchain_ledger_v1:check_balance(Payer, HNTAmount, Ledger) of
                                            {error, _}=Error ->
                                                Error;
                                            ok ->
                                                AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                                TxnFee = ?MODULE:fee(Txn),
                                                ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                                case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                                    false ->
                                                        {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
                                                    true ->
                                                        blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
                                                end
                                        end
                                    end
                            end
                    end
            end;
        Error ->
            Error
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

-spec absorb(txn_token_burn(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    HNTAmount = ?MODULE:amount(Txn),
    {ok, DCAmount} = blockchain_ledger_v1:hnt_to_dc(HNTAmount, Ledger),
    Payer = ?MODULE:payer(Txn),
    Nonce = ?MODULE:nonce(Txn),
    TxnFee = ?MODULE:fee(Txn),
    TxnHash = ?MODULE:hash(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, TxnFee, Ledger, AreFeesEnabled, TxnHash, Chain) of
        {error, _Reason}=Error -> Error;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, HNTAmount, Nonce, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    Payee = ?MODULE:payee(Txn),
                    ok = blockchain_ledger_v1:add_hnt_burned(HNTAmount, Ledger),
                    blockchain_ledger_v1:credit_dc(Payee, DCAmount, Ledger)
            end
    end.

-spec print(txn_token_burn()) -> iodata().
print(undefined) -> <<"type=token_burn, undefined">>;
print(#blockchain_txn_token_burn_v1_pb{payer=Payer, payee=Payee, amount=Amount, nonce=Nonce}) ->
    io_lib:format("type=token_burn, payer=~p, payee=~p, amount=~p, nonce=~p",
                  [?TO_B58(Payer), ?TO_B58(Payee), Amount, Nonce]).

json_type() ->
    <<"token_burn_v1">>.

-spec to_json(txn_token_burn(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      payer => ?BIN_TO_B58(payer(Txn)),
      payee => ?BIN_TO_B58(payee(Txn)),
      amount => amount(Txn),
      nonce => nonce(Txn),
      %% Encode the memo as a base64 (NOT URL encoded) 64 bit le integer. This
      %% is what console returns and cli consumes.
      memo => base64:encode(<<(memo(Txn)):64/unsigned-little-integer>>),
      fee => fee(Txn)
     }.

 %% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

  new_test() ->
    Tx0 = #blockchain_txn_token_burn_v1_pb{
        payer= <<"payer">>,
        payee= <<"payer">>,
        amount=666,
        nonce=1,
        fee=0,
        memo=0,
        signature = <<>>
    },
    ?assertEqual(Tx0, new(<<"payer">>, 666, 1)),
    Tx1 = #blockchain_txn_token_burn_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        amount=666,
        nonce=1,
        fee=?LEGACY_TXN_FEE,
        memo=0,
        signature = <<>>
    },
    ?assertEqual(Tx1, new(<<"payer">>, <<"payee">>, 666, 1)).

payer_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(<<"payer">>, payee(Tx)).

amount_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(666, amount(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(1, nonce(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

memo_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(0, memo(Tx)),
    ?assertEqual(12, memo(memo(Tx, 12))).

signature_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_token_burn_v1_pb:encode_msg(Tx1#blockchain_txn_token_burn_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Tx = new(<<"payer">>, 666, 1),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, payer, payee, amount, nonce, memo, fee])).

-endif.
