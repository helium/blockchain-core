%%-------------------------------------------------------------------
%% @doc
%% This module implements subnetwork fund, which will be driven
%% externally via reward server(s).
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_subnetwork_fund_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_vars.hrl").
-include("blockchain_txn_fees.hrl").
-include_lib("helium_proto/include/blockchain_txn_subnetwork_fund_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    token_type/1,
    amount/1,
    payer/1,
    nonce/1,
    signature/1,

    sign/2,
    fee/1,fee/2,
    fee_payer/2,
    calculate_fee/2, calculate_fee/5,
    is_valid/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_subnetwork_fund() :: #blockchain_txn_subnetwork_fund_v1_pb{}.

-export_type([txn_subnetwork_fund/0]).

%% ------------------------------------------------------------------
%% Public API
%% ------------------------------------------------------------------

-spec new(blockchain_token_v1:type(), non_neg_integer(), libp2p_crypto:pubkey_bin(), non_neg_integer()) ->
    txn_subnetwork_fund().
new(Type, Amount, Payer, Nonce) ->
    #blockchain_txn_subnetwork_fund_v1_pb{
        token_type = Type,
        amount = Amount,
        payer = Payer,
        nonce = Nonce,
        fee = ?LEGACY_TXN_FEE
    }.

-spec hash(txn_subnetwork_fund()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_subnetwork_fund_v1_pb:encode_msg(base(Txn)),
    crypto:hash(sha256, EncodedTxn).

-spec token_type(txn_subnetwork_fund()) -> blockchain_token_v1:type().
token_type(#blockchain_txn_subnetwork_fund_v1_pb{token_type = Type}) ->
    Type.

-spec amount(txn_subnetwork_fund()) -> non_neg_integer().
amount(#blockchain_txn_subnetwork_fund_v1_pb{amount = Amount}) ->
    Amount.

-spec payer(txn_subnetwork_fund()) -> libp2p_crypto:pubkey_bin().
payer(#blockchain_txn_subnetwork_fund_v1_pb{payer = Payer}) ->
    Payer.

-spec nonce(txn_subnetwork_fund()) -> non_neg_integer().
nonce(#blockchain_txn_subnetwork_fund_v1_pb{nonce = Nonce}) ->
    Nonce.

signature(#blockchain_txn_subnetwork_fund_v1_pb{signature = Sig}) ->
    Sig.

-spec sign(txn_subnetwork_fund(), libp2p_crypto:sig_fun()) -> txn_subnetwork_fund().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_subnetwork_fund_v1_pb:encode_msg(base(Txn)),
    Txn#blockchain_txn_subnetwork_fund_v1_pb{signature = SigFun(EncodedTxn)}.

-spec fee(txn_subnetwork_fund()) -> 0.
fee(#blockchain_txn_subnetwork_fund_v1_pb{fee = Fee}) ->
    Fee.

-spec fee(txn_subnetwork_fund(), non_neg_integer()) -> txn_subnetwork_fund().
fee(Txn, Fee) ->
    Txn#blockchain_txn_subnetwork_fund_v1_pb{fee = Fee}.

-spec fee_payer(txn_subnetwork_fund(), blockchain_ledger_v1:ledger()) ->
    libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    payer(Txn).

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_subnetwork_fund(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(
    txn_subnetwork_fund(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()
) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(
        Txn#blockchain_txn_subnetwork_fund_v1_pb{fee = 0, signature = <<0:512>>},
        Ledger,
        DCPayloadSize,
        TxnFeeMultiplier
    ).

-spec is_valid(txn_subnetwork_fund(), blockchain:blockchain()) ->
    ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Signature = ?MODULE:signature(Txn),
    Payer = ?MODULE:payer(Txn),
    TokenType = ?MODULE:token_type(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_subnetwork_fund_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_subnetwork_fund_v1_pb:encode_msg(BaseTxn),
    Amount = ?MODULE:amount(Txn),

    case fee_check(Txn, Chain, Ledger) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                false ->
                    {error, bad_signature};
                true ->
                    case Amount > 0 of
                        false ->
                            {error, zero_amount};
                        true ->
                            %% check the token type is valid
                            Subnetworks = blockchain_ledger_v1:subnetworks_v1(Ledger),
                            case maps:is_key(TokenType, Subnetworks) of
                                false ->
                                    {error, {unknown_token_type, TokenType}};
                                true ->
                                    %% check the payer has the funds in the right token type
                                    case blockchain_ledger_v1:find_entry(Payer, Ledger) of
                                        {ok, PayerEntry} ->
                                            TxnNonce = ?MODULE:nonce(Txn),
                                            LedgerEntryNonce = blockchain_ledger_entry_v2:nonce(PayerEntry),
                                            case TxnNonce =:= LedgerEntryNonce + 1 of
                                                false ->
                                                    {error, {bad_nonce, {subnetwork_fund_v1, TxnNonce, LedgerEntryNonce}}};
                                                true ->
                                                    PayerTTBalance = blockchain_ledger_entry_v2:balance(PayerEntry, TokenType),
                                                    case PayerTTBalance >= Amount of
                                                        false ->
                                                            {error, {insufficient_balance, PayerTTBalance, Amount}};
                                                        true ->
                                                            ok
                                                    end
                                            end;
                                        _ ->
                                            {error, ledger_entry_not_found}
                                    end
                            end
                    end
            end;
        Error ->
            Error
    end.

-spec absorb(txn_subnetwork_fund(), blockchain:blockchain()) ->
    ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    %% these fund are the same no matter the ledger
    TokenType = token_type(Txn),
    Amount = amount(Txn),
    Payer = payer(Txn),
    Nonce = nonce(Txn),
    Fee = fee(Txn),
    Hash = ?MODULE:hash(Txn),
    ShouldImplicitBurn = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Payer, Fee, Ledger, ShouldImplicitBurn, Hash, Chain) of
        {error, _Reason}=FeeError ->
            FeeError;
        ok ->
            case blockchain_ledger_v1:debit_account(Payer, #{TokenType => Amount}, Nonce, Ledger) of
                {error, _Reason} = Error ->
                    Error;
                ok ->
                    %% Add funds to the token treasury
                    {ok, Subnet} = blockchain_ledger_v1:find_subnetwork_v1(TokenType, Ledger),
                    TokenTreasury = blockchain_ledger_subnetwork_v1:token_treasury(Subnet),
                    Subnet1 = blockchain_ledger_subnetwork_v1:token_treasury(Subnet, TokenTreasury + Amount),
                    %% Save the subnetwork
                    ok = blockchain_ledger_v1:update_subnetwork(Subnet1, Ledger)
            end
    end.

-spec print(txn_subnetwork_fund()) -> iodata().
print(undefined) ->
    <<"type=subnetwork_fund_v1 undefined">>;
print(
    #blockchain_txn_subnetwork_fund_v1_pb{
        payer = Payer,
        token_type = TT,
        amount = Amount,
        nonce = Nonce
    }
) ->
    io_lib:format(
        "type=subnetwork_fund_v1 payer=~s, token_type=~s, amount=~b, nonce=~b",
        [?BIN_TO_B58(Payer), TT, Amount, Nonce]
    ).

json_type() ->
    <<"subnetwork_fund_v1">>.

-spec to_json(txn_subnetwork_fund(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    TT = ?MODULE:token_type(Txn),
    #{
        type => ?MODULE:json_type(),
        token_type => ?MAYBE_ATOM_TO_BINARY(TT),
        hash => ?BIN_TO_B64(hash(Txn)),
        payer => ?BIN_TO_B58(payer(Txn)),
        amount => amount(Txn),
        fee => fee(Txn),
        nonce => nonce(Txn)
    }.

-spec base(Txn) -> Txn when Txn :: txn_subnetwork_fund().
base(Txn) ->
    Txn#blockchain_txn_subnetwork_fund_v1_pb{signature = <<>>}.


-spec fee_check(
    Txn :: txn_subnetwork_fund(),
    Chain :: blockchain:blockchain(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> ok | {error, any()}.
fee_check(Txn, Chain, Ledger) ->
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
    Payer = ?MODULE:payer(Txn),
    TxnFee = ?MODULE:fee(Txn),
    case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
        false ->
            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
        true ->
            blockchain_ledger_v1:check_dc_or_hnt_balance(Payer, TxnFee, Ledger, AreFeesEnabled)
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->

    T = new(mobile, 1, <<"payer">>, 2),
    Json = to_json(T, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, token_type, hash, payer, amount, nonce])),
    ?assertEqual(<<"mobile">>, maps:get(token_type, Json)),
    ?assertEqual(?BIN_TO_B58(<<"payer">>), maps:get(payer, Json)),
    ?assertEqual(2, maps:get(nonce, Json)),
    ?assertEqual(1, maps:get(amount, Json)),
    ok.

-endif.
