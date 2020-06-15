%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Redeem Hashed Timelock ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_redeem_htlc_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_redeem_htlc_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    payee/1,
    address/1,
    preimage/1,
    fee/1, fee/2,
    calculate_fee/2, calculate_fee/3,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_redeem_htlc() :: #blockchain_txn_redeem_htlc_v1_pb{}.
-export_type([txn_redeem_htlc/0]).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), binary()) -> txn_redeem_htlc().
new(Payee, Address, PreImage) ->
    #blockchain_txn_redeem_htlc_v1_pb{
       payee=Payee,
       address=Address,
       preimage=PreImage,
       fee=?LEGACY_TXN_FEE,
       signature= <<>>
      }.

-spec hash(txn_redeem_htlc()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_redeem_htlc_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_redeem_htlc_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec payee(txn_redeem_htlc()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_redeem_htlc_v1_pb.payee.

-spec address(txn_redeem_htlc()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_redeem_htlc_v1_pb.address.

-spec preimage(txn_redeem_htlc()) -> binary().
preimage(Txn) ->
    Txn#blockchain_txn_redeem_htlc_v1_pb.preimage.

-spec fee(txn_redeem_htlc()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_redeem_htlc_v1_pb.fee.

-spec fee(txn_redeem_htlc(), non_neg_integer()) -> txn_redeem_htlc().
fee(Txn, Fee) ->
    Txn#blockchain_txn_redeem_htlc_v1_pb{fee=Fee}.

-spec signature(txn_redeem_htlc()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_redeem_htlc_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_redeem_htlc(), libp2p_crypto:sig_fun()) -> txn_redeem_htlc().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_redeem_htlc_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_redeem_htlc_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_redeem_htlc(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    calculate_fee(Txn, Ledger, blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_fee(txn_redeem_htlc(), blockchain_ledger_v1:ledger(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, true) ->
    ?fee(Txn#blockchain_txn_redeem_htlc_v1_pb{fee=0, signature = <<0:512>>}) * blockchain_ledger_v1:payment_txn_fee_multiplier(Ledger).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_redeem_htlc(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Redeemer = ?MODULE:payee(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Redeemer),
    BaseTxn = Txn#blockchain_txn_redeem_htlc_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_redeem_htlc_v1_pb:encode_msg(BaseTxn),
    case blockchain_txn:validate_fields([{{payee, Redeemer}, {address, libp2p}},
                                         {{preimage, ?MODULE:preimage(Txn)}, {binary, 1, 32}},
                                         {{address, ?MODULE:address(Txn)}, {binary, 32, 33}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                false ->
                    {error, bad_signature};
                true ->
                    TxnFee = ?MODULE:fee(Txn),
                    Address = ?MODULE:address(Txn),
                    case blockchain_ledger_v1:find_htlc(Address, Ledger) of
                        {error, _}=Error ->
                            Error;
                        {ok, HTLC} ->
                            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                            ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                            case ExpectedTxnFee == TxnFee orelse not AreFeesEnabled of
                                false ->
                                    {error, {wrong_txn_fee, ExpectedTxnFee, TxnFee}};
                                true ->
                                    case blockchain_ledger_v1:check_dc_or_hnt_balance(Redeemer, TxnFee, Ledger, AreFeesEnabled) of
                                        {error, _Reason}=Error ->
                                            Error;
                                        ok ->
                                            Payer = blockchain_ledger_htlc_v1:payer(HTLC),
                                            Payee = blockchain_ledger_htlc_v1:payee(HTLC),
                                            %% if the Creator of the HTLC is not the redeemer, continue to check for pre-image
                                            %% otherwise check that the timelock has expired which allows the Creator to redeem
                                            case Payer =:= Redeemer of
                                                false ->
                                                    %% check that the address trying to redeem matches the HTLC
                                                    case Redeemer =:= Payee of
                                                        true ->
                                                            Hashlock = blockchain_ledger_htlc_v1:hashlock(HTLC),
                                                            Preimage = ?MODULE:preimage(Txn),
                                                            case (crypto:hash(sha256, Preimage) =:= Hashlock) of
                                                                true ->
                                                                    ok;
                                                                false ->
                                                                    {error, invalid_preimage}
                                                            end;
                                                        false ->
                                                            {error, invalid_payee}
                                                    end;
                                                true ->
                                                    Timelock = blockchain_ledger_htlc_v1:timelock(HTLC),
                                                    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
                                                    case Timelock >= (Height+1) of
                                                        true ->
                                                            {error, timelock_not_expired};
                                                        false ->
                                                            ok
                                                    end
                                            end
                                    end
                            end
                    end
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_redeem_htlc(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Fee = ?MODULE:fee(Txn),
    Redeemer = ?MODULE:payee(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Redeemer, Fee, Ledger, AreFeesEnabled) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            Address = ?MODULE:address(Txn),
            case blockchain_ledger_v1:find_htlc(Address, Ledger) of
                {error, _}=Error ->
                    Error;
                {ok, HTLC} ->
                    Payee = blockchain_ledger_htlc_v1:payee(HTLC),
                    blockchain_ledger_v1:redeem_htlc(Address, Payee, Ledger)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_redeem_htlc()) -> iodata().
print(undefined) -> <<"type=redeem_htlc, undefined">>;
print(#blockchain_txn_redeem_htlc_v1_pb{payee=Payee, address=Address,
                                        preimage=PreImage, fee=Fee,
                                        signature=Sig}) ->
    io_lib:format("type=redeem_htlc payee=~p, address=~p, preimage=~p, fee=~p, signature=~p",
                  [?TO_B58(Payee), Address, PreImage, Fee, Sig]).

-spec to_json(txn_redeem_htlc(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"redeem_htlc_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      payee => ?BIN_TO_B58(payee(Txn)),
      address => ?BIN_TO_B58(address(Txn)),
      preimage => ?BIN_TO_B64(preimage(Txn)),
      fee => fee(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_redeem_htlc_v1_pb{
        payee= <<"payee">>,
        address= <<"address">>,
        preimage= <<"yolo">>,
        fee= ?LEGACY_TXN_FEE,
        signature= <<>>
    },
    ?assertEqual(Tx, new(<<"payee">>, <<"address">>, <<"yolo">>)).

payee_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>),
    ?assertEqual(<<"payee">>, payee(Tx)).

address_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>),
    ?assertEqual(<<"address">>, address(Tx)).

preimage_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>),
    ?assertEqual(<<"yolo">>, preimage(Tx)).

fee_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

to_json_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, payee, address, preimage, fee])).


-endif.
