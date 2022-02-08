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
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_redeem_htlc_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    payee/1,
    address/1,
    preimage/1,
    fee/1, fee/2,
    fee_payer/2,
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

-define(T, #blockchain_txn_redeem_htlc_v1_pb).

-type t() :: txn_redeem_htlc().

-type txn_redeem_htlc() :: ?T{}.

-export_type([t/0, txn_redeem_htlc/0]).

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

-spec fee_payer(txn_redeem_htlc(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    payee(Txn).

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
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_redeem_htlc(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_redeem_htlc_v1_pb{fee=0, signature = <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_redeem_htlc(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Redeemer = ?MODULE:payee(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Redeemer),
    BaseTxn = Txn#blockchain_txn_redeem_htlc_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_redeem_htlc_v1_pb:encode_msg(BaseTxn),
    FieldValidation = case blockchain:config(?txn_field_validation_version, Ledger) of
                          {ok, 1} ->
                              [{{payee, Redeemer}, {address, libp2p}},
                               {{preimage, ?MODULE:preimage(Txn)}, {binary, 32}},
                               {{address, ?MODULE:address(Txn)}, {address, libp2p}}];
                          _ ->
                              [{{payee, Redeemer}, {address, libp2p}},
                               {{preimage, ?MODULE:preimage(Txn)}, {binary, 1, 32}},
                               {{address, ?MODULE:address(Txn)}, {binary, 32, 33}}]
                      end,
    case blockchain_txn:validate_fields(FieldValidation) of
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
                            case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                false ->
                                    {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
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
-spec absorb(txn_redeem_htlc(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    Redeemer = ?MODULE:payee(Txn),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    case blockchain_ledger_v1:debit_fee(Redeemer, Fee, Ledger, AreFeesEnabled, Hash, Chain) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            Address = ?MODULE:address(Txn),
            case blockchain_ledger_v1:find_htlc(Address, Ledger) of
                {error, _}=Error ->
                    Error;
                {ok, HTLC} ->
                    Payee = blockchain_ledger_htlc_v1:payee(HTLC),
                    blockchain_ledger_v1:redeem_htlc(Address, Payee, Ledger, Chain)
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

json_type() ->
    <<"redeem_htlc_v1">>.

-spec to_json(txn_redeem_htlc(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
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

is_valid_with_extended_validation_test_() ->
    {timeout,
     30,
     fun() ->
             BaseDir = test_utils:tmp_dir("is_valid_with_extended_validation_test"),
             Block = blockchain_block:new_genesis_block([]),
             {ok, Chain} = blockchain:new(BaseDir, Block, undefined, undefined),
             meck:new(blockchain_ledger_v1, [passthrough]),

             %% These are all required
             meck:expect(blockchain_ledger_v1, config,
                         fun(?deprecate_payment_v1, _) ->
                                 {ok, false};
                            (?txn_field_validation_version, _) ->
                                 %% This is new
                                 {ok, 1};
                            (?allow_zero_amount, _) ->
                                 {ok, false};
                            (?dc_payload_size, _) ->
                                 {error, not_found};
                            (?txn_fee_multiplier, _) ->
                                 {error, not_found}
                         end),
             meck:expect(blockchain_ledger_v1, txn_fees_active, fun(_) -> true end),

             #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
             SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
             Payee = libp2p_crypto:pubkey_to_bin(PubKey),

             %% We don't check for {invalid_address, payee} because that blows up on line#117
             %% regardless (that's a pubkey check)

             %% valid payee, invalid address
             Tx1 = sign(new(Payee, <<"address">>, crypto:strong_rand_bytes(32)), SigFun),
             ?assertEqual({error, {invalid_address, address}}, is_valid(Tx1, Chain)),

             #{public := PubKey2, secret := _PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
             Address = libp2p_crypto:pubkey_to_bin(PubKey2),

             %% valid payee, valid address
             Tx2 = sign(new(Payee, Address, crypto:strong_rand_bytes(32)), SigFun),
             ?assertEqual({error, not_found}, is_valid(Tx2, Chain)),

             meck:unload(blockchain_ledger_v1),
             test_utils:cleanup_tmp_dir(BaseDir)
     end}.

-endif.
