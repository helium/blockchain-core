%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Create Hashed Timelock ==
%% == Creates a transaction that can only be redeemed
%% == by providing the correct pre-image to the hashlock
%% == within the specified timelock
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_create_htlc_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_create_htlc_v1_pb.hrl").

-export([
    new/7,
    hash/1,
    payer/1,
    payee/1,
    address/1,
    hashlock/1,
    timelock/1,
    amount/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_create_htlc() :: #blockchain_txn_create_htlc_v1_pb{}.
-export_type([txn_create_htlc/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), binary(),
          non_neg_integer(), non_neg_integer(), non_neg_integer()) -> txn_create_htlc().
new(Payer, Payee, Address, Hashlock, Timelock, Amount, Fee) ->
    #blockchain_txn_create_htlc_v1_pb{
        payer=Payer,
        payee=Payee,
        address=Address,
        hashlock=Hashlock,
        timelock=Timelock,
        amount=Amount,
        fee=Fee,
        signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_create_htlc()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_create_htlc_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_create_htlc_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_create_htlc()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.payer.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_create_htlc()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.payee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(txn_create_htlc()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hashlock(txn_create_htlc()) -> binary().
hashlock(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.hashlock.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec timelock(txn_create_htlc()) -> non_neg_integer().
timelock(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.timelock.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_create_htlc()) -> non_neg_integer().
amount(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_create_htlc()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_create_htlc()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_create_htlc_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_create_htlc(), libp2p_crypto:sig_fun()) -> txn_create_htlc().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_create_htlc_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_create_htlc_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_create_htlc()) -> boolean().
is_valid(Txn=#blockchain_txn_create_htlc_v1_pb{payer=Payer, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_create_htlc_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_create_htlc_v1_pb:encode_msg(BaseTxn),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_create_htlc(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.

absorb(Txn, Ledger) ->
    Amount = ?MODULE:amount(Txn),
    Fee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:transaction_fee(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, MinerFee} ->
            case (Amount >= 0) andalso (Fee >= MinerFee) of
                false ->
                    lager:error("amount < 0 for CreateHTLCTxn: ~p", [Txn]),
                    {error, invalid_transaction};
                true ->
                    case ?MODULE:is_valid(Txn) of
                        true ->
                            Payer = ?MODULE:payer(Txn),
                            Payee = ?MODULE:payee(Txn),
                            case blockchain_ledger_v1:find_entry(Payer, Ledger) of
                                {error, _}=Error ->
                                    Error;
                                {ok, Entry} ->
                                    Nonce = blockchain_ledger_entry_v1:nonce(Entry) + 1,
                                    case blockchain_ledger_v1:debit_account(Payer, Amount + Fee, Nonce, Ledger) of
                                        {error, _Reason}=Error ->
                                            Error;
                                        ok ->
                                            Address = ?MODULE:address(Txn),
                                            blockchain_ledger_v1:add_htlc(Address,
                                                                        Payer,
                                                                        Payee,
                                                                        Amount,
                                                                        ?MODULE:hashlock(Txn),
                                                                        ?MODULE:timelock(Txn),
                                                                        Ledger)
                                    end
                            end;
                        false ->
                            {error, bad_signature}
                    end
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_create_htlc_v1_pb{
        payer= <<"payer">>,
        payee= <<"payee">>,
        address= <<"address">>,
        hashlock= <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>,
        timelock=0,
        amount=666,
        fee=1,
        signature= <<>>
    },
    ?assertEqual(Tx, new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1)).

payer_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

payee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).

address_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"address">>, address(Tx)).

amount_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(666, amount(Tx)).

fee_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(1, fee(Tx)).

hashlock_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, hashlock(Tx)).

timelock_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(0, timelock(Tx)).

signature_test() ->
    Tx = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"payer">>, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_create_htlc_v1_pb:encode_msg(Tx1#blockchain_txn_create_htlc_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

 is_valid_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payer = libp2p_crypto:pubkey_to_bin(PubKey),
    Tx0 = new(Payer, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    Keys2 = libp2p_crypto:generate_keys(ecc_compact),
    PubKey2 = maps:get(public, Keys2),
    Payer2 = libp2p_crypto:pubkey_to_bin(PubKey2),
    Tx2 = new(Payer2, <<"payee">>, <<"address">>, <<"c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2">>, 0, 666, 1),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

-endif.
