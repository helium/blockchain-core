%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Redeem Hashed Timelock ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_redeem_htlc_v1).

-behavior(blockchain_txn).

-export([
    new/4,
    hash/1,
    payee/1,
    address/1,
    preimage/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/1,
    is/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_redeem_htlc_v1, {
    payee :: libp2p_crypto:address(),
    address :: libp2p_crypto:address(),
    preimage :: undefined | binary(),
    fee :: non_neg_integer(),
    signature :: binary()
}).

-type txn_redeem_htlc() :: #txn_redeem_htlc_v1{}.
-export_type([txn_redeem_htlc/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), libp2p_crypto:address(), binary(), non_neg_integer()) -> txn_redeem_htlc().
new(Payee, Address, PreImage, Fee) ->
    #txn_redeem_htlc_v1{
        payee=Payee,
        address=Address,
        preimage=PreImage,
        fee=Fee,
        signature= <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_redeem_htlc()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#txn_redeem_htlc_v1{signature = <<>>},
    crypto:hash(sha256, erlang:term_to_binary(BaseTxn)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payee(txn_redeem_htlc()) -> libp2p_crypto:address().
payee(Txn) ->
    Txn#txn_redeem_htlc_v1.payee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(txn_redeem_htlc()) -> libp2p_crypto:address().
address(Txn) ->
    Txn#txn_redeem_htlc_v1.address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec preimage(txn_redeem_htlc()) -> binary().
preimage(Txn) ->
    Txn#txn_redeem_htlc_v1.preimage.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_redeem_htlc()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_redeem_htlc_v1.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_redeem_htlc()) -> binary().
signature(Txn) ->
    Txn#txn_redeem_htlc_v1.signature.

%%--------------------------------------------------------------------
%% @doc
%% NOTE: payment transactions can be signed either by a worker who's part of the blockchain
%% or through the wallet? In that case presumably the wallet uses its private key to sign the
%% payment transaction.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_redeem_htlc(), libp2p_crypto:sig_fun()) -> txn_redeem_htlc().
sign(Txn, SigFun) ->
    Txn#txn_redeem_htlc_v1{signature=SigFun(erlang:term_to_binary(Txn))}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_redeem_htlc()) -> boolean().
is_valid(Txn=#txn_redeem_htlc_v1{payee=Payee, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Payee),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_redeem_htlc_v1{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_redeem_htlc_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_redeem_htlc(), blockchain_ledger:ledger()) -> {ok, blockchain_ledger:ledger()}
                                                               | {error, any()}.
absorb(Txn, Ledger0) ->
    Fee = ?MODULE:fee(Txn),
    MinerFee = blockchain_ledger:transaction_fee(Ledger0),
    case (Fee >= MinerFee) of
        false ->
            {error, insufficient_fee};
        true ->
            case ?MODULE:is_valid(Txn) of
                true ->
                    Address = ?MODULE:address(Txn),
                    HTLC =  blockchain_ledger:find_htlc(Address, blockchain_ledger:htlcs(Ledger0)),
                    Redeemer = ?MODULE:payee(Txn),
                    Payer = blockchain_ledger:htlc_payer(HTLC),
                    Payee = blockchain_ledger:htlc_payee(HTLC),
                    Entry = blockchain_ledger:find_entry(Redeemer, blockchain_ledger:entries(Ledger0)),
                    Nonce = blockchain_ledger:payment_nonce(Entry) + 1,
                    case blockchain_ledger:debit_account(Redeemer, Fee, Nonce, Ledger0) of
                        {error, _Reason}=Error ->
                            Error;
                        Ledger1 ->
                            %% if the Creator of the HTLC is not the redeemer, continue to check for pre-image
                            %% otherwise check that the timelock has expired which allows the Creator to redeem
                            case Payer =:= Redeemer of
                                false ->
                                    %% check that the address trying to redeem matches the HTLC
                                    case Redeemer =:= Payee of
                                        true ->
                                            Hashlock = blockchain_ledger:htlc_hashlock(HTLC),
                                            Preimage = ?MODULE:preimage(Txn),
                                            case (crypto:hash(sha256, Preimage) =:= Hashlock) of
                                                true ->
                                                    {ok, blockchain_ledger:redeem_htlc(Address, Payee, Ledger1)};
                                                false ->
                                                    {error, invalid_preimage}
                                            end;
                                        false ->
                                            {error, invalid_payee}
                                    end;
                                true ->
                                    Timelock = blockchain_ledger:htlc_timelock(HTLC),
                                    Height = blockchain_ledger:current_height(Ledger1),
                                    case Timelock >= Height of
                                        true ->
                                            {error, timelock_not_expired};
                                        false ->
                                            {ok, blockchain_ledger:redeem_htlc(Address, Payee, Ledger1)}
                                    end
                            end
                        end;
                false ->
                    {error, bad_signature}
        end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_redeem_htlc_v1{
        payee= <<"payee">>,
        address= <<"address">>,
        preimage= <<"yolo">>,
        fee= 1,
        signature= <<>>
    },
    ?assertEqual(Tx, new(<<"payee">>, <<"address">>, <<"yolo">>, 1)).

payee_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(<<"payee">>, payee(Tx)).

address_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(<<"address">>, address(Tx)).

preimage_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(<<"yolo">>, preimage(Tx)).

fee_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assertEqual(1, fee(Tx)).

is_valid_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Payee = libp2p_crypto:pubkey_to_address(PubKey),
    Tx0 = new(Payee, <<"address">>, <<"yolo">>, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    {_, PubKey2} = libp2p_crypto:generate_keys(),
    Payee2 = libp2p_crypto:pubkey_to_address(PubKey2),
    Tx2 = new(Payee2, <<"address">>, <<"yolo">>, 1),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

is_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assert(is(Tx)).

-endif.
