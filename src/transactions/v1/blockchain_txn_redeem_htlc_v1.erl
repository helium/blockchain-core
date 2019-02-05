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
    payee :: libp2p_crypto:pubkey_bin(),
    address :: libp2p_crypto:pubkey_bin(),
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
-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), binary(), non_neg_integer()) -> txn_redeem_htlc().
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
-spec payee(txn_redeem_htlc()) -> libp2p_crypto:pubkey_bin().
payee(Txn) ->
    Txn#txn_redeem_htlc_v1.payee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(txn_redeem_htlc()) -> libp2p_crypto:pubkey_bin().
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
    PubKey = libp2p_crypto:bin_to_pubkey(Payee),
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
-spec absorb(txn_redeem_htlc(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    Fee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:transaction_fee(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, MinerFee} ->
            case (Fee >= MinerFee) of
                false ->
                    {error, insufficient_fee};
                true ->
                    case ?MODULE:is_valid(Txn) of
                        true ->
                            Address = ?MODULE:address(Txn),
                            Redeemer = ?MODULE:payee(Txn),
                            case {blockchain_ledger_v1:find_entry(Redeemer, Ledger),
                                  blockchain_ledger_v1:find_htlc(Address, Ledger)}
                            of
                                {{error, _}=Error, _} ->
                                    Error;
                                {_, {error, _}=Error} ->
                                    Error;
                                {{ok, Entry}, {ok, HTLC}} ->
                                    Payer = blockchain_ledger_htlc_v1:payer(HTLC),
                                    Payee = blockchain_ledger_htlc_v1:payee(HTLC),
                                    Nonce = blockchain_ledger_entry_v1:nonce(Entry) + 1,
                                    case blockchain_ledger_v1:debit_account(Redeemer, Fee, Nonce, Ledger) of
                                        {error, _Reason}=Error ->
                                            Error;
                                        ok ->
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
                                                                    blockchain_ledger_v1:redeem_htlc(Address, Payee, Ledger);
                                                                false ->
                                                                    {error, invalid_preimage}
                                                            end;
                                                        false ->
                                                            {error, invalid_payee}
                                                    end;
                                                true ->
                                                    Timelock = blockchain_ledger_htlc_v1:timelock(HTLC),
                                                    case blockchain_ledger_v1:current_height(Ledger) of
                                                        {error, _}=Error ->
                                                            Error;
                                                        {ok, Height} ->
                                                            case Timelock >= Height of
                                                                true ->
                                                                    {error, timelock_not_expired};
                                                                false ->
                                                                    blockchain_ledger_v1:redeem_htlc(Address, Payee, Ledger)
                                                            end
                                                    end
                                            end
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
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Payee = libp2p_crypto:pubkey_to_bin(PubKey),
    Tx0 = new(Payee, <<"address">>, <<"yolo">>, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    Keys2 = libp2p_crypto:generate_keys(ecc_compact),
    PubKey2 = maps:get(public, Keys2),
    Payee2 = libp2p_crypto:pubkey_to_bin(PubKey2),
    Tx2 = new(Payee2, <<"address">>, <<"yolo">>, 1),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

is_test() ->
    Tx = new(<<"payee">>, <<"address">>, <<"yolo">>, 1),
    ?assert(is(Tx)).

-endif.
