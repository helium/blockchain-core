%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_token_burn_v1).

-behavior(blockchain_txn).

-include("../../pb/blockchain_txn_token_burn_v1_pb.hrl").
-include("blockchain_vars.hrl").

  -export([
    new/3, new/4,
    hash/1,
    type/1,
    payer/1,
    key/1,
    amount/1,
    nonce/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_token_burn() :: #blockchain_txn_token_burn_v1_pb{}.
-export_type([txn_token_burn/0]).

-define(TO_B58(X), libp2p_crypto:bin_to_b58(X)).
-define(TO_ANIMAL_NAME(X), element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(X)))).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(),  pos_integer(), pos_integer()) -> txn_token_burn().
new(Payer, Amount, Nonce) ->
    #blockchain_txn_token_burn_v1_pb{
        type=account,
        payer=Payer,
        amount=Amount,
        nonce=Nonce,
        signature = <<>>
    }.


-spec new(libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer(), libp2p_crypto:pubkey_bin()) -> txn_token_burn().
new(Payer, Amount, Nonce, Key) ->
    #blockchain_txn_token_burn_v1_pb{
        type=channel,
        payer=Payer,
        key=Key,
        amount=Amount,
        nonce=Nonce,
        signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_token_burn()) -> iodata().
print(undefined) -> <<"type=token_burn undefined">>;
print(#blockchain_txn_token_burn_v1_pb{type=Type, payer=Payer, key=Key,
                                       amount=Amount, nonce=Nonce,
                                       signature=Sig}) ->
  io_lib:format("type=token_burn type=~p payer=~p key=~p amount=~p nonce=~p signature=~p",
                [Type, ?TO_B58(Payer), Key, Amount, Nonce, Sig]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_token_burn()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_token_burn_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec type(txn_token_burn()) -> account | channel.
type(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.type.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_token_burn()) -> libp2p_crypto:pubkey_bin().
payer(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.payer.

 %%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec key(txn_token_burn()) -> libp2p_crypto:pubkey_bin().
key(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.key.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(txn_token_burn()) -> pos_integer().
amount(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_token_burn()) -> pos_integer().
nonce(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.nonce.

 %%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_token_burn()) -> 0.
fee(_Txn) ->
    0.

 %%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
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
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_token_burn(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Payer = ?MODULE:payer(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_token_burn_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            case blockchain_ledger_v1:config(?token_burn_exchange_rate, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                {ok, _Rate} ->
                    Amount = ?MODULE:amount(Txn),
                    blockchain_ledger_v1:check_balance(Payer, Amount, Ledger)
            end
    end.

 %%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_token_burn(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:config(?token_burn_exchange_rate, Ledger) of
        {error, _Reason}=Error ->
            Error;
        {ok, Rate} ->
            Amount = ?MODULE:amount(Txn),
            Payer = ?MODULE:payer(Txn),
            Nonce = ?MODULE:nonce(Txn),
            case blockchain_ledger_v1:debit_account(Payer, Amount, Nonce, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                ok ->
                    Credits = Amount * Rate,
                    blockchain_ledger_v1:credit_dc(Payer, Credits, Ledger)
            end
    end.

 %% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

  new_test() ->
    Tx = #blockchain_txn_token_burn_v1_pb{
        type=account,
        payer= <<"payer">>,
        key = <<>>,
        amount=666,
        nonce=1,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"payer">>, 666, 1)).

type_test() ->
    Tx0 = new(<<"payer">>, 666, 1),
    ?assertEqual(account, type(Tx0)),
    Tx1 = new(<<"payer">>, 666, 1, <<"key">>),
    ?assertEqual(channel, type(Tx1)).

payer_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

key_test() ->
    Tx0 = new(<<"payer">>, 666, 1),
    ?assertEqual(<<>>, key(Tx0)),
    Tx1 = new(<<"payer">>, 666, 1, <<"key">>),
    ?assertEqual(<<"key">>, key(Tx1)).

amount_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(666, amount(Tx)).

nonce_test() ->
    Tx = new(<<"payer">>, 666, 1),
    ?assertEqual(1, nonce(Tx)).

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

-endif.
