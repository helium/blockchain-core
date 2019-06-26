%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_token_burn_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_token_burn_v1_pb.hrl").

  -export([
    new/5,
    hash/1,
    type/1,
    payer/1,
    key/1,
    amount/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_token_burn() :: #blockchain_txn_token_burn_v1_pb{}.
-export_type([txn_token_burn/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(account | channel, libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          pos_integer(), non_neg_integer()) -> txn_token_burn().
new(Type, Payer, Key, Amount, Fee) ->
    #blockchain_txn_token_burn_v1_pb{
        type=Type,
        payer=Payer,
        key=Key,
        amount=Amount,
        fee=Fee,
        signature = <<>>
    }.

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
-spec fee(txn_token_burn()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_token_burn_v1_pb.fee.

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
is_valid(Txn, _Chain) ->
    Payer = ?MODULE:payer(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Payer),
    BaseTxn = Txn#blockchain_txn_token_burn_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_token_burn_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            ok
    end.

 %%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_token_burn(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(_Txn, _Chain) ->
    ok.


 %% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

  new_test() ->
    Tx = #blockchain_txn_token_burn_v1_pb{
        type=account,
        payer= <<"payer">>,
        key = <<"key">>,
        amount=666,
        fee=1,
        signature = <<>>
    },
    ?assertEqual(Tx, new(account, <<"payer">>, <<"key">>, 666, 1)).

type_test() ->
    Tx = new(account, <<"payer">>, <<"key">>, 666, 1),
    ?assertEqual(account, type(Tx)).

payer_test() ->
    Tx = new(account, <<"payer">>, <<"key">>, 666, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

key_test() ->
    Tx = new(account, <<"payer">>, <<"key">>, 666, 1),
    ?assertEqual(<<"key">>, key(Tx)).

amount_test() ->
    Tx = new(account, <<"payer">>, <<"key">>, 666, 1),
    ?assertEqual(666, amount(Tx)).

signature_test() ->
    Tx = new(account, <<"payer">>, <<"key">>, 666, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(account, <<"payer">>, <<"key">>, 666, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_token_burn_v1_pb:encode_msg(Tx1#blockchain_txn_token_burn_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.