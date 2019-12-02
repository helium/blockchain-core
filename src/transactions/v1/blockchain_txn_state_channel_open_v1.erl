%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Open ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_state_channel_open_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_state_channel_open_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    id/1,
    owner/1,
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

-type txn_state_channel_open() :: #blockchain_txn_state_channel_open_v1_pb{}.
-export_type([txn_state_channel_open/0]).

-spec new(binary(), libp2p_crypto:pubkey_bin(), pos_integer()) -> txn_state_channel_open().
new(ID, Owner, Amount) ->
    #blockchain_txn_state_channel_open_v1_pb{
        id=ID,
        owner=Owner,
        amount=Amount,
        signature = <<>>
    }.

-spec hash(txn_state_channel_open()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_state_channel_open_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec id(txn_state_channel_open()) -> binary().
id(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.id.

-spec owner(txn_state_channel_open()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.owner.

-spec amount(txn_state_channel_open()) -> pos_integer().
amount(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.amount.

-spec fee(txn_state_channel_open()) -> 0.
fee(_Txn) ->
    0.

-spec signature(txn_state_channel_open()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.signature.

-spec sign(txn_state_channel_open(), libp2p_crypto:sig_fun()) -> txn_state_channel_open().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_state_channel_open_v1_pb{signature=SigFun(EncodedTxn)}.

-spec is_valid(txn_state_channel_open(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = ?MODULE:owner(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Owner),
    BaseTxn = Txn#blockchain_txn_state_channel_open_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            Amount = ?MODULE:amount(Txn),
            blockchain_ledger_v1:check_dc_balance(Owner, Amount, Ledger)
    end.


-spec absorb(txn_state_channel_open(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = ?MODULE:owner(Txn),
    Amount = ?MODULE:amount(Txn),
    blockchain_ledger_v1:debit_dc(Owner, Amount, Ledger).

 %% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_state_channel_open_v1_pb{
        id = <<"id">>,
        owner= <<"owner">>,
        amount=666,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"id">>, <<"owner">>, 666)).

id_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666),
    ?assertEqual(<<"id">>, id(Tx)).

owner_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666),
    ?assertEqual(<<"owner">>, owner(Tx)).

amount_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666),
    ?assertEqual(666, amount(Tx)).

signature_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"id">>, <<"owner">>, 666),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_state_channel_open_v1_pb:encode_msg(Tx1#blockchain_txn_state_channel_open_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.
