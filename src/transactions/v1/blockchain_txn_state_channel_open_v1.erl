%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Open ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_state_channel_open_v1).

-behavior(blockchain_txn).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_state_channel_open_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    id/1,
    owner/1,
    nonce/1,
    expire_within/1,
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

-define(APPROX_BLOCKS_IN_WEEK, 10080).
-define(MIN_EXPIRE_WITHIN, 10).

-type txn_state_channel_open() :: #blockchain_txn_state_channel_open_v1_pb{}.
-export_type([txn_state_channel_open/0]).

-spec new(ID :: binary(),
          Owner :: libp2p_crypto:pubkey_bin(),
          ExpireWithin :: pos_integer(),
          Nonce :: non_neg_integer()) -> txn_state_channel_open().
new(ID, Owner, ExpireWithin, Nonce) ->
    #blockchain_txn_state_channel_open_v1_pb{
        id=ID,
        owner=Owner,
        expire_within=ExpireWithin,
        nonce=Nonce,
        signature = <<>>
    }.

-spec hash(Txn :: txn_state_channel_open()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_state_channel_open_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec id(Txn :: txn_state_channel_open()) -> binary().
id(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.id.

-spec owner(Txn :: txn_state_channel_open()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.owner.

-spec nonce(Txn :: txn_state_channel_open()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.nonce.

-spec expire_within(Txn :: txn_state_channel_open()) -> pos_integer().
expire_within(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.expire_within.

-spec fee(Txn :: txn_state_channel_open()) -> 0.
fee(_Txn) ->
    0.

-spec signature(Txn :: txn_state_channel_open()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.signature.

-spec sign(Txn :: txn_state_channel_open(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_state_channel_open().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_state_channel_open_v1_pb{signature=SigFun(EncodedTxn)}.

%% TODO: Make timer limits chain vars
-spec is_valid(Txn :: txn_state_channel_open(),
               Chain :: blockchain:blockchain()) -> ok | {error, any()}.
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
            ExpireWithin = ?MODULE:expire_within(Txn),
            case ExpireWithin > ?MIN_EXPIRE_WITHIN andalso ExpireWithin < ?APPROX_BLOCKS_IN_WEEK of
                false ->
                    {error, invalid_expire_at_block};
                true ->
                    ID = ?MODULE:id(Txn),
                    case blockchain_ledger_v1:find_state_channel(ID, Owner, Ledger) of
                        {error, not_found} ->
                            case blockchain_ledger_v1:find_dc_entry(Owner, Ledger) of
                                {error, _}=Err0 ->
                                    Err0;
                                {ok, DCEntry} ->
                                    TxnNonce = ?MODULE:nonce(Txn),
                                    NextLedgerNonce = blockchain_ledger_data_credits_entry_v1:nonce(DCEntry) + 1,
                                    case TxnNonce =:= NextLedgerNonce of
                                        false ->
                                            {error, {bad_nonce, {state_channel_open, TxnNonce, NextLedgerNonce}}};
                                        true ->
                                            ok
                                    end
                            end;
                        {ok, _} ->
                            {error, state_channel_already_exists};
                        {error, _}=Err ->
                            Err
                    end
            end
    end.

-spec absorb(Txn :: txn_state_channel_open(),
             Chain :: blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    ID = ?MODULE:id(Txn),
    Owner = ?MODULE:owner(Txn),
    ExpireWithin = ?MODULE:expire_within(Txn),
    Nonce = ?MODULE:nonce(Txn),
    case blockchain_ledger_v1:debit_dc(Owner, Nonce, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            blockchain_ledger_v1:add_state_channel(ID, Owner, ExpireWithin, Nonce, Ledger)
    end.

-spec print(txn_state_channel_open()) -> iodata().
print(undefined) -> <<"type=state_channel_open, undefined">>;
print(#blockchain_txn_state_channel_open_v1_pb{id=ID, owner=Owner, expire_within=ExpireWithin}) ->
    io_lib:format("type=state_channel_open, id=~p, owner=~p, expire_within=~p",
                  [ID, ?TO_B58(Owner), ExpireWithin]).

 %% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_state_channel_open_v1_pb{
        id = <<"id">>,
        owner= <<"owner">>,
        expire_within=10,
        nonce=1,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"id">>, <<"owner">>, 10, 1)).

id_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(<<"id">>, id(Tx)).

owner_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(<<"owner">>, owner(Tx)).

signature_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"id">>, <<"owner">>, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_state_channel_open_v1_pb:encode_msg(Tx1#blockchain_txn_state_channel_open_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.
