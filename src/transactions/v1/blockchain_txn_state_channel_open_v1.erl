%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Open ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_state_channel_open_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_state_channel_open_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    id/1,
    owner/1,
    amount/1,
    expire_at_block/1,
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

-spec new(binary(), libp2p_crypto:pubkey_bin(), non_neg_integer(), pos_integer()) -> txn_state_channel_open().
new(ID, Owner, Amount, ExpireAt) ->
    #blockchain_txn_state_channel_open_v1_pb{
        id=ID,
        owner=Owner,
        amount=Amount,
        expire_at_block=ExpireAt,
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

-spec amount(txn_state_channel_open()) -> integer().
amount(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.amount.

-spec expire_at_block(txn_state_channel_open()) -> pos_integer().
expire_at_block(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.expire_at_block.

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

% TODO: Make timer limits chain vars
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
            ExpireAt = ?MODULE:expire_at_block(Txn),
            {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),
            % 10080: approximate number of blocks in a week (1/min)
            case ExpireAt > CurrHeight+10 andalso ExpireAt < CurrHeight+10080 of
                false ->
                    {error, invalid_expire_at_block};
                true ->
                    ID = ?MODULE:id(Txn),
                    case blockchain_ledger_v1:find_state_channel(ID, Owner, Ledger) of
                        {error, not_found} ->
                            Amount = ?MODULE:amount(Txn),
                            case Amount of
                                A when A < 0 ->
                                    {error, bad_amount};
                                A when A > 0 ->
                                    blockchain_ledger_v1:check_dc_balance(Owner, Amount, Ledger);
                                0 ->
                                    case blockchain_state_channel_v1:zero_id() == ID of
                                        false -> {error, mistmaching_id};
                                        true -> ok
                                    end
                            end;
                        {ok, _} ->
                            {error, state_channel_already_exist};
                        {error, _}=Err ->
                            Err
                    end
            end
    end.

-spec absorb(txn_state_channel_open(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    ID = ?MODULE:id(Txn),
    Owner = ?MODULE:owner(Txn),
    Amount = ?MODULE:amount(Txn),
    ExpireAt = ?MODULE:expire_at_block(Txn),
    ok = blockchain_ledger_v1:add_state_channel(ID, Owner, Amount, ExpireAt, Ledger),
    case blockchain_state_channel_v1:zero_id() == ID of
        false -> blockchain_ledger_v1:debit_dc(Owner, Amount, Ledger);
        true -> ok
    end.

 %% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_state_channel_open_v1_pb{
        id = <<"id">>,
        owner= <<"owner">>,
        amount=666,
        expire_at_block=10,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"id">>, <<"owner">>, 666, 10)).

id_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666, 10),
    ?assertEqual(<<"id">>, id(Tx)).

owner_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666, 10),
    ?assertEqual(<<"owner">>, owner(Tx)).

amount_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666, 10),
    ?assertEqual(666, amount(Tx)).

signature_test() ->
    Tx = new(<<"id">>, <<"owner">>, 666, 10),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"id">>, <<"owner">>, 666, 10),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_state_channel_open_v1_pb:encode_msg(Tx1#blockchain_txn_state_channel_open_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.
