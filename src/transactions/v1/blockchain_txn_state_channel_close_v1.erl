%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Close ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_state_channel_close_v1).

-behavior(blockchain_txn).

-include_lib("pb/blockchain_txn_state_channel_close_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    state_channel/1,
    closer/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_state_channel_close() :: #blockchain_txn_state_channel_close_v1_pb{}.
-export_type([txn_state_channel_close/0]).

-spec new(blockchain_state_channel_v1:state_channel(), libp2p_crypto:pubkey_bin()) -> txn_state_channel_close().
new(SC, Closer) ->
    #blockchain_txn_state_channel_close_v1_pb{
       state_channel=SC,
       closer=Closer
    }.

-spec hash(txn_state_channel_close()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_state_channel_close_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_close_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec state_channel(txn_state_channel_close()) -> blockchain_state_channel_v1:state_channel().
state_channel(Txn) ->
    Txn#blockchain_txn_state_channel_close_v1_pb.state_channel.

-spec closer(txn_state_channel_close()) -> libp2p_crypto:pubkey_bin().
closer(Txn) ->
    Txn#blockchain_txn_state_channel_close_v1_pb.closer.

-spec fee(txn_state_channel_close()) -> 0.
fee(_Txn) ->
    0.

-spec signature(txn_state_channel_close()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_state_channel_close_v1_pb.signature.

-spec sign(txn_state_channel_close(), libp2p_crypto:sig_fun()) -> txn_state_channel_close().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_state_channel_close_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_state_channel_close_v1_pb{signature=SigFun(EncodedTxn)}.

-spec is_valid(txn_state_channel_close(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Closer = ?MODULE:closer(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Closer),
    BaseTxn = Txn#blockchain_txn_state_channel_close_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_close_v1_pb:encode_msg(BaseTxn),
    SC = ?MODULE:state_channel(Txn),
    case {libp2p_crypto:verify(EncodedTxn, Signature, PubKey),
          blockchain_state_channel_v1:validate(SC)} of
        {false, _} ->
            {error, bad_closer_signature};
        {true, {error, _}} ->
            {error, bad_state_channel_signature};
        {true, ok} ->
            ID = blockchain_state_channel_v1:id(SC),
            Owner = blockchain_state_channel_v1:owner(SC),
            case blockchain_ledger_v1:find_state_channel(ID, Owner, Ledger) of
                {error, _Reason} ->
                    {error, state_channel_not_open};
                {ok, _} ->
                    case Owner == Closer of
                        true ->
                            ok;
                        false ->
                            case blockchain_state_channel_v1:balance(Closer, SC) of
                                {error, _} -> {error, closer_not_included};
                                {ok, _} -> ok
                            end
                    end
            end
    end.

-spec absorb(txn_state_channel_close(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    SC = ?MODULE:state_channel(Txn),
    ID = blockchain_state_channel_v1:id(SC),
    Owner = blockchain_state_channel_v1:owner(SC),
    ok = blockchain_ledger_v1:close_state_channel(ID, Owner, Ledger),
    case blockchain_state_channel_v1:credits(SC) of
        Credits when Credits > 0 ->
            blockchain_ledger_v1:credit_dc(Owner, Credits, Ledger);
        _ -> ok
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>),
    Tx = #blockchain_txn_state_channel_close_v1_pb{
        state_channel=SC,
        closer= <<"closer">>
    },
    ?assertEqual(Tx, new(SC, <<"closer">>)).

state_channel_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>),
    Tx = new(SC, <<"closer">>),
    ?assertEqual(SC, state_channel(Tx)).

closer_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>),
    Tx = new(SC, <<"closer">>),
    ?assertEqual(<<"closer">>, closer(Tx)).

signature_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>),
    Tx = new(SC, <<"closer">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>),
    Closer = libp2p_crypto:pubkey_to_bin(PubKey),
    Tx0 = new(SC, Closer),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_state_channel_close_v1_pb:encode_msg(Tx1#blockchain_txn_state_channel_close_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

-endif.
