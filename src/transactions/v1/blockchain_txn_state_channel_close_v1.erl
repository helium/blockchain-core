%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Close ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_state_channel_close_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_state_channel_close_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    state_channel/1,
    closer/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1,
    to_json/2
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
    EncodedTxn = blockchain_txn_state_channel_close_v1_pb:encode_msg(Txn#blockchain_txn_state_channel_close_v1_pb{signature = <<>>}),
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
            case blockchain_state_channel_v1:state(SC) of
                open ->
                    {error, state_channel_state_open};
                closed ->
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
                                    case blockchain_state_channel_v1:get_summary(Closer, SC) of
                                        {error, _Reason}=E ->
                                            E;
                                        {ok, _Summary} ->
                                            %% This closer was part of the state channel
                                            %% Is therefore allowed to close said state channel
                                            ok
                                    end
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
    blockchain_ledger_v1:delete_state_channel(ID, Owner, Ledger).

-spec print(txn_state_channel_close()) -> iodata().
print(undefined) -> <<"type=state_channel_close, undefined">>;
print(#blockchain_txn_state_channel_close_v1_pb{state_channel=SC, closer=Closer}) ->
    io_lib:format("type=state_channel_close, state_channel=~p, closer=~p", [SC, ?TO_B58(Closer)]).

-spec to_json(txn_state_channel_close(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"state_channel_close_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      closer => ?BIN_TO_B58(closer(Txn)),
      state_channel => blockchain_state_channel_v1:to_json(state_channel(Txn), [])
     }.

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

to_json_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>),
    Tx = new(SC, <<"closer">>),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, closer, state_channel])).


-endif.
