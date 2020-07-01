%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Close ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_state_channel_close_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_state_channel_close_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    state_channel/1,
    state_channel_id/1,
    state_channel_owner/1,
    state_channel_expire_at/1,
    closer/1,
    fee/1, fee/2,
    calculate_fee/2, calculate_fee/5,
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
       closer=Closer,
       fee=0
    }.

-spec hash(txn_state_channel_close()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_state_channel_close_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_close_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec state_channel(txn_state_channel_close()) -> blockchain_state_channel_v1:state_channel().
state_channel(Txn) ->
    Txn#blockchain_txn_state_channel_close_v1_pb.state_channel.

-spec state_channel_id(txn_state_channel_close()) -> blockchain_state_channel_v1:id().
state_channel_id(Txn) ->
    blockchain_state_channel_v1:id(Txn#blockchain_txn_state_channel_close_v1_pb.state_channel).

-spec state_channel_owner(txn_state_channel_close()) -> libp2p_crypto:pubkey_bin().
state_channel_owner(Txn) ->
    blockchain_state_channel_v1:owner(Txn#blockchain_txn_state_channel_close_v1_pb.state_channel).

-spec state_channel_expire_at(txn_state_channel_close()) -> pos_integer().
state_channel_expire_at(Txn) ->
    blockchain_state_channel_v1:expire_at_block(Txn#blockchain_txn_state_channel_close_v1_pb.state_channel).

-spec closer(txn_state_channel_close()) -> libp2p_crypto:pubkey_bin().
closer(Txn) ->
    Txn#blockchain_txn_state_channel_close_v1_pb.closer.

-spec fee(txn_state_channel_close()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_state_channel_close_v1_pb.fee.

-spec fee(txn_state_channel_close(), non_neg_integer()) -> txn_state_channel_close().
fee(Txn, Fee) ->
    Txn#blockchain_txn_state_channel_close_v1_pb{fee=Fee}.

-spec signature(txn_state_channel_close()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_state_channel_close_v1_pb.signature.

-spec sign(txn_state_channel_close(), libp2p_crypto:sig_fun()) -> txn_state_channel_close().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_state_channel_close_v1_pb:encode_msg(Txn#blockchain_txn_state_channel_close_v1_pb{signature = <<>>}),
    Txn#blockchain_txn_state_channel_close_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_state_channel_close(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_state_channel_close(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    0;
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, true) ->
    0.  %% for now we are defaulting close fees to 0

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
                                            %% Verify they can afford the fee

                                            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                            TxnFee = ?MODULE:fee(Txn),
                                            %% NOTE: TMP removing fee check as SC close fees are hardcoded to zero atm and the check breaks dialyzer
                                            %% ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                            %% case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                            %%     false ->
                                            %%         {error, {wrong_txn_fee, ExpectedTxnFee, TxnFee}};
                                            %%     true ->
                                            %%         blockchain_ledger_v1:check_dc_or_hnt_balance(Closer, TxnFee, Ledger, AreFeesEnabled)
                                            %% end
                                            blockchain_ledger_v1:check_dc_or_hnt_balance(Closer, TxnFee, Ledger, AreFeesEnabled)
                                    end
                            end
                    end
            end
    end.

-spec absorb(txn_state_channel_close(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    SC = ?MODULE:state_channel(Txn),
    ID = blockchain_state_channel_v1:id(SC),
    Owner = blockchain_state_channel_v1:owner(SC),
    Closer = ?MODULE:closer(Txn),
    TxnFee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:debit_fee(Closer, TxnFee, Ledger, AreFeesEnabled) of
        {error, _Reason}=Error -> Error;
        ok -> blockchain_ledger_v1:close_state_channel(Owner, Closer, SC, ID, Ledger)
    end.


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
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 0),
    Tx = #blockchain_txn_state_channel_close_v1_pb{
        state_channel=SC,
        closer= <<"closer">>
    },
    ?assertEqual(Tx, new(SC, <<"closer">>)).

state_channel_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 0),
    Tx = new(SC, <<"closer">>),
    ?assertEqual(SC, state_channel(Tx)).

closer_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 0),
    Tx = new(SC, <<"closer">>),
    ?assertEqual(<<"closer">>, closer(Tx)).

fee_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 0),
    Tx = new(SC, <<"closer">>),
    ?assertEqual(0, fee(Tx)).

signature_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 0),
    Tx = new(SC, <<"closer">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 0),
    Closer = libp2p_crypto:pubkey_to_bin(PubKey),
    Tx0 = new(SC, Closer),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_state_channel_close_v1_pb:encode_msg(Tx1#blockchain_txn_state_channel_close_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    SC = blockchain_state_channel_v1:new(<<"id">>, <<"owner">>, 0),
    Tx = new(SC, <<"closer">>),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, closer, state_channel])).


-endif.
