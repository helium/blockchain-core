%%%------------------------------------------------------------------.
%% @doc
%% == Blockchain Proof of Coverage Receipt V2 ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_receipt_v2).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v2_pb.hrl").

-export([
         new/9,

         gateway/1,
         signal/1,
         data/1,
         origin/1,
         snr/1,
         tx_time/1,
         rx_time/1,
         time_acc/1,
         loc_acc/1,

         signature/1,
         sign/2,
         is_valid/1,

         print/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type origin() :: p2p | radio | integer() | undefined.
-type poc_receipt() :: #blockchain_poc_receipt_v2_pb{}.
-type poc_receipts() :: [poc_receipt()].

-export_type([poc_receipt/0, poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(GwPubkeyBin :: libp2p_crypto:pubkey_bin(),
          Signal :: integer(),
          Data :: binary(),
          Origin :: origin(),
          SNR :: integer(),
          RxTime :: non_neg_integer(),
          TxTime :: non_neg_integer(),
          TimeAcc :: integer(),
          LocAcc :: integer()) -> poc_receipt().
new(GwPubkeyBin,
    Signal,
    Data,
    Origin,
    SNR,
    RxTime,
    TxTime,
    TimeAcc,
    LocAcc) ->
    #blockchain_poc_receipt_v2_pb{
       gateway=GwPubkeyBin,
       signal=Signal,
       data=Data,
       origin=Origin,
       snr=SNR,
       rx_time=RxTime,
       tx_time=TxTime,
       time_acc=TimeAcc,
       loc_acc=LocAcc,
       signature = <<>>
      }.

-spec gateway(Receipt :: poc_receipt()) -> libp2p_crypto:pubkey_bin().
gateway(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.gateway.

-spec signal(Receipt :: poc_receipt()) -> integer().
signal(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.signal.

-spec data(Receipt :: poc_receipt()) -> binary().
data(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.data.

-spec origin(Receipt :: poc_receipt()) -> origin().
origin(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.origin.

-spec snr(Receipt :: poc_receipt()) -> integer().
snr(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.snr.

-spec tx_time(Receipt :: poc_receipt()) -> non_neg_integer().
tx_time(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.tx_time.

-spec rx_time(Receipt :: poc_receipt()) -> non_neg_integer().
rx_time(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.rx_time.

-spec time_acc(Receipt :: poc_receipt()) -> integer().
time_acc(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.time_acc.

-spec loc_acc(Receipt :: poc_receipt()) -> integer().
loc_acc(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.loc_acc.

-spec signature(Receipt :: poc_receipt()) -> binary().
signature(Receipt) ->
    Receipt#blockchain_poc_receipt_v2_pb.signature.

-spec sign(Receipt :: poc_receipt(), SigFun :: libp2p_crypto:sig_fun()) -> poc_receipt().
sign(Receipt, SigFun) ->
    BaseReceipt = Receipt#blockchain_poc_receipt_v2_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseReceipt),
    Receipt#blockchain_poc_receipt_v2_pb{signature=SigFun(EncodedReceipt)}.

-spec is_valid(Receipt :: poc_receipt()) -> boolean().
is_valid(Receipt=#blockchain_poc_receipt_v2_pb{gateway=Gateway, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Gateway),
    BaseReceipt = Receipt#blockchain_poc_receipt_v2_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseReceipt),
    libp2p_crypto:verify(EncodedReceipt, Signature, PubKey).

print(undefined) ->
    <<"type=receipt undefined">>;
print(#blockchain_poc_receipt_v2_pb{
         gateway=GwPubkeyBin,
         signal=Signal,
         origin=Origin,
         snr=SNR,
         rx_time=RxTime,
         tx_time=TxTime,
         time_acc=TimeAcc,
         loc_acc=LocAcc
        }) ->
    io_lib:format("type=receipt gateway: ~s signal: ~b origin: ~p snr: ~b, rx_time: ~b tx_time: ~b time_acc: ~b loc_acc: ~b",
                  [?TO_ANIMAL_NAME(GwPubkeyBin), Signal, Origin, SNR, RxTime, TxTime, TimeAcc, LocAcc]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Receipt = #blockchain_poc_receipt_v2_pb{
                 gateway = <<"gateway">>,
                 signal = -110,
                 data = <<"data">>,
                 origin = radio,
                 snr = 2,
                 rx_time = 666,
                 tx_time = 667,
                 time_acc = 1,
                 loc_acc = 1
                },
    New = ?MODULE:new(<<"gateway">>, -110, <<"data">>, radio, 2, 666, 667, 1, 1),
    ?assertEqual(Receipt, New).

gateway_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(<<"gateway">>, gateway(Receipt)).

signal_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(-110, signal(Receipt)).

data_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(<<"data">>, data(Receipt)).

rx_time_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(666, rx_time(Receipt)).

tx_time_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(667, tx_time(Receipt)).

snr_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(2, snr(Receipt)).

time_acc_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(1, time_acc(Receipt)).

loc_acc_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(1, loc_acc(Receipt)).

p2p_origin_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, 666, 667, 1, 1),
    ?assertEqual(p2p, origin(Receipt)).

radio_origin_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, radio, 2, 666, 667, 1, 1),
    ?assertEqual(radio, origin(Receipt)).

signature_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, radio, 2, 666, 667, 1, 1),
    ?assertEqual(<<>>, signature(Receipt)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(PubKey),
    Receipt0 = ?MODULE:new(Gateway, -110, <<"data">>, radio, 2, erlang:system_time(microsecond), erlang:system_time(microsecond), 1, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Receipt1 = sign(Receipt0, SigFun),
    Sig1 = signature(Receipt1),

    EncodedReceipt = blockchain_txn_poc_receipts_v2_pb:encode_msg(Receipt1#blockchain_poc_receipt_v2_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedReceipt, Sig1, PubKey)).

encode_decode_test() ->
    Receipt = ?MODULE:new(<<"gateway">>, -110, <<"data">>, p2p, 2, erlang:system_time(microsecond), erlang:system_time(microsecond), 1, 1),
    ?assertEqual({receipt, Receipt}, blockchain_poc_response_v2:decode(blockchain_poc_response_v2:encode(Receipt))),
    Receipt2 = ?MODULE:new(<<"gateway2">>, -110, <<"data">>, radio, 2, erlang:system_time(microsecond), erlang:system_time(microsecond), 1, 1),
    ?assertEqual({receipt, Receipt2}, blockchain_poc_response_v2:decode(blockchain_poc_response_v2:encode(Receipt2))).

-endif.
