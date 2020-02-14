%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Witness V2 ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_witness_v2).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v2_pb.hrl").

-export([
         new/7,
         gateway/1,
         signal/1,
         packet_hash/1,
         snr/1,
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

-type poc_witness() :: #blockchain_poc_witness_v2_pb{}.
-type poc_witnesses() :: [poc_witness()].

-export_type([poc_witness/0, poc_witnesses/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(GwPubkeyBin :: libp2p_crypto:pubkey_bin(),
          Signal :: integer(),
          PacketHash :: binary(),
          SNR :: integer(),
          RxTime :: non_neg_integer(),
          TimeAcc :: integer(),
          LocAcc :: integer()) ->  poc_witness().
new(GwPubkeyBin,
    Signal,
    PacketHash,
    SNR,
    RxTime,
    TimeAcc,
    LocAcc) ->
    #blockchain_poc_witness_v2_pb{
       gateway=GwPubkeyBin,
       signal=Signal,
       packet_hash=PacketHash,
       snr=SNR,
       rx_time=RxTime,
       time_acc=TimeAcc,
       loc_acc=LocAcc,
       signature = <<>>
      }.

-spec gateway(Witness :: poc_witness()) -> libp2p_crypto:pubkey_bin().
gateway(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.gateway.

-spec signal(Witness :: poc_witness()) -> integer().
signal(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.signal.

-spec packet_hash(Witness :: poc_witness()) -> binary().
packet_hash(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.packet_hash.

-spec snr(Witness :: poc_witness()) -> integer().
snr(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.snr.

-spec rx_time(Witness :: poc_witness()) -> non_neg_integer().
rx_time(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.rx_time.

-spec time_acc(Witness :: poc_witness()) -> integer().
time_acc(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.time_acc.

-spec loc_acc(Witness :: poc_witness()) -> integer().
loc_acc(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.loc_acc.

-spec signature(Witness :: poc_witness()) -> binary().
signature(Witness) ->
    Witness#blockchain_poc_witness_v2_pb.signature.

-spec sign(Witness :: poc_witness(), SigFun :: libp2p_crypto:sig_fun()) -> poc_witness().
sign(Witness, SigFun) ->
    BaseReceipt = Witness#blockchain_poc_witness_v2_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseReceipt),
    Witness#blockchain_poc_witness_v2_pb{signature=SigFun(EncodedReceipt)}.

-spec is_valid(Witness :: poc_witness()) -> boolean().
is_valid(Witness=#blockchain_poc_witness_v2_pb{gateway=Gateway, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Gateway),
    BaseReceipt = Witness#blockchain_poc_witness_v2_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseReceipt),
    libp2p_crypto:verify(EncodedReceipt, Signature, PubKey).

print(undefined) ->
    <<"type=witness undefined">>;
print(#blockchain_poc_witness_v2_pb{
         gateway=GwPubkeyBin,
         signal=Signal,
         snr=SNR,
         rx_time=RxTime,
         time_acc=TimeAcc,
         loc_acc=LocAcc
        }) ->
    io_lib:format("type=witness gateway: ~s signal: ~b snr: ~b, rx_time: ~b time_acc: ~b loc_acc: ~b",
                  [?TO_ANIMAL_NAME(GwPubkeyBin), Signal, SNR, RxTime, TimeAcc, LocAcc]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Witness = #blockchain_poc_witness_v2_pb{
                 gateway = <<"gateway">>,
                 signal = -110,
                 packet_hash = <<"hash">>,
                 snr = 2,
                 rx_time = 666,
                 time_acc = 1,
                 loc_acc = 1
                },
    ?assertEqual(Witness,
                 new(<<"gateway">>, -110, <<"hash">>, 2, 666, 1, 1)
                ).

gateway_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    ?assertEqual(<<"gateway">>, gateway(Witness)).

signal_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    ?assertEqual(-110, signal(Witness)).

rx_time_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, 666, 1, 1),
    ?assertEqual(666, rx_time(Witness)).

time_acc_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, 666, 1, 1),
    ?assertEqual(1, time_acc(Witness)).

loc_acc_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, 666, 1, 1),
    ?assertEqual(1, loc_acc(Witness)).

snr_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, 666, 1, 1),
    ?assertEqual(2, snr(Witness)).

packet_hash_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    ?assertEqual(<<"hash">>, packet_hash(Witness)).

signature_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    ?assertEqual(<<>>, signature(Witness)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(PubKey),
    Witness0 = new(Gateway, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Receipt1 = sign(Witness0, SigFun),
    Sig1 = signature(Receipt1),

    EncodedReceipt = blockchain_txn_poc_receipts_v2_pb:encode_msg(Receipt1#blockchain_poc_witness_v2_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedReceipt, Sig1, PubKey)).

encode_decode_test() ->
    Witness = new(<<"gateway">>, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    ?assertEqual({witness, Witness}, blockchain_poc_response_v2:decode(blockchain_poc_response_v2:encode(Witness))).

-endif.
