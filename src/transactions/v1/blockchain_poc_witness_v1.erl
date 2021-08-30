
%% @doc
%% == Blockchain Proof of Coverage Witness ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_witness_v1).

-behavior(blockchain_json).
-include("blockchain_caps.hrl").
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/4, new/6, new/8,
    gateway/1,
    timestamp/1,
    signal/1,
    packet_hash/1,
    snr/1,
    frequency/1,
    channel/1,
    datarate/1,
    signature/1,
    sign/2,
    is_valid/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type poc_witness() :: #blockchain_poc_witness_v1_pb{}.
-type poc_witnesses() :: [poc_witness()].

-export_type([poc_witness/0, poc_witnesses/0]).

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          PacketHash :: binary()) -> poc_witness().
new(Gateway, Timestamp, Signal, PacketHash) ->
    #blockchain_poc_witness_v1_pb{
        gateway=Gateway,
        timestamp=Timestamp,
        signal=Signal,
        packet_hash=PacketHash,
        signature = <<>>
    }.

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          PacketHash :: binary(),
          SNR :: float(),
          Frequency :: float()) -> poc_witness().
new(Gateway, Timestamp, Signal, PacketHash, SNR, Frequency) ->
    #blockchain_poc_witness_v1_pb{
        gateway=Gateway,
        timestamp=Timestamp,
        signal=Signal,
        packet_hash=PacketHash,
        snr=SNR,
        frequency=Frequency,
        signature = <<>>
    }.

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          PacketHash :: binary(),
          SNR :: float(),
          Frequency :: float(),
          Channel :: non_neg_integer(),
          DataRate :: binary()) -> poc_witness().
new(Gateway, Timestamp, Signal, PacketHash, SNR, Frequency, Channel, DataRate) ->
    #blockchain_poc_witness_v1_pb{
        gateway=Gateway,
        timestamp=Timestamp,
        signal=Signal,
        packet_hash=PacketHash,
        snr=SNR,
        frequency=Frequency,
        channel=Channel,
        datarate=DataRate,
        signature = <<>>
    }.

-spec gateway(Witness :: poc_witness()) -> libp2p_crypto:pubkey_bin().
gateway(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.gateway.

-spec timestamp(Witness :: poc_witness()) -> non_neg_integer().
timestamp(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.timestamp.

-spec signal(Witness :: poc_witness()) -> integer().
signal(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.signal.

-spec packet_hash(Witness :: poc_witness()) -> binary().
packet_hash(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.packet_hash.

-spec snr(Witness :: poc_witness()) -> float().
snr(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.snr.

-spec frequency(Witness :: poc_witness()) -> float().
frequency(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.frequency.

-spec channel(Witness :: poc_witness()) -> non_neg_integer().
channel(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.channel.

-spec datarate(Witness :: poc_witness()) -> list().
datarate(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.datarate.

-spec signature(Witness :: poc_witness()) -> binary().
signature(Witness) ->
    Witness#blockchain_poc_witness_v1_pb.signature.

-spec sign(Witness :: poc_witness(), SigFun :: libp2p_crypto:sig_fun()) -> poc_witness().
sign(Witness, SigFun) ->
    BaseWitness = Witness#blockchain_poc_witness_v1_pb{signature = <<>>},
    EncodedWitness = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseWitness),
    Witness#blockchain_poc_witness_v1_pb{signature=SigFun(EncodedWitness)}.

-spec is_valid(Witness :: poc_witness(), blockchain_ledger_v1:ledger()) -> boolean().
is_valid(Witness=#blockchain_poc_witness_v1_pb{gateway=Gateway, signature=Signature}, Ledger) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Gateway),
    BaseWitness = Witness#blockchain_poc_witness_v1_pb{signature = <<>>},
    EncodedWitness = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseWitness),
    case libp2p_crypto:verify(EncodedWitness, Signature, PubKey) of
        false -> false;
        true ->
            case blockchain_ledger_v1:find_gateway_mode(Gateway, Ledger) of
                {error, _Reason} ->
                    false;
                {ok, GWMode} ->
                    %% check this gateway is allowed to witness
                    blockchain_ledger_gateway_v2:is_valid_capability(GWMode, ?GW_CAPABILITY_POC_WITNESS, Ledger)
            end
    end.


print(undefined) ->
    <<"type=witness undefined">>;
print(#blockchain_poc_witness_v1_pb{
         gateway=Gateway,
         timestamp=TS,
         signal=Signal,
         frequency=Freq,
         snr=SNR
        }) ->
    io_lib:format("type=witness gateway: ~p timestamp: ~p signal: ~p snr: ~p freq ~p",
                  [
                   ?TO_ANIMAL_NAME(Gateway),
                   TS,
                   Signal,
                   SNR,
                   Freq
                  ]).
json_type() ->
    undefined.

-spec to_json(poc_witness(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Witness, Opts) ->
    Base0 = #{
             gateway => ?BIN_TO_B58(gateway(Witness)),
             timestamp => timestamp(Witness),
             signal => signal(Witness),
             packet_hash => ?BIN_TO_B64(packet_hash(Witness)),
             snr => ?MAYBE_UNDEFINED(snr(Witness)),
             frequency => ?MAYBE_UNDEFINED(frequency(Witness)),
             channel => ?MAYBE_UNDEFINED(channel(Witness)),
             datarate => ?MAYBE_UNDEFINED(?MAYBE_LIST_TO_BINARY(datarate(Witness)))
            },
    Base = case lists:keyfind(is_valid, 1, Opts) of
               false -> Base0;
               {is_valid, Valid} -> Base0#{ is_valid => Valid }
           end,
    case lists:keyfind(invalid_reason, 1, Opts) of
        false -> Base;
        {invalid_reason, InvalidReason} -> Base#{ invalid_reason => InvalidReason }
    end.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Witness = #blockchain_poc_witness_v1_pb{
        gateway= <<"gateway">>,
        timestamp= 1,
        signal=12,
        packet_hash= <<"hash">>,
        signature = <<>>
    },
    ?assertEqual(Witness, new(<<"gateway">>, 1, 12, <<"hash">>)).

gateway_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(<<"gateway">>, gateway(Witness)).

timestamp_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(1, timestamp(Witness)).

signal_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(12, signal(Witness)).

packet_hash_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(<<"hash">>, packet_hash(Witness)).

signature_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(<<>>, signature(Witness)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(PubKey),
    Witness0 = new(Gateway, 1, 12, <<"hash">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Witness1 = sign(Witness0, SigFun),
    Sig1 = signature(Witness1),

    EncodedWitness = blockchain_txn_poc_receipts_v1_pb:encode_msg(Witness1#blockchain_poc_witness_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedWitness, Sig1, PubKey)).

encode_decode_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual({witness, Witness}, blockchain_poc_response_v1:decode(blockchain_poc_response_v1:encode(Witness))).

to_json_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>, 9.8, 915.2, 8, "SF12BW500"),
    Json = to_json(Witness, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [gateway, timestamp, signal, packet_hash, datarate])).

new2_test() ->
    Witness = #blockchain_poc_witness_v1_pb{
        gateway= <<"gateway">>,
        timestamp= 1,
        signal=12,
        packet_hash= <<"hash">>,
        signature = <<>>,
        snr=9.8,
        frequency=915.2
    },
    ?assertEqual(Witness, new(<<"gateway">>, 1, 12, <<"hash">>, 9.8, 915.2)).

snr_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>, 9.8, 915.2),
    ?assertEqual(9.8, snr(Witness)).

frequency_test() ->
    Witness = new(<<"gateway">>, 1, 12, <<"hash">>, 9.8, 915.2),
    ?assertEqual(915.2, frequency(Witness)).

-endif.
