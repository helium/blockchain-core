%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Receipt ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_receipt_v1).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_caps.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/5, new/7, new/9,
    gateway/1,
    timestamp/1,
    signal/1,
    data/1,
    origin/1,
    signature/1,
    snr/1,
    frequency/1,
    channel/1,
    datarate/1,
    addr_hash/1,
    addr_hash/2,
    sign/2,
    is_valid/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type origin() :: p2p | radio | integer() | undefined.
-type poc_receipt() :: #blockchain_poc_receipt_v1_pb{}.
-type poc_receipts() :: [poc_receipt()].

-export_type([poc_receipt/0, poc_receipts/0]).

-spec new(Address :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          Data :: binary(),
          Origin :: origin()) -> poc_receipt().
new(Address, Timestamp, Signal, Data, Origin) ->
    #blockchain_poc_receipt_v1_pb{
        gateway=Address,
        timestamp=Timestamp,
        signal=Signal,
        data=Data,
        origin=Origin,
        signature = <<>>
    }.

-spec new(Address :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          Data :: binary(),
          Origin :: origin(),
          SNR :: float(),
          Frequency :: float()) -> poc_receipt().
new(Address, Timestamp, Signal, Data, Origin, SNR, Frequency) ->
    #blockchain_poc_receipt_v1_pb{
        gateway=Address,
        timestamp=Timestamp,
        signal=Signal,
        data=Data,
        origin=Origin,
        snr=SNR,
        frequency=Frequency,
        signature = <<>>
    }.

-spec new(Address :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          Data :: binary(),
          Origin :: origin(),
          SNR :: float(),
          Frequency :: float(),
          Channel :: non_neg_integer(),
          DataRate :: binary()) -> poc_receipt().
new(Address, Timestamp, Signal, Data, Origin, SNR, Frequency, Channel, DataRate) ->
    #blockchain_poc_receipt_v1_pb{
        gateway=Address,
        timestamp=Timestamp,
        signal=Signal,
        data=Data,
        origin=Origin,
        snr=SNR,
        frequency=Frequency,
        channel=Channel,
        datarate=DataRate,
        signature = <<>>
    }.

-spec gateway(Receipt :: poc_receipt()) -> libp2p_crypto:pubkey_bin().
gateway(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.gateway.

-spec timestamp(Receipt :: poc_receipt()) -> non_neg_integer().
timestamp(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.timestamp.

-spec signal(Receipt :: poc_receipt()) -> integer().
signal(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.signal.

-spec data(Receipt :: poc_receipt()) -> binary().
data(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.data.

-spec origin(Receipt :: poc_receipt()) -> origin().
origin(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.origin.

-spec signature(Receipt :: poc_receipt()) -> binary().
signature(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.signature.

-spec snr(Receipt :: poc_receipt()) -> float().
snr(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.snr.

-spec frequency(Receipt :: poc_receipt()) -> float().
frequency(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.frequency.

-spec datarate(Receipt :: poc_receipt()) -> list().
datarate(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.datarate.

-spec addr_hash(Receipt :: poc_receipt()) -> 'undefined' | binary().
addr_hash(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.addr_hash.

-spec addr_hash(Receipt :: poc_receipt(), Hash :: binary()) -> poc_receipt().
addr_hash(Receipt, Hash) when is_binary(Hash), byte_size(Hash) =< 32 ->
    Receipt#blockchain_poc_receipt_v1_pb{addr_hash = Hash}.


-spec channel(Receipt :: poc_receipt()) -> non_neg_integer().
channel(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.channel.

-spec sign(Receipt :: poc_receipt(), SigFun :: libp2p_crypto:sig_fun()) -> poc_receipt().
sign(Receipt, SigFun) ->
    BaseReceipt = Receipt#blockchain_poc_receipt_v1_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseReceipt),
    Receipt#blockchain_poc_receipt_v1_pb{signature=SigFun(EncodedReceipt)}.

-spec is_valid(Receipt :: poc_receipt(), blockchain_ledger_v1:ledger()) -> boolean().
is_valid(Receipt=#blockchain_poc_receipt_v1_pb{gateway=Gateway, signature=Signature, addr_hash=AH}, Ledger) ->
    ValidHash = case blockchain_ledger_v1:config(?poc_addr_hash_byte_count, Ledger) of
                    {ok, Bytes} when is_integer(Bytes), Bytes > 0 ->
                        AH == undefined orelse AH == <<>> orelse
                        (is_binary(AH) andalso byte_size(AH) == Bytes);
                    _ ->
                        AH == undefined orelse AH == <<>>
                end,
    case ValidHash of
        false ->
            false;
        true ->
            PubKey = libp2p_crypto:bin_to_pubkey(Gateway),
            BaseReceipt = Receipt#blockchain_poc_receipt_v1_pb{signature = <<>>, addr_hash=undefined},
            EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseReceipt),
            case libp2p_crypto:verify(EncodedReceipt, Signature, PubKey) of
                false -> false;
                true ->
                    case blockchain_gateway_cache:get(Gateway, Ledger) of
                        {error, _Reason} ->
                            false;
                        {ok, GWInfo} ->
                            %% check this GW is allowed to issue receipts
                            blockchain_ledger_gateway_v2:is_valid_capability(GWInfo, ?GW_CAPABILITY_POC_RECEIPT, Ledger)
                    end
            end
    end.

print(undefined) ->
    <<"type=receipt undefined">>;
print(#blockchain_poc_receipt_v1_pb{
         gateway=Gateway,
         timestamp=TS,
         signal=Signal,
         origin=Origin
        }) ->
    io_lib:format("type=receipt gateway: ~s timestamp: ~b signal: ~b origin: ~p",
                  [
                   ?TO_ANIMAL_NAME(Gateway),
                   TS,
                   Signal,
                   Origin
                  ]).

json_type() ->
    undefined.

-spec to_json(poc_receipt() | undefined, blockchain_json:opts()) -> blockchain_json:json_object().
to_json(undefined, _Opts) ->
    undefined;
to_json(Receipt, _Opts) ->
    #{
      gateway => ?BIN_TO_B58(gateway(Receipt)),
      timestamp => timestamp(Receipt),
      signal => signal(Receipt),
      data => ?BIN_TO_B64(data(Receipt)),
      origin => origin(Receipt),
      snr => ?MAYBE_UNDEFINED(snr(Receipt)),
      frequency => ?MAYBE_UNDEFINED(frequency(Receipt)),
      channel => ?MAYBE_UNDEFINED(channel(Receipt)),
      datarate => ?MAYBE_UNDEFINED(?MAYBE_LIST_TO_BINARY(datarate(Receipt)))
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Receipt = #blockchain_poc_receipt_v1_pb{
        gateway= <<"gateway">>,
        timestamp= 1,
        signal=12,
        data= <<"data">>,
        origin=p2p,
        signature = <<>>
    },
    ?assertEqual(Receipt, new(<<"gateway">>, 1, 12, <<"data">>, p2p)).

gateway_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    ?assertEqual(<<"gateway">>, gateway(Receipt)).

timestamp_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    ?assertEqual(1, timestamp(Receipt)).

signal_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    ?assertEqual(12, signal(Receipt)).

data_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    ?assertEqual(<<"data">>, data(Receipt)).

origin_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    ?assertEqual(p2p, origin(Receipt)).

signature_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    ?assertEqual(<<>>, signature(Receipt)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(PubKey),
    Receipt0 = new(Gateway, 1, 12, <<"data">>, p2p),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Receipt1 = sign(Receipt0, SigFun),
    Sig1 = signature(Receipt1),

    EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(Receipt1#blockchain_poc_receipt_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedReceipt, Sig1, PubKey)).

encode_decode_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    ?assertEqual({receipt, Receipt}, blockchain_poc_response_v1:decode(blockchain_poc_response_v1:encode(Receipt))),
    Receipt2 = new(<<"gateway">>, 0, 13, <<"data">>, radio),
    ?assertEqual({receipt, Receipt2}, blockchain_poc_response_v1:decode(blockchain_poc_response_v1:encode(Receipt2))).

to_json_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p),
    Json = to_json(Receipt, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [gateway, timestamp, data, signal, origin])).

new2_test() ->
    Receipt = #blockchain_poc_receipt_v1_pb{
        gateway= <<"gateway">>,
        timestamp= 1,
        signal=12,
        data= <<"data">>,
        origin=p2p,
        signature = <<>>,
        snr=9.8,
        frequency=915.2
    },
    ?assertEqual(Receipt, new(<<"gateway">>, 1, 12, <<"data">>, p2p, 9.8, 915.2)).

snr_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p, 9.8, 915.2),
    ?assertEqual(9.8, snr(Receipt)).

frequency_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>, p2p, 9.8, 915.2),
    ?assertEqual(915.2, frequency(Receipt)).

-endif.
