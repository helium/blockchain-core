%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Witness ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_witness_v1).

-include("pb/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/4,
    gateway/1,
    timestamp/1,
    signal/1,
    packet_hash/1,
    signature/1,
    sign/2,
    is_valid/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type poc_witness() :: #blockchain_poc_witness_v1_pb{}.
-type poc_witnesss() :: [poc_witness()].

-export_type([poc_witness/0, poc_witnesss/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          PacketHash :: binary()) -> poc_witness().
new(Gateway, Timestamp, Signal, PacketHash) ->
    #blockchain_poc_witness_v1_pb{
        gateway=Gateway,
        rx_timestamp=Timestamp,
        signal=Signal,
        packet_hash=PacketHash,
        signature = <<>>
    }.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway(Receipt :: poc_witness()) -> libp2p_crypto:pubkey_bin().
gateway(Receipt) ->
    Receipt#blockchain_poc_witness_v1_pb.gateway.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec timestamp(Receipt :: poc_witness()) -> non_neg_integer().
timestamp(Receipt) ->
    Receipt#blockchain_poc_witness_v1_pb.rx_timestamp.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signal(Receipt :: poc_witness()) -> integer().
signal(Receipt) ->
    Receipt#blockchain_poc_witness_v1_pb.signal.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec packet_hash(Receipt :: poc_witness()) -> binary().
packet_hash(Receipt) ->
    Receipt#blockchain_poc_witness_v1_pb.packet_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(Receipt :: poc_witness()) -> binary().
signature(Receipt) ->
    Receipt#blockchain_poc_witness_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(Receipt :: poc_witness(), SigFun :: libp2p_crypto:sig_fun()) -> poc_witness().
sign(Receipt, SigFun) ->
    BaseReceipt = Receipt#blockchain_poc_witness_v1_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseReceipt),
    Receipt#blockchain_poc_witness_v1_pb{signature=SigFun(EncodedReceipt)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(Receipt :: poc_witness()) -> boolean().
is_valid(Receipt=#blockchain_poc_witness_v1_pb{gateway=Gateway, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Gateway),
    BaseReceipt = Receipt#blockchain_poc_witness_v1_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseReceipt),
    libp2p_crypto:verify(EncodedReceipt, Signature, PubKey).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Receipt = #blockchain_poc_witness_v1_pb{
        gateway= <<"gateway">>,
        timestamp= 1,
        signal=12,
        packet_hash= <<"hash">>,
        signature = <<>>
    },
    ?assertEqual(Receipt, new(<<"gateway">>, 1, 12, <<"hash">>)).

gateway_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(<<"gateway">>, gateway(Receipt)).

timestamp_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(1, timestamp(Receipt)).

signal_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(12, signal(Receipt)).

packet_hash_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(<<"hash">>, packet_hash(Receipt)).

signature_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual(<<>>, signature(Receipt)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(PubKey),
    Receipt0 = new(Gateway, 1, 12, <<"hash">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Receipt1 = sign(Receipt0, SigFun),
    Sig1 = signature(Receipt1),

    EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(Receipt1#blockchain_poc_witness_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedReceipt, Sig1, PubKey)).

encode_decode_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"hash">>),
    ?assertEqual({witness, Receipt}, blockchain_poc_response_v1:decode(blockchain_poc_response_v1:encode(Receipt))).

-endif.
