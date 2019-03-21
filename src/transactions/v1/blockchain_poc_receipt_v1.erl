%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Receipt ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_receipt_v1).

-include("pb/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/4,
    gateway/1,
    timestamp/1,
    signal/1,
    data/1,
    signature/1,
    sign/2,
    is_valid/1,
    encode/1,
    decode/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type poc_receipt() :: #blockchain_poc_receipt_v1_pb{}.
-type poc_receipts() :: [poc_receipt()].

-export_type([poc_receipt/0, poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(Address :: libp2p_crypto:pubkey_bin(),
          Timestamp :: non_neg_integer(),
          Signal :: integer(),
          Data :: binary()) -> poc_receipt().
new(Address, Timestamp, Signal, Data) ->
    #blockchain_poc_receipt_v1_pb{
        gateway=Address,
        timestamp=Timestamp,
        signal=Signal,
        data=Data,
        signature = <<>>
    }.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway(Receipt :: poc_receipt()) -> libp2p_crypto:pubkey_bin().
gateway(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.gateway.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec timestamp(Receipt :: poc_receipt()) -> non_neg_integer().
timestamp(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.timestamp.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signal(Receipt :: poc_receipt()) -> integer().
signal(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.signal.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec data(Receipt :: poc_receipt()) -> binary().
data(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.data.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(Receipt :: poc_receipt()) -> binary().
signature(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(Receipt :: poc_receipt(), SigFun :: libp2p_crypto:sig_fun()) -> poc_receipt().
sign(Receipt, SigFun) ->
    BaseReceipt = Receipt#blockchain_poc_receipt_v1_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseReceipt),
    Receipt#blockchain_poc_receipt_v1_pb{signature=SigFun(EncodedReceipt)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(Receipt :: poc_receipt()) -> boolean().
is_valid(Receipt=#blockchain_poc_receipt_v1_pb{gateway=Gateway, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Gateway),
    BaseReceipt = Receipt#blockchain_poc_receipt_v1_pb{signature = <<>>},
    EncodedReceipt = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseReceipt),
    libp2p_crypto:verify(EncodedReceipt, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec encode(Receipt :: poc_receipt()) -> binary().
encode(Receipt) ->
    blockchain_txn_poc_receipts_v1_pb:encode_msg(Receipt).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec decode(Binary :: binary()) -> poc_receipt().
decode(Binary) ->
    blockchain_txn_poc_receipts_v1_pb:decode_msg(Binary,  blockchain_poc_receipt_v1_pb).

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
        signature = <<>>
    },
    ?assertEqual(Receipt, new(<<"gateway">>, 1, 12, <<"data">>)).

gateway_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>),
    ?assertEqual(<<"gateway">>, gateway(Receipt)).

timestamp_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>),
    ?assertEqual(1, timestamp(Receipt)).

signal_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>),
    ?assertEqual(12, signal(Receipt)).

data_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>),
    ?assertEqual(<<"data">>, data(Receipt)).

signature_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>),
    ?assertEqual(<<>>, signature(Receipt)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(PubKey),
    Receipt0 = new(Gateway, 1, 12, <<"data">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Receipt1 = sign(Receipt0, SigFun),
    Sig1 = signature(Receipt1),

    EncodedReceipt = encode(Receipt1#blockchain_poc_receipt_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedReceipt, Sig1, PubKey)).

encode_decode_test() ->
    Receipt = new(<<"gateway">>, 1, 12, <<"data">>),
    ?assertEqual(Receipt, decode(encode(Receipt))).

-endif.
