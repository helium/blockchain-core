%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Witness ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_response_v1).

-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([encode/1, decode/1]).

-spec encode(Receipt :: blockchain_poc_witness_v1:poc_witness() | blockchain_poc_receipt_v1:poc_receipt()) -> binary().
encode(#blockchain_poc_witness_v1_pb{} = Receipt) ->
    blockchain_txn_poc_receipts_v1_pb:encode_msg(#blockchain_poc_response_v1_pb{payload={witness, Receipt}});
encode(#blockchain_poc_receipt_v1_pb{} = Receipt) ->
    blockchain_txn_poc_receipts_v1_pb:encode_msg(#blockchain_poc_response_v1_pb{payload={receipt, Receipt}}).


-spec decode(Binary :: binary()) -> {witness, blockchain_poc_witness_v1:poc_witness()} | {receipt, blockchain_poc_receipt_v1:poc_receipt()}.
decode(Binary) ->
    Response = blockchain_txn_poc_receipts_v1_pb:decode_msg(Binary,  blockchain_poc_response_v1_pb),
    Response#blockchain_poc_response_v1_pb.payload.


