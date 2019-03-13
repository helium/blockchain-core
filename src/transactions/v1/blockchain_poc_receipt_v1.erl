%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Receipt ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_receipt_v1).

-include("pb/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/4,
    address/1,
    timestamp/1,
    signal/1,
    hash/1,
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
          Hash :: binary()) -> poc_receipt().
new(Address, Timestamp, Signal, Hash) ->
    #blockchain_poc_receipt_v1_pb{
        address=Address,
        timestamp=Timestamp,
        signal=Signal,
        hash=Hash,
        signature = <<>>
    }.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec address(Receipt :: poc_receipt()) -> libp2p_crypto:pubkey_bin().
address(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.address.

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
-spec hash(Receipt :: poc_receipt()) -> binary().
hash(Receipt) ->
    Receipt#blockchain_poc_receipt_v1_pb.hash.

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
is_valid(Receipt=#blockchain_poc_receipt_v1_pb{address=Address, signature=Signature}) ->
    PubKey = libp2p_crypto:bin_to_pubkey(Address),
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
        address= <<"address">>,
        timestamp= 1,
        signal=12,
        hash= <<"hash">>,
        signature = <<>>
    },
    ?assertEqual(Receipt, new(<<"address">>, 1, 12, <<"hash">>)).

address_test() ->
    Receipt = new(<<"address">>, 1, 12, <<"hash">>),
    ?assertEqual(<<"address">>, address(Receipt)).

timestamp_test() ->
    Receipt = new(<<"address">>, 1, 12, <<"hash">>),
    ?assertEqual(1, timestamp(Receipt)).

signal_test() ->
    Receipt = new(<<"address">>, 1, 12, <<"hash">>),
    ?assertEqual(12, signal(Receipt)).

hash_test() ->
    Receipt = new(<<"address">>, 1, 12, <<"hash">>),
    ?assertEqual(<<"hash">>, hash(Receipt)).

signature_test() ->
    Receipt = new(<<"address">>, 1, 12, <<"hash">>),
    ?assertEqual(<<>>, signature(Receipt)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Address = libp2p_crypto:pubkey_to_bin(PubKey),
    Receipt0 = new(Address, 1, 12, <<"hash">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Receipt1 = sign(Receipt0, SigFun),
    Sig1 = signature(Receipt1),

    EncodedReceipt = encode(Receipt1#blockchain_poc_receipt_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedReceipt, Sig1, PubKey)).

encode_decode_test() ->
    Receipt = new(<<"address">>, 1, 12, <<"hash">>),
    ?assertEqual(Receipt, decode(encode(Receipt))).

-endif.
