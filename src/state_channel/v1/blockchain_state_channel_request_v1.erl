%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Request ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_request_v1).

-export([
    new/6,
    payee/1,
    amount/1,
    payload_size/1,
    devaddr/1,
    seqnum/1,
    mic/1,
    sign/2,
    signature/1,
    is_valid/1,
    encode/1, decode/1,
    hash/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type request() :: #blockchain_state_channel_request_v1_pb{}.

-export_type([request/0]).

-spec new(Payee :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer(),
          PayloadSize :: non_neg_integer(),
          DevAddr :: binary(),
          SeqNum :: pos_integer(),
          MIC :: binary()) -> request().
new(Payee, Amount, PayloadSize, DevAddr, SeqNum, MIC) ->
    #blockchain_state_channel_request_v1_pb{
        payee=Payee,
        amount=Amount,
        payload_size=PayloadSize,
        devaddr=DevAddr,
        seqnum=SeqNum,
        mic=MIC
    }.

-spec payee(request()) -> libp2p_crypto:pubkey_bin().
payee(#blockchain_state_channel_request_v1_pb{payee=Payee}) ->
    Payee.

-spec amount(request()) -> non_neg_integer().
amount(#blockchain_state_channel_request_v1_pb{amount=Amount}) ->
    Amount.

-spec payload_size(request()) -> non_neg_integer().
payload_size(#blockchain_state_channel_request_v1_pb{payload_size=PayloadSize}) ->
    PayloadSize.

-spec devaddr(request()) -> binary().
devaddr(#blockchain_state_channel_request_v1_pb{devaddr=DevAddr}) ->
    DevAddr.

-spec seqnum(request()) -> pos_integer().
seqnum(#blockchain_state_channel_request_v1_pb{seqnum=SeqNum}) ->
    SeqNum.

-spec mic(request()) -> binary().
mic(#blockchain_state_channel_request_v1_pb{mic=MIC}) ->
    MIC.

-spec sign(request(), libp2p_crypto:sig_fun()) -> request().
sign(Req, SigFun) ->
    EncodedReq = blockchain_state_channel_request_v1_pb:encode_msg(Req),
    Req#blockchain_state_channel_request_v1_pb{signature=SigFun(EncodedReq)}.

-spec signature(request()) -> binary().
signature(Req) ->
    Req#blockchain_state_channel_request_v1_pb.signature.

-spec is_valid(request()) -> boolean().
is_valid(Req) ->
    Payee = ?MODULE:payee(Req),
    Signature = ?MODULE:signature(Req),
    PubKey = libp2p_crypto:bin_to_pubkey(Payee),
    BaseReq = Req#blockchain_state_channel_request_v1_pb{signature = <<>>},
    EncodedReq = blockchain_state_channel_request_v1_pb:encode_msg(BaseReq),
    libp2p_crypto:verify(EncodedReq, Signature, PubKey).

-spec encode(request()) -> binary().
encode(#blockchain_state_channel_request_v1_pb{}=Req) ->
    blockchain_state_channel_v1_pb:encode_msg(Req).

-spec decode(binary()) -> request().
decode(BinaryReq) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryReq, blockchain_state_channel_request_v1_pb).

-spec hash(request()) -> binary().
hash(#blockchain_state_channel_request_v1_pb{}=Req) ->
    crypto:hash(sha256, blockchain_state_channel_v1_pb:encode_msg(Req)).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Req0 = #blockchain_state_channel_request_v1_pb{
        payee= <<"payee">>,
        amount=1,
        payload_size=24,
        devaddr= <<"devaddr">>,
        seqnum=1,
        mic= <<"mic">>
    },
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(Req0, Req).

payee_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(<<"payee">>, payee(Req)).

amount_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(1, amount(Req)).

payload_size_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(24, payload_size(Req)).

devaddr_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(<<"devaddr">>, devaddr(Req)).

mic_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(<<"mic">>, mic(Req)).

seqnum_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(1, seqnum(Req)).

is_valid_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Req = ?MODULE:new(PubKeyBin, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(true, is_valid(Req)).

encode_decode_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(Req, decode(encode(Req))).

-endif.
