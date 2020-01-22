%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Request ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_request_v1).

-export([
    new/4,
    payee/1, amount/1, payload_size/1, fingerprint/1,
    validate/1,
    encode/1, decode/1,
    hash/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type request() :: #blockchain_state_channel_request_v1_pb{}.

-export_type([request/0]).

-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer(), non_neg_integer(), integer()) -> request().
new(Payee, Amount, PayloadSize, Fingerprint) -> 
    #blockchain_state_channel_request_v1_pb{
        payee=Payee,
        amount=Amount,
        payload_size=PayloadSize,
        fingerprint=Fingerprint
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

-spec fingerprint(request()) -> integer().
fingerprint(#blockchain_state_channel_request_v1_pb{fingerprint=Fingerprint}) ->
    Fingerprint.

-spec validate(request()) -> true.
validate(_Req) ->
    true.

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
    Req = #blockchain_state_channel_request_v1_pb{
        payee= <<"payee">>,
        amount=1,
        payload_size=24,
        fingerprint= 12
    },
    ?assertEqual(Req, new(<<"payee">>, 1, 24, 12)).

payee_test() ->
    Req = new(<<"payee">>, 1, 24, 12),
    ?assertEqual(<<"payee">>, payee(Req)).

amount_test() ->
    Req = new(<<"payee">>, 1, 24, 12),
    ?assertEqual(1, amount(Req)).

payload_size_test() ->
    Req = new(<<"payee">>, 1, 24, 12),
    ?assertEqual(24, payload_size(Req)).

fingerprint_test() ->
    Req = new(<<"payee">>, 1, 24, 12),
    ?assertEqual(12, fingerprint(Req)).

validate_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Req = new(PubKeyBin, 1, 24, 12),
    ?assertEqual(true, validate(Req)).

encode_decode_test() ->
    Req = new(<<"payee">>, 1, 24, 12),
    ?assertEqual(Req, decode(encode(Req))).

-endif.