%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Request ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_request_v1).

-export([
    new/3,
    payee/1, amount/1, fingerprint/1,
    validate/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type request() :: #helium_state_channel_request_v1_pb{}.

-export_type([request/0]).

-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer(), integer()) -> request().
new(Payee, Amount, Fingerprint) -> 
    #helium_state_channel_request_v1_pb{
        payee=Payee,
        amount=Amount,
        fingerprint=Fingerprint
    }.

-spec payee(request()) -> libp2p_crypto:pubkey_bin().
payee(#helium_state_channel_request_v1_pb{payee=Payee}) ->
    Payee.

-spec amount(request()) -> non_neg_integer().
amount(#helium_state_channel_request_v1_pb{amount=Amount}) ->
    Amount.

-spec fingerprint(request()) -> integer().
fingerprint(#helium_state_channel_request_v1_pb{fingerprint=Fingerprint}) ->
    Fingerprint.

-spec validate(request()) -> true | {error, any()}.
validate(Req) ->
    case ?MODULE:amount(Req) > 0 of
        true -> true;
        false -> {error, bad_amount}
    end.

-spec encode(request()) -> binary().
encode(#helium_state_channel_request_v1_pb{}=Payment) ->
    helium_state_channel_v1_pb:encode_msg(Payment).

-spec decode(binary()) -> request().
decode(BinaryPayment) ->
    helium_state_channel_v1_pb:decode_msg(BinaryPayment, helium_state_channel_request_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Req = #helium_state_channel_request_v1_pb{
        payee= <<"payee">>,
        amount=1,
        fingerprint= 12
    },
    ?assertEqual(Req, new(<<"payee">>, 1, 12)).

payee_test() ->
    Req = new(<<"payee">>, 1, 12),
    ?assertEqual(<<"payee">>, payee(Req)).

amount_test() ->
    Req = new(<<"payee">>, 1, 12),
    ?assertEqual(1, amount(Req)).

fingerprint_test() ->
    Req = new(<<"payee">>, 1, 12),
    ?assertEqual(12, fingerprint(Req)).

validate_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Req = new(PubKeyBin, 1, 12),
    ?assertEqual(true, validate(Req)).

encode_decode_test() ->
    Req = new(<<"payee">>, 1, 12),
    ?assertEqual(Req, decode(encode(Req))).

-endif.