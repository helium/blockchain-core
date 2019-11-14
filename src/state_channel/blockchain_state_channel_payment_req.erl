%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Payment Request ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_payment_req).

-export([
    new/3,
    payee/1, amount/1, fingerprint/1, signature/1,
    sign/2, validate/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type payment_req() :: #helium_state_channel_payment_req_v1_pb{}.

-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer(), integer()) -> payment_req().
new(Payee, Amount, Fingerprint) -> 
    #helium_state_channel_payment_req_v1_pb{
        payee=Payee,
        amount=Amount,
        fingerprint=Fingerprint
    }.

-spec payee(payment_req()) -> libp2p_crypto:pubkey_bin().
payee(#helium_state_channel_payment_req_v1_pb{payee=Payee}) ->
    Payee.

-spec amount(payment_req()) -> non_neg_integer().
amount(#helium_state_channel_payment_req_v1_pb{amount=Amount}) ->
    Amount.

-spec fingerprint(payment_req()) -> integer().
fingerprint(#helium_state_channel_payment_req_v1_pb{fingerprint=Fingerprint}) ->
    Fingerprint.

-spec signature(payment_req()) -> binary().
signature(#helium_state_channel_payment_req_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(payment_req(), function()) -> payment_req().
sign(Req, SigFun) ->
    EncodedReq = ?MODULE:encode(Req#helium_state_channel_payment_req_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedReq),
    Req#helium_state_channel_payment_req_v1_pb{signature=Signature}.

-spec validate(payment_req()) -> true | {error, any()}.
validate(Req) ->
    BaseReq = Req#helium_state_channel_payment_req_v1_pb{signature = <<>>},
    EncodedReq = ?MODULE:encode(BaseReq),
    Signature = ?MODULE:signature(Req),
    Payee = ?MODULE:payee(Req),
    PubKey = libp2p_crypto:bin_to_pubkey(Payee),
    case libp2p_crypto:verify(EncodedReq, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            case ?MODULE:amount(Req) > 0 of
                true -> true;
                false -> {error, bad_amount}
            end
    end.

-spec encode(payment_req()) -> binary().
encode(#helium_state_channel_payment_req_v1_pb{}=Payment) ->
    helium_state_channel_v1_pb:encode_msg(Payment).

-spec decode(binary()) -> {ok, payment_req()} | {error, any()}.
decode(BinaryPayment) ->
    try helium_state_channel_v1_pb:decode_msg(BinaryPayment, helium_state_channel_payment_req_v1_pb) of
        #helium_state_channel_payment_req_v1_pb{}=Req -> {ok, Req}
    catch _:_ -> {error, failed_to_decode}
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Req = #helium_state_channel_payment_req_v1_pb{
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

signature_test() ->
    Req = new(<<"payee">>, 1, 12),
    ?assertEqual(<<>>, signature(Req)).

sign_test() ->
    #{secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Req = new(<<"payee">>, 1, 12),
    ?assertNotEqual(<<>>, signature(sign(Req, SigFun))).

validate_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Req0 = new(PubKeyBin, 1, 12),
    Req1 = sign(Req0, SigFun),
    ?assertEqual(true, validate(Req1)).

encode_decode_test() ->
    Req = new(<<"payee">>, 1, 12),
    ?assertEqual({ok, Req}, decode(encode(Req))).

-endif.