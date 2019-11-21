%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Payment Request ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_payment_req_v1).

-export([
    new/4,
    id/1, payee/1, amount/1, fingerprint/1, signature/1,
    sign/2, validate/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type payment_req() :: #helium_state_channel_payment_req_v1_pb{}.
-type id() :: string().

-export_type([payment_req/0, id/0]).

-spec new(id(), libp2p_crypto:pubkey_bin(), non_neg_integer(), integer()) -> payment_req().
new(ID, Payee, Amount, Fingerprint) -> 
    #helium_state_channel_payment_req_v1_pb{
        id=ID,
        payee=Payee,
        amount=Amount,
        fingerprint=Fingerprint
    }.

-spec id(payment_req()) -> id().
id(#helium_state_channel_payment_req_v1_pb{id=ID}) ->
    ID.

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

-spec decode(binary()) -> payment_req().
decode(BinaryPayment) ->
    helium_state_channel_v1_pb:decode_msg(BinaryPayment, helium_state_channel_payment_req_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Req = #helium_state_channel_payment_req_v1_pb{
        id= "id",
        payee= <<"payee">>,
        amount=1,
        fingerprint= 12
    },
    ?assertEqual(Req, new("id", <<"payee">>, 1, 12)).

id_test() ->
    Req = new("id", <<"payee">>, 1, 12),
    ?assertEqual("id", id(Req)).

payee_test() ->
    Req = new("id", <<"payee">>, 1, 12),
    ?assertEqual(<<"payee">>, payee(Req)).

amount_test() ->
    Req = new("id", <<"payee">>, 1, 12),
    ?assertEqual(1, amount(Req)).

fingerprint_test() ->
    Req = new("id", <<"payee">>, 1, 12),
    ?assertEqual(12, fingerprint(Req)).

signature_test() ->
    Req = new("id", <<"payee">>, 1, 12),
    ?assertEqual(<<>>, signature(Req)).

sign_test() ->
    #{secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Req = new("id", <<"payee">>, 1, 12),
    ?assertNotEqual(<<>>, signature(sign(Req, SigFun))).

validate_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Req0 = new("id", PubKeyBin, 1, 12),
    Req1 = sign(Req0, SigFun),
    ?assertEqual(true, validate(Req1)).

encode_decode_test() ->
    Req = new("id", <<"payee">>, 1, 12),
    ?assertEqual(Req, decode(encode(Req))).

-endif.