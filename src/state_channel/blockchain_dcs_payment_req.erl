%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Payment Request ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_payment_req).

-export([
    new/3,
    payee/1, amount/1, fingerprint/1, signature/1,
    sign/2, validate/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/src/pb/helium_dcs_payment_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type dcs_payment_req() ::#helium_dcs_payment_req_v1_pb{}.

-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer(), binary()) -> dcs_payment_req().
new(Payee, Amount, Fingerprint) -> 
    #helium_dcs_payment_req_v1_pb{
        payee=Payee,
        amount=Amount,
        fingerprint=Fingerprint
    }.

-spec payee(dcs_payment_req()) -> binary().
payee(#helium_dcs_payment_req_v1_pb{payee=Payee}) ->
    Payee.

-spec amount(dcs_payment_req()) -> non_neg_integer().
amount(#helium_dcs_payment_req_v1_pb{amount=Amount}) ->
    Amount.

-spec fingerprint(dcs_payment_req()) -> binary().
fingerprint(#helium_dcs_payment_req_v1_pb{fingerprint=Fingerprint}) ->
    Fingerprint.

-spec signature(dcs_payment_req()) -> binary().
signature(#helium_dcs_payment_req_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(dcs_payment_req(), function()) -> dcs_payment_req().
sign(Req, SigFun) ->
    EncodedReq = ?MODULE:encode(Req#helium_dcs_payment_req_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedReq),
    Req#helium_dcs_payment_req_v1_pb{signature=Signature}.

-spec validate(dcs_payment_req()) -> true | {error, any()}.
validate(Req) ->
    BaseReq = Req#helium_dcs_payment_req_v1_pb{signature = <<>>},
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


-spec encode(dcs_payment_req()) -> binary().
encode(#helium_dcs_payment_req_v1_pb{}=Payment) ->
    helium_dcs_payment_v1_pb:encode_msg(Payment).

-spec decode(binary()) -> dcs_payment_req().
decode(BinaryPayment) ->
    helium_dcs_payment_v1_pb:decode_msg(BinaryPayment, helium_dcs_payment_req_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.