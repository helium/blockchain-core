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
    is_valid/2,
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
    EncodedReq = ?MODULE:encode(Req),
    Req#blockchain_state_channel_request_v1_pb{signature=SigFun(EncodedReq)}.

-spec signature(request()) -> binary().
signature(Req) ->
    Req#blockchain_state_channel_request_v1_pb.signature.

-spec is_valid(request(), blockchain_ledger_v1:ledger()) -> boolean().
is_valid(Req, Ledger) ->
    Payee = ?MODULE:payee(Req),
    Signature = ?MODULE:signature(Req),
    PubKey = libp2p_crypto:bin_to_pubkey(Payee),
    BaseReq = Req#blockchain_state_channel_request_v1_pb{signature = <<>>},
    EncodedReq = ?MODULE:encode(BaseReq),
    CheckSignature = libp2p_crypto:verify(EncodedReq, Signature, PubKey),
    case CheckSignature of
        false ->
            false;
        true ->
            AG = maps:keys(blockchain_ledger_v1:active_gateways(Ledger)),
            lists:member(Payee, AG)
    end.

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
    BaseDir = test_utils:tmp_dir("request_is_valid_test"),
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),

    %% create a yolo ledger
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    %% Add this gateway to ledger
    ok = blockchain_ledger_v1:add_gateway(<<"some_owner">>, PubKeyBin, Ledger1),
    ok = blockchain_ledger_v1:commit_context(Ledger1),

    %% create and sign request
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Req0 = ?MODULE:new(PubKeyBin, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    Req = ?MODULE:sign(Req0, SigFun),

    ?assertEqual(true, is_valid(Req, Ledger)).

encode_decode_test() ->
    Req = ?MODULE:new(<<"payee">>, 1, 24, <<"devaddr">>, 1, <<"mic">>),
    ?assertEqual(Req, decode(encode(Req))).

-endif.
