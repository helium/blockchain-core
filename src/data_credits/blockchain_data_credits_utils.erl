%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain DAta Credits Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_utils).

-export([
    new_payment/6, store_payment/3, encode_payment/1, decode_payment/1,
    new_payment_req/2, decode_payment_req/1, encode_payment_req/1,
    get_height/2
]).


-include("blockchain.hrl").
-include("../pb/blockchain_data_credits_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(HEIGHT_KEY, <<"height">>).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
new_payment(ID, #{secret := PrivKey, public := PubKey}, Height, Payer, Payee, Amount) -> 
    Payment = #blockchain_data_credits_payment_pb{
        id=ID,
        key=libp2p_crypto:pubkey_to_bin(PubKey),
        height=Height,
        payer=Payer,
        payee=Payee,
        amount=Amount
    },
    EncodedPayment = blockchain_data_credits_pb:encode_msg(Payment),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Signature = SigFun(EncodedPayment),
    Payment#blockchain_data_credits_payment_pb{signature=Signature}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
store_payment(DB, CF, #blockchain_data_credits_payment_pb{height=Height}=Payment) ->
    Encoded = blockchain_data_credits_pb:encode_msg(Payment),
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CF, <<Height>>, Encoded),
    ok = rocksdb:batch_put(Batch, CF, ?HEIGHT_KEY, <<Height>>),
    ok = rocksdb:write_batch(DB, Batch, []).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
encode_payment(Payment) ->
    blockchain_data_credits_pb:encode_msg(Payment).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
decode_payment(EncodedPayment) ->
    blockchain_data_credits_pb:decode_msg(EncodedPayment, blockchain_data_credits_payment_pb).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
new_payment_req(PubKeyBin, Amount) -> 
    #blockchain_data_credits_payment_req_pb{
        id=crypto:strong_rand_bytes(32),
        payee=PubKeyBin,	
        amount=Amount	
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
decode_payment_req(EncodedPaymentReq) ->
    blockchain_data_credits_pb:decode_msg(EncodedPaymentReq, blockchain_data_credits_payment_req_pb).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
encode_payment_req(PaymentReq) ->
    blockchain_data_credits_pb:encode_msg(PaymentReq).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_height(DB, CF) ->
    case rocksdb:get(DB, CF, ?HEIGHT_KEY, [{sync, true}]) of
        {ok, <<Height/integer>>} ->
            {ok, Height};
        not_found ->
            {error, not_found};
        _Error ->
            _Error
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.