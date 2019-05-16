%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain DAta Credits Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_utils).

-export([
    new_payment/5, store_payment/3
]).


-include("blockchain.hrl").
-include("../pb/blockchain_data_credits_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
new_payment(#{secret := PrivKey, public := PubKey}, Height, Payer, Payee, Amount) -> 
    Payment = #blockchain_data_credits_payment_pb{
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
    ok = rocksdb:put(DB, CF, <<Height>>, Encoded, [{sync, true}]).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.