-module(blockchain_txn_transfer_hotspot_v1).
-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_transfer_hotspot_v1_pb.hrl").

-define(STALE_POC_DEFAULT, 0).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/4, new/5,
         gateway/1,
         seller/1,
         buyer/1,
         seller_signature/1,
         buyer_signature/1,
         amount_to_seller/1,
         buyer_nonce/1,
         fee/2, fee/1,
         fee_payer/2,
         calculate_fee/2, calculate_fee/5,
         hash/1,
         sign/2,
         sign_seller/2,
         sign_buyer/2,
         is_valid/2,
         is_valid_seller/1,
         is_valid_buyer/1,
         absorb/2,
         print/1,
         json_type/0,
         to_json/2
]).

-type txn_transfer_hotspot() :: #blockchain_txn_transfer_hotspot_v1_pb{}.
-export_type([txn_transfer_hotspot/0]).

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Seller :: libp2p_crypto:pubkey_bin(),
          Buyer :: libp2p_crypto:pubkey_bin(),
          BuyerNonce :: non_neg_integer()
         ) -> txn_transfer_hotspot().
new(Gateway, Seller, Buyer, BuyerNonce) ->
    new(Gateway, Seller, Buyer, BuyerNonce, 0).

%% @doc AmountToSeller should be given in Bones, not raw HNT
-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Seller :: libp2p_crypto:pubkey_bin(),
          Buyer :: libp2p_crypto:pubkey_bin(),
          BuyerNonce :: non_neg_integer(),
          AmountToSeller :: non_neg_integer()) -> txn_transfer_hotspot().
new(Gateway, Seller, Buyer, BuyerNonce, AmountToSeller) when is_integer(AmountToSeller)
                                                        andalso AmountToSeller >= 0 ->
    #blockchain_txn_transfer_hotspot_v1_pb{
       gateway=Gateway,
       seller=Seller,
       buyer=Buyer,
       seller_signature= <<>>,
       buyer_signature= <<>>,
       buyer_nonce=BuyerNonce,
       amount_to_seller=AmountToSeller,
       fee=0
      }.

-spec hash(txn_transfer_hotspot()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v1_pb{seller_signature= <<>>,
                                                        buyer_signature= <<>>},
    EncodedTxn = blockchain_txn_transfer_hotspot_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec gateway(txn_transfer_hotspot()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.gateway.

-spec seller(txn_transfer_hotspot()) -> libp2p_crypto:pubkey_bin().
seller(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.seller.

-spec buyer(txn_transfer_hotspot()) -> libp2p_crypto:pubkey_bin().
buyer(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.buyer.

-spec seller_signature(txn_transfer_hotspot()) -> binary().
seller_signature(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.seller_signature.

-spec buyer_signature(txn_transfer_hotspot()) -> binary().
buyer_signature(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.buyer_signature.

-spec buyer_nonce(txn_transfer_hotspot()) -> non_neg_integer().
buyer_nonce(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.buyer_nonce.

-spec amount_to_seller(txn_transfer_hotspot()) -> non_neg_integer().
amount_to_seller(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.amount_to_seller.

-spec fee(txn_transfer_hotspot()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb.fee.

-spec fee(txn_transfer_hotspot(), non_neg_integer()) -> txn_transfer_hotspot().
fee(Txn, Fee) ->
    Txn#blockchain_txn_transfer_hotspot_v1_pb{fee=Fee}.

-spec fee_payer(txn_transfer_hotspot(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    buyer(Txn).

-spec calculate_fee(txn_transfer_hotspot(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_transfer_hotspot(),
                    blockchain_ledger_v1:ledger(),
                    pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_transfer_hotspot_v1_pb{fee=0,
                                                             buyer_signature= <<0:512>>,
                                                             seller_signature= <<0:512>>},
                   Ledger, DCPayloadSize, TxnFeeMultiplier).

-spec sign(Txn :: txn_transfer_hotspot(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_transfer_hotspot().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    BinTxn = blockchain_txn_transfer_hotspot_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_transfer_hotspot_v1_pb{seller_signature=SigFun(BinTxn)}.

-spec sign_seller(Txn :: txn_transfer_hotspot(),
                  SigFun :: libp2p_crypto:sig_fun()) -> txn_transfer_hotspot().
sign_seller(Txn, SigFun) -> sign(Txn, SigFun).

-spec sign_buyer(Txn :: txn_transfer_hotspot(),
                 SigFun :: libp2p_crypto:sig_fun()) -> txn_transfer_hotspot().
sign_buyer(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    BinTxn = blockchain_txn_transfer_hotspot_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_transfer_hotspot_v1_pb{buyer_signature=SigFun(BinTxn)}.

-spec is_valid_seller(txn_transfer_hotspot()) -> boolean().
is_valid_seller(#blockchain_txn_transfer_hotspot_v1_pb{seller=Seller,
                                                       seller_signature=SellerSig} = Txn) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    EncodedTxn = blockchain_txn_transfer_hotspot_v1_pb:encode_msg(BaseTxn),
    Pubkey = libp2p_crypto:bin_to_pubkey(Seller),
    libp2p_crypto:verify(EncodedTxn, SellerSig, Pubkey).

-spec is_valid_buyer(txn_transfer_hotspot()) -> boolean().
is_valid_buyer(#blockchain_txn_transfer_hotspot_v1_pb{buyer=Buyer,
                                                      buyer_signature=BuyerSig} = Txn) ->
    BaseTxn = Txn#blockchain_txn_transfer_hotspot_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    EncodedTxn = blockchain_txn_transfer_hotspot_v1_pb:encode_msg(BaseTxn),
    Pubkey = libp2p_crypto:bin_to_pubkey(Buyer),
    libp2p_crypto:verify(EncodedTxn, BuyerSig, Pubkey).

-spec is_valid(txn_transfer_hotspot(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(#blockchain_txn_transfer_hotspot_v1_pb{seller=Seller,
                                                buyer=Buyer,
                                                amount_to_seller=Bones}=Txn,
         Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Conditions = [{fun() -> Seller /= Buyer end, {error, seller_is_buyer}},
                  {fun() -> ?MODULE:is_valid_seller(Txn) end,
                                          {error, bad_seller_signature}},
                  {fun() -> ?MODULE:is_valid_buyer(Txn) end,
                                          {error, bad_buyer_signature}},
                  {fun() -> is_integer(Bones) andalso Bones >= 0 end,
                                          {error, invalid_hnt_to_seller}},
                  {fun() -> gateway_not_stale(Txn, Ledger) end,
                                          {error, gateway_too_stale}},
                  {fun() -> seller_owns_gateway(Txn, Ledger) end,
                                          {error, gateway_not_owned_by_seller}},
                  {fun() -> buyer_nonce_correct(Txn, Ledger) end,
                                          {error, wrong_buyer_nonce}},
                  {fun() -> txn_fee_valid(Txn, Chain, AreFeesEnabled) end,
                                          {error, wrong_txn_fee}},
                  {fun() -> buyer_has_enough_hnt(Txn, Ledger) end,
                                          {error, buyer_insufficient_hnt_balance}}],
    blockchain_utils:fold_condition_checks(Conditions).

-spec absorb(txn_transfer_hotspot(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Gateway = ?MODULE:gateway(Txn),
    Seller = ?MODULE:seller(Txn),
    Buyer = ?MODULE:buyer(Txn),
    Fee = ?MODULE:fee(Txn),
    Hash = ?MODULE:hash(Txn),
    BuyerNonce = ?MODULE:buyer_nonce(Txn),
    HNTToSeller = ?MODULE:amount_to_seller(Txn),

    {ok, GWInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    %% fees here are in DC (and perhaps converted to HNT automagically)
    case blockchain_ledger_v1:debit_fee(Buyer, Fee, Ledger, AreFeesEnabled, Hash, Chain) of
        {error, _Reason} = Error -> Error;
        ok ->
            ok = blockchain_ledger_v1:debit_account(Buyer, HNTToSeller, BuyerNonce, Ledger),
            ok = blockchain_ledger_v1:credit_account(Seller, HNTToSeller, Ledger),
            NewGWInfo = blockchain_ledger_gateway_v2:owner_address(Buyer, GWInfo),
            ok = blockchain_ledger_v1:update_gateway(GWInfo, NewGWInfo, Gateway, Ledger)
    end.

-spec print(txn_transfer_hotspot()) -> iodata().
print(undefined) -> <<"type=transfer_hotspot, undefined">>;
print(#blockchain_txn_transfer_hotspot_v1_pb{
         gateway=GW, seller=Seller, buyer=Buyer,
         seller_signature=SS, buyer_signature=BS,
         buyer_nonce=Nonce, fee=Fee, amount_to_seller=HNT}) ->
    io_lib:format("type=transfer_hotspot, gateway=~p, seller=~p, buyer=~p, seller_signature=~p, buyer_signature=~p, buyer_nonce=~p, fee=~p (dc), amount_to_seller=~p",
                  [?TO_ANIMAL_NAME(GW), ?TO_B58(Seller), ?TO_B58(Buyer), SS, BS, Nonce, Fee, HNT]).

json_type() ->
    <<"transfer_hotspot_v1">>.

-spec to_json(txn_transfer_hotspot(), blockchain_json:options()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      gateway => ?BIN_TO_B58(gateway(Txn)),
      seller => ?BIN_TO_B58(seller(Txn)),
      buyer => ?BIN_TO_B58(buyer(Txn)),
      fee => fee(Txn),
      buyer_nonce => buyer_nonce(Txn),
      amount_to_seller => amount_to_seller(Txn)
     }.

%% private functions
-spec seller_owns_gateway(txn_transfer_hotspot(), blockchain_ledger_v1:ledger()) -> boolean().
seller_owns_gateway(#blockchain_txn_transfer_hotspot_v1_pb{gateway=GW,
                                                           seller=Seller}, Ledger) ->
    case blockchain_ledger_v1:find_gateway_info(GW, Ledger) of
        {error, _} -> false;
        {ok, GwInfo} ->
            GwOwner = blockchain_ledger_gateway_v2:owner_address(GwInfo),
            Seller == GwOwner
    end.

-spec gateway_not_stale(txn_transfer_hotspot(), blockchain_ledger_v1:ledger()) -> boolean().
gateway_not_stale(#blockchain_txn_transfer_hotspot_v1_pb{gateway=GW}, Ledger) ->
    StaleInterval = get_config_or_default(?transfer_hotspot_stale_poc_blocks, Ledger),
    case blockchain_ledger_v1:find_gateway_info(GW, Ledger) of
        {error, _} -> false;
        {ok, GwInfo} ->
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            LastPOC = case blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo) of
                          undefined -> 0;
                          X when is_integer(X) -> X
                      end,
            Interval = Height - LastPOC,
            Interval >= 0 andalso Interval =< StaleInterval
    end.

-spec buyer_has_enough_hnt(txn_transfer_hotspot(), blockchain_ledger_v1:ledger()) -> boolean().
buyer_has_enough_hnt(#blockchain_txn_transfer_hotspot_v1_pb{fee=Fee,
                                                            amount_to_seller=SellerHNT,
                                                            buyer=Buyer},
                     Ledger) ->
    {ok, FeeInHNT} = blockchain_ledger_v1:dc_to_hnt(Fee, Ledger),
    TotalHNT = FeeInHNT + SellerHNT,
    case blockchain_ledger_v1:check_balance(Buyer, TotalHNT, Ledger) of
        {error, _Reason} -> false;
        ok -> true
    end.

-spec txn_fee_valid(txn_transfer_hotspot(), blockchain:blockchain(), boolean()) -> boolean().
txn_fee_valid(#blockchain_txn_transfer_hotspot_v1_pb{fee=Fee}=Txn, Chain, AreFeesEnabled) ->
    ExpectedTxnFee = calculate_fee(Txn, Chain),
    ExpectedTxnFee =< Fee orelse not AreFeesEnabled.

-spec buyer_nonce_correct(txn_transfer_hotspot(), blockchain_ledger_v1:ledger()) -> boolean().
buyer_nonce_correct(#blockchain_txn_transfer_hotspot_v1_pb{buyer_nonce=Nonce,
                                                           buyer=Buyer}, Ledger) ->
    case blockchain_ledger_v1:find_entry(Buyer, Ledger) of
        {error, _} -> false;
        {ok, Entry} ->
            Nonce =:= blockchain_ledger_entry_v1:nonce(Entry) + 1
    end.

get_config_or_default(?transfer_hotspot_stale_poc_blocks=Config, Ledger) ->
    case blockchain_ledger_v1:config(Config, Ledger) of
        {error, not_found} -> ?STALE_POC_DEFAULT;
        {ok, Value} -> Value;
        Other -> Other
    end.

-ifdef(TEST).
new_4_test() ->
    Tx = #blockchain_txn_transfer_hotspot_v1_pb{gateway= <<"gateway">>,
                                                seller= <<"seller">>,
                                                seller_signature = <<>>,
                                                buyer= <<"buyer">>,
                                                buyer_signature = <<>>,
                                                buyer_nonce=1,
                                                amount_to_seller=0,
                                                fee=0},
    ?assertEqual(Tx, new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1)).

new_5_test() ->
    Tx = #blockchain_txn_transfer_hotspot_v1_pb{gateway= <<"gateway">>,
                                                seller= <<"seller">>,
                                                seller_signature = <<>>,
                                                buyer= <<"buyer">>,
                                                buyer_signature = <<>>,
                                                buyer_nonce=1,
                                                amount_to_seller=100,
                                                fee=0},
    ?assertEqual(Tx, new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1, 100)).

gateway_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1),
    ?assertEqual(<<"gateway">>, gateway(Tx)).

seller_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1),
    ?assertEqual(<<"seller">>, seller(Tx)).

seller_signature_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1),
    ?assertEqual(<<>>, seller_signature(Tx)).

buyer_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1),
    ?assertEqual(<<"buyer">>, buyer(Tx)).

buyer_signature_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1),
    ?assertEqual(<<>>, buyer_signature(Tx)).

amount_to_seller_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1, 100),
    ?assertEqual(100, amount_to_seller(Tx)).

fee_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1, 100),
    ?assertEqual(20, fee(fee(Tx, 20))).

sign_seller_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx = new(<<"gateway">>, libp2p_crypto:pubkey_to_bin(PubKey), <<"buyer">>, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = sign(Tx, SigFun),
    ?assert(is_valid_seller(Tx0)).

sign_buyer_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx = new(<<"gateway">>, <<"seller">>, libp2p_crypto:pubkey_to_bin(PubKey), 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = sign_buyer(Tx, SigFun),
    ?assert(is_valid_buyer(Tx0)).

to_json_test() ->
    Tx = new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1, 100),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, gateway, seller, buyer, buyer_nonce, amount_to_seller, fee])).


-endif.
