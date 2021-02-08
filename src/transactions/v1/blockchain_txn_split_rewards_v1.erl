-module(blockchain_txn_split_rewards_v1).
-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_split_rewards_v1_pb.hrl").

-define(STALE_POC_DEFAULT, 0).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         new/4, new/5,
         gateway/1,
         seller/1,
         buyer/1,
         percentage/1,
         seller_signature/1,
         buyer_signature/1,
         amount_to_seller/1,
         fee/2, fee/1,
         calculate_fee/2, calculate_fee/5,
         hash/1,
         sign/2,
         sign_seller/2,
         sign_buyer/2,
         is_valid/2,
         is_valid_seller/1,
         is_valid_buyer/1,
         is_valid_percentage/2,
         seller_has_percentage/1,
         is_valid_num_splits/2,
         absorb/2,
         print/1,
         to_json/2
]).

-type txn_split_rewards() :: #blockchain_txn_split_rewards_v1_pb{}.
-export_type([txn_split_rewards/0]).

-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Seller :: libp2p_crypto:pubkey_bin(),
          Buyer :: libp2p_crypto:pubkey_bin(),
          Percentage :: non_neg_integer()
         ) -> txn_split_rewards().
new(Gateway, Seller, Buyer, Percentage) when is_integer(Percentage)
                                        andalso Percentage > 0
                                        andalso Percentage =< 100 ->
    new(Gateway, Seller, Buyer, Percentage, 0).

%% @doc AmountToSeller should be given in Bones, not raw HNT
-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Seller :: libp2p_crypto:pubkey_bin(),
          Buyer :: libp2p_crypto:pubkey_bin(),
          Percentage :: non_neg_integer(),
          AmountToSeller :: non_neg_integer()) -> txn_split_rewards().
new(Gateway, Seller, Buyer, Percentage, AmountToSeller) when is_integer(AmountToSeller)
                                                        andalso AmountToSeller >= 0
                                                        andalso is_integer(Percentage)
                                                        andalso Percentage > 0
                                                        andalso Percentage =< 100 ->
    #blockchain_txn_split_rewards_v1_pb{
       gateway=Gateway,
       seller=Seller,
       buyer=Buyer,
       percentage=Percentage,
       seller_signature= <<>>,
       buyer_signature= <<>>,
       amount_to_seller=AmountToSeller,
       fee=0
      }.

-spec hash(txn_split_rewards()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_split_rewards_v1_pb{seller_signature= <<>>,
                                                        buyer_signature= <<>>},
    EncodedTxn = blockchain_txn_split_rewards_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec gateway(txn_split_rewards()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_split_rewards_v1_pb.gateway.

-spec seller(txn_split_rewards()) -> libp2p_crypto:pubkey_bin().
seller(Txn) ->
    Txn#blockchain_txn_split_rewards_v1_pb.seller.

-spec buyer(txn_split_rewards()) -> libp2p_crypto:pubkey_bin().
buyer(Txn) ->
    Txn#blockchain_txn_split_rewards_v1_pb.buyer.

-spec percentage(txn_split_rewards()) -> libp2p_crypto:pubkey_bin().
percentage(Txn) ->
  Txn#blockchain_txn_split_rewards_v1_pb.percentage.

-spec seller_signature(txn_split_rewards()) -> binary().
seller_signature(Txn) ->
    Txn#blockchain_txn_split_rewards_v1_pb.seller_signature.

-spec buyer_signature(txn_split_rewards()) -> binary().
buyer_signature(Txn) ->
    Txn#blockchain_txn_split_rewards_v1_pb.buyer_signature.

-spec amount_to_seller(txn_split_rewards()) -> non_neg_integer().
amount_to_seller(Txn) ->
    Txn#blockchain_txn_split_rewards_v1_pb.amount_to_seller.

-spec fee(txn_split_rewards()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_split_rewards_v1_pb.fee.

-spec fee(txn_split_rewards(), non_neg_integer()) -> txn_split_rewards().
fee(Txn, Fee) ->
    Txn#blockchain_txn_split_rewards_v1_pb{fee=Fee}.

-spec calculate_fee(txn_split_rewards(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_split_rewards(),
                    blockchain_ledger_v1:ledger(),
                    pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_split_rewards_v1_pb{fee=0,
                                                             buyer_signature= <<0:512>>,
                                                             seller_signature= <<0:512>>},
                   Ledger, DCPayloadSize, TxnFeeMultiplier).

-spec sign(Txn :: txn_split_rewards(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_split_rewards().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_split_rewards_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    BinTxn = blockchain_txn_split_rewards_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_split_rewards_v1_pb{seller_signature=SigFun(BinTxn)}.

-spec sign_seller(Txn :: txn_split_rewards(),
                  SigFun :: libp2p_crypto:sig_fun()) -> txn_split_rewards().
sign_seller(Txn, SigFun) -> sign(Txn, SigFun).

-spec sign_buyer(Txn :: txn_split_rewards(),
                 SigFun :: libp2p_crypto:sig_fun()) -> txn_split_rewards().
sign_buyer(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_split_rewards_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    BinTxn = blockchain_txn_split_rewards_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_split_rewards_v1_pb{buyer_signature=SigFun(BinTxn)}.

-spec is_valid_seller(txn_split_rewards()) -> boolean().
is_valid_seller(#blockchain_txn_split_rewards_v1_pb{seller=Seller,
                                                       seller_signature=SellerSig} = Txn) ->
    BaseTxn = Txn#blockchain_txn_split_rewards_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    EncodedTxn = blockchain_txn_split_rewards_v1_pb:encode_msg(BaseTxn),
    Pubkey = libp2p_crypto:bin_to_pubkey(Seller),
    libp2p_crypto:verify(EncodedTxn, SellerSig, Pubkey).

-spec is_valid_buyer(txn_split_rewards()) -> boolean().
is_valid_buyer(#blockchain_txn_split_rewards_v1_pb{buyer=Buyer,
                                                      buyer_signature=BuyerSig} = Txn) ->
    BaseTxn = Txn#blockchain_txn_split_rewards_v1_pb{buyer_signature= <<>>,
                                                        seller_signature= <<>>},
    EncodedTxn = blockchain_txn_split_rewards_v1_pb:encode_msg(BaseTxn),
    Pubkey = libp2p_crypto:bin_to_pubkey(Buyer),
    libp2p_crypto:verify(EncodedTxn, BuyerSig, Pubkey).

%% These can be made private
 -spec is_valid_percentage(non_neg_integer(), blockchain_ledger_v1:ledger()) -> boolean().
is_valid_percentage(#blockchain_txn_transfer_hotspot_v1_pb{percentage=Percentage},Ledger) ->
    {ok, RewardTransferMinimum} = blockchain:config(?reward_transfer_minimum, Ledger),
    {ok, RewardTransferMaximum} = blockchain:config(?reward_transfer_maximum, Ledger),
    case is_integer(Percentage) andalso Percentage >= RewardTransferMinimum
                                andalso Percentage =< RewardTransferMaximum of
      {error,_Reason} -> false;
      ok -> true
    end.

 -spec seller_has_percentage(txn_split_rewards()) -> boolean().
seller_has_percentage(#blockchain_txn_transfer_hotspot_v1_pb{gateway=Gateway,
                                                             seller=Seller,
                                                             percentage=Percentage}) ->
     OwnedPercentage = blockchain_ledger_gateway_v2:get_split(Gateway,Seller),
     case OwnedPercentage =< Percentage of
       {error,_Reason} -> false;
       ok -> true
     end.

 -spec is_valid_num_splits(txn_split_rewards(), blockchain_ledger_v1:ledger()) -> boolean().
is_valid_num_splits(#blockchain_txn_transfer_hotspot_v1_pb{gateway=Gateway},
                    Ledger) ->
     {ok, MaxNumSplits} = blockchain:config(?max_num_splits, Ledger),
     NumSplits = blockchain_ledger_gateway_v2:num_splits(Gateway),
     case NumSplits =< MaxNumSplits of
       {error,_Reason} -> false;
       ok -> true
     end.

 -spec is_valid(txn_split_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(#blockchain_txn_split_rewards_v1_pb{seller=Seller,
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
                  {fun() -> is_valid_percentage(Txn,Ledger) end,
                                          {error, invalid_percentage}},
                  {fun() -> seller_has_percentage(Txn) end,
                                          {error, seller_insufficient_percentage}},
                  {fun() -> is_valid_num_splits(Txn, Ledger) end,
                                          {error, too_many_splits}},
                  {fun() -> gateway_not_stale(Txn, Ledger) end,
                                          {error, gateway_too_stale}},
                  {fun() -> txn_fee_valid(Txn, Chain, AreFeesEnabled) end,
                                          {error, wrong_txn_fee}},
                  {fun() -> buyer_has_enough_hnt(Txn, Ledger) end,
                                          {error, buyer_insufficient_hnt_balance}}],
    blockchain_utils:fold_condition_checks(Conditions).

%% This only handles the case where the buyer already owns a %. Has to be reworked to 
%% handle case where buyer is not in the gateway's reward map
-spec absorb(txn_split_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Gateway = ?MODULE:gateway(Txn),
    Seller = ?MODULE:seller(Txn),
    Buyer = ?MODULE:buyer(Txn),
    Percentage = ?MODULE:percentage(Txn),
    OldSellerPercentage = blockchain_ledger_gateway_v2:get_split(Gateway,Seller),
    OldBuyerPercentage = blockchain_ledger_gateway_v2:get_split(Gateway,Buyer),
    NewSellerPercentage = OldSellerPercentage - Percentage,
    NewBuyerPercentage = OldBuyerPercentage + Percentage,
    Fee = ?MODULE:fee(Txn),
    HNTToSeller = ?MODULE:amount_to_seller(Txn),

    %% fees here are in DC (and perhaps converted to HNT automagically)
    case blockchain_ledger_v1:debit_fee(Buyer, Fee, Ledger, AreFeesEnabled) of
        {error, _Reason} = Error -> Error;
        ok ->
          %% Not sure if nonce is necessary here
            ok = blockchain_ledger_v1:debit_account(Buyer, HNTToSeller, Ledger),
            ok = blockchain_ledger_v1:credit_account(Seller, HNTToSeller, Ledger),
            ok = blockchain_ledger_v2:set_split(Gateway, Buyer, NewBuyerPercentage),
            ok = blockchain_ledger_v2:set_split(Gateway, Seller, NewSellerPercentage)
    end.

-spec print(txn_split_rewards()) -> iodata().
print(undefined) -> <<"type=transfer_hotspot, undefined">>;
print(#blockchain_txn_split_rewards_v1_pb{
         gateway=GW, seller=Seller, buyer=Buyer,
         seller_signature=SS, buyer_signature=BS,
         fee=Fee, amount_to_seller=HNT}) ->
    io_lib:format("type=transfer_hotspot, gateway=~p, seller=~p, buyer=~p, seller_signature=~p, buyer_signature=~p, fee=~p (dc), amount_to_seller=~p",
                  [?TO_ANIMAL_NAME(GW), ?TO_B58(Seller), ?TO_B58(Buyer), SS, BS, Fee, HNT]).

-spec to_json(txn_split_rewards(), blockchain_json:options()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"transfer_hotspot_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      gateway => ?BIN_TO_B58(gateway(Txn)),
      seller => ?BIN_TO_B58(seller(Txn)),
      buyer => ?BIN_TO_B58(buyer(Txn)),
      percentage => ?BIN_TO_B58(percentage(Txn)),
      fee => fee(Txn),
      amount_to_seller => amount_to_seller(Txn)
     }.

%% private functions
-spec seller_owns_gateway(txn_split_rewards(), blockchain_ledger_v1:ledger()) -> boolean().
seller_owns_gateway(#blockchain_txn_split_rewards_v1_pb{gateway=GW,
                                                           seller=Seller}, Ledger) ->
    case blockchain_gateway_cache:get(GW, Ledger) of
        {error, _} -> false;
        {ok, GwInfo} ->
            GwOwner = blockchain_ledger_gateway_v2:owner_address(GwInfo),
            Seller == GwOwner
    end.

-spec gateway_not_stale(txn_split_rewards(), blockchain_ledger_v1:ledger()) -> boolean().
gateway_not_stale(#blockchain_txn_split_rewards_v1_pb{gateway=GW}, Ledger) ->
    StaleInterval = get_config_or_default(?transfer_hotspot_stale_poc_blocks, Ledger),
    case blockchain_gateway_cache:get(GW, Ledger) of
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

-spec buyer_has_enough_hnt(txn_split_rewards(), blockchain_ledger_v1:ledger()) -> boolean().
buyer_has_enough_hnt(#blockchain_txn_split_rewards_v1_pb{fee=Fee,
                                                            amount_to_seller=SellerHNT,
                                                            buyer=Buyer},
                     Ledger) ->
    {ok, FeeInHNT} = blockchain_ledger_v1:dc_to_hnt(Fee, Ledger),
    TotalHNT = FeeInHNT + SellerHNT,
    case blockchain_ledger_v1:check_balance(Buyer, TotalHNT, Ledger) of
        {error, _Reason} -> false;
        ok -> true
    end.

-spec txn_fee_valid(txn_split_rewards(), blockchain:blockchain(), boolean()) -> boolean().
txn_fee_valid(#blockchain_txn_split_rewards_v1_pb{fee=Fee}=Txn, Chain, AreFeesEnabled) ->
    ExpectedTxnFee = calculate_fee(Txn, Chain),
    ExpectedTxnFee =< Fee orelse not AreFeesEnabled.


%% Not sure if I can delete this function and keep get_config_or_default
%% -spec buyer_nonce_correct(txn_split_rewards(), blockchain_ledger_v1:ledger()) -> boolean().
%% buyer_nonce_correct(#blockchain_txn_split_rewards_v1_pb{buyer_nonce=Nonce,
%%                                                          buyer=Buyer}, Ledger) ->
%%    case blockchain_ledger_v1:find_entry(Buyer, Ledger) of
%%        {error, _} -> false;
%%        {ok, Entry} ->
%%            Nonce =:= blockchain_ledger_entry_v1:nonce(Entry) + 1
%%    end.

-spec get_config_or_default(txn_split_rewards(), blockchain_ledger_v1:ledger()) -> boolean().
get_config_or_default(?transfer_hotspot_stale_poc_blocks=Config, Ledger) ->
    case blockchain_ledger_v1:config(Config, Ledger) of
        {error, not_found} -> ?STALE_POC_DEFAULT;
        {ok, Value} -> Value;
        Other -> Other
    end.

-ifdef(TEST).
new_4_test() ->
    Tx = #blockchain_txn_split_rewards_v1_pb{gateway= <<"gateway">>,
                                                seller= <<"seller">>,
                                                seller_signature = <<>>,
                                                buyer= <<"buyer">>,
                                                buyer_signature = <<>>,
                                                amount_to_seller=0,
                                                fee=0},
    ?assertEqual(Tx, new(<<"gateway">>, <<"seller">>, <<"buyer">>, 1)).

new_5_test() ->
    Tx = #blockchain_txn_split_rewards_v1_pb{gateway= <<"gateway">>,
                                                seller= <<"seller">>,
                                                seller_signature = <<>>,
                                                buyer= <<"buyer">>,
                                                buyer_signature = <<>>,
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
                      [type, hash, gateway, seller, buyer, amount_to_seller, fee])).


-endif.
