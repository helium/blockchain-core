%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain HTLC Redemption Receipt ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_htlc_receipt).

-include("blockchain_json.hrl").

-export([
    new/7,
    payer/1, payer/2,
    payee/1, payee/2,
    address/1, address/2,
    balance/1, balance/2,
    hashlock/1, hashlock/2,
    timelock/1, timelock/2,
    redeemed_at/1, redeemed_at/2,
    serialize/1, deserialize/1,
    to_json/2
]).

-record(htlc_receipt, {
    payer :: libp2p_crypto:pubkey_bin(),
    payee :: libp2p_crypto:pubkey_bin(),
    address :: libp2p_crypto:pubkey_bin(),
    balance :: non_neg_integer(),
    hashlock :: binary(),
    timelock :: non_neg_integer(),
    redeemed_at :: non_neg_integer()
}).

-type htlc_receipt() :: #htlc_receipt{}.

-export_type([htlc_receipt/0]).

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), non_neg_integer(), binary(), non_neg_integer(), non_neg_integer()) -> htlc_receipt().
new(Payer, Payee, Address, Balance, Hashlock, Timelock, RedeemedAt) ->
    #htlc_receipt{payer=Payer, payee=Payee, address=Address, balance=Balance, hashlock=Hashlock, timelock=Timelock, redeemed_at=RedeemedAt }.

-spec payer(htlc_receipt()) -> libp2p_crypto:pubkey_bin().
payer(#htlc_receipt{payer=Payer}) ->
    Payer.

-spec payer(libp2p_crypto:pubkey_bin(), htlc_receipt()) -> htlc_receipt().
payer(Payer, HTLCReceipt) ->
    HTLCReceipt#htlc_receipt{payer=Payer}.

-spec payee(htlc_receipt()) -> libp2p_crypto:pubkey_bin().
payee(#htlc_receipt{payee=Payee}) ->
    Payee.

-spec payee(libp2p_crypto:pubkey_bin(), htlc_receipt()) -> htlc_receipt().
payee(Payee, HTLCReceipt) ->
    HTLCReceipt#htlc_receipt{payee=Payee}.

-spec address(htlc_receipt()) -> libp2p_crypto:pubkey_bin().
address(#htlc_receipt{address=Address}) ->
    Address.

-spec address(libp2p_crypto:pubkey_bin(), htlc_receipt()) -> htlc_receipt().
address(Address, HTLCReceipt) ->
    HTLCReceipt#htlc_receipt{address=Address}.

-spec balance(htlc_receipt()) -> non_neg_integer().
balance(#htlc_receipt{balance=Balance}) ->
    Balance.

-spec balance(non_neg_integer(), htlc_receipt()) -> htlc_receipt().
balance(Balance, HTLCReceipt) ->
    HTLCReceipt#htlc_receipt{balance=Balance}.

-spec hashlock(htlc_receipt()) -> binary().
hashlock(#htlc_receipt{hashlock=Hashlock}) ->
    Hashlock.

-spec hashlock(binary(), htlc_receipt()) -> htlc_receipt().
hashlock(Hashlock, HTLCReceipt) ->
    HTLCReceipt#htlc_receipt{hashlock=Hashlock}.

-spec timelock(htlc_receipt()) -> non_neg_integer().
timelock(#htlc_receipt{timelock=Timelock}) ->
    Timelock.

-spec timelock(non_neg_integer(), htlc_receipt()) -> htlc_receipt().
timelock(Timelock, HTLCReceipt) ->
    HTLCReceipt#htlc_receipt{timelock=Timelock}.

-spec redeemed_at(htlc_receipt()) -> non_neg_integer().
redeemed_at(#htlc_receipt{redeemed_at=RedeemedAt}) ->
    RedeemedAt.

-spec redeemed_at(non_neg_integer(), htlc_receipt()) -> htlc_receipt().
redeemed_at(RedeemedAt, HTLCReceipt) ->
    HTLCReceipt#htlc_receipt{redeemed_at=RedeemedAt}.

-spec serialize(htlc_receipt()) -> binary().
serialize(HTLCReceipt) ->
    BinEntry = erlang:term_to_binary(HTLCReceipt, [compressed]),
    <<1, BinEntry/binary>>.

-spec deserialize(binary()) -> htlc_receipt().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

-spec to_json(htlc_receipt(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(HTLCReceipt, _Opts) ->
    #{
      payer => ?BIN_TO_B58(payer(HTLCReceipt)),
      payee => ?BIN_TO_B58(payee(HTLCReceipt)),
      address => ?BIN_TO_B58(address(HTLCReceipt)),
      balance => balance(HTLCReceipt),
      hashlock => ?BIN_TO_B64(hashlock(HTLCReceipt)),
      timelock => timelock(HTLCReceipt),
      redeemed_at => redeemed_at(HTLCReceipt)
    }.