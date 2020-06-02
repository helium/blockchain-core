-module(blockchain_ledger_oracle_price_entry).

-export([
    new/4,
    timestamp/1,
    price/1,
    block_height/1,
    public_key/1,
    timestamp/2,
    price/2,
    block_height/2,
    public_key/2
]).

-record(oracle_price_entry, {
    timestamp = 0 :: non_neg_integer(),
    block_height = 0 :: non_neg_integer(),
    public_key = undefined :: libp2p_crypto:pubkey_bin() | undefined,
    price = 0 :: non_neg_integer()
}).

-type oracle_price_entry() :: #oracle_price_entry{}.
-export_type([oracle_price_entry/0]).

-spec new( Timestamp :: non_neg_integer(),
           BlockHeight :: non_neg_integer(),
           PublicKey :: libp2p_crypto:pubkey_bin(),
           Price :: non_neg_integer() ) -> oracle_price_entry().
%% @doc Create a new price entry
new(Timestamp, BlockHeight, PublicKey, Price) ->
    #oracle_price_entry{
       timestamp = Timestamp,
       block_height = BlockHeight,
       public_key = PublicKey,
       price = Price
    }.

-spec timestamp( Entry :: oracle_price_entry() ) -> non_neg_integer().
%% @doc Return the timestamp from the given price entry.
timestamp(#oracle_price_entry{ timestamp = T }) -> T.

-spec price( Entry :: oracle_price_entry() ) -> non_neg_integer().
%% @doc Return the price from the given price entry.
price(#oracle_price_entry{ price = P }) -> P.

-spec block_height( Entry :: oracle_price_entry() ) -> non_neg_integer().
%% @doc Return the block_height from the given price entry.
block_height(#oracle_price_entry{ block_height = H }) -> H.

-spec public_key( Entry :: oracle_price_entry() ) -> libp2p_crypto:pubkey_bin() | undefined.
%% @doc Return the public_key from the given price entry.
public_key(#oracle_price_entry{ public_key = K }) -> K.

-spec timestamp( Entry :: oracle_price_entry(),
                 Value :: non_neg_integer() ) -> Updated :: oracle_price_entry().
%% @doc Update the timestamp value for the given price entry and return the update
timestamp(Entry = #oracle_price_entry{}, Value) when is_integer(Value)
                                                andalso Value >= 0 ->
    Entry#oracle_price_entry{ timestamp = Value }.

-spec block_height( Entry :: oracle_price_entry(),
                    Value :: non_neg_integer() ) -> Updated :: oracle_price_entry().
%% @doc Update the block height value for the given price entry and return the update
block_height(Entry = #oracle_price_entry{}, Value) when is_integer(Value)
                                                andalso Value >= 0 ->
    Entry#oracle_price_entry{ block_height = Value }.

-spec public_key( Entry :: oracle_price_entry(),
                  Value :: libp2p_crypto:pubkey_bin() ) -> Updated :: oracle_price_entry().
%% @doc Update the public key value for the given price entry and return the update
public_key(Entry = #oracle_price_entry{}, Value) when is_binary(Value) ->
    Entry#oracle_price_entry{ public_key = Value }.

-spec price( Entry :: oracle_price_entry(),
             Value :: non_neg_integer() ) -> Updated :: oracle_price_entry().
%% @doc Update the price value for the given price entry and return the update
price(Entry = #oracle_price_entry{}, Value) when is_integer(Value)
                                            andalso Value >= 0 ->
    Entry#oracle_price_entry{ price = Value }.
