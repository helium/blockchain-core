-record(oracle_price_entry, {
          timestamp = 0 :: non_neg_integer(),
          block_height = 0 :: non_neg_integer(),
          public_key = undefined :: binary() | undefined,
          price = 0 :: non_neg_integer()
}).

-type oracle_price_entry() :: #oracle_price_entry{}.
