
-define(DC_TO_USD, 0.00001).
-define(USD_TO_DC, 100000).
-define(BONES_PER_HNT, 100000000).
-define(ORACLE_PRICE_SCALING_FACTOR, 100000000).
-define(LEGACY_STAKING_FEE, 1).
-define(LEGACY_TXN_FEE, 0).

%% Macros
-define(fee(Txn), trunc(byte_size(blockchain_txn:serialize(Txn)) / 24)).
