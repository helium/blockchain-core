
-define(DC_PRICE, 0.00001).
-define(BONES_PER_HNT, 100000000).
-define(LEGACY_STAKING_FEE, 1).
-define(LEGACY_TXN_FEE, 0).

%% Macros
-define(fee(Txn), trunc(byte_size(blockchain_txn:serialize(Txn)) / 24)).
