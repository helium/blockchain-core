-define(DC_TO_USD, 0.00001).

-define(USD_TO_DC, 100000).

-define(BONES_PER_HNT, 100000000).

-define(ORACLE_PRICE_SCALING_FACTOR, 100000000).

-define(LEGACY_STAKING_FEE, 1).

-define(LEGACY_TXN_FEE, 0).

%% Macros
-define(fee(Txn, Ledger),
    TxnFeeMultiplier = blockchain_ledger_v1:txn_fee_multiplier(Ledger),
    blockchain_utils:calculate_dc_amount(Ledger, byte_size(blockchain_txn:serialize(Txn))) *
        TxnFeeMultiplier
).
