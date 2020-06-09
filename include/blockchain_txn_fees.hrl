
-define(DC_PRICE, 0.00001).
-define(BONES_PER_HNT, 100000000).

%% Fees below are quoted in USD
-define(STAKING_FEES, [
    {blockchain_txn_rewards_v1, 0},
    {blockchain_txn_vars_v1, 0},
    {blockchain_txn_consensus_group_v1, 0},
    {blockchain_txn_coinbase_v1, 0},
    {blockchain_txn_security_coinbase_v1, 0},
    {blockchain_txn_dc_coinbase_v1, 0},
    {blockchain_txn_gen_gateway_v1, 10},
    {blockchain_txn_token_burn_exchange_rate_v1, 0},
    {blockchain_txn_oui_v1, 100},
    {blockchain_txn_routing_v1, 0},
    {blockchain_txn_create_htlc_v1, 0},
    {blockchain_txn_payment_v1, 0},
    {blockchain_txn_security_exchange_v1, 0},
    {blockchain_txn_add_gateway_v1, 0},
    {blockchain_txn_assert_location_v1, 0},
    {blockchain_txn_redeem_htlc_v1, 0},
    {blockchain_txn_poc_request_v1, 0},
    {blockchain_txn_poc_receipts_v1, 0},
    {blockchain_txn_payment_v2, 0},
    {blockchain_txn_state_channel_open_v1, 0},
    {blockchain_txn_update_gateway_oui_v1, 0},
    {blockchain_txn_state_channel_close_v1, 0}
]).

%% expressed as percent
-define(STAKING_FEE_MARGIN, 5).

%% special txn resource fees
-define(OUI_FEE_PER_ADDRESS, 100).


%% Macros
-define(staking_fee(TxnType),
    case lists:keyfind(TxnType, 1, ?STAKING_FEES) of
        {_Type, Index} -> Index;
        false -> 0
    end).


-define(fee(Txn), trunc(byte_size(blockchain_txn:serialize(Txn)) / 24)).
