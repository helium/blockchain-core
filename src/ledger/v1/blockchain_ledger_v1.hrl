-record(hook,
        {
         cf :: atom(),
         predicate :: undefined | fun(),
         hook_inc_fun :: fun(),  %% fun called for each incremental/partial update for the relevant CF
         hook_end_fun :: fun(),  %% fun called after all incremental/partial updates are complete for the relevant cf
         ref :: undefined | reference(),
         include_height=false :: boolean()
        }).

-record(ledger_v1, {
    dir :: file:filename_all(),
    db :: rocksdb:db_handle(),
    blocks_db :: rocksdb:db_handle(),
    blocks_cf :: rocksdb:cf_handle(),
    heights_cf :: rocksdb:cf_handle(),
    info_cf :: rocksdb:cf_handle(),
    snapshots :: ets:tid(),
    mode = active :: mode(),
    active :: sub_ledger(),
    delayed :: sub_ledger(),
    snapshot :: undefined | rocksdb:snapshot_handle(),
    commit_hooks = [] :: [#hook{}],
    aux :: undefined | aux_ledger()
}).

-record(sub_ledger_v1, {
    default :: rocksdb:cf_handle(),
    active_gateways :: rocksdb:cf_handle(),
    gw_denorm :: rocksdb:cf_handle(),
    entries :: rocksdb:cf_handle(),
    dc_entries :: rocksdb:cf_handle(),
    htlcs :: rocksdb:cf_handle(),
    pocs :: rocksdb:cf_handle(),
    securities :: rocksdb:cf_handle(),
    routing :: rocksdb:cf_handle(),
    subnets :: rocksdb:cf_handle(),
    state_channels :: rocksdb:cf_handle(),
    h3dex :: rocksdb:cf_handle(),
    validators :: rocksdb:cf_handle(),
    cache :: undefined | direct | ets:tid(),
    gateway_cache :: undefined | ets:tid()
}).

-record(aux_ledger_v1, {
          %% aux-ledger is maintained in a separate database
          dir :: file:filename_all(),
          %% with its own db handle
          db :: rocksdb:db_handle(),
          %% it however maintains same subledger structure as the active ledger
          aux :: sub_ledger(),
          %% it provides this extra aux_heights column to differentiate actual
          %% rewards vs aux rewards (for now, but can be extended to show other differences)
          aux_heights :: rocksdb:cf_handle(),
          aux_heights_md :: rocksdb:cf_handle(),
          aux_heights_diff :: rocksdb:cf_handle(),
          aux_heights_diffsum :: rocksdb:cf_handle()

         }).

%% This record is for managing validator stake cooldown information in the
%% ledger
-record(validator_stake_v1, {
          owner = undefined :: undefined | libp2p_crypto:pubkey_bin(),
          validator = undefined :: undefined | libp2p_crypto:pubkey_bin(),
          stake = 0 :: non_neg_integer(),
          stake_release_height = 0 :: non_neg_integer() %% aka `target_block'
         }).

-define(DB_FILE, "ledger.db").
-define(CURRENT_HEIGHT, <<"current_height">>).
-define(CONSENSUS_MEMBERS, <<"consensus_members">>).
-define(ELECTION_HEIGHT, <<"election_height">>).
-define(ELECTION_EPOCH, <<"election_epoch">>).
-define(OUI_COUNTER, <<"oui_counter">>).
-define(MASTER_KEY, <<"master_key">>).
-define(MULTI_KEYS, <<"multi_keys">>).
-define(VARS_NONCE, <<"vars_nonce">>).
-define(BURN_RATE, <<"token_burn_exchange_rate">>).
-define(HNT_BURNED, <<"hnt_burned">>).
-define(NET_OVERAGE, <<"net_overage">>).
-define(CURRENT_ORACLE_PRICE, <<"current_oracle_price">>). %% stores the current calculated price
-define(ORACLE_PRICES, <<"oracle_prices">>). %% stores a rolling window of prices
-define(hex_list, <<"$hex_list">>).
-define(hex_prefix, "$hex_").
-define(aux_height_prefix, "aux_height_").
-define(aux_height_md_prefix, "aux_height_md_").
-define(aux_height_diff_prefix, "aux_height_diff_").
-define(aux_height_diffsum_prefix, "aux_height_diffsum_").

-define(CACHE_TOMBSTONE, '____ledger_cache_tombstone____').

-define(BITS_23, 8388607). %% biggest unsigned number in 23 bits
-define(BITS_25, 33554431). %% biggest unsigned number in 25 bits
-define(DEFAULT_ORACLE_PRICE, 0).

-type ledger() :: #ledger_v1{}.
-type sub_ledger() :: #sub_ledger_v1{}.
-type aux_ledger() :: #aux_ledger_v1{}.
-type mode() :: active | delayed | aux | aux_load.
