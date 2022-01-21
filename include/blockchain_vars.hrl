%%%% all chain vars should be defined in this file.  please don't call
%%%% without using the macros defined here.  running:
%%%% `git grep :config\( | grep -v \?` should not return any lines


%% key: {is_aux, vars_nonce, var_name}
-define(VAR_CACHE, var_cache).

%%%
%%% election vars
%%%

%% current election version
-define(election_version, election_version).

%% the likelihood of a particular node being selected or removed from
%% the consensus group.
-define(election_selection_pct, election_selection_pct).

%% the likelihood of a particular node being selected or removed from
%% the consensus group.
-define(election_removal_pct, election_removal_pct).

%% the h3 resolution to use to determine parent hexagons for
%% clustering detection
-define(election_cluster_res, election_cluster_res).

%% the fraction of the consensus group that will be removed in the
%% first election.  this grows over time, see below
-define(election_replacement_factor, election_replacement_factor).

%% a tunable factor to slow or speed up how much of the consensus
%% group will be replaced as delay accumulates.  increase this in size
%% to slow, make it smaller to speed things up.
-define(election_replacement_slope, election_replacement_slope).

%% number of blocks between a new consensus group txn being added to
%% the chain and a new dkg election process beginning.
-define(election_interval, election_interval).

%% number of blocks before a running dkg is canceled and a new one is initiated
-define(election_restart_interval, election_restart_interval).
-define(election_restart_interval_range, election_restart_interval_range).

%% per-block penalty for consensus nodes that don't finish a
%% particular bba for a round.
-define(election_bba_penalty, election_bba_penalty).

%% per-block penalty for consensus nodes aren't seen by their peers in
%% a round
-define(election_seen_penalty, election_seen_penalty).

%%%
%%% ledger vars
%%%

%% the number of blocks before keep a gateway from affecting the
%% outcome of threshold var application.
-define(var_gw_inactivity_threshold, var_gw_inactivity_threshold).

%% the number of blocks before a random subset of gatways refresh
%% their witnesses
-define(witness_refresh_interval, witness_refresh_interval).
%% seeding the random number for witness_refresh_interval
-define(witness_refresh_rand_n, witness_refresh_rand_n).

%%%
%%% meta vars
%%%

%% the number of blocks between a var txn being accepted either
%% without a threshold, or once a threshold has been excceeded, and
%% the var actually being set in the ledger.
-define(vars_commit_delay, vars_commit_delay).

%% the initial version of the chain vars txn only signed the vars,
%% which would allow for replay attacks and nonce advancement attacks.
-define(chain_vars_version, chain_vars_version).

%% the percentage of active hotspots that need to have passed the
%% predicate value in order for a particular var txn to be applied.
-define(predicate_threshold, predicate_threshold).

%% These variables are used in the miner to determine which function should be called to provide
%% whatever it is that is checked by the predicate.
%% At the current time they provide a monotonic stream of integers.
-define(predicate_callback_mod, predicate_callback_mod). %% Currently set to: miner
-define(predicate_callback_fun, predicate_callback_fun). %% Currently set to: version

%%%
%%% miner vars
%%%

%% The number of consensus members that collectively mine a block. Specified as a positive int.
-define(num_consensus_members, num_consensus_members).

%% The interval between blocks that the chain attempts to maintain. Specified in milliseconds.
-define(block_time, block_time).

%% This is passed onto hbbft from the miner. The number of transactions each consensus member is allowed
%% to propose in each hbbft round = batch_size/num_consensus_members.
-define(batch_size, batch_size).

%% Currently accepted block version by the running chain. Set to v1.
-define(block_version, block_version).

%% Curve over which DKG is run. Set to SS512 currently. Accepts an atom.
-define(dkg_curve, dkg_curve).

%%%
%%% burn vars
%%%

-define(token_burn_exchange_rate, token_burn_exchange_rate).

%%%
%%% poc related vars
%%%

%% H3 Ring size to exclude when considering the next neighbor hop
-define(h3_exclusion_ring_dist, h3_exclusion_ring_dist).

%% Maximum number of hexagons to consider for neighbors
-define(h3_max_grid_distance, h3_max_grid_distance).

%% Scaling resolution for all poc path neighbors
-define(h3_neighbor_res, h3_neighbor_res).

%% Required minimum score for neighbors to be included in poc path
-define(min_score, min_score).

%% Required minimum h3 assert location resolution for assert_loc txn
-define(min_assert_h3_res, min_assert_h3_res).

%% Number of blocks to wait before a hotspot can submit a poc challenge request
-define(poc_challenge_interval, poc_challenge_interval).

%% Allow to switch POC version
-define(poc_version, poc_version).

%% Number of blocks to wait before a hotspot can be eligible to participate in a poc
%% challenge. This would avoid new hotspots getting challenged before they sync to an
%% acceptable height.
%% Only trigger with poc_version >= 2.
-define(poc_challenge_sync_interval, poc_challenge_sync_interval).

%% Number of hotspots allowed in a poc path
-define(poc_path_limit, poc_path_limit).

%% whether to fix some typos in the PoC generation/validation code
-define(poc_typo_fixes, poc_typo_fixes).

%% Number of witnesses allowed to be considered per step of a path or
%% during targeting
-define(poc_witness_consideration_limit, poc_witness_consideration_limit).


-define(poc_addr_hash_byte_count, poc_addr_hash_byte_count).

%% define loss factor for SNR curve, floating point number
-define(fspl_loss, fspl_loss).
%% define poc_distance_limit, set to some max distance in Kms, pos_integer
-define(poc_distance_limit, poc_distance_limit).
%% define whether to check snr or not, boolean
-define(check_snr, check_snr).

%% resolution for h3 polyfills, defaulted to 7 before we set it
-define(polyfill_resolution, polyfill_resolution).
%%%
%%% score vars
%%%

%% Rate of decay for score alpha parameter
%% This acts like network gravity and keeps hotspots from staying at the top of the score graph
%% for longer periods of time without actually participating in POC
-define(alpha_decay, alpha_decay).

%% Rate of decay for score beta parameter
%% This acts like network gravity and keeps hotspots from staying at the bottom of the score graph
%% for longer periods of time without actually participating in POC
-define(beta_decay, beta_decay).

%% Acts as a limiting factor to avoid overflowing the decay
-define(max_staleness, max_staleness).

%%%
%%% reward vars
%%%

%% Pretty much all of these are self-explanatory
-define(reward_version, reward_version).
-define(monthly_reward, monthly_reward).
-define(securities_percent, securities_percent).
-define(consensus_percent, consensus_percent).
-define(poc_challengees_percent, poc_challengees_percent).
-define(poc_witnesses_percent, poc_witnesses_percent).
-define(poc_challengers_percent, poc_challengers_percent).
-define(dc_percent, dc_percent).
-define(witness_redundancy, witness_redundancy).
-define(poc_reward_decay_rate, poc_reward_decay_rate).
-define(rewards_txn_version, rewards_txn_version).
-define(hip15_tx_reward_unit_cap, hip15_tx_reward_unit_cap).
-define(witness_reward_decay_rate, witness_reward_decay_rate).
-define(witness_reward_decay_exclusion, witness_reward_decay_exclusion).

%%%
%%% bundle txn vars
%%%

%% Only allow txns of this bundle length to appear on chain
-define(max_bundle_size, max_bundle_size). %% default: 5

%% POC V4 vars
%% ------------------------------------------------------------------
%% Normalize witnesses to this parent resolution.
-define(poc_v4_parent_res, poc_v4_parent_res). %% default: 11
%% Number of grid cells to exclude when building a path.
-define(poc_v4_exclusion_cells, poc_v4_exclusion_cells). %% default: 10 for parent_res 11
%% Exlusion cells from challenger -> target
-define(poc_v4_target_exclusion_cells, poc_v4_target_exclusion_cells). %% default: 6000
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% RSSI probabilities
%% Probability associated with a next hop having no rssi information
-define(poc_v4_prob_no_rssi, poc_v4_prob_no_rssi). %% default: 0.5
%% Probability associated with a next hop having good rssi information
-define(poc_v4_prob_good_rssi, poc_v4_prob_good_rssi). %% default: 1.0
%% Probability associated with a next hop having bad rssi information
-define(poc_v4_prob_bad_rssi, poc_v4_prob_bad_rssi). %% default: 0.01
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% RSSI probability weights, these MUST sum to 1.0
%% Weight associated with next hop rssi probability
-define(poc_v4_prob_rssi_wt, poc_v4_prob_rssi_wt). %% default: 0.3
%% Weight associated with next hop recent time probability
-define(poc_v4_prob_time_wt, poc_v4_prob_time_wt). %% default: 0.3
%% Weight associated with next hop witness count probability
-define(poc_v4_prob_count_wt, poc_v4_prob_count_wt). %% default: 0.3
%% This quantifies how much randomness we want when assigning
%% probabilities to the witnesses.
%% ------------------------------------------------------------------
-define(poc_v4_randomness_wt, poc_v4_randomness_wt). %% default: 0.1

%% ------------------------------------------------------------------

%% A potential target must have a last poc challenge within this challenge_age
-define(poc_v4_target_challenge_age, poc_v4_target_challenge_age). %% default: 300
%% Score curve to calculate the target score probability
-define(poc_v4_target_score_curve, poc_v4_target_score_curve). %% default: 5

%% ------------------------------------------------------------------
%% Target probability weights, these MUST sum to 1.0
%% Weight associated with target score probability
-define(poc_v4_target_prob_score_wt, poc_v4_target_prob_score_wt). %% default: 0.8
%% Weight associated with target being loosely connected probability
-define(poc_v4_target_prob_edge_wt, poc_v4_target_prob_edge_wt). %% default: 0.2
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%%% POC V5 vars
%% Dictates how much randomness we want in the target selection
-define(poc_v5_target_prob_randomness_wt, poc_v5_target_prob_randomness_wt).

%% Hierarchical targeting variables
%% Create hexes at this resolution for all the hotspots on the network.
-define(poc_target_hex_parent_res, poc_target_hex_parent_res).

%% RSSI Bucketing variables
%% Weight associated with biasing for RSSI centrality measures
-define(poc_centrality_wt, poc_centrality_wt).
%% Lower bound for known good rssi bucket
-define(poc_good_bucket_low, poc_good_bucket_low).
%% Upper bound for known good rssi bucket
-define(poc_good_bucket_high, poc_good_bucket_high).
%% Maximum allowed h3 grid cells for a potential next hop
-define(poc_max_hop_cells, poc_max_hop_cells).
%% Maximum allow witnesses per hop
-define(poc_per_hop_max_witnesses, poc_per_hop_max_witnesses).

%% ------------------------------------------------------------------
%%
%%
%% ------------------------------------------------------------------
%% Txn Payment V2 vars
%%
%% Max payments allowed within a single payment_v2 transaction
-define(max_payments, max_payments).
%% Var to switch off legacy payment txn
-define(deprecate_payment_v1, deprecate_payment_v1).
%% Enable payment-v2 memos
-define(allow_payment_v2_memos, allow_payment_v2_memos).

%% Set this var to false to disable zero amount txns (payment_v1, payment_v2, htlc_create)
-define(allow_zero_amount, allow_zero_amount).

%% General txn vars

%% Enable more robust validation on some legacy transactions with incorrect on-chain txns
%% this is behind a chain variable to allow the bad txns to be synced through
-define(txn_field_validation_version, txn_field_validation_version).

%% ------------------------------------------------------------------
%% State channel related vars
%%
%% Min state channel expiration (# of blocks), set to 10
-define(min_expire_within, min_expire_within).
%% Max open state channels per router, set to 2
-define(max_open_sc, max_open_sc).
%% Max xor filter size, set to 1024*100
-define(max_xor_filter_size, max_xor_filter_size).
%% Max number of xor filters, set to 5
-define(max_xor_filter_num, max_xor_filter_num).
%% Max subnet size, 65536
-define(max_subnet_size, max_subnet_size).
%% Min subnet size, 8
-define(min_subnet_size, min_subnet_size).
%% Max subnet num
-define(max_subnet_num, max_subnet_num).
%% Grace period (in num of blocks) for state channels to get GCd
-define(sc_grace_blocks, sc_grace_blocks).
%% DC Payload size, set to 24
-define(dc_payload_size, dc_payload_size).
%% state channel version
-define(sc_version, sc_version).
%% state channel overcommit multiplier
-define(sc_overcommit, sc_overcommit).
%% Number of attempts we get to fix state_channel bugs, we'll set it to 1, max = 50
-define(sc_open_validation_bugfix, sc_open_validation_bugfix).
%% Number of attempts we get to fix state_channel causality bugs, we'll set it to 1, max = 1
-define(sc_causality_fix, sc_causality_fix).
%% Block interval to try to GC state channels
-define(sc_gc_interval, sc_gc_interval).
%% Max number of actors (hotspots) allowed per state channel
-define(sc_max_actors, sc_max_actors).
%% Whether to include _all_ state channels in "active" count.
%% The "old" behavior (i.e., `false') was to count all state
%% channels regardless of their open or closed state.
%%
%% The "new" behavior (i.e., `true') is to count state channels
%% that are only in the open state and ignore closed channels.
-define(sc_only_count_open_active, sc_only_count_open_active).
%% How to handle rewards when a state channel close is disputed.
%%  - 0 :: reconcile as much as possible
%%  - 1 :: accept first dispute, drop all DC from opener, no rewards
-define(sc_dispute_strategy_version, sc_dispute_strategy_version).

%% ------------------------------------------------------------------
%% snapshot vars

%% snapshot version, presence indicates if snapshots are enabled or not.
-define(snapshot_version, snapshot_version).

%% how often we attempt to take a snapshot of the ledger
-define(snapshot_interval, snapshot_interval).
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% Price oracle variables
%%
%% Oracle public keys - encoded like so...
%% <<Len1:8/unsigned-integer, Key1/binary, Len2:8/unsigned-integer, Key2/binary, ...>>
-define(price_oracle_public_keys, price_oracle_public_keys).
%% How many blocks between price recalculations
-define(price_oracle_refresh_interval, price_oracle_refresh_interval).
%% How much delta between the current blockchain height and the transaction is allowed
-define(price_oracle_height_delta, price_oracle_height_delta).
%% How many seconds to delay scanning for prices.
-define(price_oracle_price_scan_delay, price_oracle_price_scan_delay).
%% How many seconds to stop scanning for oracle prices.
%% (Will also affect what prices get dropped from the cached list of prices.)
-define(price_oracle_price_scan_max, price_oracle_price_scan_max).
%% ------------------------------------------------------------------


%% ------------------------------------------------------------------
%% transaction fee vars, denominated in DC

%% determines whether txn fees are enabled, boolean value expected
-define(txn_fees, txn_fees).
%% valid staking server keys, encoded via <<Len1:8/unsigned-integer, Key1/binary, Len2:8/unsigned-integer, Key2/binary, ...>>
-define(staking_keys, staking_keys).
%% staking server keys with a mapping to gateway type ( dataonly, light and full )
-define(staking_keys_to_mode_mappings, staking_keys_to_mode_mappings).
%% the staking fee in DC for each OUI
-define(staking_fee_txn_oui_v1, staking_fee_txn_oui_v1).
%% the staking fee in DC for each OUI/routing address
-define(staking_fee_txn_oui_v1_per_address, staking_fee_txn_oui_v1_per_address).
%% the staking fee in DC for adding a full gateway
-define(staking_fee_txn_add_gateway_v1, staking_fee_txn_add_gateway_v1).
%% the staking fee in DC for adding a dataonly gateway
-define(staking_fee_txn_add_dataonly_gateway_v1, staking_fee_txn_add_dataonly_gateway_v1).
%% the staking fee in DC for adding a light gateway
-define(staking_fee_txn_add_light_gateway_v1, staking_fee_txn_add_light_gateway_v1).
%% the staking fee in DC for asserting a location
-define(staking_fee_txn_assert_location_v1, staking_fee_txn_assert_location_v1).
%% the staking fee in DC for asserting a location for a dataonly gateway
-define(staking_fee_txn_assert_location_dataonly_gateway_v1, staking_fee_txn_assert_location_dataonly_gateway_v1).
%% the staking fee in DC for asserting a location for a light gateway
-define(staking_fee_txn_assert_location_light_gateway_v1, staking_fee_txn_assert_location_light_gateway_v1).

%% a mutliplier which will be applied to the txn fee of all txns, in order to make their DC costs meaningful
-define(txn_fee_multiplier, txn_fee_multiplier).

%% ------------------------------------------------------------------
%% Radio data gathering
%%
-define(data_aggregation_version, data_aggregation_version).

%% Multi-key
-define(use_multi_keys, use_multi_keys).

%% transfer hotspot
%% This is the number of blocks after which a hotspot which hasn't had a
%% POC challenge for X blocks would be considered stale for the purposes
%% of a hotspot transfer. (We do not allow stale hotspots to be transferred.)
-define(transfer_hotspot_stale_poc_blocks, transfer_hotspot_stale_poc_blocks).

%% ------------------------------------------------------------------
%% HIP 17 vars
%%
%% For every possible h3 resolution, we will define:
%% - number of siblings
%% - density_tgt
%% - density_max
%%
%% So hip17_res_0 value could be: 2, 10000, 10000 for example;
%% where num_siblings=2, density_tgt=10000, density_max=10000
%%
%% We'd specify any of the below variables like so: <<"2,10000,10000">>
%% We expect the value of any of these variables to be in format: <<"int,int,int">>
-define(hip17_res_0, hip17_res_0).
-define(hip17_res_1, hip17_res_1).
-define(hip17_res_2, hip17_res_2).
-define(hip17_res_3, hip17_res_3).
-define(hip17_res_4, hip17_res_4).
-define(hip17_res_5, hip17_res_5).
-define(hip17_res_6, hip17_res_6).
-define(hip17_res_7, hip17_res_7).
-define(hip17_res_8, hip17_res_8).
-define(hip17_res_9, hip17_res_9).
-define(hip17_res_10, hip17_res_10).
-define(hip17_res_11, hip17_res_11).
-define(hip17_res_12, hip17_res_12).
-define(density_tgt_res, density_tgt_res).
-define(hip17_interactivity_blocks, hip17_interactivity_blocks).

%% ------------------------------------------------------------------
%% assert_location_v2 transaction related vars

%% Allowed values: 1, 2
-define(assert_loc_txn_version, assert_loc_txn_version).

%% Known antenna gains:
%% Helium Hotspot (US 915) - 1.2 dBi
%% Helium Hotspot (EU 868) - 2.3 dBi
%% RAK Hotspot Miner (US 915) - 2.3 dBi
%% RAK Hotspot Miner (EU 868) - 2.8 dBi
%% Nebra Outdoor Hotspot - 3 dBi
%% Bobcat Miner 300 (All) - 4 dBi
%% Syncrob.it (US 915) - 1.2 dBi
%% Syncrob.it (EU 868) - 2.3 dBi

%% NOTE: Allow min_antenna_gain - max_antenna_gain (both inclusive)
%% Both will be set as dBi x 10, so, 1 dBi = 10, 15 dBi = 150,
%% Any gain value between min-max is acceptable
-define(min_antenna_gain, min_antenna_gain).        %% Set to 10 (1 dBi)
-define(max_antenna_gain, max_antenna_gain).        %% Set to 150 (15 dBi)

%% ------------------------------------------------------------------
%% the mask value to represent the capabilities of dataonly gateways, defined as an integer and used as a bitmask
-define(dataonly_gateway_capabilities_mask, dataonly_gateway_capabilities_mask).
%% the mask value to represent the capabilities of light gateways, defined as an integer and used as a bitmask
-define(light_gateway_capabilities_mask, light_gateway_capabilities_mask).
%% the mask value to represent the capabilities of full gateways, defined as an integer and used as a bitmask
-define(full_gateway_capabilities_mask, full_gateway_capabilities_mask).

%%%
%%% validators variables
%%%

-define(validator_version, validator_version).
-define(validator_minimum_stake, validator_minimum_stake).  % bones
-define(validator_liveness_interval, validator_liveness_interval).  % blocks
-define(validator_liveness_grace_period, validator_liveness_grace_period).  % blocks
-define(validator_penalty_filter, validator_penalty_filter). % float
-define(validator_key_check, validator_key_check). % boolean
-define(stake_withdrawal_cooldown, stake_withdrawal_cooldown). % blocks
-define(stake_withdrawal_max, stake_withdrawal_max). % blocks
%% -define(maximum_overstake, maximum_overstake). % float multiple of min stake

-define(penalty_history_limit, penalty_history_limit). % blocks
-define(dkg_penalty, dkg_penalty). % float
-define(tenure_penalty, tenure_penalty). % float

%%%
%%% net emissions vars
%%%

%% use net emissions code paths
-define(net_emissions_enabled, net_emissions_enabled). % boolean
%% maximum amount of burnt hnt per epoch to return to the rewards pool
%% initial proposed max 34.24 HNT
-define(net_emissions_max_rate, net_emissions_max_rate). % bones

%% ------------------------------------------------------------------
%% regulatory_region related variables

%% This is a comma separated string like so:
%% <<"region_as923_1,region_as923_2,region_as923_3,region_as923_4,region_au915,region_cn470,region_eu433,region_eu868,region_in865,region_kr920,region_ru864,region_us915">>
-define(regulatory_regions, regulatory_regions).

%% Each of the former regions is associated with a dynamic var of the same name which is is a serialized form of an h3_region set determined at h3_res: 7
%% For more information, check: https://github.com/JayKickliter/lorawan-h3-regions and https://github.com/helium/erlang-h3/pull/29

%% Additionally, each of the former regions is associated with a dynamic var of the form <regionname>_params, eg region_us915_params which
%% contains the RF parameters for that region.

%% The values these take is binary obtained by serializing
%% a valid channel param specification.
%%
%% The validation of these is highly specific and will only
%% validate a binary value we know beforehand, usually evaluated
%% by hand. We don't expect these to change often but whence
%% they do, we will re-evaluate accordingly.
%%
%% Refer to blockchain_region_SUITE for an example

%% ------------------------------------------------------------------
%% This is a general purpose variable. We can use it for any transaction
%% to specify which version of it we will support on chain.
%%
%% As an example, blockchain_txn_transfer_hotspot_v2, will get enabled
%% when this value is set to >= 2.
-define(transaction_validity_version, transaction_validity_version).    % pos_integer


%% Boolean chain variable to discard witnesses with 0.0 frequency
-define(discard_zero_freq_witness, discard_zero_freq_witness).

%% Block size limit variable (in bytes). Set to 25 * 1024 * 1024.
-define(block_size_limit, block_size_limit).
