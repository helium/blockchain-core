%%%% all chain vars should be defined in this file.  please don't call
%%%% without using the macros defined here.  running:
%%%% `git grep :config\( | grep -v \?` should not return any lines

%%%
%%% election vars
%%%

%% the likelihood of a particular node being selected or removed from
%% the consensus group.
-define(election_selection_pct, election_selection_pct).

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

%%%
%%% ledger vars
%%%

%% the number of blocks before keep a gateway from affecting the
%% outcome of threshold var application.
-define(var_gw_inactivity_threshold, var_gw_inactivity_threshold).

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
-define(monthly_reward, monthly_reward).
-define(securities_percent, securities_percent).
-define(consensus_percent, consensus_percent).
-define(poc_challengees_percent, poc_challengees_percent).
-define(poc_witnesses_percent, poc_witnesses_percent).
-define(poc_challengers_percent, poc_challengers_percent).
-define(dc_percent, dc_percent).
