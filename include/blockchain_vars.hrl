%%%% all chain vars should be defined in this file.  please don't call
%%%% without using the macros defined here.  running:
%%%% `git grep :config\( | grep -v \?` should not return any lines

%%%
%%% chain vars
%%%

%% Block time
-define(block_time, block_time).

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
-define(var_gw_inactivity_thresh, var_gw_inactivity_thresh).

%%%
%%% meta vars
%%%

%% the number of blocks between a var txn being accepted either
%% without a threshold, or once a threshold has been excceeded, and
%% the var actually being set in the ledger.
-define(vars_commit_delay, vars_commit_delay).

%% the percentage of active hotspots that need to have passed the
%% predicate value in order for a particular var txn to be applied.
-define(predicate_threshold, predicate_threshold).

%% Defaults to `miner`
-define(predicate_callback_mod, predicate_callback_mod).

%% Defaults to `version`
-define(predicate_callback_fun, predicate_callback_fun).

%%%
%%% miner vars
%%%

-define(num_consensus_members, num_consensus_members).

%%%
%%% burn vars
%%%

-define(token_burn_exchange_rate, token_burn_exchange_rate).

%% H3 Ring size to exclude when considering the next neighbor hop
-define(h3_exclusion_ring_dist, h3_exclusion_ring_dist).

%% Maximum number of hexagons to consider for neighbors
-define(h3_max_grid_dist, h3_max_grid_dist).

%% Scaling resolution for all poc path neighbors
-define(h3_neighbor_res, h3_neighbor_res).

%% Required minimum score for neighbors to be included in poc path
-define(min_score, min_score).

%%%
%%% txn vars
%%%

%% Required minimum h3 assert location resolution
-define(min_assert_h3_res, min_assert_h3_res).

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
