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

%%%
%%% miner vars
%%%

-define(num_consensus_members, num_consensus_members).

%%%
%%% burn vars
%%%

-define(token_burn_exchange_rate, token_burn_exchange_rate).
