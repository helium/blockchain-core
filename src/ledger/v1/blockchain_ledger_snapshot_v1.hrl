%% this is temporary, something to work with easily while we nail the
%% format and functionality down.  once it's final we can move on to a
%% more permanent and less flexible format, like protobufs, or
%% cauterize.

-record(blockchain_snapshot_v1,
        {
         %% meta stuff here
         previous_snapshot_hash :: binary(),
         leading_hash :: binary(),

         %% ledger stuff here
         current_height :: pos_integer(),
         transaction_fee :: non_neg_integer(),
         consensus_members :: [libp2p_crypto:pubkey_bin()],

         %% these are updated but never used.  Not sure what to do!
         election_height :: pos_integer(),
         election_epoch :: pos_integer(),

         delayed_vars :: [any()],
         threshold_txns :: [any()],

         master_key :: binary(),
         vars_nonce :: pos_integer(),
         vars :: [any()],

         gateways :: [any()],
         pocs :: [any()],

         accounts :: [any()],
         dc_accounts :: [any()],

         token_burn_rate :: non_neg_integer(),

         security_accounts :: [any()],

         htlcs :: [any()],

         ouis :: [any()],
         subnets :: [any()],
         oui_counter :: pos_integer(),

         hexes :: [any()],

         state_channels :: [any()],

         blocks :: [blockchain_block:block()]
        }).

-record(blockchain_snapshot_v2,
        {
         %% meta stuff here
         previous_snapshot_hash :: binary(),
         leading_hash :: binary(),

         %% ledger stuff here
         current_height :: pos_integer(),
         transaction_fee :: non_neg_integer(),
         consensus_members :: [libp2p_crypto:pubkey_bin()],

         %% these are updated but never used.  Not sure what to do!
         election_height :: pos_integer(),
         election_epoch :: pos_integer(),

         delayed_vars :: [any()],
         threshold_txns :: [any()],

         master_key :: binary(),
         vars_nonce :: pos_integer(),
         vars :: [any()],

         gateways :: [any()],
         pocs :: [any()],

         accounts :: [any()],
         dc_accounts :: [any()],

         token_burn_rate :: non_neg_integer(),

         security_accounts :: [any()],

         htlcs :: [any()],

         ouis :: [any()],
         subnets :: [any()],
         oui_counter :: pos_integer(),

         hexes :: [any()],

         state_channels :: [any()],

         blocks :: [blockchain_block:block()],

         oracle_price = 0 :: non_neg_integer(),
         oracle_price_list = [] :: [any()]
        }).

-record(blockchain_snapshot_v3,
        {
         %% ledger stuff here
         current_height :: pos_integer(),
         transaction_fee :: non_neg_integer(),
         consensus_members :: [libp2p_crypto:pubkey_bin()],

         %% these are updated but never used.  Not sure what to do!
         election_height :: pos_integer(),
         election_epoch :: pos_integer(),

         delayed_vars :: [any()],
         threshold_txns :: [any()],

         master_key :: binary(),
         vars_nonce :: pos_integer(),
         vars :: [any()],

         gateways :: [any()],
         pocs :: [any()],

         accounts :: [any()],
         dc_accounts :: [any()],

         security_accounts :: [any()],

         htlcs :: [any()],

         ouis :: [any()],
         subnets :: [any()],
         oui_counter :: pos_integer(),

         hexes :: [any()],

         state_channels :: [any()],

         blocks :: [blockchain_block:block()],

         oracle_price = 0 :: non_neg_integer(),
         oracle_price_list = [] :: [any()]
        }).

-record(blockchain_snapshot_v4,
        {
         %% ledger stuff here
         current_height :: pos_integer(),
         transaction_fee :: non_neg_integer(),
         consensus_members :: [libp2p_crypto:pubkey_bin()],

         %% these are updated but never used.  Not sure what to do!
         election_height :: pos_integer(),
         election_epoch :: pos_integer(),

         delayed_vars :: [any()],
         threshold_txns :: [any()],

         master_key :: binary(),
         multi_keys = [] :: [binary()],
         vars_nonce :: pos_integer(),
         vars :: [any()],

         gateways :: [any()],
         pocs :: [any()],

         accounts :: [any()],
         dc_accounts :: [any()],

         security_accounts :: [any()],

         htlcs :: [any()],

         ouis :: [any()],
         subnets :: [any()],
         oui_counter :: pos_integer(),

         hexes :: [any()],

         state_channels :: [any()],

         blocks :: [blockchain_block:block()],

         oracle_price = 0 :: non_neg_integer(),
         oracle_price_list = [] :: [any()]
        }).
