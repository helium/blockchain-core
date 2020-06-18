% Protocols
-define(GOSSIP_PROTOCOL_V1, "blockchain_gossip/1.0.0").
-define(SYNC_PROTOCOL_V1, "blockchain_sync/1.1.0").
-define(FASTFORWARD_PROTOCOL_V1, "blockchain_fastforward/1.0.0").

-define(SYNC_PROTOCOL_V2, "blockchain_sync/1.2.0").
-define(FASTFORWARD_PROTOCOL_V2, "blockchain_fastforward/1.2.0").

-define(SUPPORTED_GOSSIP_PROTOCOLS, [?GOSSIP_PROTOCOL_V1]).
-define(SUPPORTED_SYNC_PROTOCOLS, [?SYNC_PROTOCOL_V2, ?SYNC_PROTOCOL_V1]).
-define(SUPPORTED_FASTFORWARD_PROTOCOLS, [?FASTFORWARD_PROTOCOL_V2, ?FASTFORWARD_PROTOCOL_V1]).

-define(SNAPSHOT_PROTOCOL, "blockchain_snapshot/1.0.0").


-define(TX_PROTOCOL, "blockchain_txn/1.0.0").
-define(LOC_ASSERTION_PROTOCOL, "loc_assertion/1.0.0").
-define(STATE_CHANNEL_PROTOCOL_V1, "state_channel/1.0.0").

% B58 Address Versions
-define(B58_MAINNET_VER, 0).
-define(B58_TESTNET_VER, 2).
-define(B58_HTLC_VER, 24).

% Misc
-define(EVT_MGR, blockchain_event_mgr).

-define(BC_UPGRADE_NAMES, [<<"gateway_v2">>, <<"hex_targets">>, <<"gateway_oui">>]).
