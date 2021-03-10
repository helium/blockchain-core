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

-define(BC_UPGRADE_NAMES, [<<"gateway_v2">>, <<"hex_targets">>, <<"gateway_oui">>,
                           <<"h3dex">>, <<"h3dex2">>,
                           <<"gateway_lg">>]).
-define(BC_UPGRADE_NAMES, [<<"gateway_v2">>, <<"hex_targets">>, <<"gateway_oui">>, <<"h3dex">>, <<"h3dex2">>]).

%% gateway capabilities, managed via a bitmask
-define(GW_CAPABILITY_ROUTE_PACKETS, 16#01).                               %% determines if a GW can route packets
-define(GW_CAPABILITY_POC_CHALLENGER, 16#02).                              %% determines if a GW can issue POC Challenges
-define(GW_CAPABILITY_POC_CHALLENGEE, 16#04).                              %% determines if a GW can accept POC Challenges
-define(GW_CAPABILITY_POC_WITNESS, 16#08).                                 %% determines if a GW can witness challenges
-define(GW_CAPABILITY_CONSENSUS_GROUP, 16#016).                            %% determines if a GW can participate in consensus group

-define(GW_CAPABILITIES_SET(Capabilities), lists:foldl(fun(Capability, Acc) -> Acc bor Capability end, 0,Capabilities)).
-define(GW_CAPABILITY_QUERY(Mask, Capability), (Mask band Capability) == Capability).

