%%
%% A gateway can be set to one of 3 modes, dataonly, light and full
%% each mode then has a bitmask defined via a chain var which
%% determines the range of capabilities applicable for the given mode
%%

%% The current full range of possible capabilities supported across the various gateway modes
-define(GW_CAPABILITY_ROUTE_PACKETS,    16#01).     %% determines if a GW can route packets
-define(GW_CAPABILITY_POC_CHALLENGER,   16#02).     %% determines if a GW can issue POC Challenges
-define(GW_CAPABILITY_POC_CHALLENGEE,   16#04).     %% determines if a GW can accept POC Challenges
-define(GW_CAPABILITY_POC_WITNESS,      16#08).     %% determines if a GW can witness challenges
-define(GW_CAPABILITY_POC_RECEIPT,      16#10).     %% determines if a GW can issue receipts
-define(GW_CAPABILITY_CONSENSUS_GROUP,  16#20).     %% determines if a GW can participate in consensus group

%%
%% V1 capabilities for each gateway type defined below
%% In practise we should never need to use these but they cover a case
%% in blockchain_ledger_gateway_v2:mask_for_gateway_mode/2 whereby we have a GW with its mode field set
%% but the corresponding bitmask chain var is not found....which should never really happen but
%% I feel better for covering the scenario
%%
-define(GW_CAPABILITIES_DATAONLY_GATEWAY_V1, 0 bor ?GW_CAPABILITY_ROUTE_PACKETS).
-define(GW_CAPABILITIES_LIGHT_GATEWAY_V1, 0 bor ?GW_CAPABILITY_ROUTE_PACKETS
                                                    bor ?GW_CAPABILITY_POC_CHALLENGER
                                                    bor ?GW_CAPABILITY_POC_CHALLENGEE
                                                    bor ?GW_CAPABILITY_POC_WITNESS
                                                    bor ?GW_CAPABILITY_POC_RECEIPT
).
-define(GW_CAPABILITIES_FULL_GATEWAY_V1, 0  bor ?GW_CAPABILITY_ROUTE_PACKETS
                                            bor ?GW_CAPABILITY_POC_CHALLENGER
                                            bor ?GW_CAPABILITY_POC_CHALLENGEE
                                            bor ?GW_CAPABILITY_POC_WITNESS
                                            bor ?GW_CAPABILITY_POC_RECEIPT
                                            bor ?GW_CAPABILITY_CONSENSUS_GROUP
).

%% helper macros
-define(GW_CAPABILITIES_SET(Capabilities), lists:foldl(fun(Capability, Acc) -> Acc bor Capability end, 0,Capabilities)).
-define(GW_CAPABILITY_QUERY(Mask, Capability), (Mask band Capability) == Capability).