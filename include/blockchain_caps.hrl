%% gateway capabilities, managed via a bitmask
-define(GW_CAPABILITY_ROUTE_PACKETS, 16#01).                               %% determines if a GW can route packets
-define(GW_CAPABILITY_POC_CHALLENGER, 16#02).                              %% determines if a GW can issue POC Challenges
-define(GW_CAPABILITY_POC_CHALLENGEE, 16#04).                              %% determines if a GW can accept POC Challenges
-define(GW_CAPABILITY_POC_WITNESS, 16#08).                                 %% determines if a GW can witness challenges
-define(GW_CAPABILITY_POC_RECEIPT, 16#016).                                 %% determines if a GW can issue receipts
-define(GW_CAPABILITY_CONSENSUS_GROUP, 16#032).                            %% determines if a GW can participate in consensus group

%% default capabilities for each gateway type
-define(GW_CAPABILITIES_LIGHT_GATEWAY, 0 bor ?GW_CAPABILITY_ROUTE_PACKETS).
-define(GW_CAPABILITIES_NON_CONSENSUS_GATEWAY, 0 bor ?GW_CAPABILITY_ROUTE_PACKETS
                                                 bor ?GW_CAPABILITY_POC_CHALLENGER
                                                 bor ?GW_CAPABILITY_POC_CHALLENGEE
                                                 bor ?GW_CAPABILITY_POC_WITNESS
                                                 bor ?GW_CAPABILITY_POC_RECEIPT
).
%% TODO: maybe a full gateway should just have all bits set by default ?  that way it will always be able to do ev thing
%% this approach would complicate things if in the future we do want to restrict a full gateway in some way...
%% but then its not a full gateway, right?
-define(GW_CAPABILITIES_FULL_GATEWAY, 0 bor ?GW_CAPABILITY_ROUTE_PACKETS
                                        bor ?GW_CAPABILITY_POC_CHALLENGER
                                        bor ?GW_CAPABILITY_POC_CHALLENGEE
                                        bor ?GW_CAPABILITY_POC_WITNESS
                                        bor ?GW_CAPABILITY_POC_RECEIPT
                                        bor ?GW_CAPABILITY_CONSENSUS_GROUP
).

-define(GW_CAPABILITIES_SET(Capabilities), lists:foldl(fun(Capability, Acc) -> Acc bor Capability end, 0,Capabilities)).
-define(GW_CAPABILITY_QUERY(Mask, Capability), (Mask band Capability) == Capability).