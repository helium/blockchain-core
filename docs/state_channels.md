# State Channels Work Flow

1. `blockchain_state_channels_client` receives a packet via `packet/1`
2. `blockchain_state_channels_client` find routing information via `oui`
3. `blockchain_state_channels_client` crafts and send payment request to server via `blockchain_state_channel_handler`
4. `blockchain_state_channels_server` validate the request via `blockchain_state_channel_request_v1:is_valid`
5. `blockchain_state_channels_server` selects a proper state channel
6. `blockchain_state_channels_server` updates, saves state channel
7. `blockchain_state_channels_server` sends updated state channels
8. `blockchain_state_channels_client` receives updated state channel via `blockchain_state_channel_handler`
9. `blockchain_state_channels_client` checks pending request, if match is found, send packet back to `blockchain_state_channels_server`

# Data Credits Work Flow

1. Create a `blockchain_txn_state_channel_open_v1` transaction
2. The corresponding `blockchain_state_channels_server` will pick up the transaction once it's mined and create the state channel.

# ZERO State channel

A state channel holding 0 data credits can be created via `blockchain_txn_state_channel_open_v1` using an `amount` of `0` and an `id` `blockchain_state_channel_v1:zero_id/0`. This will allow to route packets, for free, to your own OUI/router.
The `blockchain_state_channels_client` will realize that the gateway is owned by the same owner as the OUI and will craft a payment req with the amount 0.
