%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Summary ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_summary_v1).

-export([
    new/1, new/3,
    client_pubkeybin/1,
    num_dcs/1, num_dcs/2,
    num_packets/1, num_packets/2,
    update/3
]).

-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-type summary() :: #blockchain_state_channel_summary_v1_pb{}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new(ClientPubkeyBin :: libp2p_crypto:pubkey_bin()) -> summary().
new(ClientPubkeyBin) ->
    #blockchain_state_channel_summary_v1_pb{
       client_pubkeybin=ClientPubkeyBin,
       num_dcs=0,
       num_packets=0
    }.

-spec new(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
          NumPackets :: non_neg_integer(),
          NumDCs :: non_neg_integer()) -> summary().
new(ClientPubkeyBin, NumPackets, NumDCs) ->
    #blockchain_state_channel_summary_v1_pb{
       client_pubkeybin=ClientPubkeyBin,
       num_dcs=NumDCs,
       num_packets=NumPackets
    }.

-spec client_pubkeybin(Summary :: summary()) -> libp2p_crypto:pubkey_bin().
client_pubkeybin(#blockchain_state_channel_summary_v1_pb{client_pubkeybin=ClientPubkeyBin}) ->
    ClientPubkeyBin.

-spec num_packets(Summary :: summary()) -> non_neg_integer().
num_packets(#blockchain_state_channel_summary_v1_pb{num_packets=NumPackets}) ->
    NumPackets.

-spec num_packets(NumPackets :: non_neg_integer(),
                  Summary :: summary()) -> summary().
num_packets(NumPackets, Summary) ->
    Summary#blockchain_state_channel_summary_v1_pb{num_packets=NumPackets}.

-spec num_dcs(Summary :: summary()) -> non_neg_integer().
num_dcs(#blockchain_state_channel_summary_v1_pb{num_dcs=NumDCs}) ->
    NumDCs.

-spec num_dcs(NumDCs :: non_neg_integer(),
              Summary :: summary()) -> summary().
num_dcs(NumDCs, Summary) ->
    Summary#blockchain_state_channel_summary_v1_pb{num_dcs=NumDCs}.

-spec update(NumDCs :: non_neg_integer(),
             NumPackets :: non_neg_integer(),
             Summary :: summary()) -> summary().
update(NumDCs, NumPackets, Summary) ->
    Summary#blockchain_state_channel_summary_v1_pb{num_dcs=NumDCs, num_packets=NumPackets}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% TODO: Add some eunits...

-endif.
