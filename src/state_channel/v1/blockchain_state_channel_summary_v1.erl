%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Summary ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_summary_v1).

-export([
    new/1,
    client_pubkeybin/1,
    summary_for/2,
    num_bytes/1, num_bytes/2,
    num_packets/1, num_packets/2
]).

-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-type summary() :: #blockchain_state_channel_summary_v1_pb{}.
-type summaries() :: [summary()].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new(ClientPubkeyBin :: libp2p_crypto:pubkey_bin()) -> summary().
new(ClientPubkeyBin) ->
    #blockchain_state_channel_summary_v1_pb{
        client_pubkeybin=ClientPubkeyBin,
        num_bytes=0,
        num_packets=0
    }.

-spec client_pubkeybin(Summary :: summary()) -> libp2p_crypto:pubkey_bin().
client_pubkeybin(#blockchain_state_channel_summary_v1_pb{client_pubkeybin=ClientPubkeyBin}) ->
    ClientPubkeyBin.

-spec num_packets(Summary :: summary()) -> non_neg_integer().
num_packets(#blockchain_state_channel_summary_v1_pb{num_packets=NumPackets}) ->
    NumPackets.

-spec num_bytes(Summary :: summary()) -> non_neg_integer().
num_bytes(#blockchain_state_channel_summary_v1_pb{num_bytes=NumBytes}) ->
    NumBytes.

-spec summary_for(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                  Summaries :: summaries()) -> {ok, summary()} | {error, not_found}.
summary_for(ClientPubkeyBin, Summaries) ->
    Filter = fun(Summary) -> ?MODULE:client_pubkeybin(Summary) == ClientPubkeyBin end,
    case lists:filter(Filter, Summaries) of
        L when L /= [] ->
            Summary = hd(L),
            {ok, Summary};
        [] ->
            {error, not_found}
    end.

-spec num_packets(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                  Summaries :: summaries()) -> {ok, non_neg_integer()} | {error, not_found}.
num_packets(ClientPubkeyBin, Summaries) ->
    Filter = fun(Summary) -> ?MODULE:client_pubkeybin(Summary) == ClientPubkeyBin end,
    case lists:filter(Filter, Summaries) of
        L when L /= [] ->
            Summary = hd(L),
            {ok, ?MODULE:num_packets(Summary)};
        [] ->
            {error, not_found}
    end.

-spec num_bytes(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                Summaries :: summaries()) -> {ok, non_neg_integer()} | {error, not_found}.
num_bytes(ClientPubkeyBin, Summaries) ->
    Filter = fun(Summary) -> ?MODULE:client_pubkeybin(Summary) == ClientPubkeyBin end,
    case lists:filter(Filter, Summaries) of
        L when L /= [] ->
            Summary = hd(L),
            {ok, ?MODULE:num_bytes(Summary)};
        [] ->
            {error, not_found}
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% TODO: Add some eunits...

-endif.
