%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Banner ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_banner_v1).

-export([
    new/0, new/1,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type banner() :: #blockchain_state_channel_banner_v1_pb{}.
-export_type([banner/0]).

-spec new() -> banner().
new() ->
    #blockchain_state_channel_banner_v1_pb{}.

-spec new(SC :: blockchain_state_channel_v1:state_channel()) -> banner().
new(SC) ->
    #blockchain_state_channel_banner_v1_pb{sc=SC}.

-spec encode(banner()) -> binary().
encode(#blockchain_state_channel_banner_v1_pb{}=Banner) ->
    blockchain_state_channel_v1_pb:encode_msg(Banner).

-spec decode(binary()) -> banner().
decode(BinaryBanner) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryBanner, blockchain_state_channel_banner_v1_pb).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-endif.
