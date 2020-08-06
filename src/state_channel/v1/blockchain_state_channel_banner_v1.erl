%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Banner ==
%%
%% The SC Banner serves as a "greeting" of sorts, essentially letting the
%% sc client know that it can get ready for the offer/purchase flow.
%%
%% Anytime the state channel on the sc server changes, it sends out a new
%% banner to all it's known streams at which point the client's can (if need be)
%% update their respective states.
%%
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_banner_v1).

-export([
    new/0, new/1,
    sc/1,
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

-spec sc(Banner :: banner()) -> undefined | blockchain_state_channel_v1:state_channel().
sc(#blockchain_state_channel_banner_v1_pb{sc=SC}) ->
    SC.

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

encode_decode_test() ->
    SC = blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>, 10),
    Banner = new(SC),
    ?assertEqual(Banner, decode(encode(Banner))).

sc_test() ->
    SC = blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>, 10),
    Banner = new(SC),
    ?assertEqual(SC, sc(Banner)).

-endif.
