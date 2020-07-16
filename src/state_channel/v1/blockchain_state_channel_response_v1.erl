%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Response ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_response_v1).

-export([
    new/1, new/2,
    accepted/1, accepted/2,
    downlink/1, downlink/2
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-type downlink() :: blockchain_helium_packet_v1:packet() | undefined.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type response() :: #blockchain_state_channel_response_v1_pb{}.
-export_type([response/0]).

new(Accepted) when is_boolean(Accepted) ->
    #blockchain_state_channel_response_v1_pb{accepted=Accepted}.

new(true=Accepted, Downlink) ->
    %% accepted false with a downlink is meaningless..
    #blockchain_state_channel_response_v1_pb{accepted=Accepted, downlink=Downlink}.

-spec accepted(Resp :: response(), Accepted :: boolean()) -> response().
accepted(Resp, Accepted) ->
    Resp#blockchain_state_channel_response_v1_pb{accepted=Accepted}.

-spec accepted(response()) -> boolean().
accepted(#blockchain_state_channel_response_v1_pb{accepted=Accepted}) ->
    Accepted.

-spec downlink(Resp :: response(),
               Downlink :: downlink()) -> response().
downlink(Resp, Downlink) ->
    Resp#blockchain_state_channel_response_v1_pb{downlink=Downlink}.

-spec downlink(response()) -> downlink().
downlink(#blockchain_state_channel_response_v1_pb{downlink=Downlink}) ->
    Downlink.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

%% TODO: add some...

-endif.
