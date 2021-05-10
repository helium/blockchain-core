%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Singular Paramater ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_param_v1).

%% TODO
%% -behavior(blockchain_json).

-include("blockchain_region.hrl").
-include("blockchain_json.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    new/4,

    channel_frequency/1,
    bandwidth/1,
    max_eirp/1,
    spreading/1
]).

-type region_param_v1() :: #blockchain_region_param_v1_pb{}.

-export_type([region_param_v1/0]).

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec new(
    ChannelFreq :: undefined | non_neg_integer(),
    Bandwidth :: undefined | non_neg_integer(),
    MaxEIRP :: undefined | non_neg_integer(),
    Spreading :: undefined | blockchain_region_spreading_v1:spreading()
) -> region_param_v1().
new(ChannelFreq, Bandwidth, MaxEIRP, Spreading) ->
    #blockchain_region_param_v1_pb{
        channel_frequency = ChannelFreq,
        bandwidth = Bandwidth,
        max_eirp = MaxEIRP,
        spreading = Spreading
    }.

-spec channel_frequency(Param :: region_param_v1()) -> undefined | non_neg_integer().
channel_frequency(Param) ->
    Param#blockchain_region_param_v1_pb.channel_frequency.

-spec bandwidth(Param :: region_param_v1()) -> undefined | non_neg_integer().
bandwidth(Param) ->
    Param#blockchain_region_param_v1_pb.bandwidth.

-spec max_eirp(Param :: region_param_v1()) -> undefined | non_neg_integer().
max_eirp(Param) ->
    Param#blockchain_region_param_v1_pb.max_eirp.

-spec spreading(Param :: region_param_v1()) ->
    undefined | blockchain_region_spreading_v1:spreading().
spreading(Param) ->
    Param#blockchain_region_param_v1_pb.spreading.
