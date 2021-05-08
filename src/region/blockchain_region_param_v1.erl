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
    new/4
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
