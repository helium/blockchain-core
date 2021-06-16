%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Spreading ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_spreading_v1).

%% TODO
%% -behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    new/1,

    max_packet_size/1,
    region_spreading/1
]).

-type region_spreading_v1() :: #blockchain_region_spreading_v1_pb{}.
-type tagged_spreading() :: #tagged_spreading_pb{}.

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec new(Spreads :: [{non_neg_integer(), atom()}]) -> region_spreading_v1().
new(Spreads) ->
    Tags = [new_tagged_spread(Size, Spread) || {Size, Spread} <- Spreads],
    #blockchain_region_spreading_v1_pb{tagged_spreading = Tags}.

-spec max_packet_size(TaggedSpreading :: tagged_spreading()) -> non_neg_integer().
max_packet_size(TaggedSpreading) ->
    TaggedSpreading#tagged_spreading_pb.max_packet_size.

-spec region_spreading(TaggedSpreading :: tagged_spreading()) -> undefined | atom().
region_spreading(TaggedSpreading) ->
    TaggedSpreading#tagged_spreading_pb.region_spreading.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

-spec new_tagged_spread(
    Size :: undefined | non_neg_integer(),
    Spreading :: undefined | atom()
) -> tagged_spreading().
new_tagged_spread(Size, Spreading) ->
    #tagged_spreading_pb{max_packet_size = Size, region_spreading = Spreading}.
