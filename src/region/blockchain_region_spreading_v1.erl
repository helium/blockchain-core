%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Spreading ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_spreading_v1).

%% TODO
%% -behavior(blockchain_json).
-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    new/1,
    tagged_spreading/1,

    select_spreading/2,
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

-spec tagged_spreading(Spreading :: region_spreading_v1()) -> [tagged_spreading()].
tagged_spreading(Spreading) ->
    Spreading#blockchain_region_spreading_v1_pb.tagged_spreading.

-spec max_packet_size(TaggedSpreading :: tagged_spreading()) -> non_neg_integer().
max_packet_size(TaggedSpreading) ->
    TaggedSpreading#tagged_spreading_pb.max_packet_size.

-spec region_spreading(TaggedSpreading :: tagged_spreading()) -> undefined | atom().
region_spreading(TaggedSpreading) ->
    TaggedSpreading#tagged_spreading_pb.region_spreading.

-spec select_spreading(
    TaggedSpreading :: [tagged_spreading()],
    PacketSize :: non_neg_integer()
) -> {ok, atom()} | {error, any()}.
select_spreading(TaggedSpreading, PacketSize) ->
    %% FIXME: This is not quite right...
    FilterFun = fun(Tagged) ->
        max_packet_size(Tagged) >= PacketSize
    end,
    case lists:filter(FilterFun, TaggedSpreading) of
        [] -> {error, unable_to_get_spreading};
        Thing ->
            Tag = hd(Thing),
            {ok, region_spreading(Tag)}
    end.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

-spec new_tagged_spread(
    Size :: undefined | non_neg_integer(),
    Spreading :: undefined | atom()
) -> tagged_spreading().
new_tagged_spread(Size, Spreading) ->
    #tagged_spreading_pb{max_packet_size = Size, region_spreading = Spreading}.
