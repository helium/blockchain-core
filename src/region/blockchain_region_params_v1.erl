%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Parameters ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_params_v1).

%% TODO
%% -behavior(blockchain_json).

-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    for_region/2,
    get_spreading/2,

    new/1,
    serialize/1,
    deserialize/1
]).

-type region_params_v1() :: #blockchain_region_params_v1_pb{}.

-export_type([region_params_v1/0]).

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec for_region(RegionVar :: atom(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, [blockchain_region_param_v1:region_param_v1()]} | {error, any()}.
for_region(RegionVar, Ledger) ->
    case blockchain:config(RegionVar, Ledger) of
        {ok, Bin} ->
            Deser = deserialize(Bin),
            {ok, region_params(Deser)};
        _ ->
            {error, {not_set, RegionVar}}
    end.

-spec get_spreading(
    Params :: region_params_v1(),
    PacketSize :: non_neg_integer()
) -> {ok, atom()} | {error, any()}.
get_spreading(Params, PacketSize) ->
    %% The spreading does not change per channel frequency
    %% So just get one and do selection depending on max_packet_size
    FirstParam = hd(region_params(Params)),
    Spreading = blockchain_region_param_v1:spreading(FirstParam),
    TaggedSpreading = blockchain_region_spreading_v1:tagged_spreading(Spreading),
    blockchain_region_spreading_v1:select_spreading(TaggedSpreading, PacketSize).

-spec new(RegionParams :: [blockchain_region_param_v1:region_param_v1()]) -> region_params_v1().
new(RegionParams) ->
    #blockchain_region_params_v1_pb{region_params = RegionParams}.

-spec serialize(region_params_v1()) -> binary().
serialize(#blockchain_region_params_v1_pb{} = RegionParams) ->
    blockchain_region_param_v1_pb:encode_msg(RegionParams, blockchain_region_params_v1_pb).

-spec deserialize(binary()) -> region_params_v1().
deserialize(Bin) ->
    blockchain_region_param_v1_pb:decode_msg(Bin, blockchain_region_params_v1_pb).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

-spec region_params(RegionParams :: region_params_v1()) ->
    [blockchain_region_param_v1:region_params_v1()].
region_params(RegionParams) ->
    RegionParams#blockchain_region_params_v1_pb.region_params.
