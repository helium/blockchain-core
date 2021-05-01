%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region API ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_param_v1).

%% TODO
%% -behavior(blockchain_json).

-include("blockchain.hrl").
-include("blockchain_json.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    get_params/1,

    serialized_us915/0,

    new_param/4,
    new_params/1,
    new_spreading/1,
    serialize_params/1,
    deserialize_params/1
]).

-type region_param_v1() :: #blockchain_region_param_v1_pb{}.
-type region_params_v1() :: #blockchain_region_params_v1_pb{}.
-type region_spreading_v1() :: #blockchain_region_spreading_v1_pb{}.

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec serialized_us915() -> binary().
serialized_us915() ->
    blockchain_region_param_v1:serialize_params(get_params(us915)).

-spec get_params(atom()) -> region_params_v1().
get_params(us915) ->
    Params =
        lists:foldl(
            fun(P, Acc) ->
                CF = proplists:get_value(<<"channel_frequency">>, P),
                BW = proplists:get_value(<<"bandwidth">>, P),
                MP = proplists:get_value(<<"max_power">>, P),
                Spreading = new_spreading([
                    to_spreading(I)
                    || I <- proplists:get_value(<<"spreading">>, P)
                ]),
                [new_param(CF, BW, MP, Spreading) | Acc]
            end,
            [],
            ?REGION_PARAMS_US915
        ),
    new_params(Params).

-spec new_param(
    ChannelFreq :: undefined | non_neg_integer(),
    Bandwidth :: undefined | non_neg_integer(),
    MaxPower :: undefined | non_neg_integer(),
    Spreading :: undefined | region_spreading_v1()
) -> region_param_v1().
new_param(ChannelFreq, Bandwidth, MaxPower, Spreading) ->
    #blockchain_region_param_v1_pb{
        channel_frequency = ChannelFreq,
        bandwidth = Bandwidth,
        max_power = MaxPower,
        spreading = Spreading
    }.

-spec new_params(RegionParams :: [region_param_v1()]) -> region_params_v1().
new_params(RegionParams) ->
    #blockchain_region_params_v1_pb{region_params = RegionParams}.

-spec serialize_params(region_params_v1()) -> binary().
serialize_params(#blockchain_region_params_v1_pb{} = RegionParams) ->
    blockchain_region_param_v1_pb:encode_msg(RegionParams, blockchain_region_params_v1_pb).

-spec deserialize_params(binary()) -> region_params_v1().
deserialize_params(Bin) ->
    blockchain_region_param_v1_pb:decode_msg(Bin, blockchain_region_params_v1_pb).

-spec new_spreading([atom()]) -> region_spreading_v1().
new_spreading(Spreading) ->
    #blockchain_region_spreading_v1_pb{region_spreading = Spreading}.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

-spec to_spreading(binary()) -> atom().
to_spreading(<<"sf7">>) -> 'SF7';
to_spreading(<<"sf8">>) -> 'SF8';
to_spreading(<<"sf9">>) -> 'SF9';
to_spreading(<<"sf10">>) -> 'SF10';
to_spreading(<<"sf11">>) -> 'SF11';
to_spreading(<<"sf12">>) -> 'SF12';
to_spreading(_) -> undefined.
