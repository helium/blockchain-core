%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Parameters ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_params_v1).

%% TODO
%% -behavior(blockchain_json).

-include("blockchain_region.hrl").
-include("blockchain_json.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    serialized_us915/0,
    serialized_eu868/0,

    new/1,
    fetch/1,
    serialize/1,
    deserialize/1
]).

-type region_params_v1() :: #blockchain_region_params_v1_pb{}.

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec serialized_us915() -> binary().
serialized_us915() ->
    serialize(fetch(us915)).

-spec serialized_eu868() -> binary().
serialized_eu868() ->
    serialize(fetch(eu868)).

-spec fetch(atom()) -> region_params_v1().
fetch(us915) ->
    Params = make_params(?REGION_PARAMS_US915),
    new(Params);
fetch(eu868) ->
    Params = make_params(?REGION_PARAMS_EU868),
    new(Params).

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

make_params(RegionParams) ->
    lists:foldl(
        fun(P, Acc) ->
            Param = construct_param(P),
            [Param | Acc]
        end,
        [],
        RegionParams
    ).

construct_param(P) ->
    CF = proplists:get_value(<<"channel_frequency">>, P),
    BW = proplists:get_value(<<"bandwidth">>, P),
    MP = proplists:get_value(<<"max_power">>, P),
    Spreading = blockchain_region_spreading_v1:new(proplists:get_value(<<"spreading">>, P)),
    blockchain_region_param_v1:new(CF, BW, MP, Spreading).

