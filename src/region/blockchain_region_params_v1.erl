%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Parameters ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_params_v1).

%% TODO
%% -behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    for_region/2,

    new/1,
    serialize/1,
    deserialize/1
]).

-type region_params_v1() :: #blockchain_region_params_v1_pb{}.

-define(REGION_PARAM_MAP, #{
    us915 => ?region_params_us915,
    eu868 => ?region_params_eu868,
    as923_1 => ?region_params_as923_1,
    as923_2 => ?region_params_as923_2,
    as923_3 => ?region_params_as923_3,
    au915 => ?region_params_au915,
    ru864 => ?region_params_ru864,
    cn470 => ?region_params_cn470,
    in865 => ?region_params_in865,
    kr920 => ?region_params_kr920,
    eu433 => ?region_params_eu433
}).

-export_type([region_params_v1/0]).

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec for_region(Region :: atom(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, [blockchain_region_param_v1:region_param_v1()]} | {error, any()}.
for_region(Region, Ledger) ->
    Var = maps:get(Region, ?REGION_PARAM_MAP),
    case blockchain:config(Var, Ledger) of
        {ok, Bin} ->
            Deser = deserialize(Bin),
            {ok, region_params(Deser)};
        _ ->
            {error, {not_set, Var}}
    end.

-spec new(RegionParams :: [blockchain_region_param_v1:region_param_v1()]) -> region_params_v1().
new(RegionParams) ->
    #blockchain_region_params_v1_pb{region_params = RegionParams}.

-spec serialize(region_params_v1()) -> binary().
serialize(#blockchain_region_params_v1_pb{} = RegionParams) ->
    blockchain_region_param_v1_pb:encode_msg(RegionParams, blockchain_region_params_v1_pb).

-spec deserialize(binary()) -> region_params_v1().
deserialize(Bin) ->
    blockchain_region_param_v1_pb:decode_msg(Bin, blockchain_region_params_v1_pb).

-spec region_params(RegionParams :: region_params_v1()) -> [blockchain_region_param_v1:region_params_v1()].
region_params(RegionParams) ->
    RegionParams#blockchain_region_params_v1_pb.region_params.
