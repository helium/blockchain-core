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
    params_for_region/2,

    serialized_us915/0,
    serialized_eu868/0,
    serialized_au915/0,
    serialized_as923_1/0,
    serialized_as923_2/0,
    serialized_as923_3/0,
    serialized_ru864/0,
    serialized_cn470/0,
    serialized_in865/0,
    serialized_kr920/0,
    serialized_eu433/0,

    new/1,
    fetch/1,
    serialize/1,
    deserialize/1,
    region_params/1
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

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec params_for_region(Region :: atom(), Ledger :: blockchain_ledger_v1:ledger()) ->
    {ok, region_params_v1()} | {error, any()}.
params_for_region(Region, Ledger) ->
    Var = maps:get(Region, ?REGION_PARAM_MAP),
    case blockchain:config(Var, Ledger) of
        {ok, Bin} ->
            {ok, deserialize(Bin)};
        _ ->
            {error, {not_set, Var}}
    end.

-spec serialized_us915() -> binary().
serialized_us915() ->
    serialize(fetch(us915)).

-spec serialized_eu868() -> binary().
serialized_eu868() ->
    serialize(fetch(eu868)).

-spec serialized_au915() -> binary().
serialized_au915() ->
    serialize(fetch(au915)).

-spec serialized_as923_1() -> binary().
serialized_as923_1() ->
    serialize(fetch(as923_1)).

-spec serialized_as923_2() -> binary().
serialized_as923_2() ->
    serialize(fetch(as923_2)).

-spec serialized_as923_3() -> binary().
serialized_as923_3() ->
    serialize(fetch(as923_3)).

-spec serialized_ru864() -> binary().
serialized_ru864() ->
    serialize(fetch(ru864)).

-spec serialized_cn470() -> binary().
serialized_cn470() ->
    serialize(fetch(cn470)).

-spec serialized_in865() -> binary().
serialized_in865() ->
    serialize(fetch(in865)).

-spec serialized_kr920() -> binary().
serialized_kr920() ->
    serialize(fetch(kr920)).

-spec serialized_eu433() -> binary().
serialized_eu433() ->
    serialize(fetch(eu433)).

-spec fetch(atom()) -> region_params_v1().
fetch(us915) ->
    Params = make_params(?REGION_PARAMS_US915),
    new(Params);
fetch(eu868) ->
    Params = make_params(?REGION_PARAMS_EU868),
    new(Params);
fetch(au915) ->
    Params = make_params(?REGION_PARAMS_AU915),
    new(Params);
fetch(as923_1) ->
    Params = make_params(?REGION_PARAMS_AS923_1),
    new(Params);
fetch(as923_2) ->
    Params = make_params(?REGION_PARAMS_AS923_2),
    new(Params);
fetch(as923_3) ->
    Params = make_params(?REGION_PARAMS_AS923_3),
    new(Params);
fetch(ru864) ->
    Params = make_params(?REGION_PARAMS_RU864),
    new(Params);
fetch(cn470) ->
    Params = make_params(?REGION_PARAMS_CN470),
    new(Params);
fetch(in865) ->
    Params = make_params(?REGION_PARAMS_IN865),
    new(Params);
fetch(kr920) ->
    Params = make_params(?REGION_PARAMS_KR920),
    new(Params);
fetch(eu433) ->
    Params = make_params(?REGION_PARAMS_EU433),
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

-spec region_params(RegionParams :: region_params_v1()) -> [blockchain_region_param_v1:region_params_v1()].
region_params(RegionParams) ->
    RegionParams#blockchain_region_params_v1_pb.region_params.

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
    MaxEIRP = proplists:get_value(<<"max_eirp">>, P),
    Spreading = blockchain_region_spreading_v1:new(proplists:get_value(<<"spreading">>, P)),
    blockchain_region_param_v1:new(CF, BW, MaxEIRP, Spreading).
