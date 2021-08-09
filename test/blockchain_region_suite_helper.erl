-module(blockchain_region_suite_helper).
-include("blockchain_region_test.hrl").

-export([
    serialized_us915/0,
    serialized_eu868/0,
    serialized_au915/0,
    serialized_as923_1/0,
    serialized_as923_2/0,
    serialized_as923_3/0,
    serialized_as923_4/0,
    serialized_ru864/0,
    serialized_cn470/0,
    serialized_in865/0,
    serialized_kr920/0,
    serialized_eu433/0,
    fetch/1
]).

-spec serialized_us915() -> binary().
serialized_us915() ->
    blockchain_region_params_v1:serialize(fetch(us915)).

-spec serialized_eu868() -> binary().
serialized_eu868() ->
    blockchain_region_params_v1:serialize(fetch(eu868)).

-spec serialized_au915() -> binary().
serialized_au915() ->
    blockchain_region_params_v1:serialize(fetch(au915)).

-spec serialized_as923_1() -> binary().
serialized_as923_1() ->
    blockchain_region_params_v1:serialize(fetch(as923_1)).

-spec serialized_as923_2() -> binary().
serialized_as923_2() ->
    blockchain_region_params_v1:serialize(fetch(as923_2)).

-spec serialized_as923_3() -> binary().
serialized_as923_3() ->
    blockchain_region_params_v1:serialize(fetch(as923_3)).

-spec serialized_as923_4() -> binary().
serialized_as923_4() ->
    blockchain_region_params_v1:serialize(fetch(as923_4)).

-spec serialized_ru864() -> binary().
serialized_ru864() ->
    blockchain_region_params_v1:serialize(fetch(ru864)).

-spec serialized_cn470() -> binary().
serialized_cn470() ->
    blockchain_region_params_v1:serialize(fetch(cn470)).

-spec serialized_in865() -> binary().
serialized_in865() ->
    blockchain_region_params_v1:serialize(fetch(in865)).

-spec serialized_kr920() -> binary().
serialized_kr920() ->
    blockchain_region_params_v1:serialize(fetch(kr920)).

-spec serialized_eu433() -> binary().
serialized_eu433() ->
    blockchain_region_params_v1:serialize(fetch(eu433)).

-spec fetch(atom()) -> blockchain_region_params_v1:region_params_v1().
fetch(us915) ->
    Params = make_params(?REGION_PARAMS_US915),
    blockchain_region_params_v1:new(Params);
fetch(eu868) ->
    Params = make_params(?REGION_PARAMS_EU868),
    blockchain_region_params_v1:new(Params);
fetch(au915) ->
    Params = make_params(?REGION_PARAMS_AU915),
    blockchain_region_params_v1:new(Params);
fetch(as923_1) ->
    Params = make_params(?REGION_PARAMS_AS923_1),
    blockchain_region_params_v1:new(Params);
fetch(as923_2) ->
    Params = make_params(?REGION_PARAMS_AS923_2),
    blockchain_region_params_v1:new(Params);
fetch(as923_3) ->
    Params = make_params(?REGION_PARAMS_AS923_3),
    blockchain_region_params_v1:new(Params);
fetch(as923_4) ->
    Params = make_params(?REGION_PARAMS_AS923_4),
    blockchain_region_params_v1:new(Params);
fetch(ru864) ->
    Params = make_params(?REGION_PARAMS_RU864),
    blockchain_region_params_v1:new(Params);
fetch(cn470) ->
    Params = make_params(?REGION_PARAMS_CN470),
    blockchain_region_params_v1:new(Params);
fetch(in865) ->
    Params = make_params(?REGION_PARAMS_IN865),
    blockchain_region_params_v1:new(Params);
fetch(kr920) ->
    Params = make_params(?REGION_PARAMS_KR920),
    blockchain_region_params_v1:new(Params);
fetch(eu433) ->
    Params = make_params(?REGION_PARAMS_EU433),
    blockchain_region_params_v1:new(Params).

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
