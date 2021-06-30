%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Parameters ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_params_v1).

%% TODO
%% -behavior(blockchain_json).

-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    for_region/2,

    get_spreading/2,
    get_bandwidth/1,

    new/1,
    serialize/1,
    deserialize/1
]).

-type region_params_v1() :: #blockchain_region_params_v1_pb{}.
-type region_param_var() ::
    'region_params_us915'
    | 'region_params_eu868'
    | 'region_params_as923_1'
    | 'region_params_as923_2'
    | 'region_params_as923_3'
    | 'region_params_au915'
    | 'region_params_ru864'
    | 'region_params_cn470'
    | 'region_params_in865'
    | 'region_params_kr920'
    | 'region_params_eu433'.

-export_type([region_params_v1/0]).

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec for_region(
    RegionVar :: blockchain_region_v1:region_var(),
    Ledger :: blockchain_ledger_v1:ledger()
) ->
    {ok, [blockchain_region_param_v1:region_param_v1()]} | {error, any()}.
for_region(RegionVar, Ledger) ->
    case blockchain:config(region_param(RegionVar), Ledger) of
        {ok, Bin} ->
            Deser = deserialize(Bin),
            {ok, region_params(Deser)};
        _ ->
            {error, {not_set, RegionVar}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Map region to region parameters
%% @end
%%-------------------------------------------------------------------
-spec region_param(blockchain_region_v1:region_var()) -> region_param_var().
region_param(?region_as923_1) -> ?region_params_as923_1;
region_param(?region_as923_2) -> ?region_params_as923_2;
region_param(?region_as923_3) -> ?region_params_as923_3;
region_param(?region_au915) -> ?region_params_au915;
region_param(?region_cn470) -> ?region_params_cn470;
region_param(?region_eu433) -> ?region_params_eu433;
region_param(?region_eu868) -> ?region_params_eu868;
region_param(?region_in865) -> ?region_params_in865;
region_param(?region_kr920) -> ?region_params_kr920;
region_param(?region_ru864) -> ?region_params_ru864;
region_param(?region_us915) -> ?region_params_us915;

%% NOTE: This _may_ be required for working with existing miner_lora and miner_onion_server
region_param('AS923_1') -> ?region_params_as923_1;
region_param('AS923_2') -> ?region_params_as923_2;
region_param('AS923_3') -> ?region_params_as923_3;
region_param('AU915') -> ?region_params_au915;
region_param('CN470') -> ?region_params_cn470;
region_param('EU433') -> ?region_params_eu433;
region_param('EU868') -> ?region_params_eu868;
region_param('IN865') -> ?region_params_in865;
region_param('KR920') -> ?region_params_kr920;
region_param('RU864') -> ?region_params_ru864;
region_param('US915') -> ?region_params_us915.

-spec get_spreading(
    Params :: region_params_v1() | [blockchain_region_param_v1:region_param_v1()],
    PacketSize :: non_neg_integer()
) -> {ok, atom()} | {error, any()}.
get_spreading(Params, PacketSize) when is_list(Params) ->
    %% The spreading does not change per channel frequency
    %% So just get one and do selection depending on max_packet_size
    FirstParam = hd(Params),
    Spreading = blockchain_region_param_v1:spreading(FirstParam),
    TaggedSpreading = blockchain_region_spreading_v1:tagged_spreading(Spreading),
    blockchain_region_spreading_v1:select_spreading(TaggedSpreading, PacketSize);
get_spreading(Params, PacketSize) ->
    %% The spreading does not change per channel frequency
    %% So just get one and do selection depending on max_packet_size
    FirstParam = hd(region_params(Params)),
    Spreading = blockchain_region_param_v1:spreading(FirstParam),
    TaggedSpreading = blockchain_region_spreading_v1:tagged_spreading(Spreading),
    blockchain_region_spreading_v1:select_spreading(TaggedSpreading, PacketSize).

-spec get_bandwidth(Params :: region_params_v1() | [blockchain_region_param_v1:region_param_v1()]) ->
    pos_integer().
get_bandwidth(Params) when is_list(Params) ->
    %% The bandwidth does not change per channel frequency, so just get one
    FirstParam = hd(Params),
    blockchain_region_param_v1:bandwidth(FirstParam);
get_bandwidth(Params) ->
    FirstParam = hd(region_params(Params)),
    blockchain_region_param_v1:bandwidth(FirstParam).

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
