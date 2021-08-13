%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Parameters ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_params_v1).

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
-type region_param_var() :: atom().

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
%% NOTE: Adding these for maintaining compatibility with old style
%% regions (miner's CSV file). Further miner_lora and/or miner_onion_server
%% may invoke this path on their bootup. It's not ideal but its probably
%% required for the transition period. Maybe we can remove it once a majority
%% of the fleet has transitioned after activation of region variables on chain.
region_param('AS923_1') -> region_as923_1_params;
region_param('AS923_2') -> region_as923_2_params;
region_param('AS923_3') -> region_as923_3_params;
region_param('AS923_4') -> region_as923_4_params;
region_param('AU915') -> region_au915_params;
region_param('CN470') -> region_cn470_params;
region_param('EU433') -> region_eu433_params;
region_param('EU868') -> region_eu868_params;
region_param('IN865') -> region_in865_params;
region_param('KR920') -> region_kr920_params;
region_param('RU864') -> region_ru864_params;
region_param('US915') -> region_us915_params;

%% in all other cases we simply append 'params' to the provided region
%% and we assume the region name came from the chain var that lists the region
%% names
region_param(Region) -> list_to_atom(io_lib:format("~s_params", [Region])).

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
