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
    clear_cached_region_params/0,

    get_spreading/2,
    get_bandwidth/1,

    new/1,
    serialize/1,
    deserialize/1
]).

-type region_params_v1() :: #blockchain_region_params_v1_pb{}.
-type region_param_var() :: atom().

-export_type([region_params_v1/0]).

-ifdef(TEST).
-export([region_params/1]).
-endif.

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec for_region(
    RegionVar :: blockchain_region_v1:region_var(),
    Ledger :: blockchain_ledger_v1:ledger()
) ->
    {ok, [blockchain_region_param_v1:region_param_v1()]} | {error, any()}.
for_region(RegionVar, Ledger) ->
    RegionParamVar = region_param(RegionVar),
    case persistent_term:get(RegionParamVar, not_found) of
        not_found ->
            case ?get_var(RegionParamVar, Ledger) of
                {ok, Bin} ->
                    Deser = deserialize(Bin),
                    Params = region_params(Deser),
                    _ = persistent_term:put(RegionParamVar, Params),
                    {ok, Params};
                _ ->
                    {error, {not_set, RegionVar}}
            end;
        Params ->
            {ok, Params}
    end.

-spec clear_cached_region_params() -> ok.
clear_cached_region_params() ->
    lists:foreach(fun(R)-> _ = persistent_term:erase(R) end, all_cached_region_params()).
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
region_param('US915') -> region_us915_params;
region_param('EU868') -> region_eu868_params;
region_param('EU433') -> region_eu433_params;
region_param('CN470') -> region_cn470_params;
region_param('AU915') -> region_au915_params;
region_param('AS923_1') -> region_as923_1_params;
region_param('KR920') -> region_kr920_params;
region_param('IN865') -> region_in865_params;
region_param('AS923_2') -> region_as923_2_params;
region_param('AS923_3') -> region_as923_3_params;
region_param('AS923_4') -> region_as923_4_params;
region_param('AS923_1B') -> region_as923_1b_params;
region_param('CD900_1A') -> region_cd900_1a_params;
region_param('RU864') -> region_ru864_params;
region_param('EU868_A') -> region_eu868_a_params;
region_param('EU868_B') -> region_eu868_b_params;
region_param('EU868_C') -> region_eu868_c_params;
region_param('EU868_D') -> region_eu868_d_params;
region_param('EU868_E') -> region_eu868_e_params;
region_param('EU868_F') -> region_eu868_f_params;
region_param('AU915_SB1') -> region_au915_sb1_params;
region_param('AU915_SB2') -> region_au915_sb2_params;
region_param('AS923_1A') -> region_as923_1a_params;
region_param('AS923_1C') -> region_as923_1c_params;
region_param('AS923_1D') -> region_as923_1d_params;
region_param('AS923_1E') -> region_as923_1e_params;
region_param('AS923_1F') -> region_as923_1f_params;

%% in all other cases we simply append 'params' to the provided region
%% and we assume the region name came from the chain var that lists the region
%% names
region_param(Region) -> list_to_atom(io_lib:format("~s_params", [Region])).

%% return a list of all supported regions
%% NOTE: if a new region is added to a chain var
%% it will need to be added to this list
%% in the same way as its requires a mapping in the
%% list above
-spec all_cached_region_params() -> [atom].
all_cached_region_params() ->
    [
        region_us915_params,
        region_eu868_params,
        region_eu433_params,
        region_cn470_params,
        region_au915_params,
        region_as923_1_params,
        region_kr920_params,
        region_in865_params,
        region_as923_2_params,
        region_as923_3_params,
        region_as923_4_params,
        region_as923_1b_params,
        region_cd900_1a_params,
        region_ru864_params,
        region_eu868_a_params,
        region_eu868_b_params,
        region_eu868_c_params,
        region_eu868_d_params,
        region_eu868_e_params,
        region_eu868_f_params,
        region_au915_sb1_params,
        region_au915_sb2_params,
        region_as923_1a_params,
        region_as923_1c_params,
        region_as923_1d_params,
        region_as923_1e_params,
        region_as923_1f_params
    ].

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
