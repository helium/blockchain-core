%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Region Spreading ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_region_spreading_v1).

%% TODO
%% -behavior(blockchain_json).

-include("blockchain_region.hrl").
-include("blockchain_json.hrl").
-include("blockchain_vars.hrl").

-include_lib("helium_proto/include/blockchain_region_param_v1_pb.hrl").

-export([
    new/1
]).

-type region_spreading_v1() :: #blockchain_region_spreading_v1_pb{}.

%%--------------------------------------------------------------------
%% api
%%--------------------------------------------------------------------

-spec new(Spreading :: [binary()]) -> region_spreading_v1().
new(Spreading) ->
    #blockchain_region_spreading_v1_pb{region_spreading = [to_spreading(I) || I <- Spreading]}.

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
