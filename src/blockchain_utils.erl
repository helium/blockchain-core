%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_utils).

-export([
    shuffle_from_hash/2,
    rand_from_hash/1,
    haversine_distance/2,
    normalize_float/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(EARTHRADIUS, 3961).
-type coordinate() :: {float(), float()}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec shuffle_from_hash(binary(), list()) -> list().
shuffle_from_hash(Hash, L) ->
    ?MODULE:rand_from_hash(Hash),
    [X ||{_, X} <- lists:sort([{rand:uniform(), E} || E <- L])].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rand_from_hash(binary()) -> any().
rand_from_hash(Hash) ->
    <<I1:86/integer, I2:85/integer, I3:85/integer, _/binary>> = Hash,
    rand:seed(exs1024, {I1, I2, I3}).

-spec haversine_distance(coordinate(), coordinate()) -> float().
haversine_distance({Lat1, Long1}, {Lat2, Long2}) ->
    V = math:pi()/180,
    DeltaLat = (Lat2 - Lat1) * V,
    DeltaLong = (Long2 - Long1) * V,
    A = math:pow(math:sin(DeltaLat/2), 2) + math:cos(Lat1 * V) * math:cos(Lat2 * V) * math:pow(math:sin(DeltaLong/2), 2),
    C = 2 * math:atan2(math:sqrt(A), math:sqrt(1-A)),
    ?EARTHRADIUS * C.

%%--------------------------------------------------------------------
%% @doc normalize a float by converting it to fixed point and back
%% using 16 bits of exponent precision. This should be well above
%% the floating point error threshold and doing this will prevent
%% errors from accumulating.
%% @end
%%--------------------------------------------------------------------
normalize_float(Float) ->
    round(Float * 65536) / 65536.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.
