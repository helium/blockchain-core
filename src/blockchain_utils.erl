%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_utils).

-export([
    shuffle_from_hash/2,
    rand_from_hash/1,
    serialize_hash/1, deserialize_hash/1,
    hex_to_bin/1, bin_to_hex/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec serialize_hash(binary()) -> string().
serialize_hash(Hash) ->
    base58:binary_to_base58(Hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize_hash(string()) -> binary().
deserialize_hash(String) ->
    base58:base58_to_binary(String).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec bin_to_hex(binary()) -> string().
bin_to_hex(Bin) ->
    lists:flatten([[io_lib:format("~2.16.0b",[X]) || <<X:8>> <= Bin ]]).

-spec hex_to_bin(binary()) -> binary().
hex_to_bin(Hex) ->
    << begin {ok, [V], []} = io_lib:fread("~16u", [X, Y]), <<V:8/integer-little>> end || <<X:8/integer, Y:8/integer>> <= Hex >>.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

serialize_deserialize_test() ->
    Hash = <<"123abc">>,
    ?assertEqual(Hash, deserialize_hash(serialize_hash(Hash))).

-endif.