%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utility Functions ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_util).

-export([
    atomic_save/2
    ,serialize_hash/1, deserialize_hash/1
    ,hex_to_bin/1, bin_to_hex/1
    ,serial_version/1
]).

-type serial_version() :: v1 | v2 | v3.
-export_type([serial_version/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec atomic_save(file:filename_all(), binary() | string()) -> ok | {error, any()}.
atomic_save(File, Bin) ->
    ok = filelib:ensure_dir(File),
    TmpFile = File ++ "-tmp",
    ok = file:write_file(TmpFile, Bin),
    file:rename(TmpFile, File).

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

-spec hex_to_bin(string()) -> binary().
hex_to_bin(Hex) ->
  << begin {ok, [V], []} = io_lib:fread("~16u", [X, Y]), <<V:8/integer-little>> end || <<X:8/integer, Y:8/integer>> <= Hex >>.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec serial_version(string()) -> serial_version().
serial_version(Dir) ->
    case
        {string:find(Dir, "v1")
         ,string:find(Dir, "v2")
         ,string:find(Dir, "v3")}
    of
        {Str, nomatch, nomatch} when is_list(Str) -> v1;
        {nomatch, Str,nomatch} when is_list(Str) -> v2;
        {nomatch, nomatch, Str} when is_list(Str) -> v3;
        _ -> v1
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

serialize_deserialize_test() ->
    Hash = <<"123abc">>,
    ?assertEqual(Hash, deserialize_hash(serialize_hash(Hash))).

serial_version_test() ->
    ?assertEqual(v1, serial_version("data_v1/blockchain")),
    ?assertEqual(v2, serial_version("data_v2/blockchain")),
    ?assertEqual(v3, serial_version("data_v3/blockchain")),
    ?assertEqual(v1, serial_version("anything else")).

-endif.
