%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utility Functions ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_util).

-export([
    hexdump/1
    ,atomic_save/2
    ,serialize_hash/1, deserialize_hash/1
]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hexdump(binary()) -> string().
hexdump(Bin) ->
    lists:flatten([[io_lib:format("~2.16.0b",[X]) || <<X:8>> <= Bin]]).

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
