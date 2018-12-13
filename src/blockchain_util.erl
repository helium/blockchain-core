%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utility Functions ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_util).

-export([
    atomic_save/2,
    serialize_hash/1, deserialize_hash/1,
    hex_to_bin/1, bin_to_hex/1,
    create_block/2
]).


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

-spec hex_to_bin(binary()) -> binary().
hex_to_bin(Hex) ->
  << begin {ok, [V], []} = io_lib:fread("~16u", [X, Y]), <<V:8/integer-little>> end || <<X:8/integer, Y:8/integer>> <= Hex >>.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

serialize_deserialize_test() ->
    Hash = <<"123abc">>,
    ?assertEqual(Hash, deserialize_hash(serialize_hash(Hash))).

-endif.

%% Test helper
create_block(ConsensusMembers, Txs) ->
    Blockchain = blockchain_worker:blockchain(),
    PrevHash = blockchain:head_hash(Blockchain),
    Height = blockchain_block:height(blockchain:head_block(Blockchain)) + 1,
    Block0 = blockchain_block:new(PrevHash, Height, Txs, <<>>, #{}),
    BinBlock = erlang:term_to_binary(blockchain_block:remove_signature(Block0)),
    Signatures = signatures(ConsensusMembers, BinBlock),
    Block1 = blockchain_block:sign_block(erlang:term_to_binary(Signatures), Block0),
    Block1.

signatures(ConsensusMembers, BinBlock) ->
    lists:foldl(
        fun({A, _P, F}, Acc) ->
            Sig = F(BinBlock),
            [{A, Sig}|Acc]
        end,
        [],
        ConsensusMembers
    ).
