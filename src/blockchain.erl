%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain).

-export([
    new/2,
    genesis_hash/1 ,genesis_block/1,
    head_hash/1, head_block/1,
    ledger/1,
    dir/1,
    blocks/1,
    add_block/2,
    get_block/2,
    save/1, load/1, load/2,
    build/3,
    base_dir/1,
    load_genesis/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(blockchain, {
    genesis :: {blockchain_block:hash(), blockchain_block:block()},
    dir :: file:filename_all(),
    db :: rockdb:db_handle(),
    default,
    blocks,
    ledger,
    heights
}).

% TODO: Save ledger to rocksdb

-type blocks() :: #{blockchain_block:hash() => blockchain_block:block()}.
-type blockchain() :: #blockchain{}.
-export_type([blockchain/0, blocks/0]).

%%--------------------------------------------------------------------
%% @doc
%% This function must only be called once, specifically in
%% blockchain_worker:integrate_genesis_block
%% @end
%%--------------------------------------------------------------------
-spec new(blockchain_block:block(), file:filename_all()) -> blockchain().
new(GenesisBlock, Dir) ->
    Hash = blockchain_block:hash_block(GenesisBlock),
    Transactions = blockchain_block:transactions(GenesisBlock),
    {ok, Ledger} = blockchain_transactions:absorb(Transactions, blockchain_ledger_v1:new()),
    BaseDir = base_dir(Dir),
    DBOptions = [{create_if_missing, true}],
    {ok, DB, [Default,Blocks, Heights]} = rocksdb:open_with_cf(BaseDir, [{create_if_missing, true}], [{"default", DBOptions}, {"blocks", DBOptions}, {heights, DBOptions}]),
    #blockchain{
        genesis={Hash, GenesisBlock},
        dir=BaseDir,
        db=DB,
        default=Default,
        blocks=Blocks,
        heights=Heights
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_hash(blockchain()) -> blockchain_block:hash().
genesis_hash(#blockchain{genesis={Hash, _}}) ->
    Hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_block(blockchain()) -> blockchain_block:block().
genesis_block(#blockchain{genesis={_, Block}}) ->
    Block.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_hash(blockchain()) -> blockchain_block:hash().
head_hash(Blockchain) ->
    {ok, Hash} = rocksdb:get(Blockchain#blockchain.db, Blockchain#blockchain.default, <<"head">>),
    Hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_block(blockchain()) -> blockchain_block:block().
head_block(Blockchain) ->
    get_block(head, Blockchain).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec ledger(blockchain()) -> blockchain_ledger_v1:ledger().
ledger(Blockchain) ->
    V = blockchain_util:serial_version(Blockchain#blockchain.dir),
    {ok, Ledger} = rocksdb:get(Blockchain#blockchain.db, Blockchain#blockchain.default, <<"ledger">>),
    blockchain_ledger_v1:deserialize(V, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec dir(blockchain()) -> file:filename_all().
dir(Blockchain) ->
    Blockchain#blockchain.dir.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec blocks(blockchain()) -> #{blockchain_block:hash() => blockchain_block:block()}.
blocks(Blockchain) ->
    BaseDir = ?MODULE:dir(Blockchain),
    Dir = blockchain_block:dir(BaseDir),
    V = blockchain_util:serial_version(Dir),
    rocksdb:fold(Blockchain#blockchain.db, Blockchain#blockchain.blocks,
        fun({Hash, Binary}, Acc) ->
                Block = blockchain_block:deserialize(V, Binary),
                maps:put(Hash, Block, Acc)
        end,
        #{},
        []
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_block(blockchain_block:block(), blockchain()) -> blockchain().
add_block(Block, Blockchain) ->
    Hash = blockchain_block:hash_block(Block),
    Ledger0 = ?MODULE:ledger(Blockchain),
    case blockchain_transactions:absorb(blockchain_block:transactions(Block), Ledger0) of
        {ok, Ledger1} ->
            ok = save_block(Blockchain, Block),
            ok = save_ledger(Blockchain, Ledger1),
            ok = save_head(Blockchain, Block),
            Blockchain;
        {error, Reason} ->
            lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [Hash, Reason]),
            Blockchain
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_block(blockchain_block:hash() | head | genesis, blockchain()) ->
    {ok, blockchain_block:block()} | {error, any()}.
get_block(Hash, Blockchain) when is_record(Blockchain, blockchain) ->
    {ok, Block} = rocksdb:get(Blockchain#blockchain.db, Blockchain#blockchain.blocks, Hash),
    V = blockchain_util:serial_version(Blockchain#blockchain.dir),
    blockchain_block:deserialize(V, Block);
get_block(head, Blockchain) ->
    {ok, Hash} = rocksdb:get(Blockchain#blockchain.db, Blockchain#blockchain.default, <<"head">>),
    get_block(Hash, Blockchain);
get_block(genesis, Blockchain) ->
    {GenHash, GenBlock} = Blockchain#blockchain.genesis,
    GenBlock.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save(blockchain()) -> ok.
save(Blockchain) ->
    Dir = ?MODULE:dir(Blockchain),
    ok = save_genesis(Blockchain, blockchain:genesis_block(Blockchain)),
    ok = save_head(blockchain:head_block(Blockchain), Dir),
    ok = blockchain_ledger_v1:save(?MODULE:ledger(Blockchain), Dir),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all()) -> blockchain() | undefined.
load(Dir) ->
    BaseDir = base_dir(Dir),
    DBOptions = [{create_if_missing, false}],
    {ok, DB, [Default,Blocks, Heights, Ledger]} = rocksdb:open_with_cf(BaseDir, [{create_if_missing, false}], [{"default", DBOptions}, {"blocks", DBOptions}, {heights, DBOptions}, {"ledger", DBOptions}]),
    #blockchain{
        genesis={Hash, GenesisBlock},
        dir=BaseDir,
        db=DB,
        default=Default,
        blocks=Blocks,
        heights=Heights,
        ledger=Ledger
    }.

%%--------------------------------------------------------------------
%% @doc
%% Compare genesis block given before loading
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all(), file:filename_all() | undefined) -> blockchain() | undefined | {update, file:filename_all()}.
load(BaseDir, undefined) ->
    load(BaseDir);
load(BaseDir, GenDir) ->
    case load_genesis(GenDir) of
        {error, _} ->
            load(BaseDir);
        {ok, _}=LoadRes ->
            case LoadRes =:= load_genesis(base_dir(BaseDir)) of
                true ->
                    load(BaseDir);
                false ->
                    ok = clean(BaseDir),
                    {update, GenDir}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build(blockchain_block:block(), file:filename_all(), non_neg_integer()) -> [blockchain_block:block()].
build(StartingBlock, BaseDir, Limit) ->
    build(StartingBlock, BaseDir, Limit, []).

-spec build(blockchain_block:block(), file:filename_all(), non_neg_integer(), [blockchain_block:block()]) -> [blockhain_block:block()].
build(_StartingBlock, _BaseDir, 0, Acc) ->
    lists:reverse(Acc);
build(StartingBlock, BaseDir, N, Acc) ->
    case blockchain_block:find_next(StartingBlock, BaseDir) of
        {ok, NextBlock} ->
            build(NextBlock, BaseDir, N-1, [NextBlock|Acc]);
        {error, _Reason} ->
            lists:reverse(Acc)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec base_dir(file:filename_all()) -> file:filename_all().
base_dir(BaseDir) ->
    filename:join(BaseDir, ?BASE_DIR).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_genesis(file:filename_all()) -> {ok, blockchain_block:block()} | {error, any()}.
load_genesis(Dir) ->
    File = filename:join(Dir, ?GEN_HASH_FILE),
    case file:read_file(File) of
        {error, _Reason}=Error ->
            Error;
        {ok, Binary} ->
            {ok, blockchain_block:deserialize(blockchain_util:serial_version(Dir), Binary)}
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_block(blockchain_block:block(), file:filename_all()) -> ok.
save_block(Blockchain, Block) ->
    V = blockchain_util:serial_version(Blockchain#blockchain.dir),
    Height = blockchain_block:height(Block),
    ok = rocksdb:put(Blockchain#blockchain.db, Blockchain#blockchain.blocks, blockchain:hash_block(Block), blockchain_block:serialize(V, Block)),
    %% lexiographic ordering works better with big endian
    ok = rocksdb:put(Blockchain#blockchain.db, Blockchain#blockchain.heights, <<Height:64/integer-unsigned-big>>, blockchain_block:serialize(V, Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_head(blockchain_block:block(), file:filename_all()) -> ok.
save_head(Blockchain, Block) ->
    rocksdb:put(Blockchain#blockchain.db, Blockchain#blockchain.default, <<"head">>, blockchain_block:hash_block(Block)).

-spec save_genesis(blockchain_block:block(), file:filename_all()) -> ok.
save_genesis(Blockchain, Block) ->
    rocksdb:put(Blockchain#blockchain.db, Blockchain#blockchain.default, <<"genesis">>, blockchain_block:hash_block(Block)),
    save_block(Blockchain, Block).

-spec save_ledger(blockchain_block:block(), file:filename_all()) -> ok.
save_ledger(Blockchain, Ledger) ->
    V = blockchain_util:serial_version(Blockchain#blockchain.dir),
    rocksdb:put(Blockchain#blockchain.db, Blockchain#blockchain.default, <<"ledger">>, blockchain_ledger_v1:serialize(V, Ledger)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec clean(file:filename_all()) ->  ok | {error, any()}.
clean(Dir) ->
    Paths = filelib:wildcard(Dir ++ "/**"),
    {Dirs, Files} = lists:partition(fun filelib:is_dir/1, Paths),
    ok = lists:foreach(fun file:delete/1, Files),
    Sorted = lists:reverse(lists:sort(Dirs)),
    ok = lists:foreach(fun file:del_dir/1, Sorted).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    BaseDir = test_utils:tmp_dir("new_test"),
    Block = blockchain_block:new_genesis_block([]),
    Hash = blockchain_block:hash_block(Block),
    Chain = new(Block, BaseDir),
    ?assertEqual({Hash, Block}, Chain#blockchain.genesis),
    ?assertEqual({Hash, Block}, Chain#blockchain.head),
    ?assertEqual(blockchain_ledger_v1:increment_height(blockchain_ledger_v1:new()), ledger(Chain)),
    ?assertEqual(BaseDir ++ "/blockchain", Chain#blockchain.dir).

genesis_hash_test() ->
    Block = blockchain_block:new_genesis_block([]),
    Hash = blockchain_block:hash_block(Block),
    Chain = new(Block, test_utils:tmp_dir("genesis_hash_test")),
    ?assertEqual(Hash, genesis_hash(Chain)).

genesis_block_test() ->
    Block = blockchain_block:new_genesis_block([]),
    Chain = new(Block, test_utils:tmp_dir("genesis_block_test")),
    ?assertEqual(Block, genesis_block(Chain)).

head_hash_test() ->
    Block = blockchain_block:new_genesis_block([]),
    Hash = blockchain_block:hash_block(Block),
    Chain = new(Block, test_utils:tmp_dir("head_hash_test")),
    ?assertEqual(Hash, head_hash(Chain)).

head_block_test() ->
    Block = blockchain_block:new_genesis_block([]),
    Chain = new(Block, test_utils:tmp_dir("head_block_test")),
    ?assertEqual(Block, head_block(Chain)).

ledger_test() ->
    Block = blockchain_block:new_genesis_block([]),
    Chain = new(Block, test_utils:tmp_dir("ledger_test")),
    ?assertEqual(blockchain_ledger_v1:increment_height(blockchain_ledger_v1:new()), ledger(Chain)).

dir_test() ->
    BaseDir = test_utils:tmp_dir("dir_test"),
    Block = blockchain_block:new_genesis_block([]),
    Chain = new(Block, BaseDir),
    ?assertEqual(BaseDir ++ "/blockchain", dir(Chain)).

blocks_test() ->
    GenBlock = blockchain_block:new_genesis_block([]),
    GenHash = blockchain_block:hash_block(GenBlock),
    Chain = new(GenBlock, test_utils:tmp_dir("blocks_test")),
    Block = blockchain_block:new(GenHash, 2, [], <<>>, #{}),
    Hash = blockchain_block:hash_block(Block),
    Chain2 = add_block(Block, Chain),
    Map = #{Hash => Block},
    ?assertMatch(Map, blocks(Chain2)).

blocks_size_test() ->
    Block = blockchain_block:new_genesis_block([]),
    Chain = new(Block, test_utils:tmp_dir("blocks_size_test")),
    ?assertEqual(1, blocks_size(Chain)).

get_block_test() ->
    GenBlock = blockchain_block:new_genesis_block([]),
    GenHash = blockchain_block:hash_block(GenBlock),
    Chain = new(GenBlock, test_utils:tmp_dir("get_block_test")),
    Block = blockchain_block:new(GenHash, 2, [], <<>>, #{}),
    Hash = blockchain_block:hash_block(Block),
    Chain2 = add_block(Block, Chain),
    ?assertMatch({ok, Block}, get_block(Hash, Chain2)).

save_load_test() ->
    BaseDir = test_utils:tmp_dir("save_load_test"),
    GenBlock = blockchain_block:new_genesis_block([]),
    Chain = new(GenBlock, BaseDir),
    ?assertEqual(ok, save(Chain)),
    ?assertEqual(Chain, load(BaseDir)).


-endif.
