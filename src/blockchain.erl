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
    blocks_size/1,
    add_block/2,
    get_block/2,
    save/1, load/1,
    build/3,
    reindex/1,
    base_dir/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(blockchain, {
    genesis :: {blockchain_block:hash(), blockchain_block:block()},
    head :: {blockchain_block:hash(), blockchain_block:block()},
    ledger :: blockchain_ledger_v1:ledger(),
    dir :: file:filename_all()
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
    #blockchain{
        genesis={Hash, GenesisBlock},
        head={Hash, GenesisBlock},
        ledger=Ledger,
        dir=base_dir(Dir)
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
head_hash(#blockchain{head={Hash, _}}) ->
    Hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_block(blockchain()) -> blockchain_block:block().
head_block(#blockchain{head={_, Block}}) ->
    Block.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec ledger(blockchain()) -> blockchain_ledger_v1:ledger().
ledger(Blockchain) ->
    Blockchain#blockchain.ledger.

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
    lists:foldl(
        fun(File, Acc) ->
            case file:read_file(filename:join(Dir, File)) of
                {error, _Reason} ->
                    Acc;
                {ok, Binary} ->
                    Hash = blockchain_util:deserialize_hash(File),
                    V = blockchain_util:serial_version(Dir),
                    Block = blockchain_block:deserialize(V, Binary),
                    maps:put(Hash, Block, Acc)
            end
        end,
        #{},
        list_block_files(Blockchain)
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec blocks_size(blockchain()) -> integer().
blocks_size(Blockchain) ->
    erlang:length(list_block_files(Blockchain)) + 1.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_block(blockchain_block:block(), blockchain()) -> blockchain().
add_block(Block, Blockchain) ->
    Hash = blockchain_block:hash_block(Block),
    Ledger0 = ?MODULE:ledger(Blockchain),
    {ok, Ledger1} = blockchain_transactions:absorb(blockchain_block:transactions(Block), Ledger0),
    Dir = ?MODULE:dir(Blockchain),
    ok = blockchain_block:save(Hash, Block, Dir),
    ok = blockchain_ledger_v1:save(Ledger1, Dir),
    ok = save_head(Block, Dir),
    Blockchain#blockchain{head={Hash, Block}, ledger=Ledger1}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_block(blockchain_block:hash() | head | genesis, blockchain() | string()) ->
    {ok, blockchain_block:block()} | {error, any()}.
get_block(Hash, Blockchain) when is_record(Blockchain, blockchain) ->
    BaseDir = ?MODULE:dir(Blockchain),
    get_block(Hash, BaseDir);
get_block(head, BaseDir) ->
    load_head(BaseDir);
get_block(genesis, BaseDir) ->
    load_genesis(BaseDir);
get_block(Hash, BaseDir) ->
    blockchain_block:load(Hash, BaseDir).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save(blockchain()) -> ok.
save(Blockchain) ->
    Dir = ?MODULE:dir(Blockchain),
    ok = save_genesis(blockchain:genesis_block(Blockchain), Dir),
    ok = save_head(blockchain:head_block(Blockchain), Dir),
    ok = blockchain_ledger_v1:save(?MODULE:ledger(Blockchain), Dir),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all()) -> blockchain() | undefined.
load(BaseDir) ->
    Dir = base_dir(BaseDir),
    case
        {load_genesis(Dir),
         load_head(Dir),
         blockchain_ledger_v1:load(Dir)}
    of
        {{error, _}, _, _} -> undefined;
        {_, {error, _}, _} -> undefined;
        {_, _, {error, _}} -> undefined;
        {{ok, GenesisBlock}, {ok, HeadBlock}, {ok, Ledger}} ->
            GenesisHash = blockchain_block:hash_block(GenesisBlock),
            HeadHash = blockchain_block:hash_block(HeadBlock),
            #blockchain{
                genesis={GenesisHash, GenesisBlock},
                head={HeadHash, HeadBlock},
                ledger=Ledger,
                dir=Dir
            }
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
-spec reindex(file:filename_all()) -> ok.
reindex(BaseDir) ->
    V = blockchain_util:serial_version(BaseDir),
    BlockDir = blockchain_block:dir(BaseDir),
    lists:foreach(
        fun(File) ->
                case file:read_file(filename:join(BlockDir, File)) of
                {error, _Reason} ->
                    lager:error("failed to red file ~p: ~p", [File, _Reason]);
                {ok, Binary} ->
                    Block = blockchain_block:deserialize(V, Binary),
                    Hash = blockchain_block:hash_block(Block),
                    R = blockchain_block:save_link(Hash, Block, BaseDir),
                    Height = blockchain_block:height(Block),
                    lager:info("block index ~p restored: ~p", [Height, R])
            end
        end,
        list_block_files(BaseDir)
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec base_dir(file:filename_all()) -> file:filename_all().
base_dir(BaseDir) ->
    filename:join(BaseDir, ?BASE_DIR).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec list_block_files(blockchain() | file:filename_all()) -> [file:filename_all()].
list_block_files(Blockchain) when is_record(Blockchain, blockchain)  ->
    BaseDir = ?MODULE:dir(Blockchain),
    list_block_files(BaseDir);
list_block_files(BaseDir) ->
    Dir = blockchain_block:dir(BaseDir),
    case file:list_dir(Dir) of
        {error, _Reason} -> [];
        {ok, Filenames} -> Filenames
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_genesis(blockchain_block:block(), file:filename_all()) -> ok.
save_genesis(Block, Dir) ->
    File = filename:join(Dir, ?GEN_HASH_FILE),
    V = blockchain_util:serial_version(Dir),
    ok = blockchain_util:atomic_save(File, blockchain_block:serialize(V, Block)),
    ok.

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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_head(blockchain_block:block(), file:filename_all()) -> ok.
save_head(Block, Dir) ->
    File = filename:join(Dir, ?HEAD_FILE),
    V = blockchain_util:serial_version(Dir),
    ok = blockchain_util:atomic_save(File, blockchain_block:serialize(V, Block)),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_head(file:filename_all()) -> {ok, blockchain_block:block()} | {error, any()}.
load_head(Dir) ->
    File = filename:join(Dir, ?HEAD_FILE),
    case file:read_file(File) of
        {error, _Reason}=Error ->
            Error;
        {ok, Binary} ->
            {ok, blockchain_block:deserialize(blockchain_util:serial_version(Dir), Binary)}
    end.

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
