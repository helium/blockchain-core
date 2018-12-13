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
    blocks/1, add_block/2, get_block/2,
    load/2, build/3
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(blockchain, {
    dir :: file:filename_all(),
    db :: rockdb:db_handle(),
    default :: rockdb:cf_handle(),
    blocks :: rockdb:cf_handle(),
    ledger :: rockdb:cf_handle(),
    heights :: rockdb:cf_handle()
}).

-define(DB_FILE, "blockchain.db").
-define(HEAD, <<"head">>).
-define(GENESIS, <<"genesis">>).

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
    GenHash = blockchain_block:hash_block(GenesisBlock),

    % Transactions = blockchain_block:transactions(GenesisBlock),
    % {ok, Ledger} = blockchain_transactions:absorb(Transactions, blockchain_ledger_v1:new()),

    {ok, DB, [DefaultCF, BlocksCF, HeightsCF]} = open_db(Dir),

    Blockchain = #blockchain{
        dir=base_dir(Dir),
        db=DB,
        default=DefaultCF,
        blocks=BlocksCF,
        heights=HeightsCF
    },
    ok = save_block(GenesisBlock, Blockchain),

    GenBin = blockchain_block:serialize(GenesisBlock),
    % TODO: Make this a batch
    ok = rocksdb:put(DB, DefaultCF, GenHash, GenBin, []),
    ok = rocksdb:put(DB, DefaultCF, ?GENESIS, GenHash, []),
    Blockchain.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_hash(blockchain()) -> blockchain_block:hash().
genesis_hash(#blockchain{db=DB, default=DefaultCF}) ->
   {ok, Hash} = rocksdb:get(DB, DefaultCF, ?GENESIS, []),
    Hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_block(blockchain()) -> blockchain_block:block().
genesis_block(Blockchain) ->
    Hash = ?MODULE:genesis_hash(Blockchain),
    {ok, Block} = get_block(Hash, Blockchain),
    Block.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_hash(blockchain()) -> blockchain_block:hash().
head_hash(#blockchain{db=DB, default=DefaultCF}) ->
    {ok, Hash} = rocksdb:get(DB, DefaultCF, ?HEAD, []),
    Hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_block(blockchain()) -> blockchain_block:block().
head_block(Blockchain) ->
    Hash = ?MODULE:head_hash(Blockchain),
    {ok, Block} = get_block(Hash, Blockchain),
    Block.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec ledger(blockchain()) -> blockchain_ledger_v1:ledger().
ledger(_Blockchain) ->
    % TODO: Fix
    blockchain_ledger_v1:new().

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
blocks(#blockchain{db=DB, blocks=BlocksCF}) ->
    rocksdb:fold(
        DB,
        BlocksCF,
        fun({Hash, Binary}, Acc) ->
                Block = blockchain_block:deserialize(Binary),
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
            ok = save_block(Block, Blockchain),
            ok = save_ledger(Ledger1, Blockchain),
            Blockchain;
        {error, Reason} ->
            lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [Hash, Reason]),
            Blockchain
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_block(blockchain_block:hash() | integer(), blockchain()) -> {ok, blockchain_block:block()} | {error, any()}.
get_block(Hash, #blockchain{db=DB, blocks=BlocksCF}) when is_binary(Hash) ->
    case rocksdb:get(DB, BlocksCF, Hash, []) of
        {ok, BinBlock} ->
            {ok, blockchain_block:deserialize(BinBlock)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end;
get_block(Height, #blockchain{db=DB, heights=HeightsCF}=Blockchain) ->
    case rocksdb:get(DB, HeightsCF, <<Height:64/integer-unsigned-big>>, []) of
        {ok, Hash} ->
           ?MODULE:get_block(Hash, Blockchain);
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Compare genesis block given before loading
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all(), file:filename_all() | undefined) -> blockchain() | {update, blockchain_block:block()}.
load(BaseDir, undefined) ->
    load(BaseDir);
load(BaseDir, GenDir) ->
    case load_genesis(GenDir) of
        {error, _} ->
            load(BaseDir);
        {ok, GenBlock} ->
            Blockchain = load(BaseDir),
            case ?MODULE:genesis_hash(Blockchain) =:= blockchain_block:hash_block(GenBlock) of
                false ->
                    _ = rocksdb:close(Blockchain#blockchain.db),
                    ok = clean(BaseDir),
                    {update, GenBlock};
                true ->
                    Blockchain
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build(blockchain_block:block(), blockchain(), non_neg_integer()) -> [blockchain_block:block()].
build(StartingBlock, Blockchain, Limit) ->
    build(StartingBlock, Blockchain, Limit, []).

-spec build(blockchain_block:block(), blockchain(), non_neg_integer(), [blockchain_block:block()]) -> [blockhain_block:block()].
build(_StartingBlock, _Blockchain, 0, Acc) ->
    lists:reverse(Acc);
build(StartingBlock, Blockchain, N, Acc) ->
    Height = blockchain_block:height(StartingBlock) + 1,
    case ?MODULE:get_block(Height, Blockchain) of
        {ok, NextBlock} ->
            build(NextBlock, Blockchain, N-1, [NextBlock|Acc]);
        {error, _Reason} ->
            lists:reverse(Acc)
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all()) -> blockchain().
load(BaseDir) ->
    {ok, DB, [DefaultCF, BlocksCF, HeightsCF]} = open_db(BaseDir),
    #blockchain{
        dir=base_dir(BaseDir),
        db=DB,
        default=DefaultCF,
        blocks=BlocksCF,
        heights=HeightsCF
    }.

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
            {ok, blockchain_block:deserialize(Binary)}
    end.

-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir) ->
    BaseDir = base_dir(Dir),
    DBDir = filename:join(BaseDir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    DBOptions = [{create_if_missing, true}],
    DefaultCFs = ["default", "blocks", "heights"],

    ExistingCFs = 
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,


    {ok, DB, OpenedCFs} = rocksdb:open_with_cf(DBDir, DBOptions,  [{CF, []} || CF <- ExistingCFs]),

    L1 = lists:zip(ExistingCFs, OpenedCFs),
    L2 = lists:map(
        fun(CF) ->
            {ok, CF1} = rocksdb:create_column_family(DB, CF, []),
            {CF, CF1}
        end,
        DefaultCFs -- ExistingCFs
    ),
    L3 = L1 ++ L2,
    {ok, DB, [proplists:get_value(X, L3) || X <- DefaultCFs]}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_block(blockchain_block:block(), blockchain()) -> ok.
save_block(Block, #blockchain{db=DB, default=DefaultCF, blocks=BlocksCF, heights=HeightsCF}) ->
    Height = blockchain_block:height(Block),
    Hash = blockchain_block:hash_block(Block),
    % TODO: Bath this
    ok = rocksdb:put(DB, BlocksCF, Hash, blockchain_block:serialize(Block), []),
    ok = rocksdb:put(DB, DefaultCF, ?HEAD, Hash, []),
    %% lexiographic ordering works better with big endian
    ok = rocksdb:put(DB, HeightsCF, <<Height:64/integer-unsigned-big>>, Hash, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_ledger(blockchain_ledger_v1:ledger(), blockchain()) -> ok.
save_ledger(_Ledger, _Blockchain) ->
    ok.

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
    ?assertEqual(Hash, genesis_hash(Chain)),
    ?assertEqual(Block, genesis_block(Chain)),
    ?assertEqual(Hash, head_hash(Chain)),
    ?assertEqual(Block, head_block(Chain)),
    ?assertEqual({ok, Block}, get_block(Hash, Chain)),
    ?assertEqual({ok, Block}, get_block(1, Chain)).

% ledger_test() ->
%     Block = blockchain_block:new_genesis_block([]),
%     Chain = new(Block, test_utils:tmp_dir("ledger_test")),
%     ?assertEqual(blockchain_ledger_v1:increment_height(blockchain_ledger_v1:new()), ledger(Chain)).

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
    Map = #{
        GenHash => GenBlock,
        Hash => Block
    },
    ?assertMatch(Map, blocks(Chain2)).

get_block_test() ->
    GenBlock = blockchain_block:new_genesis_block([]),
    GenHash = blockchain_block:hash_block(GenBlock),
    Chain = new(GenBlock, test_utils:tmp_dir("get_block_test")),
    Block = blockchain_block:new(GenHash, 2, [], <<>>, #{}),
    Hash = blockchain_block:hash_block(Block),
    Chain2 = add_block(Block, Chain),
    ?assertMatch({ok, Block}, get_block(Hash, Chain2)).

% load_test() ->
%     BaseDir = test_utils:tmp_dir("save_load_test"),
%     GenBlock = blockchain_block:new_genesis_block([]),
%     Chain = new(GenBlock, BaseDir),
%     ?assertEqual(Chain, load(BaseDir, undefined)).


-endif.
