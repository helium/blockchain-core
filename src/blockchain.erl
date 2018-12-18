%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain).

-export([
    new/2, integrate_genesis/2,
    genesis_hash/1 ,genesis_block/1,
    head_hash/1, head_block/1,
    height/1,
    ledger/1,
    dir/1,
    blocks/1, add_block/2, get_block/2, add_blocks/2,
    build/3,
    close/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(blockchain, {
    dir :: file:filename_all(),
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    blocks :: rocksdb:cf_handle(),
    heights :: rocksdb:cf_handle(),
    ledger :: blockchain_ledger_v1:ledger()
}).

-define(GEN_HASH_FILE, "genesis").
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
-spec new(file:filename_all(), file:filename_all()
                               | blockchain_block:block()
                               | undefined) -> {ok, blockchain()} | {no_genesis, blockchain()}.
new(Dir, Genesis) when is_list(Genesis) ->
    case load_genesis(Genesis) of
        {error, _} ->
            ?MODULE:new(Dir, undefined);
        {ok, Block} ->
            ?MODULE:new(Dir, Block)
    end;
new(Dir, undefined) ->
    lager:info("loading blockchain from ~p", [Dir]),
    case load(Dir) of
        {Blockchain, {error, _}} ->
            lager:info("no genesis block found"),
            {no_genesis, Blockchain};
        {Blockchain, {ok, _GenBlock}} ->
            {ok, Blockchain}
    end;
new(Dir, GenBlock) ->
    lager:info("loading blockchain from ~p and checking ~p", [Dir, GenBlock]),
    case load(Dir) of
        {Blockchain, {error, _}} ->
            lager:warning("failed to load genesis, integrating new one"),
            ok = ?MODULE:integrate_genesis(GenBlock, Blockchain),
            {ok, Blockchain};
        {Blockchain, {ok, GenBlock}} ->
            lager:info("new gen = old gen"),
            {ok, Blockchain};
        {Blockchain, {ok, _OldGenBlock}} ->
            lager:info("replacing old genesis block"),
            ok = clean(Blockchain),
            new(Dir, GenBlock)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
integrate_genesis(GenesisBlock, #blockchain{db=DB, default=DefaultCF}=Blockchain) ->
    GenHash = blockchain_block:hash_block(GenesisBlock),

    Ledger = ?MODULE:ledger(Blockchain),
    ok = blockchain_transactions:absorb(GenesisBlock, Ledger),

    ok = save_block(GenesisBlock, Blockchain),

    GenBin = blockchain_block:serialize(GenesisBlock),
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, DefaultCF, GenHash, GenBin),
    ok = rocksdb:batch_put(Batch, DefaultCF, ?GENESIS, GenHash),
    ok = rocksdb:write_batch(DB, Batch, []),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_hash(blockchain()) -> {ok, blockchain_block:hash()} | {error, any()}.
genesis_hash(#blockchain{db=DB, default=DefaultCF}) ->
   case rocksdb:get(DB, DefaultCF, ?GENESIS, []) of
       {ok, Hash} ->
            {ok, Hash};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_block(blockchain()) -> {ok, blockchain_block:block()} | {error, any()}.
genesis_block(Blockchain) ->
    case ?MODULE:genesis_hash(Blockchain) of
        {error, _}=Error ->
            Error;
        {ok, Hash} ->
            get_block(Hash, Blockchain)
    end.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_hash(blockchain()) -> {ok, blockchain_block:hash()} | {error, any()}.
head_hash(#blockchain{db=DB, default=DefaultCF}) ->
    case rocksdb:get(DB, DefaultCF, ?HEAD, []) of
       {ok, Hash} ->
            {ok, Hash};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head_block(blockchain()) -> {ok, blockchain_block:block()} | {error, any()}.
head_block(Blockchain) ->
    case ?MODULE:head_hash(Blockchain) of
        {error, _}=Error ->
            Error;
        {ok, Hash} ->
            get_block(Hash, Blockchain)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec height(blockchain()) -> {ok, non_neg_integer()} | {error, any()}.
height(Blockchain) ->
    case ?MODULE:head_hash(Blockchain) of
        {error, _}=Error ->
            Error;
        {ok, Hash} ->
            case get_block(Hash, Blockchain) of
                {error, _Reason}=Error -> Error;
                {ok, Block} ->
                    {ok, blockchain_block:height(Block)}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec ledger(blockchain()) -> blockchain_ledger_v1:ledger().
ledger(#blockchain{ledger=Ledger}) ->
    Ledger.

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
-spec add_block(blockchain_block:block(), blockchain()) -> ok | {error, any()} | {error, disjoint_chain}.
add_block(Block, Blockchain) ->
    Hash = blockchain_block:hash_block(Block),
    case blockchain:head_block(Blockchain) of
        {error, Reason}=Error ->
            lager:error("could not get head hash ~p", [Reason]),
            Error;
        {ok, HeadBlock} ->
            HeadHash = blockchain_block:hash_block(HeadBlock),
            case blockchain_block:prev_hash(Block) =:= HeadHash andalso
                 blockchain_block:height(Block) == blockchain_block:height(HeadBlock) + 1
            of
                false when HeadHash =:= Hash ->
                    lager:info("Already have this block"),
                    ok;
                false ->
                    lager:warning("gossipped block doesn't fit with our chain,
                                  block_height: ~p, head_block_height: ~p", [blockchain_block:height(Block),
                                                                             blockchain_block:height(HeadBlock)]),
                    {error, disjoint_chain};
                true ->
                    lager:info("prev hash matches the gossiped block"),
                    Ledger = blockchain:ledger(Blockchain),
                    case blockchain_ledger_v1:consensus_members(Ledger) of
                        {error, _Reason}=Error ->
                            lager:error("could not get consensus_members ~p", [_Reason]),
                            Error;
                        {ok, ConsensusAddrs} ->
                            N = length(ConsensusAddrs),
                            F = (N-1) div 3,
                            case blockchain_block:verify_signature(Block,
                                                                   ConsensusAddrs,
                                                                   blockchain_block:signature(Block),
                                                                   N-F)
                            of
                                false ->
                                    {error, failed_verify_signature};
                                {true, _} ->
                                    case blockchain_transactions:absorb(Block, Ledger) of
                                        ok ->
                                            save_block(Block, Blockchain),
                                            ok = blockchain_worker:notify({add_block, HeadHash, false});
                                        {error, Reason}=Error ->
                                            lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [blockchain_block:hash_block(Block), Reason]),
                                            Error
                                    end
                            end
                    end
            end
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
%% @end
%%--------------------------------------------------------------------
-spec add_blocks([blockchain_block:block()], blockchain()) -> ok | {error, any()}.
add_blocks([], _Chain) -> ok;
add_blocks([Head | Tail], Chain) ->
    case ?MODULE:add_block(Head, Chain) of
        ok -> add_blocks(Tail, Chain);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec build(blockchain_block:block(), blockchain(), non_neg_integer()) -> [blockchain_block:block()].
build(StartingBlock, Blockchain, Limit) ->
    build(StartingBlock, Blockchain, Limit, []).

-spec build(blockchain_block:block(), blockchain(), non_neg_integer(), [blockchain_block:block()]) -> [blockchain_block:block()].
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
close(#blockchain{db=DB, ledger=Ledger}) ->
    ok = blockchain_ledger_v1:close(Ledger),
    rocksdb:close(DB).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
clean(#blockchain{dir=Dir, db=DB}=Blockchain) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = rocksdb:close(DB),
    ok = rocksdb:destroy(DBDir, []),
    ok = blockchain_ledger_v1:clean(?MODULE:ledger(Blockchain)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all()) -> {blockchain(), {ok, blockchain_block:block()} | {error, any()}}.
load(Dir) ->
    {ok, DB, [DefaultCF, BlocksCF, HeightsCF]} = open_db(Dir),
    Ledger = blockchain_ledger_v1:new(Dir),
    Blockchain = #blockchain{
        dir=Dir,
        db=DB,
        default=DefaultCF,
        blocks=BlocksCF,
        heights=HeightsCF,
        ledger=Ledger
    },
    {Blockchain, ?MODULE:genesis_block(Blockchain)}.

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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
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
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, BlocksCF, Hash, blockchain_block:serialize(Block)),
    ok = rocksdb:batch_put(Batch, DefaultCF, ?HEAD, Hash),
    %% lexiographic ordering works better with big endian
    ok = rocksdb:batch_put(Batch, HeightsCF, <<Height:64/integer-unsigned-big>>, Hash),
    ok = rocksdb:write_batch(DB, Batch, []).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    BaseDir = test_utils:tmp_dir("new_test"),
    Block = blockchain_block:new_genesis_block([]),
    Hash = blockchain_block:hash_block(Block),
    {ok, Chain} = new(BaseDir, Block),
    ?assertEqual({ok, Hash}, genesis_hash(Chain)),
    ?assertEqual({ok, Block}, genesis_block(Chain)),
    ?assertEqual({ok, Hash}, head_hash(Chain)),
    ?assertEqual({ok, Block}, head_block(Chain)),
    ?assertEqual({ok, Block}, get_block(Hash, Chain)),
    ?assertEqual({ok, Block}, get_block(1, Chain)).

% ledger_test() ->
%     Block = blockchain_block:new_genesis_block([]),
%     Chain = new(Block, test_utils:tmp_dir("ledger_test")),
%     ?assertEqual(blockchain_ledger_v1:increment_height(blockchain_ledger_v1:new()), ledger(Chain)).

%% XXX: fix these
blocks_test() ->
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, consensus_members, fun(_) ->
        {ok, lists:seq(1, 7)}
    end),
    meck:new(blockchain_block, [passthrough]),
    meck:expect(blockchain_block, verify_signature, fun(_, _, _, _) ->
        {true, undefined}
    end),
    meck:new(blockchain_worker, [passthrough]),
    meck:expect(blockchain_worker, notify, fun(_) ->
        ok
    end),

    GenBlock = blockchain_block:new_genesis_block([]),
    GenHash = blockchain_block:hash_block(GenBlock),
    {ok, Chain} = new(test_utils:tmp_dir("blocks_test"), GenBlock),
    Block = blockchain_block:new(GenHash, 2, [], <<>>, #{}),
    Hash = blockchain_block:hash_block(Block),
    ok = add_block(Block, Chain),
    Map = #{
        GenHash => GenBlock,
        Hash => Block
    },
    ?assertMatch(Map, blocks(Chain)),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain_block)),
    meck:unload(blockchain_block),
    ?assert(meck:validate(blockchain_worker)),
    meck:unload(blockchain_worker).

get_block_test() ->
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, consensus_members, fun(_) ->
        {ok, lists:seq(1, 7)}
    end),
    meck:new(blockchain_block, [passthrough]),
    meck:expect(blockchain_block, verify_signature, fun(_, _, _, _) ->
        {true, undefined}
    end),
    meck:new(blockchain_worker, [passthrough]),
    meck:expect(blockchain_worker, notify, fun(_) ->
        ok
    end),

    GenBlock = blockchain_block:new_genesis_block([]),
    GenHash = blockchain_block:hash_block(GenBlock),
    {ok, Chain} = new(test_utils:tmp_dir("get_block_test"), GenBlock),
    Block = blockchain_block:new(GenHash, 2, [], <<>>, #{}),
    Hash = blockchain_block:hash_block(Block),
    ok = add_block(Block, Chain),
    ?assertMatch({ok, Block}, get_block(Hash, Chain)),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain_block)),
    meck:unload(blockchain_block),
    ?assert(meck:validate(blockchain_worker)),
    meck:unload(blockchain_worker).


-endif.
