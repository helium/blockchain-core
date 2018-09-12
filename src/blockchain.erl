%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain).


-export([
    new/3
    ,genesis_hash/1
    ,blocks/1
    ,blocks_size/1
    ,ledger/1
    ,head/1
    ,current_block/1
    ,add_block/3
    ,get_block/2
    ,trim_blocks/2
    ,save/2, load/1

]).

-include("blockchain.hrl").

-record(blockchain, {
    genesis_hash :: blockchain_block:hash()
    ,blocks = #{} :: blocks()
    ,ledger = #{} :: blockchain_ledger:ledger()
    ,head :: blockchain_block:hash()
}).

-type blocks() :: #{blockchain_block:hash() => blockchain_block:block()}.
-type blockchain() :: #blockchain{}.
-export_type([blockchain/0, blocks/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(blockchain_block:hash(), blockchain_block:block(), blockchain_ledger:ledger()) -> blockchain().
new(GenesisHash, GenesisBlock, Ledger) ->
    #blockchain{
        genesis_hash=GenesisHash
        ,blocks=#{GenesisHash => GenesisBlock}
        ,ledger=Ledger
        ,head=GenesisHash
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec genesis_hash(blockchain()) -> blockchain_block:hash().
genesis_hash(Blockchain) ->
    Blockchain#blockchain.genesis_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec blocks(blockchain()) -> #{blockchain_block:hash() => blockchain_block:block()}.
blocks(Blockchain) ->
    Blockchain#blockchain.blocks.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec blocks_size(blockchain()) -> integer().
blocks_size(Blockchain) ->
    Blocks = ?MODULE:blocks(Blockchain),
    maps:size(Blocks).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec ledger(blockchain()) -> blockchain_ledger:ledger().
ledger(Blockchain) ->
    Blockchain#blockchain.ledger.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec head(blockchain()) -> blockchain_block:hash().
head(Blockchain) ->
    Blockchain#blockchain.head.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec current_block(blockchain()) -> blockchain_block:block().
current_block(Blockchain) ->
    Head = ?MODULE:head(Blockchain),
    ?MODULE:get_block(Blockchain, Head).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_block(blockchain(), blockchain_block:block(), string()) -> blockchain().
add_block(Blockchain, Block, BaseDir) ->
    Hash = blockchain_block:hash_block(Block),
    Blocks0 = ?MODULE:blocks(Blockchain),
    Blocks1 = maps:put(Hash, Block, Blocks0),
    Ledger0 = ?MODULE:ledger(Blockchain),
    {ok, Ledger1} = blockchain_transaction:absorb_transactions(blockchain_block:transactions(Block), Ledger0),
    Dir = base_dir(BaseDir),
    ok = blockchain_block:save(Hash, Block, Dir),
    ok = blockchain_ledger:save(Ledger1, Dir),
    ok = save_head(Hash, Dir),
    Blockchain#blockchain{blocks=Blocks1, ledger=Ledger1, head=Hash}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_block(blockchain(), blockchain_block:hash()) -> blockchain_block:block().
get_block(Blockchain, Hash) ->
    Blocks = ?MODULE:blocks(Blockchain),
    case ?MODULE:genesis_hash(Blockchain) of
        Hash ->
            maps:get(Hash, Blocks);
        GenesisHash ->
            GenesisBlock = ?MODULE:get_block(Blockchain, GenesisHash),
            maps:get(Hash, Blocks, GenesisBlock)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec trim_blocks(blockchain(), non_neg_integer()) -> blockchain().
trim_blocks(Blockchain, Limit) ->
    Head = blockchain:head(Blockchain),
    GenesisHash = ?MODULE:genesis_hash(Blockchain),
    {_, Hashs} = lists:foldl(
        fun(_, {Hash, Acc}) ->
            Block = ?MODULE:get_block(Blockchain, Hash),
            case blockchain_block:height(Block) of
                1 ->
                    {Hash, Acc};
                _ ->
                    PrevHash = blockchain_block:prev_hash(Block),
                    {PrevHash, [Hash|Acc]}
            end
        end
        ,{Head, [GenesisHash]}
        ,lists:seq(1, Limit-1)
    ),
    Blocks = maps:filter(
        fun(Hash, _Block) ->
            lists:member(Hash, Hashs)
        end
        ,?MODULE:blocks(Blockchain)
    ),
    Blockchain#blockchain{blocks=Blocks}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save(blockchain(), string()) -> ok.
save(Blockchain, BaseDir) ->
    Dir = base_dir(BaseDir),
    ok = save_blocks(blockchain:blocks(Blockchain), Dir),
    ok = save_genesis_hash(blockchain:genesis_hash(Blockchain), Dir),
    ok = save_head(blockchain:head(Blockchain), Dir),
    ok = blockchain_ledger:save(?MODULE:ledger(Blockchain), Dir),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(string()) -> blockchain().
load(BaseDir) ->
    Dir = base_dir(BaseDir),
    case
        {load_genesis_hash(Dir)
         ,load_blocks(Dir)
         ,blockchain_ledger:load(Dir)
         ,load_head(Dir)}
    of
        {undefined, _, _, _} -> undefined;
        {_, undefined, _, _} -> undefined;
        {_, _, undefined, _} -> undefined;
        {_, _, _, undefined} -> undefined;
        {GenesisHash, Blocks, Ledger, Head} ->
            #blockchain{
                genesis_hash=GenesisHash
                ,blocks=Blocks
                ,ledger=Ledger
                ,head=Head
            }
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec base_dir(string()) -> string().
base_dir(BaseDir) ->
    filename:join(BaseDir, ?BASE_DIR).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_blocks(blocks(), string()) -> ok.
save_blocks(Blocks, BaseDir) ->
    maps:fold(
        fun(Hash, Block, _Acc) ->
            ok = blockchain_block:save(Hash, Block, BaseDir),
            ok
        end
        ,ok
        ,Blocks
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_blocks(string()) -> blocks() | undefined.
load_blocks(BaseDir) ->
    Dir = filename:join(BaseDir, ?BLOCKS_DIR),
    case file:list_dir(Dir) of
        {error, _Reason} ->
            undefined;
        {ok, Filenames} ->
            lists:foldl(
                fun(File, Acc) ->
                    case file:read_file(filename:join(Dir, File)) of
                        {error, _Reason} ->
                            Acc;
                        {ok, Binary} ->
                            Hash = blockchain_util:deserialize_hash(File),
                            V = blockchain_util:serial_version(BaseDir),
                            Block = blockchain_block:deserialize(V, Binary),
                            maps:put(Hash, Block, Acc)
                    end
                end
                ,#{}
                ,Filenames
            )
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_genesis_hash(blockchain_block:hash(), string()) -> ok.
save_genesis_hash(Hash, BaseDir) ->
    File = filename:join(BaseDir, ?GEN_HASH_FILE),
    ok = blockchain_util:atomic_save(File, blockchain_util:serialize_hash(Hash)),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_genesis_hash(string()) -> blockchain_block:hash() | undefined.
load_genesis_hash(BaseDir) ->
    File = filename:join(BaseDir, ?GEN_HASH_FILE),
    case file:read_file(File) of
        {error, _Reason} ->
            undefined;
        {ok, Binary} ->
            blockchain_util:deserialize_hash(binary:bin_to_list(Binary))
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_head(blockchain_block:hash(), string()) -> ok.
save_head(Head, BaseDir) ->
    File = filename:join(BaseDir, ?HEAD_FILE),
    ok = blockchain_util:atomic_save(File, blockchain_util:serialize_hash(Head)),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_head(string()) -> blockchain_block:hash() | undefined.
load_head(BaseDir) ->
    File = filename:join(BaseDir, ?HEAD_FILE),
    case file:read_file(File) of
        {error, _Reason} ->
            undefined;
        {ok, Binary} ->
            blockchain_util:deserialize_hash(binary:bin_to_list(Binary))
    end.
