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
    ,ledger/1
    ,head/1
    ,current_block/1
    ,add_block/2
    ,get_block/2
    ,save/2, load/1
]).

-record(blockchain, {
    genesis_hash :: blockchain_block:hash()
    ,blocks = #{} :: #{blockchain_block:hash() => blockchain_block:block()}
    ,ledger = #{} :: blockchain_ledger:ledger()
    ,head :: blockchain_block:hash()
}).

-type blockchain() :: #blockchain{}.

-export_type([blockchain/0]).

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
-spec add_block(blockchain(), blockchain_block:block()) -> blockchain().
add_block(Blockchain, Block) ->
    Hash = blockchain_block:hash_block(Block),
    Blocks0 = ?MODULE:blocks(Blockchain),
    Blocks1 = maps:put(Hash, Block, Blocks0),
    Ledger0 = ?MODULE:ledger(Blockchain),
    {ok, Ledger1} = blockchain_transaction:absorb_transactions(blockchain_block:transactions(Block), Ledger0),
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
            maps:get(Hash, Blocks, ?MODULE:get_block(Blockchain, GenesisHash))
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save(blockchain(), string()) -> ok.
save(Blockchain, BaseDir) ->
    Dir = filename:join(BaseDir, "blockchain"),
    ok = save_blocks(Blockchain, Dir),
    ok = save_genesis_hash(Blockchain, Dir),
    ok = save_head(Blockchain, Dir),
    ok = blockchain_ledger:save(?MODULE:ledger(Blockchain), Dir),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(string()) -> blockchain().
load(BaseDir) ->
    Dir = filename:join(BaseDir, "blockchain"),
    #blockchain{
        genesis_hash=load_genesis_hash(Dir)
        ,blocks=load_blocks(Dir)
        ,ledger=blockchain_ledger:load(Dir)
        ,head=load_head(Dir)
    }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save_blocks(blockchain:blockchain(), string()) -> ok.
save_blocks(Blockchain, BaseDir) ->
    maps:fold(
        fun(Hash, Block, _Acc) ->
            ok = blockchain_block:save(Hash, Block, BaseDir),
            ok
        end
        ,ok
        ,blockchain:blocks(Blockchain)
    ),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_blocks(string()) -> #{blockchain_block:hash() => blockchain_block:block()}.
load_blocks(BaseDir) ->
    Dir = filename:join(BaseDir, "blocks"),
    case file:list_dir(Dir) of
        {error, _Reason} ->
            #{};
        {ok, Filenames} ->
            lager:warning("[~p:~p:~p] MARKER ~p~n", [?MODULE, ?FUNCTION_NAME, ?LINE, Filenames]),
            lists:foldl(
                fun(File, Acc) ->
                    case file:read_file(filename:join(Dir, File)) of
                        {error, _Reason} ->
                            Acc;
                        {ok, Binary} ->

                            Hash = blockchain_util:deserialize_hash(File),
                            Block = blockchain_block:deserialize(Binary),
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
-spec save_genesis_hash(blockchain:blockchain(), string()) -> ok.
save_genesis_hash(Blockchain, BaseDir) ->
    Hash = blockchain:genesis_hash(Blockchain),
    File = filename:join(BaseDir, "genesis_hash"),
    ok = blockchain_util:atomic_save(File, blockchain_util:serialize_hash(Hash)),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_genesis_hash(string()) -> blockchain_block:hash() | undefined.
load_genesis_hash(BaseDir) ->
    File = filename:join(BaseDir, "genesis_hash"),
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
-spec save_head(blockchain:blockchain(), string()) -> ok.
save_head(Blockchain, BaseDir) ->
    Head = blockchain:head(Blockchain),
    File = filename:join(BaseDir, "head"),
    ok = blockchain_util:atomic_save(File, blockchain_util:serialize_hash(Head)),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load_head(string()) -> blockchain_block:hash() | undefined.
load_head(BaseDir) ->
    File = filename:join(BaseDir, "head"),
    case file:read_file(File) of
        {error, _Reason} ->
            undefined;
        {ok, Binary} ->
            blockchain_util:deserialize_hash(binary:bin_to_list(Binary))
    end.
