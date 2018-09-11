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
        genesis_hash=GenesisHash,
        blocks=#{GenesisHash => GenesisBlock},
        ledger=Ledger,
        head=GenesisHash
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

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
