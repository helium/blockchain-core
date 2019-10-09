%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain).

-export([
    new/3, integrate_genesis/2,
    genesis_hash/1 ,genesis_block/1,
    head_hash/1, head_block/1,
    sync_hash/1,
    height/1,
    sync_height/1,
    ledger/1, ledger/2, ledger_at/2,
    dir/1,
    blocks/1, get_block/2,
    add_blocks/2, add_block/2, add_block/3,
    delete_block/2,
    config/2,
    fees_since/2,
    build/3,
    close/1,

    reset_ledger/1, reset_ledger/2
]).

-include("blockchain.hrl").

-ifdef(TEST).
-export([save_block/2]).
%% export a macro so we can interpose block saving to test failure
-define(save_block(Block, Chain), ?MODULE:save_block(Block, Chain)).
-include_lib("eunit/include/eunit.hrl").
-else.
-define(save_block(Block, Chain), save_block(Block, Chain)).
-endif.

-record(blockchain, {
    dir :: file:filename_all(),
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    blocks :: rocksdb:cf_handle(),
    heights :: rocksdb:cf_handle(),
    temp_blocks :: rocksdb:cf_handle(),
    ledger :: blockchain_ledger_v1:ledger()
}).

-define(GEN_HASH_FILE, "genesis").
-define(DB_FILE, "blockchain.db").
-define(HEAD, <<"head">>).
-define(TEMP_HEADS, <<"temp_heads">>).
-define(GENESIS, <<"genesis">>).
-define(ASSUMED_VALID, blockchain_core_assumed_valid_block_hash).

%% upgrades are listed here as a two-tuple of a key (stored in the
%% ledger), and a function to run with the ledger as an argument.  if
%% the key is already true, then the function will not be run.  when
%% we start a clean chain, we mark all existing keys are true and do
%% not run their code later.
-define(upgrades,
        [{<<"gateway_v2">>, fun upgrade_gateways_v2/1}]).


-type blocks() :: #{blockchain_block:hash() => blockchain_block:block()}.
-type blockchain() :: #blockchain{}.
-export_type([blockchain/0, blocks/0]).

%%--------------------------------------------------------------------
%% @doc
%% This function must only be called once, specifically in
%% blockchain_worker:integrate_genesis_block
%% @end
%%--------------------------------------------------------------------
-spec new(Dir :: file:filename_all(),
          Genesis :: file:filename_all() | blockchain_block:block() | undefined,
          AssumedValidBlockHash :: blockchain_block:hash()
         ) -> {ok, blockchain()} | {no_genesis, blockchain()}.
new(Dir, Genesis, AssumedValidBlockHash) when is_list(Genesis) ->
    case load_genesis(Genesis) of
        {error, _Reason} ->
            lager:warning("could not load genesis file: ~p ~p", [Genesis, _Reason]),
            ?MODULE:new(Dir, undefined, AssumedValidBlockHash);
        {ok, Block} ->
            ?MODULE:new(Dir, Block, AssumedValidBlockHash)
    end;
new(Dir, undefined, AssumedValidBlockHash) ->
    lager:info("loading blockchain from ~p", [Dir]),
    case load(Dir) of
        {Blockchain, {error, {corruption, _Corrupted}}} ->
            lager:error("DB corrupted cleaning up ~p", [_Corrupted]),
            ok = clean(Blockchain),
            new(Dir, undefined, AssumedValidBlockHash);
        {Blockchain, {error, _Reason}} ->
            lager:info("no genesis block found: ~p", [_Reason]),
            {no_genesis, init_assumed_valid(Blockchain, AssumedValidBlockHash)};
        {Blockchain, {ok, _GenBlock}} ->
            Ledger = blockchain:ledger(Blockchain),
            process_upgrades(?upgrades, Ledger),
            {ok, init_assumed_valid(Blockchain, AssumedValidBlockHash)}
    end;
new(Dir, GenBlock, AssumedValidBlockHash) ->
    lager:info("loading blockchain from ~p and checking ~p", [Dir, GenBlock]),
    case load(Dir) of
        {Blockchain, {error, {corruption, _Corrupted}}} ->
            lager:error("DB corrupted cleaning up ~p", [_Corrupted]),
            ok = clean(Blockchain),
            new(Dir, GenBlock, AssumedValidBlockHash);
        {Blockchain, {error, _Reason}} ->
            lager:warning("failed to load genesis, integrating new one: ~p", [_Reason]),
            ok = ?MODULE:integrate_genesis(GenBlock, Blockchain),
            Ledger = blockchain:ledger(Blockchain),
            mark_upgrades(?upgrades, Ledger),
            {ok, init_assumed_valid(Blockchain, AssumedValidBlockHash)};
        {Blockchain, {ok, GenBlock}} ->
            lager:info("new gen = old gen"),
            Ledger = blockchain:ledger(Blockchain),
            process_upgrades(?upgrades, Ledger),
            {ok, init_assumed_valid(Blockchain, AssumedValidBlockHash)};
        {Blockchain, {ok, _OldGenBlock}} ->
            lager:info("replacing old genesis block"),
            ok = clean(Blockchain),
            new(Dir, GenBlock, AssumedValidBlockHash)
    end.

process_upgrades([], _Ledger) ->
    ok;
process_upgrades([{Key, Fun} | Tail], Ledger) ->
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    case blockchain_ledger_v1:check_key(Key, Ledger1) of
        true ->
            process_upgrades(Tail, Ledger1);
        false ->
            Fun(Ledger1),
            blockchain_ledger_v1:mark_key(Key, Ledger1)
    end,
    blockchain_ledger_v1:commit_context(Ledger1),
    ok.

mark_upgrades(Upgrades, Ledger) ->
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    lists:foreach(fun({Key, _}) ->
                          blockchain_ledger_v1:mark_key(Key, Ledger1)
                  end, Upgrades),
    blockchain_ledger_v1:commit_context(Ledger1),
    ok.

upgrade_gateways_v2(Ledger) ->
    upgrade_gateways_v2_(Ledger),
    Ledger1 = blockchain_ledger_v1:mode(delayed, Ledger),
    Ledger2 = blockchain_ledger_v1:new_context(Ledger1),
    upgrade_gateways_v2_(Ledger2),
    blockchain_ledger_v1:commit_context(Ledger2).

upgrade_gateways_v2_(Ledger) ->
    %% the initial load here will automatically convert these into v2 records
    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
    %% find all neighbors for everyone
    maps:map(
      fun(A, G) ->
              G1 = case blockchain_ledger_gateway_v2:location(G) of
                       undefined -> G;
                       _ ->
                           Neighbors = blockchain_poc_path:neighbors(A, Gateways, Ledger),
                           blockchain_ledger_gateway_v2:neighbors(Neighbors, G)
                   end,
              blockchain_ledger_v1:update_gateway(G1, A, Ledger)
      end, Gateways),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
integrate_genesis(GenesisBlock, #blockchain{db=DB, default=DefaultCF}=Blockchain) ->
    GenHash = blockchain_block:hash_block(GenesisBlock),
    ok = blockchain_txn:absorb_and_commit(GenesisBlock, Blockchain, fun() ->
        {ok, Batch} = rocksdb:batch(),
        ok = save_block(GenesisBlock, Batch, Blockchain),
        GenBin = blockchain_block:serialize(GenesisBlock),
        ok = rocksdb:batch_put(Batch, DefaultCF, GenHash, GenBin),
        ok = rocksdb:batch_put(Batch, DefaultCF, ?GENESIS, GenHash),
        ok = rocksdb:write_batch(DB, Batch, [{sync, true}])
    end),
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


-spec sync_hash(blockchain()) -> {ok, blockchain_block:hash()} | {error, any()}.
sync_hash(Blockchain=#blockchain{db=DB, default=DefaultCF}) ->
    case persistent_term:get(?ASSUMED_VALID, undefined) of
        undefined ->
            ?MODULE:head_hash(Blockchain);
        _ ->
            case rocksdb:get(DB, DefaultCF, ?TEMP_HEADS, []) of
                {ok, BinHashes} ->
                    Hashes = binary_to_term(BinHashes),
                    RandomHash = lists:nth(rand:uniform(length(Hashes)), Hashes),
                    {ok, RandomHash};
                _ ->
                    ?MODULE:head_hash(Blockchain)
            end
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

%% like height/1 but takes accumulated 'assumed valid' blocks into account
-spec sync_height(blockchain()) -> {ok, non_neg_integer()} | {error, any()}.
sync_height(Blockchain = #blockchain{db=DB, temp_blocks=TempBlocksCF, default=DefaultCF}) ->
    case persistent_term:get(?ASSUMED_VALID, undefined) of
        undefined ->
            ?MODULE:height(Blockchain);
        _ ->
            case rocksdb:get(DB, DefaultCF, ?TEMP_HEADS, []) of
                {ok, BinHashes} ->
                    Hashes = binary_to_term(BinHashes),
                    MinHeight = case ?MODULE:height(Blockchain) of
                                    {ok, H} ->
                                        H;
                                    _ ->
                                        0
                                end,
                    MaxHeight = lists:foldl(fun(Hash, AccHeight) ->
                                                    case rocksdb:get(DB, TempBlocksCF, Hash, []) of
                                                        {ok, BinBlock} ->
                                                            Block = blockchain_block:deserialize(BinBlock),
                                                            max(AccHeight, blockchain_block:height(Block));
                                                        _ ->
                                                            %% this is unlikely to happen, but roll with it
                                                            AccHeight
                                                    end
                                            end, MinHeight, Hashes),
                    {ok, MaxHeight};
                _ ->
                    ?MODULE:height(Blockchain)
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
-spec ledger(blockchain_ledger_v1:ledger(), blockchain()) -> blockchain().
ledger(Ledger, Chain) ->
    Chain#blockchain{ledger=Ledger}.

%%--------------------------------------------------------------------
%% @doc Obtain a version of the ledger at a given height.
%%
%% NOTE this function, if it suceeds will ALWAYS create a new context.
%% This is to help prevent side effects and contamination and to help
%% with the cleanup assumptions. Any ledger successfully obtained with
%% this function should be deleted when it's no longer needed by using
%% `delete_context'.
%% @end
%%--------------------------------------------------------------------
-spec ledger_at(pos_integer(), blockchain()) -> {ok, blockchain_ledger_v1:ledger()} | {error, any()}.
ledger_at(Height, Chain0) ->
    Ledger = ?MODULE:ledger(Chain0),
      case blockchain_ledger_v1:current_height(Ledger) of
        {ok, CurrentHeight} when Height > CurrentHeight->
            {error, invalid_height};
        {ok, Height} ->
            %% Current height is the height we want, just return a new context
            {ok, blockchain_ledger_v1:new_context(Ledger)};
        {ok, CurrentHeight} ->
            DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
            case blockchain_ledger_v1:current_height(DelayedLedger) of
                {ok, Height} ->
                    %% Delayed height is the height we want, just return a new context
                    {ok, blockchain_ledger_v1:new_context(DelayedLedger), Chain0};
                {ok, DelayedHeight} when Height > DelayedHeight andalso Height < CurrentHeight ->
                    Chain1 = lists:foldl(
                        fun(H, ChainAcc) ->
                            {ok, Block} = ?MODULE:get_block(H, Chain0),
                            {ok, Chain1} = blockchain_txn:absorb_block(Block, ChainAcc),
                            Chain1
                        end,
                        ?MODULE:ledger(blockchain_ledger_v1:new_context(DelayedLedger), Chain0),
                        lists:seq(DelayedHeight+1, Height)
                    ),
                    {ok, ?MODULE:ledger(Chain1)};
                {ok, DelayedHeight} when Height < DelayedHeight ->
                    {error, height_too_old};
                {error, _}=Error ->
                    Error
            end;
        {error, _}=Error ->
            Error
    end.

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
add_blocks([LastBlock | []], Chain) ->
    ?MODULE:add_block(LastBlock, Chain, false);
add_blocks([Block | Blocks], Chain) ->
    case ?MODULE:add_block(Block, Chain, true) of
        ok -> add_blocks(Blocks, Chain);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_block(blockchain_block:block(), blockchain()) -> ok | {error, any()}.
add_block(Block, Blockchain) ->
    add_block(Block, Blockchain, false).

-spec add_block(blockchain_block:block(), blockchain(), boolean()) -> ok | {error, any()}.
add_block(Block, Blockchain, Syncing) ->
    blockchain_lock:acquire(),
    try
        case persistent_term:get(?ASSUMED_VALID, undefined) of
            undefined ->
                add_block_(Block, Blockchain, Syncing);
            AssumedValidHash ->
                add_assumed_valid_block(AssumedValidHash, Block, Blockchain, Syncing)
        end
    catch What:Why:Stack ->
            lager:warning("error adding block: ~p:~p, ~p", [What, Why, Stack]),
            {error, Why}
    after
        blockchain_lock:release()
    end.

add_block_(Block, Blockchain, Syncing) ->
    Hash = blockchain_block:hash_block(Block),
    {ok, GenesisHash} = blockchain:genesis_hash(Blockchain),
    case blockchain_block:is_genesis(Block) of
        true when Hash =:= GenesisHash ->
            ok;
        true ->
            {error, unknown_genesis_block};
        false ->
            case blockchain:head_block(Blockchain) of
                {error, Reason}=Error ->
                    lager:error("could not get head hash ~p", [Reason]),
                    Error;
                {ok, HeadBlock} ->
                    HeadHash = blockchain_block:hash_block(HeadBlock),
                    Ledger = blockchain:ledger(Blockchain),
                    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
                    {ok, BlockchainHeight} = blockchain:height(Blockchain),
                    Height = blockchain_block:height(Block),
                    case
                        {blockchain_block:prev_hash(Block) =:= HeadHash andalso
                         Height =:= blockchain_block:height(HeadBlock) + 1,
                         BlockchainHeight =:= LedgerHeight}
                    of
                        {_, false} ->
                            lager:warning("ledger and chain height don't match (L:~p, C:~p)", [LedgerHeight, BlockchainHeight]),
                            ok = blockchain_worker:mismatch(),
                            {error, mismatch_ledger_chain};
                        {false, _} when HeadHash =:= Hash ->
                            lager:debug("Already have this block"),
                            ok;
                        {false, _} ->
                            case ?MODULE:get_block(Hash, Blockchain) of
                                {ok, Block} ->
                                    %% we already have this, thanks
                                    %% don't error here incase there's more blocks coming that *are* valid
                                    ok;
                                _ ->
                                    lager:warning("block doesn't fit with our chain,
                                          block_height: ~p, head_block_height: ~p", [blockchain_block:height(Block),
                                                                                     blockchain_block:height(HeadBlock)]),
                                    {error, disjoint_chain}
                            end;
                        {true, true} ->
                            lager:info("prev hash matches the gossiped block"),
                            case blockchain_ledger_v1:consensus_members(Ledger) of
                                {error, _Reason}=Error ->
                                    lager:error("could not get consensus_members ~p", [_Reason]),
                                    Error;
                                {ok, ConsensusAddrs} ->
                                    N = length(ConsensusAddrs),
                                    F = (N-1) div 3,
                                    {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
                                    Txns = blockchain_block:transactions(Block),
                                    case blockchain_block:verify_signatures(Block,
                                                                            ConsensusAddrs,
                                                                            blockchain_block:signatures(Block),
                                                                            N - F,
                                                                            MasterKey)
                                    of
                                        false ->
                                            {error, failed_verify_signatures};
                                        {true, _, Rescue} ->
                                            SortedTxns = lists:sort(fun blockchain_txn:sort/2, Txns),
                                            case Txns == SortedTxns of
                                                false ->
                                                    {error, wrong_txn_order};
                                                true ->
                                                    BeforeCommit = fun() ->
                                                        lager:info("adding block ~p", [Height]),
                                                        ok = ?save_block(Block, Blockchain)
                                                    end,
                                                    case blockchain_ledger_v1:new_snapshot(Ledger) of
                                                        {error, Reason}=Error ->
                                                            lager:error("Error creating snapshot, Reason: ~p", [Reason]),
                                                            Error;
                                                        {ok, NewLedger} ->
                                                            case blockchain_txn:absorb_and_commit(Block, Blockchain, BeforeCommit, Rescue) of
                                                                {error, Reason}=Error ->
                                                                    lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [blockchain_block:hash_block(Block), Reason]),
                                                                    Error;
                                                                ok ->
                                                                    lager:info("Notifying new block ~p", [Height]),
                                                                    ok = blockchain_worker:notify({add_block, Hash, Syncing, NewLedger})
                                                            end
                                                    end
                                            end
                                    end
                            end
                    end
            end
    end.

add_assumed_valid_block(AssumedValidHash, Block, Blockchain=#blockchain{db=DB, blocks=BlocksCF, temp_blocks=TempBlocksCF}, Syncing) ->
    Hash = blockchain_block:hash_block(Block),
    %% check if this is the block we've been waiting for
    case AssumedValidHash == Hash of
        true ->
            ParentHash = blockchain_block:prev_hash(Block),
            %% check if we have the parent of this block in the temp_block storage
            %% which would mean we have obtained all the assumed valid blocks
            case rocksdb:get(DB, TempBlocksCF, ParentHash, []) of
                {ok, _BinBlock} ->
                    %% save this block so if we get interrupted or crash
                    %% we don't need to wait for another sync to resume
                    ok = save_temp_block(Block, Blockchain),
                    %% ok, now build the chain back to the oldest block in the temporary
                    %% storage or to the head of the main chain, whichever comes first
                    case blockchain:head_hash(Blockchain) of
                        {ok, MainChainHeadHash} ->
                            Chain = build_hash_chain(MainChainHeadHash, Block, Blockchain, TempBlocksCF),
                            %% note that 'Chain' includes 'Block' here.
                            %%
                            %% check the oldest block in the temporary chain connects with
                            %% the HEAD block in the main chain
                            {ok, OldestTempBlock} = get_temp_block(hd(Chain), Blockchain),
                            case blockchain_block:prev_hash(OldestTempBlock) of
                                MainChainHeadHash ->
                                    %% Ok, this looks like we have everything we need.
                                    %% Absorb all the transactions without validating them
                                    absorb_temp_blocks(Chain, Blockchain, Syncing);
                                Res ->
                                    lager:info("temp Chain ~p", [length(Chain)]),
                                    lager:warning("Saw assumed valid block, but cannot connect it to the main chain ~p", [Res]),
                                    {error, disjoint_assumed_valid_block}
                            end;
                        _Error ->
                            lager:warning("Unable to get head hash of main chain ~p", [_Error]),
                            {error, unobtainable_head_hash}
                    end;
                _ ->
                    %% it's possible that we've seen everything BUT the assumed valid block, and done a full validate + absorb
                    %% already, so check for that
                    case blockchain:get_block(ParentHash, Blockchain) of
                        {ok, _} ->
                            ok = save_temp_block(Block, Blockchain),
                            absorb_temp_blocks([blockchain_block:hash_block(Block)], Blockchain, Syncing);
                        _ ->
                            lager:notice("Saw the assumed_valid block, but don't have its parent"),
                            {error, disjoint_assumed_valid_block}
                    end
            end;
        false ->
            ParentHash = blockchain_block:prev_hash(Block),
            case rocksdb:get(DB, TempBlocksCF, ParentHash, []) of
                {ok, _} ->
                    save_temp_block(Block, Blockchain);
                _Error ->
                    case rocksdb:get(DB, BlocksCF, ParentHash, []) of
                        {ok, _} ->
                            save_temp_block(Block, Blockchain);
                        _ ->
                            {error, disjoint_assumed_valid_block}
                    end
            end
    end.

absorb_temp_blocks([],#blockchain{db=DB, temp_blocks=TempBlocksCF, default=DefaultCF}, _Syncing) ->
    %% we did it!
    persistent_term:erase(?ASSUMED_VALID),
    ok = rocksdb:delete(DB, DefaultCF, ?TEMP_HEADS, [{sync, true}]),
    ok = rocksdb:drop_column_family(TempBlocksCF),
    ok = rocksdb:destroy_column_family(TempBlocksCF),
    ok;
absorb_temp_blocks([BlockHash|Chain], Blockchain, Syncing) ->
    {ok, Block} = get_temp_block(BlockHash, Blockchain),
    Height = blockchain_block:height(Block),
    Hash = blockchain_block:hash_block(Block),
    Ledger = blockchain:ledger(Blockchain),
    BeforeCommit = fun() ->
                           lager:info("adding block ~p", [Height]),
                           ok = ?save_block(Block, Blockchain)
                   end,
    case blockchain_ledger_v1:new_snapshot(Ledger) of
        {error, Reason}=Error ->
            lager:error("Error creating snapshot, Reason: ~p", [Reason]),
            Error;
        {ok, NewLedger} ->
            case blockchain_txn:unvalidated_absorb_and_commit(Block, Blockchain, BeforeCommit, blockchain_block:is_rescue_block(Block)) of
                {error, Reason}=Error ->
                    lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [blockchain_block:hash_block(Block), Reason]),
                    Error;
                ok ->
                    lager:info("Notifying new block ~p", [Height]),
                    ok = blockchain_worker:notify({add_block, Hash, Syncing, NewLedger}),
                    absorb_temp_blocks(Chain, Blockchain, Syncing)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delete_block(blockchain_block:block(), blockchain()) -> ok.
delete_block(Block, #blockchain{db=DB, default=DefaultCF,
                                blocks=BlocksCF, heights=HeightsCF}=Chain) ->
    {ok, Batch} = rocksdb:batch(),
    Hash = blockchain_block:hash_block(Block),
    PrevHash = blockchain_block:prev_hash(Block),
    Height = blockchain_block:height(Block),
    lager:warning("deleting block ~p height: ~p, Prev Hash ~p", [Hash, Height, PrevHash]),
    ok = rocksdb:batch_delete(Batch, BlocksCF, Hash),
    {ok, HeadHash} = ?MODULE:head_hash(Chain),
    case HeadHash =:= Hash of
        false -> ok;
        true ->
            ok = rocksdb:batch_put(Batch, DefaultCF, ?HEAD, PrevHash)
    end,
    ok = rocksdb:batch_delete(Batch, HeightsCF, <<Height:64/integer-unsigned-big>>),
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]).

config(ConfigName, Ledger) ->
    blockchain_ledger_v1:config(ConfigName, Ledger). % ignore using "?"

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fees_since(non_neg_integer(), blockchain()) -> {ok, non_neg_integer()} | {error, any()}.
fees_since(Height, Chain) ->
    {ok, CurrentHeight} = ?MODULE:height(Chain),
    fees_since(Height, CurrentHeight, Chain).

-spec fees_since(non_neg_integer(), non_neg_integer(), blockchain()) -> {ok, non_neg_integer()} | {error, any()}.
fees_since(1, _CurrentHeight, _Chain) ->
    {error, bad_height};
fees_since(Height, CurrentHeight, Chain) when CurrentHeight > Height ->
    Txns = lists:foldl(
        fun(H, Acc) ->
            {ok, Block} = ?MODULE:get_block(H, Chain),
            Acc ++ blockchain_block:transactions(Block)
        end,
        [],
        lists:seq(Height, CurrentHeight)
    ),
    Fees = lists:foldl(
        fun(Txn, Acc) ->
            Type = blockchain_txn:type(Txn),
            Fee = Type:fee(Txn),
            Fee + Acc
        end,
        0,
        Txns
    ),
    {ok, Fees};
fees_since(_Height, _CurrentHeight, _Chain) ->
    {error, bad_height}.


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


-spec build_hash_chain(blockchain_block:hash(), blockchain_block:block(), blockchain(), rocksdb:cf_handle()) -> [blockchain_block:hash(), ...].
build_hash_chain(StopHash,StartingBlock, Blockchain, CF) ->
    BlockHash = blockchain_block:hash_block(StartingBlock),
    ParentHash = blockchain_block:prev_hash(StartingBlock),
    build_hash_chain_(StopHash, CF, Blockchain, [ParentHash, BlockHash]).

-spec build_hash_chain_(blockchain_block:hash(), rocksdb:cf_handle(), blockchain(), [blockchain_block:hash(), ...]) -> [blockchain_block:hash()].
build_hash_chain_(StopHash, CF, Blockchain = #blockchain{db=DB}, [ParentHash|Tail]=Acc) ->
    case ParentHash == StopHash of
        true ->
            %% reached the end
            Tail;
        false ->
            case rocksdb:get(DB, CF, ParentHash, []) of
                {ok, BinBlock} ->
                    build_hash_chain_(StopHash, CF, Blockchain, [blockchain_block:prev_hash(blockchain_block:deserialize(BinBlock))|Acc]);
                _ ->
                    Acc
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
close(#blockchain{db=DB, ledger=Ledger}) ->
    persistent_term:erase(?ASSUMED_VALID),
    ok = blockchain_ledger_v1:close(Ledger),
    rocksdb:close(DB).

reset_ledger(Chain) ->
    {ok, Height} = height(Chain),
    reset_ledger(Height, Chain).

reset_ledger(Height, #blockchain{db = DB,
                                 dir = Dir,
                                 default = DefaultCF,
                                 blocks = BlocksCF,
                                 heights = HeightsCF} = Chain) ->
    blockchain_lock:acquire(),
    %% check this is safe to do
    {ok, StartBlock} = get_block(Height, Chain),
    {ok, GenesisHash} = genesis_hash(Chain),
    %% note that this will not include the genesis block
    HashChain = build_hash_chain(GenesisHash, StartBlock, Chain, BlocksCF),
    LastKnownBlock = case get_block(hd(HashChain), Chain) of
                         {ok, LKB} ->
                             LKB;
                         {error, not_found} ->
                             {ok, LKB} = get_block(hd(tl(HashChain)), Chain),
                             LKB
                     end,
    %% check the height is what we'd expect
    case length(HashChain) == Height - 1 andalso
         %% check that the previous block is the genesis block
         GenesisHash == blockchain_block:prev_hash(LastKnownBlock)  of
        false ->
            %% can't do this, we're missing a block somwewhere along the line
            MissingHash = blockchain_block:prev_hash(LastKnownBlock),
            MissingHeight = blockchain_block:height(LastKnownBlock) - 1,
            blockchain_lock:release(),
            {error, {missing_block, MissingHash, MissingHeight}};
        true ->
            %% delete the existing and trailing ledgers
            ok = blockchain_ledger_v1:clean(ledger(Chain)),
            %% delete blocks from Height + 1 to the top of the chain
            {ok, TopHeight} = height(Chain),
            _ = lists:foreach(
                  fun(H) ->
                          case get_block(H, Chain) of
                              {ok, B} ->
                                  delete_block(B, Chain);
                              _ ->
                                  ok
                          end
                  end,
                  %% this will be a noop in the case where Height == TopHeight
                  lists:reverse(lists:seq(Height + 1, TopHeight))),
            {ok, TargetBlock} = get_block(Height, Chain),
            case head_hash(Chain) ==  {ok, blockchain_block:hash_block(TargetBlock)} of
                false ->
                    %% the head hash needs to be corrected (we probably had a hole in the chain)
                    ok = rocksdb:put(DB, DefaultCF, ?HEAD, blockchain_block:hash_block(TargetBlock), [{sync, true}]);
                true ->
                    %% nothing to do
                    ok
            end,

            %% recreate the ledgers and notify the application of the
            %% new chain
            Ledger1 = blockchain_ledger_v1:new(Dir),
            Chain1 = ledger(Ledger1, Chain),
            blockchain_worker:blockchain(Chain1),

            %% reapply the blocks
            Chain2 =
            lists:foldl(
              fun(H, CAcc) ->
                      case rocksdb:get(DB, HeightsCF, <<H:64/integer-unsigned-big>>, []) of
                          {ok, Hash0} ->
                              Hash =
                              case H of
                                  1 ->
                                      {ok, GHash} = genesis_hash(Chain),
                                      GHash;
                                  _ ->
                                      Hash0
                              end,
                              {ok, BinBlock} = rocksdb:get(DB, BlocksCF, Hash, []),
                              Block = blockchain_block:deserialize(BinBlock),
                              lager:info("absorbing block ~p ?= ~p", [H, blockchain_block:height(Block)]),
                              ok = blockchain_txn:unvalidated_absorb_and_commit(Block, CAcc, fun() -> ok end, blockchain_block:is_rescue_block(Block)),
                              CAcc;
                          _ ->
                              lager:warning("couldn't absorb block at ~p", [H]),
                              ok
                      end
              end,
              %% this will be a noop in the case where Height == TopHeight
              Chain1,
              lists:seq(1, Height)),

            blockchain_lock:release(),
            {ok, Chain2}
    end.


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
    {ok, DB, [DefaultCF, BlocksCF, HeightsCF, TempBlocksCF]} = open_db(Dir),
    Ledger = blockchain_ledger_v1:new(Dir),
    Blockchain = #blockchain{
        dir=Dir,
        db=DB,
        default=DefaultCF,
        blocks=BlocksCF,
        heights=HeightsCF,
        temp_blocks=TempBlocksCF,
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
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    DefaultCFs = ["default", "blocks", "heights", "temp_blocks"],
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

    CFOpts = GlobalOpts,

    {ok, DB, OpenedCFs} = rocksdb:open_with_cf(DBDir, DBOptions,  [{CF, CFOpts} || CF <- ExistingCFs]),

    L1 = lists:zip(ExistingCFs, OpenedCFs),
    L2 = lists:map(
        fun(CF) ->
            {ok, CF1} = rocksdb:create_column_family(DB, CF, CFOpts),
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
save_block(Block, Chain = #blockchain{db=DB}) ->
    {ok, Batch} = rocksdb:batch(),
    save_block(Block, Batch,  Chain),
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]).

-spec save_block(blockchain_block:block(), rocksdb:batch_handle(), blockchain()) -> ok.
save_block(Block, Batch, #blockchain{default=DefaultCF, blocks=BlocksCF, heights=HeightsCF}) ->
    Height = blockchain_block:height(Block),
    Hash = blockchain_block:hash_block(Block),
    ok = rocksdb:batch_put(Batch, BlocksCF, Hash, blockchain_block:serialize(Block)),
    ok = rocksdb:batch_put(Batch, DefaultCF, ?HEAD, Hash),
    %% lexiographic ordering works better with big endian
    ok = rocksdb:batch_put(Batch, HeightsCF, <<Height:64/integer-unsigned-big>>, Hash).

save_temp_block(Block, #blockchain{db=DB, temp_blocks=TempBlocks, default=DefaultCF}) ->
    Hash = blockchain_block:hash_block(Block),
    case rocksdb:get(DB, TempBlocks, Hash, []) of
        {ok, _} ->
            %% already got it, thanks
            ok;
        _ ->
            {ok, Batch} = rocksdb:batch(),
            PrevHash = blockchain_block:prev_hash(Block),
            ok = rocksdb:batch_put(Batch, TempBlocks, Hash, blockchain_block:serialize(Block)),
            %% So, we have to be careful here because we might have multiple seemingly valid chains
            %% only one of which will lead to the assumed valid block. We need to keep track of all
            %% the potentials heads and use each of them randomly as our 'sync hash' until we can
            %% get a valid chain to the 'assumed valid` block. Ugh.
            TempHeads = case rocksdb:get(DB, DefaultCF, ?TEMP_HEADS, []) of
                            {ok, BinHeadsList} ->
                                binary_to_term(BinHeadsList);
                            _ ->
                                []
                        end,
            Height = blockchain_block:height(Block),
            lager:info("temp block height is at least ~p", [Height]),
            ok = rocksdb:batch_put(Batch, DefaultCF, ?TEMP_HEADS, term_to_binary([Hash|TempHeads] -- [PrevHash])),
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}])
    end.

get_temp_block(Hash, #blockchain{db=DB, temp_blocks=TempBlocksCF}) ->
    case rocksdb:get(DB, TempBlocksCF, Hash, []) of
        {ok, BinBlock} ->
            Block = blockchain_block:deserialize(BinBlock),
            {ok, Block};
        Other ->
            Other
    end.

init_assumed_valid(Blockchain, undefined) ->
    Blockchain;
init_assumed_valid(Blockchain, Hash) when is_binary(Hash) ->
    case ?MODULE:get_block(Hash, Blockchain) of
        {ok, _} ->
            %% already got it, chief
            %% TODO make sure we delete the column family, in case it has stale crap in it!
            ok;
        _ ->
            #blockchain{db=DB, temp_blocks=TempBlocksCF} = Blockchain,
            case rocksdb:get(DB, TempBlocksCF, Hash, []) of
                {ok, BinBlock} ->
                    %% we already have it, try to process it
                    Block = blockchain_block:deserialize(BinBlock),
                    %% spawn a worker process to do this in the background so we don't
                    %% block application startup
                    spawn_link(fun() ->
                                  case add_assumed_valid_block(Hash, Block, Blockchain, false) of
                                      ok ->
                                          %% we did it!
                                          ok;
                                      {error, Reason} ->
                                          %% something went wrong!
                                          lager:warning("assume valid processing failed on init with assumed_valid hash present ~p", [Reason]),
                                          %% TODO we should probably drop the column family here?
                                          ok = persistent_term:put(?ASSUMED_VALID, Hash)
                                  end
                          end),
                    ok;
                _ ->
                    %% need to wait for it
                    ok = persistent_term:put(?ASSUMED_VALID, Hash)
            end
    end,
    Blockchain.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    BaseDir = test_utils:tmp_dir("new_test"),
    Block = blockchain_block:new_genesis_block([]),
    Hash = blockchain_block:hash_block(Block),
    {ok, Chain} = new(BaseDir, Block, undefined),
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

blocks_test() ->
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, consensus_members, fun(_) ->
        {ok, []}
    end),
    meck:new(blockchain_block, [passthrough]),
    meck:expect(blockchain_block, verify_signatures, fun(_, _, _, _) ->
        {true, undefined}
    end),
    meck:expect(blockchain_block, verify_signatures, fun(_, _, _, _, _) ->
        {true, undefined, false}
    end),
    meck:new(blockchain_worker, [passthrough]),
    meck:expect(blockchain_worker, notify, fun(_) ->
        ok
    end),
    meck:new(blockchain_election, [passthrough]),
    meck:expect(blockchain_election, has_new_group, fun(_) ->
        false
    end),
    {ok, Pid} = blockchain_lock:start_link(),

    #{secret := Priv, public := Pub} = libp2p_crypto:generate_keys(ecc_compact),
    BinPub = libp2p_crypto:pubkey_to_bin(Pub),

    Vars = #{chain_vars_version => 2},
    Txn = blockchain_txn_vars_v1:new(Vars, 1, #{master_key => BinPub}),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn),
    VarTxns = [blockchain_txn_vars_v1:key_proof(Txn, Proof)],

    GenBlock = blockchain_block:new_genesis_block(VarTxns),
    GenHash = blockchain_block:hash_block(GenBlock),
    {ok, Chain} = new(test_utils:tmp_dir("blocks_test"), GenBlock, undefined),
    Block = blockchain_block_v1:new(#{prev_hash => GenHash,
                                      height => 2,
                                      transactions => [],
                                      signatures => [],
                                      time => 1,
                                      hbbft_round => 1,
                                      election_epoch => 1,
                                      epoch_start => 0
                                     }),
    Hash = blockchain_block:hash_block(Block),
    ok = add_block(Block, Chain),
    Map = #{
        GenHash => GenBlock,
        Hash => Block
    },
    ?assertMatch(Map, blocks(Chain)),

    ok = gen_server:stop(Pid),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain_block)),
    meck:unload(blockchain_block),
    ?assert(meck:validate(blockchain_worker)),
    meck:unload(blockchain_worker),
    ?assert(meck:validate(blockchain_election)),
    meck:unload(blockchain_election).


get_block_test() ->
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, consensus_members, fun(_) ->
        {ok, []}
    end),
    meck:new(blockchain_block, [passthrough]),
    meck:expect(blockchain_block, verify_signatures, fun(_, _, _, _) ->
        {true, undefined}
    end),
    meck:expect(blockchain_block, verify_signatures, fun(_, _, _, _, _) ->
        {true, undefined, false}
    end),
    meck:new(blockchain_worker, [passthrough]),
    meck:expect(blockchain_worker, notify, fun(_) ->
        ok
    end),
    meck:new(blockchain_election, [passthrough]),
    meck:expect(blockchain_election, has_new_group, fun(_) ->
        false
    end),


    {ok, Pid} = blockchain_lock:start_link(),

    #{secret := Priv, public := Pub} = libp2p_crypto:generate_keys(ecc_compact),
    BinPub = libp2p_crypto:pubkey_to_bin(Pub),

    Vars = #{chain_vars_version => 2},
    Txn = blockchain_txn_vars_v1:new(Vars, 1, #{master_key => BinPub}),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn),
    VarTxns = [blockchain_txn_vars_v1:key_proof(Txn, Proof)],

    GenBlock = blockchain_block:new_genesis_block(VarTxns),
    GenHash = blockchain_block:hash_block(GenBlock),
    {ok, Chain} = new(test_utils:tmp_dir("get_block_test"), GenBlock, undefined),
    Block = blockchain_block_v1:new(#{prev_hash => GenHash,
                                      height => 2,
                                      transactions => [],
                                      signatures => [],
                                      time => 1,
                                      hbbft_round => 1,
                                      election_epoch => 1,
                                      epoch_start => 0
                                     }),
    Hash = blockchain_block:hash_block(Block),
    ok = add_block(Block, Chain),
    ?assertMatch({ok, Block}, get_block(Hash, Chain)),

    ok = gen_server:stop(Pid),

    ?assert(meck:validate(blockchain_ledger_v1)),
    meck:unload(blockchain_ledger_v1),
    ?assert(meck:validate(blockchain_block)),
    meck:unload(blockchain_block),
    ?assert(meck:validate(blockchain_worker)),
    meck:unload(blockchain_worker),
    ?assert(meck:validate(blockchain_election)),
    meck:unload(blockchain_election).


-endif.
