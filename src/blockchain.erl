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

    last_block_add_time/1,

    analyze/1, repair/1,

    reset_ledger/1, reset_ledger/2, reset_ledger/3
]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-export([save_block/2, bootstrap_hexes/1]).
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
-define(MISSING_BLOCK, <<"missing_block">>).
-define(GENESIS, <<"genesis">>).
-define(ASSUMED_VALID, blockchain_core_assumed_valid_block_hash_and_height).
-define(LAST_BLOCK_ADD_TIME, <<"last_block_add_time">>).

%% upgrades are listed here as a two-tuple of a key (stored in the
%% ledger), and a function to run with the ledger as an argument.  if
%% the key is already true, then the function will not be run.  when
%% we start a clean chain, we mark all existing keys are true and do
%% not run their code later.
-define(upgrades,
        [{<<"gateway_v2">>, fun upgrade_gateways_v2/1},
         {<<"hex_targets">>, fun bootstrap_hexes/1}]).
%% NB: we need to keep this in sync with the filter in the fingerprints


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
          AssumedValidBlockHashAndHeight :: {blockchain_block:hash(), pos_integer()}
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
        {Blockchain, {error, Reason}} ->
            lager:warning("failed to load genesis block ~p, integrating new one", [Reason]),
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
            lager:info("replacing old genesis block with new one"),
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

bootstrap_hexes(Ledger) ->
    bootstrap_hexes_(Ledger),
    Ledger1 = blockchain_ledger_v1:mode(delayed, Ledger),
    Ledger2 = blockchain_ledger_v1:new_context(Ledger1),
    bootstrap_hexes_(Ledger2),
    blockchain_ledger_v1:commit_context(Ledger2).

bootstrap_hexes_(Ledger) ->
    %% hardcode this until we have the var update hook.
    Res = 5,
    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
    Hexes =
        maps:fold(
          fun(Addr, Gw, A) ->
                  case blockchain_ledger_gateway_v2:location(Gw) of
                      undefined -> A;
                      Loc ->
                          Hex = h3:parent(Loc, Res),
                          maps:update_with(Hex, fun(X) -> [Addr | X] end, [Addr], A)
                  end
          end,
          #{}, Gateways),
    HexMap = maps:map(fun(_K, V) -> length(V) end, Hexes),
    ok = blockchain_ledger_v1:set_hexes(HexMap, Ledger),
    _ = maps:map(
          fun(Hex, Addresses) ->
                  blockchain_ledger_v1:set_hex(Hex, Addresses, Ledger)
          end, Hexes),
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
    case missing_block(Blockchain) of
        {ok, Hash} ->
            %% this is the hash of the last block before the gap
            {ok, Hash};
        _ ->
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
    ledger_at(Height, Chain0, false).

-spec ledger_at(pos_integer(), blockchain(), boolean()) -> {ok, blockchain_ledger_v1:ledger()} | {error, any()}.
ledger_at(Height, Chain0, ForceRecalc) ->
    Ledger = ?MODULE:ledger(Chain0),
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, CurrentHeight} when Height > CurrentHeight andalso not ForceRecalc ->
            {error, invalid_height};
        {ok, Height} when not ForceRecalc ->
            %% Current height is the height we want, just return a new context
            {ok, blockchain_ledger_v1:new_context(Ledger)};
        {ok, CurrentHeight} ->
            DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
            case blockchain_ledger_v1:current_height(DelayedLedger) of
                {ok, Height} ->
                    %% Delayed height is the height we want, just return a new context
                    {ok, blockchain_ledger_v1:new_context(DelayedLedger)};
                {ok, DelayedHeight} when Height > DelayedHeight andalso Height =< CurrentHeight ->
                    case blockchain_ledger_v1:has_snapshot(Height, DelayedLedger) of
                        {ok, SnapshotLedger} when not ForceRecalc ->
                            {ok, SnapshotLedger};
                        _ ->
                            case fold_chain(Chain0, DelayedHeight, DelayedLedger, Height) of
                                {ok, Chain1} ->
                                    Ledger1 = ?MODULE:ledger(Chain1),
                                    Ctxt = blockchain_ledger_v1:get_context(Ledger1),
                                    blockchain_ledger_v1:context_snapshot(Ctxt, Ledger1),
                                    {ok, Ledger1};
                                Error ->
                                    Error
                            end
                    end;
                {ok, DelayedHeight} when Height < DelayedHeight ->
                    {error, height_too_old};
                {error, _}=Error ->
                    Error
            end;
        {error, _}=Error ->
            Error
    end.

fold_chain(Chain0, DelayedHeight, DelayedLedger, Height) ->
    lists:foldl(
      fun(H, {ok, ChainAcc}) ->
              case ?MODULE:get_block(H, Chain0) of
                  {ok, Block} ->
                      case blockchain_txn:absorb_block(Block, ChainAcc) of
                          {ok, Chain1} ->
                              {ok, Chain1};
                          {error, Reason} ->
                              {error, {block_absorb_failed, H, Reason}}
                      end;
                  {error, not_found} ->
                      {error, {missing_block, H}}
              end;
         (_H, Error) ->
            %% keep returning the error
            Error
      end,
      {ok, ?MODULE:ledger(blockchain_ledger_v1:new_context(DelayedLedger), Chain0)},
      lists:seq(DelayedHeight+1, Height)
     ).

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
add_blocks(Blocks, Chain) ->
    blockchain_lock:acquire(),
    try
        add_blocks_(Blocks, Chain)
    catch C:E:S ->
            lager:warning("crash adding blocks: ~p:~p ~p", [C, E, S]),
            {error, add_blocks_error}
    after
        blockchain_lock:release()
    end.

add_blocks_([], _Chain) ->  ok;
add_blocks_([LastBlock | []], Chain) ->
    ?MODULE:add_block(LastBlock, Chain, false);
add_blocks_([Block | Blocks], Chain) ->
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
add_block(Block, #blockchain{db=DB, blocks=BlocksCF, heights=HeightsCF, default=DefaultCF} = Blockchain, Syncing) ->
    blockchain_lock:acquire(),
    try
        PrevHash = blockchain_block:prev_hash(Block),
        case missing_block(Blockchain) of
            {ok, PrevHash} ->
                {ok, Batch} = rocksdb:batch(),
                %% this is the block we've been missing
                Height = blockchain_block:height(Block),
                Hash = blockchain_block:hash_block(Block),
                %% check if it fits between the 2 known good blocks
                case get_block(Height + 1, Blockchain) of
                    {ok, NextBlock} ->
                        case blockchain_block:prev_hash(NextBlock) of
                            Hash ->
                                %% everything lines up
                                ok = rocksdb:batch_put(Batch, BlocksCF, Hash, blockchain_block:serialize(Block)),
                                %% lexiographic ordering works better with big endian
                                ok = rocksdb:batch_put(Batch, HeightsCF, <<Height:64/integer-unsigned-big>>, Hash),
                                ok = rocksdb:batch_delete(Batch, DefaultCF, ?MISSING_BLOCK),
                                ok = rocksdb:write_batch(DB, Batch, [{sync, true}]);
                            _ ->
                                {error, bad_resynced_block}
                        end;
                    _ ->
                        {error, resynced_block_child_missing}
                end;
            _ ->
                case persistent_term:get(?ASSUMED_VALID, undefined) of
                    undefined ->
                        add_block_(Block, Blockchain, Syncing);
                    AssumedValidHashAndHeight ->
                        add_assumed_valid_block(AssumedValidHashAndHeight, Block, Blockchain, Syncing)
                end
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
                        {_, false} when BlockchainHeight =:= (LedgerHeight + 1) ->
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
                            MyAddress = blockchain_swarm:pubkey_bin(),
                            case blockchain_ledger_v1:consensus_members(Ledger) of
                                {error, _Reason}=Error ->
                                    lager:error("could not get consensus_members ~p", [_Reason]),
                                    Error;
                                {ok, ConsensusAddrs} ->
                                    N = length(ConsensusAddrs),
                                    F = (N-1) div 3,
                                    {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
                                    Txns = blockchain_block:transactions(Block),
                                    Sigs = blockchain_block:signatures(Block),
                                    case blockchain_block:verify_signatures(Block,
                                                                            ConsensusAddrs,
                                                                            Sigs,
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
                                                    {Signers, _Signatures} = lists:unzip(Sigs),
                                                    Fun = case lists:member(MyAddress, Signers) of
                                                              true -> unvalidated_absorb_and_commit;
                                                              _ -> absorb_and_commit
                                                          end,
                                                    case blockchain_txn:Fun(Block, Blockchain, BeforeCommit, Rescue) of
                                                        {error, Reason}=Error ->
                                                            lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [blockchain_block:hash_block(Block), Reason]),
                                                            Error;
                                                        ok ->
                                                            try
                                                                ok = blockchain_ledger_v1:maybe_gc_pocs(Blockchain)
                                                            catch C:E ->
                                                                    lager:info("poc gc failed with ~p:~p", [C,E]),
                                                                    ok
                                                            end,
                                                            Ledger = blockchain:ledger(Blockchain),
                                                            case blockchain_ledger_v1:new_snapshot(Ledger) of
                                                                {error, Reason}=Error ->
                                                                    lager:error("Error creating snapshot, Reason: ~p", [Reason]),
                                                                    Error;
                                                                {ok, NewLedger} ->
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

add_assumed_valid_block({AssumedValidHash, AssumedValidHeight}, Block, Blockchain=#blockchain{db=DB, blocks=BlocksCF, temp_blocks=TempBlocksCF}, Syncing) ->
    Hash = blockchain_block:hash_block(Block),
    Height = blockchain_block:height(Block),
    %% check if this is the block we've been waiting for
    case AssumedValidHash == Hash andalso AssumedValidHeight == Height of
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
        false when Hash /= AssumedValidHash andalso Height < AssumedValidHeight ->
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
            end;
        false ->
            {error, block_higher_than_assumed_valid_height}
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
    BeforeCommit = fun() ->
                           lager:info("adding block ~p", [Height]),
                           ok = ?save_block(Block, Blockchain)
                   end,
    case blockchain_txn:unvalidated_absorb_and_commit(Block, Blockchain, BeforeCommit, blockchain_block:is_rescue_block(Block)) of
        {error, Reason}=Error ->
            lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [blockchain_block:hash_block(Block), Reason]),
            Error;
        ok ->
            case blockchain_ledger_v1:new_snapshot(blockchain:ledger(Blockchain)) of
                {error, Reason}=Error ->
                    lager:error("Error creating snapshot, Reason: ~p", [Reason]),
                    Error;
                {ok, NewLedger} ->
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
    reset_ledger(Height, Chain, false).

reset_ledger(Chain, Validate) when is_boolean(Validate) ->
    {ok, Height} = height(Chain),
    reset_ledger(Height, Chain, Validate);
reset_ledger(Height, Chain) ->
    reset_ledger(Height, Chain, false).

reset_ledger(Height,
             #blockchain{db = DB,
                         dir = Dir,
                         default = DefaultCF,
                         blocks = BlocksCF,
                         heights = HeightsCF} = Chain,
             Revalidate) ->
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
            {ok, Ledger1} = blockchain_worker:new_ledger(Dir),
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
                              case Revalidate of
                                  false ->
                                      ok = blockchain_txn:unvalidated_absorb_and_commit(Block, CAcc, fun() -> ok end,
                                                                                        blockchain_block:is_rescue_block(Block));
                                  true ->
                                      ok = blockchain_txn:absorb_and_commit(Block, CAcc,  fun() -> ok end,
                                                                            blockchain_block:is_rescue_block(Block))
                              end,
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

last_block_add_time(#blockchain{default=DefaultCF, db=DB}) ->
    case rocksdb:get(DB, DefaultCF, ?LAST_BLOCK_ADD_TIME, []) of
        {ok, <<Time:64/integer-unsigned-little>>} ->
            Time;
        _ ->
            0
    end;
last_block_add_time(_) ->
    0.

check_common_keys(Blockchain) ->
    case genesis_block(Blockchain) of
        {ok, _} ->
            case head_block(Blockchain) of
                {ok, _} ->
                    ok;
                _ ->
                    case head_hash(Blockchain) of
                        {ok, _} ->
                            {error, missing_head_block};
                        _ ->
                            {error, missing_head_hash}
                    end
            end;
        _ ->
            case genesis_hash(Blockchain) of
                {ok, _} ->
                    {error, missing_genesis_block};
                _ ->
                    {error, missing_genesis_hash}
            end
    end.

check_recent_blocks(Blockchain) ->
    Ledger = ledger(Blockchain),
    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
    {ok, DelayedLedgerHeight} = blockchain_ledger_v1:current_height(blockchain_ledger_v1:mode(delayed, Ledger)),
    case get_block(DelayedLedgerHeight, Blockchain) of
        {ok, DelayedHeadBlock} ->
            DelayedHeadHash = blockchain_block_v1:hash_block(DelayedHeadBlock),
            case get_block(LedgerHeight, Blockchain) of
                {ok, HeadBlock} ->
                    Hashes = build_hash_chain(DelayedHeadHash, HeadBlock, Blockchain, Blockchain#blockchain.blocks),
                    case get_block(hd(Hashes), Blockchain) of
                        {ok, FirstBlock} ->
                            case blockchain_block:prev_hash(FirstBlock) == DelayedHeadHash of
                                true ->
                                    ok;
                                false ->
                                    case length(Hashes) == (LedgerHeight - DelayedLedgerHeight) of
                                        true ->
                                            %% this is real bad
                                            {error, {noncontiguous_recent_blocks, length(Hashes), LedgerHeight, DelayedLedgerHeight}};
                                        false ->
                                            %% get the head of the hash chain we managed to make
                                            case get_block(hd(Hashes), Blockchain) of
                                                {ok, Block} ->
                                                    {error, {missing_block, blockchain_block:height(Block) - 1}};
                                                _ ->
                                                    case get_block(hd(tl(Hashes)), Blockchain) of
                                                        {ok, Block} ->
                                                            {error, {missing_block, blockchain_block:height(Block) - 1}};
                                                        _ ->
                                                            %% what the hell is happening
                                                            {error, space_wolves}
                                                    end
                                            end
                                    end
                            end;
                        _ ->
                            case get_block(hd(tl(Hashes)), Blockchain) of
                                {ok, Block} ->
                                    {error, {missing_block, blockchain_block:height(Block) - 1}};
                                _ ->
                                    %% what the hell is happening
                                    {error, space_wolves}
                            end
                    end;
                _ ->
                    {error, {missing_block, LedgerHeight}}
            end;
        _ ->
            {error, {missing_block, DelayedLedgerHeight}}
    end.

crosscheck(Blockchain) ->
    {ok, Height} = height(Blockchain),
    Ledger = ledger(Blockchain),
    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
    {ok, DelayedLedgerHeight} = blockchain_ledger_v1:current_height(blockchain_ledger_v1:mode(delayed, Ledger)),
    BlockDelay = blockchain_txn:block_delay(),
    %% check for the important keys like ?HEAD, ?GENESIS, etc
    case check_common_keys(Blockchain) of
        ok ->
            %% check we have all the blocks between the leading and lagging ledger
            case check_recent_blocks(Blockchain) of
                ok ->
                    %% check the delayed ledger is the right number of blocks behind
                    case LedgerHeight - DelayedLedgerHeight  of
                        Lag when Lag > BlockDelay ->
                            {error, {ledger_delayed_ledger_lag_too_large, Lag}};
                        Lag ->
                            %% compare the leading ledger and the lagging ledger advanced to the leading ledger's height for consistency
                            case ledger_at(LedgerHeight, Blockchain, true) of
                                {ok, RecalcLedger} ->
                                    {ok, FP} = blockchain_ledger_v1:raw_fingerprint(Ledger, true),
                                    {ok, RecalcFP} = blockchain_ledger_v1:raw_fingerprint(RecalcLedger, true),
                                    case FP == RecalcFP of
                                        false ->
                                            Mismatches = [ K || K <- maps:keys(FP), maps:get(K, FP) /= maps:get(K, RecalcFP), K /= <<"ledger_fingerprint">> ],
                                            MismatchesWithChanges = lists:map(fun(M) ->
                                                                                      X = blockchain_ledger_v1:cf_fold(fp_to_cf(M), fun({K, V}, Acc) -> maps:put(K, V, Acc) end, #{}, Ledger),
                                                                                      {AllKeys, Changes0} = blockchain_ledger_v1:cf_fold(fp_to_cf(M), fun({K, V}, {Keys, Acc}) ->
                                                                                                                                                              Acc1 = case maps:find(K, X) of
                                                                                                                                                                         {ok, V} ->
                                                                                                                                                                             Acc;
                                                                                                                                                                         {ok, Other} ->
                                                                                                                                                                             [{changed, K, Other, V}|Acc];
                                                                                                                                                                         error ->
                                                                                                                                                                             [{added, K, V}|Acc]
                                                                                                                                                                     end,
                                                                                                                                                              {[K|Keys], Acc1}
                                                                                                                                                      end, {[], []}, RecalcLedger),
                                                                                      Changes = maps:fold(fun(K, V, A) ->
                                                                                                                  [{deleted, K, V}|A]
                                                                                                          end, Changes0, maps:without(AllKeys, X)),
                                                                                      {fp_to_cf(M), Changes}
                                                                              end, Mismatches),
                                            {error, {fingerprint_mismatch, MismatchesWithChanges}};
                                        true ->
                                            %% check if the ledger is the same height as the chain
                                            case Height == LedgerHeight of
                                                false ->
                                                    {error, {chain_ledger_height_mismatch, Height, LedgerHeight}};
                                                true ->
                                                    case Lag < BlockDelay of
                                                        true ->
                                                            {error, {ledger_delayed_ledger_lag_too_small, Lag}};
                                                        false ->
                                                            ok
                                                    end
                                            end
                                    end;
                                Error ->
                                    Error
                            end
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

analyze(Blockchain) ->
  case crosscheck(Blockchain) of
      {error, {fingerprint_mismatch, Mismatches}} ->
          {error, {fingerprint_mismatch, lists:map(fun({M, Changes}) ->
                                                           Sum = lists:foldl(fun({changed, _, _, _}, Acc) ->
                                                                                     maps:update_with(changed, fun(V) -> V+1 end, 1, Acc);
                                                                                ({added, _, _}, Acc) ->
                                                                                     maps:update_with(added, fun(V) -> V+1 end, 1, Acc);
                                                                                ({deleted, _, _}, Acc) ->
                                                                                     maps:update_with(deleted, fun(V) -> V+1 end, 1, Acc)
                                                                             end, #{}, Changes),
                                                           {M, maps:to_list(Sum)}
                                                   end, Mismatches)}};
      E ->
          E
  end.

%% TODO it'd be nice if the fingerprint function was consistent with the column family names
fp_to_cf(<<"core_fingerprint">>) -> default;
fp_to_cf(<<"gateways_fingerprint">>) -> active_gateways;
fp_to_cf(<<"poc_fingerprint">>) -> pocs;
fp_to_cf(<<"entries_fingerprint">>) -> entries;
fp_to_cf(<<"dc_entries_fingerprint">>) -> dc_entries;
fp_to_cf(<<"securities_fingerprint">>) -> securities;
fp_to_cf(<<"htlc_fingerprint">>) -> htlcs;
fp_to_cf(<<"routings_fingerprint">>) -> routing.

repair(#blockchain{db=DB, default=DefaultCF} = Blockchain) ->
    case crosscheck(Blockchain) of
        {error, {fingerprint_mismatch, Mismatches}} ->
            Ledger = ledger(Blockchain),
            blockchain_ledger_v1:apply_raw_changes(Mismatches, Ledger);
        {error, {missing_block, Height}} ->
            %% we need to sync this block from someone
            %% first, we need to find the hash of the parent
            {ok, MaxHeight} = height(Blockchain),
            case {get_block(Height - 1, Blockchain), find_upper_height(Height+1, Blockchain, MaxHeight)} of
                {{ok, Block}, TopHeight} when TopHeight == Height + 1 ->
                    %% this is a bit experimental and thus we guard it behind a var
                    case application:get_env(blockchain, enable_missing_block_refetch, false) of
                        true ->
                            %% TODO it's possible we only lost the lookup by height, we might be able to find it by
                            %% the next block's parent hash
                            %%
                            %% flag this block as the block we must sync next
                            ok = rocksdb:put(DB, DefaultCF, ?MISSING_BLOCK, blockchain_block:hash_block(Block), [{sync, true}]),
                            {ok, {resyncing_block, Height}};
                        _ ->
                            {error, {missing_block, Height}}
                    end;
                {{ok, _Block}, TopHeight} ->
                    %% we can probably fix this but it's a bit complicated and hopefully rare
                    {error, {missing_too_many_blocks, Height, TopHeight}};
                Error ->
                    {error, {resyncing_block, Height, Error}}
            end;
        {error, {ledger_delayed_ledger_lag_too_large, Lag}} ->
            {ok, Block} = head_block(Blockchain),
            %% because of memory constraints, this can take a few applications to complete
            lists:foldl(fun(_, ok) ->
                                  blockchain_txn:absorb_delayed(Block, Blockchain);
                           (_, Err) ->
                                Err
                          end, ok, lists:seq(1, ceil(Lag / blockchain_txn:block_delay())));
        {error, {chain_ledger_height_mismatch,ChainHeight,LedgerHeight}} when ChainHeight > LedgerHeight ->
            maybe_continue_resync(Blockchain, true),
            ok;
        Other -> Other
    end.

find_upper_height(Height, Blockchain, Max) when Height =< Max ->
    case get_block(Height, Blockchain) of
        {ok, _} ->
            Height;
        _ ->
            find_upper_height(Height + 1, Blockchain, Max)
    end;
find_upper_height(Height, _, _) ->
    Height.

missing_block(#blockchain{db=DB, default=DefaultCF}) ->
    case application:get_env(blockchain, enable_missing_block_refetch, false) of
        true ->
            rocksdb:get(DB, DefaultCF, ?MISSING_BLOCK, []);
        false ->
            not_found
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
    DBOptions = [{create_if_missing, true}, {atomic_flush, true}] ++ GlobalOpts,
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
    ok = rocksdb:batch_put(Batch, DefaultCF, ?LAST_BLOCK_ADD_TIME, <<(erlang:system_time(second)):64/integer-unsigned-little>>),
    %% lexiographic ordering works better with big endian
    ok = rocksdb:batch_put(Batch, HeightsCF, <<Height:64/integer-unsigned-big>>, Hash).

save_temp_block(Block, #blockchain{db=DB, temp_blocks=TempBlocks, default=DefaultCF}=Chain) ->
    Hash = blockchain_block:hash_block(Block),
    case rocksdb:get(DB, TempBlocks, Hash, []) of
        {ok, _} ->
            %% already got it, thanks
            ok;
        _ ->
            case get_block(Hash, Chain) of
                {ok, _} ->
                    %% have it on the main chain
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
                    ok = rocksdb:batch_put(Batch, DefaultCF, ?LAST_BLOCK_ADD_TIME, <<(erlang:system_time(second)):64/integer-unsigned-little>>),
                    ok = rocksdb:write_batch(DB, Batch, [{sync, true}])
            end
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
    maybe_continue_resync(Blockchain);
init_assumed_valid(Blockchain, HashAndHeight={Hash, Height}) when is_binary(Hash), is_integer(Height) ->
    case ?MODULE:get_block(Hash, Blockchain) of
        {ok, _} ->
            %% already got it, chief
            %% TODO make sure we delete the column family, in case it has stale crap in it!
            maybe_continue_resync(Blockchain);
        _ ->
            %% set this up here, it will get cleared if we're able to add all the assume valid blocks
            ok = persistent_term:put(?ASSUMED_VALID, HashAndHeight),
            #blockchain{db=DB, temp_blocks=TempBlocksCF} = Blockchain,
            %% the chain and the ledger need to be in sync for this to have any chance of working
            IsInSync = blockchain:height(Blockchain) == blockchain_ledger_v1:current_height(blockchain:ledger(Blockchain)),
            case rocksdb:get(DB, TempBlocksCF, Hash, []) of
                {ok, BinBlock} when IsInSync == true ->
                    %% we already have it, try to process it
                    Block = blockchain_block:deserialize(BinBlock),
                    case blockchain_block:height(Block) == Height of
                        true ->
                            %% spawn a worker process to do this in the background so we don't
                            %% block application startup
                            spawn_link(fun() ->
                                               blockchain_lock:acquire(),
                                               try add_assumed_valid_block(HashAndHeight, Block, Blockchain, false) of
                                                   ok ->
                                                       %% we did it!
                                                       ok;
                                                   {error, Reason} ->
                                                       %% something went wrong!
                                                       lager:warning("assume valid processing failed on init with assumed_valid hash present ~p", [Reason]),
                                                       %% TODO we should probably drop the column family here?
                                                       ok
                                               catch C:E:S ->
                                                       lager:warning("assume valid processing failed on init with assumed_valid hash present ~p:~p ~p", [C, E, S]),
                                                       ok
                                               after
                                                   blockchain_lock:force_release()
                                               end
                                       end),
                            Blockchain;
                        false ->
                            %% block must be bad somehow
                            rocksdb:delete(DB, TempBlocksCF, Hash, []),
                            maybe_continue_resync(Blockchain)
                    end;
                _ ->
                    %% need to wait for it
                    ok = persistent_term:put(?ASSUMED_VALID, HashAndHeight),
                    maybe_continue_resync(Blockchain)
            end
    end.

maybe_continue_resync(Blockchain) ->
    maybe_continue_resync(Blockchain, false).

maybe_continue_resync(Blockchain, Blocking) ->
    case {blockchain:height(Blockchain), blockchain_ledger_v1:current_height(blockchain:ledger(Blockchain))} of
        {X, X} ->
            %% they're the same, everything is great
            Blockchain;
        {{ok, ChainHeight}, {ok, LedgerHeight}} when ChainHeight > LedgerHeight ->
            lager:warning("blockchain height ~p is ahead of ledger height ~p~n", [ChainHeight, LedgerHeight]),
            %% likely an interrupted resync
            case Blocking of
                true ->
                    %% block the caller
                    resync_fun(ChainHeight, LedgerHeight, Blockchain);
                false ->
                    spawn_link(fun() -> resync_fun(ChainHeight, LedgerHeight, Blockchain) end)
            end,
            Blockchain;
        {{ok, ChainHeight}, {ok, LedgerHeight}} when ChainHeight < LedgerHeight ->
            %% we're likely in a real pickle here unless we're just a handful of blocks behind
            %% and we can use the delayed ledger to save ourselves
            lager:warning("ledger height ~p is ahead of blockchain height ~p", [LedgerHeight, ChainHeight]),
            Blockchain;
        _ ->
            %% probably no chain or ledger
            Blockchain
    end.


resync_fun(ChainHeight, LedgerHeight, Blockchain) ->
    blockchain_lock:acquire(),
    case get_block(LedgerHeight, Blockchain) of
        {error, _} ->
            %% chain is missing the block the ledger is stuck at
            %% it is unclear what we should do here.
            lager:warning("cannot resume ledger resync, missing block ~p", [LedgerHeight]),
            blockchain_lock:release();
        {ok, LedgerLastBlock} ->
            {ok, StartBlock} = blockchain:head_block(Blockchain),
            EndHash = blockchain_block:hash_block(LedgerLastBlock),
            HashChain = build_hash_chain(EndHash, StartBlock, Blockchain, Blockchain#blockchain.blocks),
            LastKnownBlock = case get_block(hd(HashChain), Blockchain) of
                                 {ok, LKB} ->
                                     LKB;
                                 {error, not_found} ->
                                     {ok, LKB} = get_block(hd(tl(HashChain)), Blockchain),
                                     LKB
                             end,
            %% check we have a contiguous chain back to the current ledger head from the blockchain head
            case length(HashChain) == (ChainHeight - LedgerHeight) andalso
                 EndHash == blockchain_block:prev_hash(LastKnownBlock)  of
                false ->
                    %% we are missing one of the blocks between the ledger height and the chain height
                    %% we can probably find the highest contiguous chain and resync to there and repair the chain by rewinding
                    %% the head and deleting the orphaned blocks
                    lager:warning("cannot resume ledger resync, missing block ~p", [blockchain_block:height(LastKnownBlock)]),
                    blockchain_lock:release();
                true ->
                    %% ok, we can keep replying blocks
                    try
                        lists:foreach(fun(Hash) ->
                                              {ok, Block} = get_block(Hash, Blockchain),
                                              lager:info("absorbing block ~p", [blockchain_block:height(Block)]),
                                              ok = blockchain_txn:unvalidated_absorb_and_commit(Block, Blockchain, fun() -> ok end, blockchain_block:is_rescue_block(Block))
                                      end, HashChain)
                    after
                        blockchain_lock:release()
                    end
            end
    end.


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
    meck:new(blockchain_swarm, [passthrough]),
    meck:expect(blockchain_swarm, pubkey_bin, fun() ->
        crypto:strong_rand_bytes(33)
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
    meck:unload(blockchain_election),
    ?assert(meck:validate(blockchain_swarm)),
    meck:unload(blockchain_swarm).



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
    meck:new(blockchain_swarm, [passthrough]),
    meck:expect(blockchain_swarm, pubkey_bin, fun() ->
        crypto:strong_rand_bytes(33)
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
    meck:unload(blockchain_election),
    ?assert(meck:validate(blockchain_swarm)),
    meck:unload(blockchain_swarm).


-endif.
