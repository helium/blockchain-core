%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain).

-include_lib("kernel/include/file.hrl").

-export([
    new/4, integrate_genesis/2,
    genesis_hash/1 ,genesis_block/1,
    head_hash/1, head_block/1, head_block_info/1,
    sync_hash/1,
    height/1,
    sync_height/1,
    ledger/0, ledger/1, ledger/2, ledger_at/2, ledger_at/3,
    dir/1,

    blocks/1,
    get_block/2, get_block_hash/2, get_block_hash/3, get_block_height/2, get_raw_block/2,
    put_block_height/3,
    put_block_info/3, get_block_info/2,
    mk_block_info/2,
    save_block/2,
    has_block/2,
    find_first_block_after/2,

    add_blocks/2, add_blocks/3, add_block/2, add_block/3,
    delete_block/2, rocksdb_gc/2,
    config/2,
    fees_since/2,
    build/3,
    close/1,
    compact/1,

    is_block_plausible/2,
    get_plausible_block/2,
    get_raw_plausibles/2,
    have_plausible_block/2,
    save_plausible_blocks/2,
    check_plausible_blocks/2,

    last_block_add_time/1,

    resync_fun/3,
    absorb_temp_blocks_fun/3,
    delete_temp_blocks/1,

    analyze/1, repair/1,
    crosscheck/1, crosscheck/2,
    compare/2,

    fold_chain/4,

    reset_ledger/1, reset_ledger/2, reset_ledger/3,
    reset_ledger_to_snap/0,

    replay_blocks/4,

    init_assumed_valid/2,

    add_gateway_txn/4,
    assert_loc_txn/4, assert_loc_txn/6,

    add_snapshot/2, add_bin_snapshot/4,
    have_snapshot/2, get_snapshot/2, find_last_snapshot/1,
    find_last_snapshots/2,

    add_implicit_burn/3,
    get_implicit_burn/2,

    add_htlc_receipt/3,
    get_htlc_receipt/2,

    mark_upgrades/2, unmark_upgrades/2, get_upgrades/1, bootstrap_h3dex/1,
    snapshot_height/1,

    db_handle/1,
    blocks_cf/1,
    heights_cf/1,
    info_cf/1

]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-export([bootstrap_hexes/1, can_add_block/2, get_plausible_blocks/1, clean/1]).
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
    info :: rocksdb:cf_handle(),
    temp_blocks :: rocksdb:cf_handle(),
    plausible_blocks :: rocksdb:cf_handle(),
    snapshots :: rocksdb:cf_handle(),
    implicit_burns :: rocksdb:cf_handle(),
    htlc_receipts :: rocksdb:cf_handle(),
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
-define(OP_SIZE, (1024*1024)). % output garbage collection in megabytes

-define(BC_UPGRADE_FUNS, [fun upgrade_gateways_v2/1,
                          fun bootstrap_hexes/1,
                          fun upgrade_gateways_oui/1,
                          %% NOTE: Duplication is not an error, we do this because
                          %% we have had to delete a previously build h3dex so we are
                          %% reinitializing it with a different name specified in the hrl
                          fun bootstrap_h3dex/1,
                          fun bootstrap_h3dex/1,
                          fun upgrade_gateways_lg/1,
                          fun clear_witnesses/1,
                          fun upgrade_gateways_score/1,
                          fun upgrade_gateways_score/1,
                          fun upgrade_nonce_rescue/1,
                          fun blockchain_ledger_v1:upgrade_pocs/1]).

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
          QuickSyncMode :: assumed_valid | blessed_snapshot,
          QuickSyncData :: {blockchain_block:hash(), pos_integer()}
         ) -> {ok, blockchain()} | {no_genesis, blockchain()}.
new(Dir, Genesis, QuickSyncMode, QuickSyncData) when is_list(Genesis) ->
    case load_genesis(Genesis) of
        {error, _Reason} ->
            lager:warning("could not load genesis file: ~p ~p", [Genesis, _Reason]),
            ?MODULE:new(Dir, undefined, QuickSyncMode, QuickSyncData);
        {ok, Block} ->
            ?MODULE:new(Dir, Block, QuickSyncMode, QuickSyncData)
    end;
new(Dir, undefined, QuickSyncMode, QuickSyncData) ->
    lager:info("loading blockchain from ~p", [Dir]),
    case load(Dir, QuickSyncMode) of
        {error, {db_open,"Corruption:" ++ _Reason}} ->
            lager:error("DB could not be opened corrupted ~p, cleaning up", [_Reason]),
            ok = clean(Dir),
            new(Dir, undefined, QuickSyncMode, QuickSyncData);
        {Blockchain, {error, {corruption, _Corrupted}}} ->
            lager:error("DB corrupted cleaning up ~p", [_Corrupted]),
            ok = clean(Blockchain),
            new(Dir, undefined, QuickSyncMode, QuickSyncData);
        {Blockchain, {error, _Reason}} ->
            lager:info("no genesis block found: ~p", [_Reason]),
            {no_genesis, Blockchain};
        {Blockchain, {ok, _GenBlock}} ->
            lager:info("stuff is ok"),
            {ok, init_quick_sync(QuickSyncMode, Blockchain, QuickSyncData)}
    end;
new(Dir, GenBlock, QuickSyncMode, QuickSyncData) ->
    lager:info("loading blockchain from ~p and checking ~p", [Dir, GenBlock]),
    case load(Dir, QuickSyncMode) of
        {error, {db_open,"Corruption:" ++ _Reason}} ->
            lager:error("DB could not be opened corrupted ~p, cleaning up", [_Reason]),
            ok = clean(Dir),
            new(Dir, GenBlock, QuickSyncMode, QuickSyncData);
        {Blockchain, {error, {corruption, _Corrupted}}} ->
            lager:error("DB corrupted cleaning up ~p", [_Corrupted]),
            ok = clean(Blockchain),
            new(Dir, GenBlock, QuickSyncMode, QuickSyncData);
        {Blockchain, {error, Reason}} ->
            lager:warning("failed to load genesis block ~p, integrating new one", [Reason]),
            try ?MODULE:integrate_genesis(GenBlock, Blockchain) of
                ok ->
                    Ledger = blockchain:ledger(Blockchain),
                    mark_upgrades(?BC_UPGRADE_NAMES, Ledger),
                    {ok, init_quick_sync(QuickSyncMode, Blockchain, QuickSyncData)}
            catch What:Why ->
                    lager:warning("failed to integrate genesis block ~p, wiping chain", [{What, Why}]),
                    ok = clean(Blockchain),
                    new(Dir, GenBlock, QuickSyncMode, QuickSyncData)
            end;
        {Blockchain, {ok, GenBlock}} ->
            lager:info("new gen = old gen"),
            {ok, init_quick_sync(QuickSyncMode, Blockchain, QuickSyncData)};
        {Blockchain, {ok, _OldGenBlock}} ->
            lager:info("replacing old genesis block with new one"),
            ok = clean(Blockchain),
            new(Dir, GenBlock, QuickSyncMode, QuickSyncData)
    end.

process_upgrades(Chain) ->
    Ledger = blockchain:ledger(Chain),
    Upgrades = lists:zip(?BC_UPGRADE_NAMES, ?BC_UPGRADE_FUNS),
    ok = process_upgrades(Upgrades, Ledger),
    Chain.

process_upgrades([], _Ledger) ->
    ok;
process_upgrades([{Key, Fun} | Tail], Ledger) ->
    case blockchain_ledger_v1:check_key(Key, Ledger) of
        true ->
            ok;
        false ->
            lager:info("running ledger upgrade ~p", [Key]),
            Ledger1 = blockchain_ledger_v1:new_context(Ledger),
            Fun(Ledger1),
            blockchain_ledger_v1:mark_key(Key, Ledger1),
            blockchain_ledger_v1:commit_context(Ledger1),
            Ledger2_0 = blockchain_ledger_v1:mode(delayed, Ledger),
            Ledger2 = blockchain_ledger_v1:new_context(Ledger2_0),
            Fun(Ledger2),
            blockchain_ledger_v1:commit_context(Ledger2)
    end,
    process_upgrades(Tail, Ledger).

mark_upgrades(Upgrades, Ledger) ->
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    lists:foreach(fun(Key) ->
                          blockchain_ledger_v1:mark_key(Key, Ledger1)
                  end, Upgrades),
    blockchain_ledger_v1:commit_context(Ledger1),
    ok.

unmark_upgrades(Upgrades, Ledger) ->
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    lists:foreach(fun(Key) ->
                          blockchain_ledger_v1:unmark_key(Key, Ledger1)
                  end, Upgrades),
    blockchain_ledger_v1:commit_context(Ledger1),
    ok.

upgrade_gateways_v2(Ledger) ->
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
              blockchain_ledger_v1:update_gateway(G, G1, A, Ledger)
      end, Gateways),
    ok.

upgrade_gateways_lg(Ledger) ->
    blockchain_ledger_v1:cf_fold(
      active_gateways,
      fun({Addr, BinGw}, _) ->
              %% deser will do the conversion
              Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
              %% check if re-serialization changes and only write if so
              case blockchain_ledger_gateway_v2:serialize(Gw) of
                  BinGw -> ok;
                  _ ->
                      blockchain_ledger_v1:update_gateway(new, Gw, Addr, Ledger)
              end
      end,
      whatever,
      Ledger).

upgrade_gateways_score(Ledger) ->
    case blockchain:config(?election_version, Ledger) of
        %% election v4 removed score from consideration
        {ok, EV} when EV >= 4 ->
            blockchain_ledger_v1:cf_fold(
              active_gateways,
              fun({Addr, BinGw}, _) ->
                      Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
                      Gw1 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(0.0, 0.0, 0, Gw),
                      case blockchain_ledger_gateway_v2:serialize(Gw1) of
                          BinGw -> ok;
                          _ ->
                              blockchain_ledger_v1:update_gateway(Gw, Gw1, Addr, Ledger)
                      end
              end,
              whatever,
              Ledger);
        _ -> ok
    end.

%% we need this to respond to a particular desync halt caused by a rescue block and then a var with
%% a messed up nonce, hence the particular heights.  we mark as complete if we're past it so we
%% never run again
upgrade_nonce_rescue(Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case blockchain_ledger_v1:vars_nonce(Ledger) of
        {ok, Nonce} ->
            case blockchain_ledger_v1:mode(Ledger) of
                delayed ->
                    %% note the 4 ------v
                    case Height =< 1107944 andalso Nonce == 106 of
                        true ->
                            ok = blockchain_ledger_v1:vars_nonce(105, Ledger);
                        false ->
                            ok
                    end;
                _ActiveOrAux ->
                    %% note the 9 ------v
                    case Height =< 1107994 andalso Nonce == 106 of
                        true ->
                            ok = blockchain_ledger_v1:vars_nonce(105, Ledger);
                        false ->
                            ok
                    end
            end;
        %% starting a new chain, just ignore this
        {error, not_found} ->
            ok
    end.

-spec get_upgrades(blockchain_ledger_v1:ledger()) -> [binary()].
get_upgrades(Ledger) ->
    [ Key || Key <- ?BC_UPGRADE_NAMES, blockchain_ledger_v1:check_key(Key, Ledger) ].

bootstrap_hexes(Ledger) ->
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

upgrade_gateways_oui(Ledger) ->
    %% the initial load here will automatically convert these into
    %% records with oui slots
    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
    %% find all neighbors for everyone
    maps:map(
      fun(A, G) ->
              blockchain_ledger_v1:update_gateway(new, G, A, Ledger)
      end, Gateways),
    ok.

clear_witnesses(Ledger) ->
    blockchain_ledger_v1:cf_fold(
      active_gateways,
      fun({Addr, BinGw}, _) ->
              %% deser will do the conversion
              Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
              Gw1 = blockchain_ledger_gateway_v2:clear_witnesses(Gw),
              Gw2 = blockchain_ledger_gateway_v2:last_location_nonce(0, Gw1),
              %% check if re-serialization changes and only write if so
              case blockchain_ledger_gateway_v2:serialize(Gw2) of
                  BinGw -> ok;
                  _ ->
                      blockchain_ledger_v1:update_gateway(Gw, Gw2, Addr, Ledger)
              end
      end,
      whatever,
      Ledger).

-spec bootstrap_h3dex(blockchain_ledger_v1:ledger()) -> ok.
%% @doc Bootstrap the H3Dex for both the active and delayed ledgers
bootstrap_h3dex(Ledger) ->
    blockchain_ledger_v1:bootstrap_h3dex(Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
integrate_genesis(GenesisBlock, #blockchain{db=DB, default=DefaultCF}=Blockchain) ->
    GenHash = blockchain_block:hash_block(GenesisBlock),
    ok = blockchain_txn:absorb_and_commit(GenesisBlock, Blockchain, fun(_, _) ->
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

-spec head_block_info(blockchain()) ->
          {ok, #block_info_v2{}} | {error, any()}.
head_block_info(Blockchain) ->
    case ?MODULE:head_hash(Blockchain) of
        {error, _}=Error ->
            Error;
        {ok, Hash} ->
            {ok, Height} = get_block_height(Hash, Blockchain),
            get_block_info(Height, Blockchain)
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
            get_block_height(Hash, Blockchain)
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

-spec ledger() -> blockchain_ledger_v1:ledger().
ledger() ->
    ledger(blockchain_worker:blockchain()).

-spec ledger(blockchain()) -> blockchain_ledger_v1:ledger().
ledger(#blockchain{ledger=Ledger}) ->
    Ledger.

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
    Ledger0 = ?MODULE:ledger(Chain0),
    Ledger = case blockchain_ledger_v1:mode(Ledger0) of
        delayed ->
            blockchain_ledger_v1:mode(active, Ledger0);
        _ ->
            Ledger0
    end,
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
                        R ->
                            %% remove a context if we created one we don't need
                            case R of
                                {ok, UnusedLedger} -> blockchain_ledger_v1:delete_context(UnusedLedger);
                                _ ->
                                    ok
                            end,
                            case fold_blocks(Chain0, DelayedHeight, DelayedLedger, Height, ForceRecalc) of
                                {ok, Chain1} ->
                                    Ledger1 = ?MODULE:ledger(Chain1),
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

fold_blocks(Chain0, DelayedHeight, DelayedLedger, Height, ForceRecalc) ->
    %% to minimize work, check backwards for snapshots
    {HighestSnapHeight, HighestLedger} =
        lists:foldl(
          fun(_, Acc) when is_tuple(Acc) ->
                  Acc;
             (H, none) when H == (DelayedHeight+1); ForceRecalc == true ->
                  {DelayedHeight, blockchain_ledger_v1:new_context(DelayedLedger)};
             (H, none) ->
                  case blockchain_ledger_v1:has_snapshot(H, DelayedLedger) of
                      {ok, Snap} ->
                          {H, Snap};
                      _ ->
                          none
                  end
          end,
          none,
          lists:seq(Height, DelayedHeight+1, -1)),
    lists:foldl(
      fun(H, {ok, ChainAcc}) ->
              case ?MODULE:get_block(H, Chain0) of
                  {ok, Block} ->
                      case blockchain_txn:absorb_block(Block, ChainAcc) of
                          {ok, Chain1} ->
                              {ok, H} = blockchain_ledger_v1:current_height(blockchain:ledger(Chain1)),
                              Hash = blockchain_block:hash_block(Block),
                              ok = run_gc_hooks(ChainAcc, Hash),

                              case ForceRecalc of
                                  false ->
                                      %% take an intermediate snapshot here to
                                      %% make things faster in the future
                                      Ledger1 = ?MODULE:ledger(Chain1),
                                      {ok, NewLedger} = blockchain_ledger_v1:context_snapshot(Ledger1),
                                      {ok, blockchain:ledger(NewLedger, Chain1)};
                                  _ ->
                                      {ok, Chain1}
                              end;
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
      {ok, ?MODULE:ledger(HighestLedger, Chain0)},
      lists:seq(HighestSnapHeight+1, Height)
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

get_block_hash(Height, Chain) ->
    get_block_hash(Height, Chain, true).

get_block_hash(Height, #blockchain{db=DB, heights=HeightsCF} = Chain, Fallback) ->
    case rocksdb:get(DB, HeightsCF, <<Height:64/integer-unsigned-big>>, []) of
       {ok, Hash} ->
            {ok, Hash};
        not_found when Fallback == false ->
            {error, not_found};
        not_found when Fallback == true ->
            case get_block_info(Height, Chain) of
                {ok, #block_info_v2{hash = Hash}} ->
                    {ok, Hash};
                {error, not_found} ->
                    {error, not_found}
            end;
        Error ->
            Error
    end.

-spec get_block_height(Hash :: blockchain_block:hash(), Blockchain :: blockchain()) -> {ok, non_neg_integer()} | {error, any()}.
get_block_height(Hash, #blockchain{db=DB, heights=HeightsCF, blocks=BlocksCF}) ->
    case rocksdb:get(DB, HeightsCF, Hash, []) of
        {ok, <<Height:64/integer-unsigned-big>>} ->
            {ok, Height};
        not_found ->
            case rocksdb:get(DB, BlocksCF, Hash, []) of
                {ok, BinBlock} ->
                    Height = blockchain_block:height(blockchain_block:deserialize(BinBlock)),
                    ok = rocksdb:put(DB, HeightsCF, Hash, <<Height:64/integer-unsigned-big>>, []),
                    {ok, Height};
                not_found ->
                    {error, not_found};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

put_block_height(Hash, Height, #blockchain{db=DB, heights=HeightsCF}) ->
    rocksdb:put(DB, HeightsCF, Hash, <<Height:64/integer-unsigned-big>>, []).

-spec put_block_info(Height :: pos_integer(),
                     Info :: #block_info{} | #block_info_v2{},
                     Blockchain :: blockchain()) ->
          ok | {error, any()}.
put_block_info(Height, Info, _Chain = #blockchain{db=DB, info=InfoCF}) ->
    rocksdb:put(DB, InfoCF, <<Height:64/integer-unsigned-big>>, serialize_block_info(Info), []).

-spec get_block_info(Height :: pos_integer(), Blockchain :: blockchain()) ->
          {ok, #block_info_v2{}} | {error, any()}.
get_block_info(Height, Chain = #blockchain{db=DB, info=InfoCF}) ->
    case rocksdb:get(DB, InfoCF, <<Height:64/integer-unsigned-big>>, []) of
        {ok, BinInfo} ->
            {ok, deserialize_block_info(BinInfo, Chain)};
        not_found ->
            case get_block(Height, Chain) of
                {ok, Block} ->
                    Hash = blockchain_block:hash_block(Block),
                    Info = mk_block_info(Hash, Block),
                    InfoBin = serialize_block_info(Info),
                    ok = rocksdb:put(DB, InfoCF, <<Height:64/integer-unsigned-big>>, InfoBin, []),
                    {ok, Info};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


-spec mk_block_info(blockchain_block:hash(), blockchain_block:block()) -> #block_info_v2{}.
mk_block_info(Hash, Block) ->
    PoCs = lists:flatmap(
             fun(Txn) ->
                     case blockchain_txn:type(Txn) of
                         blockchain_txn_poc_request_v1 ->
                             [{blockchain_txn_poc_request_v1:onion_key_hash(Txn),
                               blockchain_txn_poc_request_v1:block_hash(Txn)}];
                         _ -> []
                     end
             end,
             blockchain_block:transactions(Block)),

    #block_info_v2{time = blockchain_block:time(Block),
                   hash = Hash,
                   height = blockchain_block:height(Block),
                   pocs = maps:from_list(PoCs),
                   hbbft_round = blockchain_block:hbbft_round(Block),
                   election_info = blockchain_block_v1:election_info(Block),
                   penalties = {blockchain_block_v1:bba_completion(Block), blockchain_block_v1:seen_votes(Block)}}.

-spec serialize_block_info(#block_info{} | #block_info_v2{}) -> binary().
serialize_block_info(BlockInfo) ->
    erlang:term_to_binary(BlockInfo).

-spec deserialize_block_info(binary() | #block_info{} | #block_info_v2{}, blockchain())-> #block_info_v2{}.
deserialize_block_info(Bin, Chain) when is_binary(Bin)->
    deserialize_block_info(erlang:binary_to_term(Bin), Chain);
deserialize_block_info(V1BlockInfo = #block_info{height = Height}, Chain) ->
    case get_block(Height, Chain) of
        {ok, Block} ->
            upgrade_block_info(V1BlockInfo, Block, Chain);
        _Error ->
            %% if we dont have the block stub out the upgraded fields
            #block_info{time = Time, hash = Hash, pocs = PoCs} = V1BlockInfo,
            #block_info_v2{time = Time,
                           hash = Hash,
                           height = Height,
                           pocs = PoCs,
                           hbbft_round = 0,
                           election_info = {0,0},
                           penalties = {<<>>, []}}

    end;
deserialize_block_info(V2BlockInfo = #block_info_v2{}, _Chain) ->
    V2BlockInfo.

-spec upgrade_block_info(#block_info{}, blockchain_block_v1:block(),  blockchain()) -> #block_info_v2{}.
upgrade_block_info(#block_info{hash = Hash, height = Height}, Block, Chain = #blockchain{db=DB, info=InfoCF}) ->
    Info = mk_block_info(Hash, Block),
    InfoBin = serialize_block_info(Info),
    ok = rocksdb:put(DB, InfoCF, <<Height:64/integer-unsigned-big>>, InfoBin, []),
    deserialize_block_info(InfoBin, Chain).

%% @doc read blocks from the db without deserializing them
-spec get_raw_block(blockchain_block:hash() | integer(), blockchain()) ->
    {ok, binary()} | not_found | {error, any()}.
get_raw_block(<<Hash/binary>>, #blockchain{db=DB, blocks=BlocksCF}) ->
    rocksdb:get(DB, BlocksCF, Hash, []);
get_raw_block(Height, #blockchain{db=DB, heights=HeightsCF}=Blockchain) when is_integer(Height) ->
    case rocksdb:get(DB, HeightsCF, <<Height:64/integer-unsigned-big>>, []) of
       {ok, Hash} ->
           ?MODULE:get_raw_block(Hash, Blockchain);
        Other ->
            Other
    end.

%% checks if we have this block in any of the places we store blocks
%% note that if we only use this for gossip we will never see assume valid blocks
%% so we don't need to check for them
-spec has_block(BlockOrHash :: blockchain_block:block() | blockchain_block:hash(), blockchain()) -> boolean().
has_block(Hash, #blockchain{db=DB, info=InfoCF,
                             plausible_blocks=PlausibleBlocks}) when is_binary(Hash) ->
    case rocksdb:get(DB, InfoCF, Hash, []) of
        {ok, _} ->
            true;
        not_found ->
            case rocksdb:get(DB, PlausibleBlocks, Hash, []) of
                {ok, _} ->
                    true;
                not_found ->
                    false;
                _Error ->
                    false
            end;
        _Error ->
            false
    end;
has_block(Block, Chain) ->
    Hash = blockchain_block:hash_block(Block),
    has_block(Hash, Chain).


find_first_height_after(MinHeight0, #blockchain{db=DB, heights=HeightsCF}) ->
    MinHeight = max(0, MinHeight0),
    {ok, Iter} = rocksdb:iterator(DB, HeightsCF, []),
    rocksdb:iterator_move(Iter, {seek, <<(MinHeight):64/integer-unsigned-big>>}),
    case rocksdb:iterator_move(Iter, next) of
        {ok, <<Height:64/integer-unsigned-big>>, Hash} ->
            {ok, Height, Hash};
        {error, _} ->
            {error, not_found}
    end.

find_first_block_after(MinHeight, Blockchain) ->
    case find_first_height_after(MinHeight, Blockchain) of
        {ok, Height, Hash} ->
            case get_block(Hash, Blockchain) of
                {ok, Block} ->
                    {ok, Height, Block};
                Other ->
                    Other
            end;
        {error, _} ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_blocks([blockchain_block:block()], blockchain()) -> ok | exists | plausible | {error, any()}.
add_blocks(Blocks, Chain) ->
    add_blocks(Blocks, <<>>, Chain).

add_blocks(Blocks, GossipedHash, Chain) ->
    blockchain_lock:acquire(),
    try
        Res = add_blocks_(Blocks, GossipedHash, Chain),
        check_plausible_blocks(Chain, GossipedHash),
        Res
    catch C:E:S ->
            lager:warning("crash adding blocks: ~p:~p ~p", [C, E, S]),
            {error, add_blocks_error}
    after
        blockchain_lock:release()
    end.

add_blocks_([], _, _Chain) ->  ok;
add_blocks_([Block | Blocks], GossipedHash, Chain) ->
    case ?MODULE:add_block(Block, Chain, GossipedHash /= blockchain_block:hash_block(Block)) of
        Res when Res == ok; Res == plausible; Res == exists ->
            add_blocks_(Blocks, GossipedHash, Chain);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_block(blockchain_block:block(), blockchain()) -> ok | exists | plausible | {error, any()}.
add_block(Block, Blockchain) ->
    add_block(Block, Blockchain, false).

-spec add_block(blockchain_block:block(), blockchain(), boolean()) -> ok | exists | plausible | {error, any()}.
add_block(Block, #blockchain{db=DB, blocks=BlocksCF, heights=HeightsCF, default=DefaultCF} = Blockchain, Syncing) ->
    blockchain_lock:acquire(),
    try
        PrevHash = blockchain_block:prev_hash(Block),
        case missing_block(Blockchain) of
            {ok, PrevHash} ->
                %% this is the block we've been missing
                Height = blockchain_block:height(Block),
                Hash = blockchain_block:hash_block(Block),
                %% check if it fits between the 2 known good blocks
                case get_block(Height + 1, Blockchain) of
                    {ok, NextBlock} ->
                        case blockchain_block:prev_hash(NextBlock) of
                            Hash ->
                                {ok, Batch} = rocksdb:batch(),
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
                        case add_block_(Block, Blockchain, Syncing) of
                            ok ->
                                check_plausible_blocks(Blockchain),
                                ok;
                            Res ->  Res
                        end;
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

can_add_block(Block, Blockchain) ->
    Hash = blockchain_block:hash_block(Block),
    {ok, GenesisHash} = blockchain:genesis_hash(Blockchain),
    case blockchain_block:is_genesis(Block) of
        true when Hash =:= GenesisHash ->
            exists;
        true ->
            {error, unknown_genesis_block};
        false ->
            case blockchain:head_block_info(Blockchain) of
                {error, Reason}=Error ->
                    lager:error("could not get head hash ~p", [Reason]),
                    Error;
                {ok, #block_info_v2{hash=HeadHash, height=HeadHeight}} ->
                    Height = blockchain_block:height(Block),
                    {ok, ChainHeight} = blockchain:height(Blockchain),
                    %% compute the ledger at the height of the chain in case we're
                    %% re-adding a missing block (that was absorbed into the ledger)
                    %% that's on the wrong side of an election or a chain var
                    DelayedLedger = blockchain_ledger_v1:mode(delayed, blockchain:ledger(Blockchain)),
                    {ok, DelayedHeight} = blockchain_ledger_v1:current_height(DelayedLedger),
                    {ok, Ledger} = case Height < ChainHeight andalso Height >= DelayedHeight of
                                       true -> blockchain:ledger_at(ChainHeight, Blockchain);
                                       false -> {ok, blockchain:ledger(Blockchain)}
                                   end,
                    case
                        blockchain_block:prev_hash(Block) =:= HeadHash andalso
                         Height =:= HeadHeight + 1
                    of
                        false when HeadHash =:= Hash ->
                            lager:debug("Already have this block"),
                            exists;
                        false ->
                            case ?MODULE:has_block(Block, Blockchain) of
                                true ->
                                    %% we already have this, thanks
                                    %% don't error here incase there's more blocks coming that *are* valid
                                    exists;
                                false ->
                                    %% check the block is not contiguous
                                    case Height > (ChainHeight + 1) of
                                        true ->
                                            case is_block_plausible(Block, Blockchain) of
                                                true -> plausible;
                                                false ->
                                                    lager:warning("higher block doesn't fit with our chain, block_height: ~p, head_block_height: ~p", [blockchain_block:height(Block),
                                                                                                                                                HeadHeight]),
                                                    {error, disjoint_chain}
                                            end;
                                        false ->
                                            lager:warning("lower block doesn't fit with our chain, block_height: ~p, head_block_height: ~p", [blockchain_block:height(Block),
                                                                                                                                              HeadHeight]),
                                            %% if the block height is lower we probably don't care about it
                                            {error, disjoint_chain}
                                    end
                            end;
                        true ->
                            lager:debug("prev hash matches the gossiped block"),
                            case blockchain_ledger_v1:consensus_members(Ledger) of
                                {error, _Reason}=Error ->
                                    lager:error("could not get consensus_members ~p", [_Reason]),
                                    Error;
                                {ok, ConsensusAddrs} ->
                                    N = length(ConsensusAddrs),
                                    F = (N-1) div 3,
                                    {ok, KeyOrKeys} = get_key_or_keys(Ledger),
                                    blockchain_ledger_v1:delete_context(Ledger),
                                    Txns = blockchain_block:transactions(Block),
                                    Sigs = blockchain_block:signatures(Block),
                                    case blockchain_block:verify_signatures(Block,
                                                                            ConsensusAddrs,
                                                                            Sigs,
                                                                            N - F,
                                                                            KeyOrKeys)
                                    of
                                        false ->
                                            {error, failed_verify_signatures};
                                        {true, _, IsRescue} ->
                                            SortedTxns = lists:sort(fun blockchain_txn:sort/2, Txns),
                                            case Txns == SortedTxns of
                                                false ->
                                                    %% this double check is for just one block as
                                                    %% far as we know; there was a bug with a few
                                                    %% txns not being in the sorting order and they
                                                    %% got in there wrong in that one block
                                                    Filter =
                                                        fun(T) ->
                                                                blockchain_txn:type(T) /= blockchain_txn_state_channel_close_v1
                                                        end,
                                                    Txns2 = lists:filter(Filter, Txns),
                                                    Sorted2 = lists:filter(Filter, SortedTxns),
                                                    SortedTxns2 = lists:sort(fun blockchain_txn:sort/2, Sorted2),
                                                    case Txns2 == SortedTxns2 of
                                                        false ->
                                                            {error, wrong_txn_order};
                                                        true ->
                                                            {true, IsRescue}
                                                    end;
                                                true ->
                                                    {true, IsRescue}
                                            end
                                    end
                            end
                    end
            end
    end.

get_key_or_keys(Ledger) ->
    case blockchain:config(?use_multi_keys, Ledger) of
        {ok, true} ->
            case blockchain_ledger_v1:multi_keys(Ledger) of
                {ok, _} = Result -> Result;
                _ -> blockchain_ledger_v1:master_key(Ledger)
            end;
        _ ->
            blockchain_ledger_v1:master_key(Ledger)
    end.

add_block_(Block, Blockchain, Syncing) ->
    %% TODO: we know that swarm calls can block, it would be nice if we had all the pubkey bin and
    %% tid calls threaded through from the top
    Ledger = blockchain:ledger(Blockchain),
    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
    {ok, BlockchainHeight} = blockchain:height(Blockchain),
    FollowMode = follow_mode(),
    case LedgerHeight == BlockchainHeight of
        true ->
            case can_add_block(Block, Blockchain) of
                {true, IsRescue} ->
                    Height = blockchain_block:height(Block),
                    Hash = blockchain_block:hash_block(Block),
                    Sigs = blockchain_block:signatures(Block),
                    MyAddress = try blockchain_swarm:pubkey_bin() catch _:_ -> nomatch end,
                    BeforeCommit = fun(FChain, FHash) ->
                                           lager:debug("adding block ~p", [Height]),
                                           ok = ?save_block(Block, Blockchain),
                                           ok = run_gc_hooks(FChain, FHash)
                                   end,
                    {Signers, _Signatures} = lists:unzip(Sigs),
                    Fun = case lists:member(MyAddress, Signers) orelse FollowMode of
                              true -> unvalidated_absorb_and_commit;
                              _ -> absorb_and_commit
                          end,
                    %% 0 can never be true below
                    SnapHeight = application:get_env(blockchain, blessed_snapshot_block_height, 0),
                    case blockchain_block_v1:snapshot_hash(Block) of
                        <<>> ->
                            ok;
                        %% check the snap height as it's pointless to do this work for the snapshot
                        %% we're currently in the process of loading
                        ConsensusHash when Height /= (SnapHeight - 1) ->
                            process_snapshot(ConsensusHash, MyAddress, Signers,
                                             Ledger, Height, Blockchain);
                        _ -> ok
                    end,
                    case blockchain_txn:Fun(Block, Blockchain, BeforeCommit, IsRescue) of
                        {error, Reason}=Error ->
                            lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [blockchain_block:hash_block(Block), Reason]),
                            case application:get_env(blockchain, drop_snapshot_cache_on_absorb_failure, true) of
                                true ->
                                    lager:info("dropping all snapshots from cache"),
                                    blockchain_ledger_v1:drop_snapshots(Ledger);
                                false ->
                                    ok
                            end,
                            Error;
                        ok ->
                            run_absorb_block_hooks(Syncing, Hash, Blockchain)
                    end;
                plausible ->
                    %% regossip plausible blocks
                    Hash = blockchain_block:hash_block(Block),
                    case save_plausible_block(Block, Hash, Blockchain) of
                        exists -> ok; %% already have it
                        ok -> plausible %% tell the gossip handler
                    end;
                Other ->
                    %% this can either be an error tuple or `ok' when its the genesis block we already have
                    Other
            end;
        false when BlockchainHeight < LedgerHeight ->
            %% ledger is higher than blockchain, try to validate this block
            %% and see if we can save it
            case can_add_block(Block, Blockchain) of
                {true, _IsRescue} ->
                    ?save_block(Block, Blockchain);
                Other ->
                    Other
            end;
        false ->
            %% blockchain is higher than ledger, try to play the chain forwards
            replay_blocks(Blockchain, Syncing, LedgerHeight, BlockchainHeight)
    end.

process_snapshot(ConsensusHash, MyAddress, Signers,
                 Ledger, Height, Blockchain) ->
    case lists:member(MyAddress, Signers) orelse follow_mode() of
        true ->
            %% signers add the snapshot as when the block is created?
            %% otherwise we have to make it twice?
            %% also followers get overwhelmed by how big snaps are now, don't waste the space/time
            ok;
        false ->
            %% hash here is *pre*absorb.
            try
                case get_snapshot(ConsensusHash, Blockchain) of
                    {ok, _Snap} ->
                        %% already have this
                        ok;
                    {error, sentinel} ->
                        lager:info("skipping previously failed snapshot at height ~p", [Height]);
                    _ ->
                        case blockchain_ledger_snapshot_v1:get_blocks(Blockchain) of
                            {error, encountered_a_rescue_block} ->
                                lager:warning(
                                    "Aborting current snapshot creation attempt: "
                                    "blocks contain a rescue block. "
                                    "Will retry later."
                                );
                            {ok, Blocks} ->
                                Infos = blockchain_ledger_snapshot_v1:get_infos(Blockchain),
                                case blockchain_ledger_snapshot_v1:snapshot(Ledger, Blocks, Infos) of
                                    {ok, Snap} ->
                                        case blockchain_ledger_snapshot_v1:hash(Snap) of
                                            ConsensusHash ->
                                                {ok, _, ConsensusHash} = add_snapshot(Snap, ConsensusHash, Blockchain);
                                            OtherHash ->
                                                lager:info("bad snapshot hash: ~p good ~p",
                                                           [OtherHash, ConsensusHash]),
                                                case application:get_env(blockchain, save_bad_snapshot, false) of
                                                    true ->
                                                        lager:info("saving bad snapshot ~p", [OtherHash]),
                                                        {ok, _, OtherHash} = add_snapshot(Snap, OtherHash, Blockchain);
                                                    false ->
                                                        ok
                                                end,
                                                %% TODO: this is currently called basically for the
                                                %% logging. it does not reset, or halt
                                                blockchain_worker:async_reset(Height)
                                        end;
                                    {error, SnapReason} ->
                                        lager:info("error ~p taking snapshot", [SnapReason]),
                                        ok
                                end
                        end
                end
            catch What:Why ->
                    lager:info("error ~p taking snapshot", [{What, Why}]),
                    ok
            end
    end.

replay_blocks(Chain, Syncing, LedgerHeight, ChainHeight) ->
    lists:foldl(
      fun(H, ok) ->
              {ok, B} = get_block(H, Chain),
              Hash = blockchain_block:hash_block(B),
              Fun = case follow_mode() of
                        true -> unvalidated_absorb_and_commit;
                        _ -> absorb_and_commit
                    end,
              case blockchain_txn:Fun(B, Chain,
                                      fun(FChain, FHash) -> ok = run_gc_hooks(FChain, FHash) end,
                                      blockchain_block:is_rescue_block(B)) of
                  ok ->
                      run_absorb_block_hooks(Syncing, Hash, Chain);
                  {error, Reason} ->
                      lager:error("Error absorbing transaction, "
                                  "Ignoring Hash: ~p, Reason: ~p",
                                  [Hash, Reason])
              end;
         (_, Error) -> Error
      end, ok, lists:seq(LedgerHeight+1, ChainHeight)).

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
                    absorb_temp_blocks(Block, Blockchain, Syncing);
                _ ->
                    %% it's possible that we've seen everything BUT the assumed valid block, and done a full validate + absorb
                    %% already, so check for that
                    case blockchain:get_block(ParentHash, Blockchain) of
                        {ok, _} ->
                            ok = save_temp_block(Block, Blockchain),
                            absorb_temp_blocks(Block, Blockchain, Syncing);
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

absorb_temp_blocks(Block, Blockchain, Syncing) ->
    ok = blockchain_worker:set_absorbing(Block, Blockchain, Syncing).

absorb_temp_blocks_fun(Block, Blockchain=#blockchain{temp_blocks=TempBlocksCF}, Syncing) ->
    ok = blockchain_lock:acquire(),
    {ok, MainChainHeadHash} = blockchain:head_hash(Blockchain),
    %% ok, now build the chain back to the oldest block in the temporary
    %% storage or to the head of the main chain, whichever comes first
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
            absorb_temp_blocks_fun_(Chain, Blockchain, Syncing);
        Res ->
            lager:info("temp Chain ~p", [length(Chain)]),
            lager:warning("Saw assumed valid block, but cannot connect it to the main chain ~p", [Res]),
            blockchain_lock:release(),
            ok
    end.

absorb_temp_blocks_fun_([], Chain, _Syncing) ->
    %% we did it!
    ok = blockchain_worker:absorb_done(),
    delete_temp_blocks(Chain),
    blockchain_lock:release(),
    ok;
absorb_temp_blocks_fun_([BlockHash|Chain], Blockchain, Syncing) ->
    {ok, Block} = get_temp_block(BlockHash, Blockchain),
    Height = blockchain_block:height(Block),
    Hash = blockchain_block:hash_block(Block),
    BeforeCommit = fun(FChain, FHash) ->
                           lager:info("adding block ~p", [Height]),
                           ok = ?save_block(Block, Blockchain),
                           ok = run_gc_hooks(FChain, FHash)
                   end,
    case blockchain_txn:unvalidated_absorb_and_commit(Block, Blockchain, BeforeCommit, blockchain_block:is_rescue_block(Block)) of
        {error, Reason}=Error ->
            lager:error("Error absorbing transaction, Ignoring Hash: ~p, Reason: ~p", [blockchain_block:hash_block(Block), Reason]),
            Error;
        ok ->
            run_absorb_block_hooks(Syncing, Hash, Blockchain),
            absorb_temp_blocks_fun_(Chain, Blockchain, Syncing)
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
    Height = blockchain_block:height(Block),
    ok = rocksdb:batch_delete(Batch, BlocksCF, Hash),
    {ok, HeadHash} = ?MODULE:head_hash(Chain),
    case HeadHash =:= Hash of
        false ->
            lager:warning("deleting non-head block ~p height: ~p", [Hash, Height]),
            ok;
        true ->
            PrevHash = blockchain_block:prev_hash(Block),
            lager:warning("deleting head block ~p height: ~p, new head is ~p", [Hash, Height, PrevHash]),
            ok = rocksdb:batch_put(Batch, DefaultCF, ?HEAD, PrevHash)
    end,
    ok = rocksdb:batch_delete(Batch, HeightsCF, <<Height:64/integer-unsigned-big>>),
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]).

-spec config(ConfigName :: atom(), Ledger :: blockchain_ledger_v1:ledger()) -> {ok, any()} | {error, any()}.
config(ConfigName, Ledger) ->
    blockchain_utils:get_var(ConfigName, Ledger).

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
-spec build(blockchain_block:block() | non_neg_integer(), blockchain(), non_neg_integer()) -> [{non_neg_integer(), binary()}].
build(StartingHeight, Blockchain, Limit) when is_integer(StartingHeight) ->
    build(StartingHeight + 1, Blockchain, Limit, []);
build(StartingBlock, Blockchain, Limit) ->
    Height = blockchain_block:height(StartingBlock) + 1,
    build(Height, Blockchain, Limit, []).

-spec build(non_neg_integer(), blockchain(), non_neg_integer(), [{non_neg_integer(), binary()}]) -> [{non_neg_integer(), binary()}].
build(_Height, _Blockchain, 0, Acc) ->
    lists:reverse(Acc);
build(Height, Blockchain, N, Acc) ->
    case ?MODULE:get_raw_block(Height, Blockchain) of
        {ok, NextBlock} ->
            build(Height + 1, Blockchain, N-1, [{Height, NextBlock}|Acc]);
        _ ->
            %% ran out of verified blocks, see if we have any plausible ones
            case get_raw_plausibles(Height, Blockchain) of
                [] ->
                    lists:reverse(Acc);
                Plausibles ->
                    lager:info("Found ~p plausibles at height ~p", [length(Plausibles), Height]),
                    build(Height + 1, Blockchain, N - length(Plausibles), [{Height, Plausible} || Plausible <- Plausibles] ++ Acc)
            end
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

-spec fold_chain(fun((Blk :: blockchain_block:block(), AccIn :: any()) -> NewAcc :: any()),
                     Acc0 :: any(),
                    Block :: blockchain_block:block(),
                    Chain :: blockchain()) -> AccOut :: any().
%% @doc fold blocks in the chain `Chain' backwards from `Block' until a hole in the chain, the genesis block or the function returns `return'.
fold_chain(Fun, Acc0, Block, Chain) ->
    case Fun(Block, Acc0) of
        return ->
            %% return early
            Acc0;
        Acc ->
            ParentHash = blockchain_block:prev_hash(Block),
            case get_block(ParentHash, Chain) of
                {ok, NextBlock} ->
                    fold_chain(Fun, Acc, NextBlock, Chain);
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
    catch blockchain_ledger_v1:close(Ledger),
    catch rocksdb:close(DB).

compact(#blockchain{db=DB, default=Default, blocks=BlocksCF, heights=HeightsCF, temp_blocks=TempBlocksCF, implicit_burns=ImplicitBurnsCF, htlc_receipts=HTLCReceiptsCF}) ->
    rocksdb:compact_range(DB, undefined, undefined, []),
    rocksdb:compact_range(DB, Default, undefined, undefined, []),
    rocksdb:compact_range(DB, BlocksCF, undefined, undefined, []),
    rocksdb:compact_range(DB, HeightsCF, undefined, undefined, []),
    rocksdb:compact_range(DB, TempBlocksCF, undefined, undefined, []),
    rocksdb:compact_range(DB, ImplicitBurnsCF, undefined, undefined, []),
    rocksdb:compact_range(DB, HTLCReceiptsCF, undefined, undefined, []),
    ok.

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
                              BeforeCommit = fun(FChain, FHash) ->
                                                     ok = run_gc_hooks(FChain, FHash)
                                             end,
                              case Revalidate of
                                  false ->
                                      ok = blockchain_txn:unvalidated_absorb_and_commit(Block, CAcc, BeforeCommit,
                                                                                        blockchain_block:is_rescue_block(Block));
                                  true ->
                                      ok = blockchain_txn:absorb_and_commit(Block, CAcc, BeforeCommit,
                                                                            blockchain_block:is_rescue_block(Block))
                              end,
                              Hash = blockchain_block:hash_block(Block),
                              %% can only get here if the absorb suceeded
                              run_absorb_block_hooks(true, Hash, CAcc),
                              CAcc;
                          _ ->
                              lager:warning("couldn't absorb block at ~p", [H]),
                              ok
                      end
              end,
              %% this will be a noop in the case where Height == TopHeight
              Chain1,
              lists:seq(1, Height)),

            blockchain_worker:blockchain(Chain1),

            blockchain_lock:release(),
            {ok, Chain2}
    end.

reset_ledger_to_snap() ->
    case {application:get_env(blockchain, blessed_snapshot_block_hash, undefined),
          application:get_env(blockchain, blessed_snapshot_block_height, undefined)} of
        {undefined, _} ->
            {error, no_snapshot_defined};
        {_, undefined} ->
            {error, no_snapshot_defined};
        {Hash, Height} ->
            case application:get_env(blockchain, honor_quick_sync, false) of
                true ->
                    ok = blockchain_worker:reset_ledger_to_snap(Hash, Height);
                _ ->
                    {error, snapshots_disabled}
            end
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

-type crosscheck_error()
        :: {ledger_delayed_ledger_lag_too_large, non_neg_integer()}
        |  {ledger_delayed_ledger_lag_too_small, non_neg_integer()}
        |  {chain_ledger_height_mismatch, non_neg_integer(), non_neg_integer()}
        |  {fingerprint_mismatch, [term()]}
        |  {missing_block, non_neg_integer()}
        .

-spec crosscheck(blockchain()) -> ok | {error, crosscheck_error()}.
crosscheck(Blockchain) ->
    crosscheck(Blockchain, true).

-spec crosscheck(blockchain(), boolean()) -> ok | {error, crosscheck_error()}.
crosscheck(Blockchain, Recalc) ->
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
                            %% compare the leading ledger and the lagging ledger advanced to the
                            %% leading ledger's height for consistency
                            case ledger_at(LedgerHeight, Blockchain, Recalc) of
                                {ok, RecalcLedger} ->
                                    case compare(Ledger, RecalcLedger) of
                                        ok ->
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
                                            end;
                                        Error ->
                                            Error
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

-spec compare(LedgerA :: blockchain_ledger_v1:ledger(), LedgerB :: blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
compare(LedgerA, LedgerB) ->
    {ok, FPA} = blockchain_ledger_v1:raw_fingerprint(LedgerA, true),
    {ok, FPB} = blockchain_ledger_v1:raw_fingerprint(LedgerB, true),
    case FPA == FPB of
        false ->
            Mismatches = [ K || K <- maps:keys(FPA), maps:get(K, FPB) /= maps:get(K, FPB),
                                K /= <<"ledger_fingerprint">> ],
            MismatchesWithChanges =
                lists:map(
                  fun(M) ->
                          X = blockchain_ledger_v1:cf_fold(
                                fp_to_cf(M),
                                fun({K, V}, Acc) -> maps:put(K, V, Acc) end, #{}, LedgerA),
                          {AllKeys, Changes0} = blockchain_ledger_v1:cf_fold(
                                                  fp_to_cf(M),
                                                  fun({K, V}, {Keys, Acc}) ->
                                                          Acc1 = case maps:find(K, X) of
                                                                     {ok, V} ->
                                                                         Acc;
                                                                     {ok, Other} ->
                                                                         [{changed, K, Other, V}|Acc];
                                                                     error ->
                                                                         [{added, K, V}|Acc]
                                                                 end,
                                                          {[K|Keys], Acc1}
                                                  end, {[], []}, LedgerB),
                          Changes = maps:fold(fun(K, V, A) ->
                                                      [{deleted, K, V}|A]
                                              end, Changes0, maps:without(AllKeys, X)),
                          {fp_to_cf(M), Changes}
                  end, Mismatches),
            {error, {fingerprint_mismatch, MismatchesWithChanges}};
        true ->
            ok
    end.

-spec analyze(blockchain()) -> ok | {error, crosscheck_error()}.
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
fp_to_cf(<<"routings_fingerprint">>) -> routing;
fp_to_cf(<<"state_channels_fingerprint">>) -> state_channels.

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

-spec add_snapshot(blockchain_ledger_snapshot:snapshot(), blockchain()) ->
    {ok, pos_integer(), binary()} | {error, any()}.
add_snapshot(Snapshot, Chain) ->
    try
        Hash = blockchain_ledger_snapshot_v1:hash(Snapshot),
        add_snapshot(Snapshot, Hash, Chain)
    catch What:Why:Stack ->
            lager:warning("error adding snapshot: ~p:~p, ~p", [What, Why, Stack]),
            {error, Why}
    end.

-spec add_snapshot(blockchain_ledger_snapshot:snapshot(), binary(), blockchain()) ->
    {ok, pos_integer(), binary()} | {error, any()}.
add_snapshot(Snapshot, Hash, #blockchain{db=DB, snapshots=SnapshotsCF}=Chain) ->
    try
        Height = blockchain_ledger_snapshot_v1:height(Snapshot),

        %% write a sentinel value to mark we were trying to build this
        %% so we can skip it next time if we crash out
        {ok, Batch0} = rocksdb:batch(),
        ok = rocksdb:batch_put(Batch0, SnapshotsCF, Hash, <<"__sentinel__">>),
        %% lexiographic ordering works better with big endian
        ok = rocksdb:batch_put(Batch0, SnapshotsCF, <<Height:64/integer-unsigned-big>>, Hash),
        ok = rocksdb:write_batch(DB, Batch0, []),
        BinSnap = blockchain_ledger_snapshot_v1:serialize(Snapshot),
        case add_bin_snapshot(BinSnap, Height, Hash, Chain) of
            ok -> {ok, Height, Hash};
            Other -> Other
        end
    catch What:Why:Stack ->
            lager:warning("error adding snapshot: ~p:~p, ~p", [What, Why, Stack]),
            {error, Why}
    end.

-spec add_bin_snapshot(blockchain_ledger_snapshot:snapshot(), integer(),
                       none | binary(), blockchain()) ->
                              ok | {error, any()}.
add_bin_snapshot(_BinSnap, _Height, none, _Chain) -> {error, no_snapshot_hash};
add_bin_snapshot(BinSnap, Height, Hash, #blockchain{db=DB, dir=Dir, snapshots=SnapshotsCF}) when is_binary(Hash) ->
    try
        SnapDir = filename:join(Dir, "saved-snaps"),
        SnapFile = list_to_binary(io_lib:format("snap-~s", [blockchain_utils:bin_to_hex(Hash)])),
        OhSnap = filename:join(SnapDir, SnapFile),
        ok = filelib:ensure_dir(OhSnap),
        case BinSnap of
            {file, Filename} ->
                case filelib:is_regular(OhSnap) of
                    true ->
                        ok = file:delete(OhSnap);
                    false ->
                        ok
                end,
                ok = file:make_link(Filename, OhSnap);
            B when is_binary(B); is_list(B) ->
                %% can be a binary or an iolist if it was generated locally
                %% and we can avoid constructing a large binary by just dumping the
                %% iolist to disk
                ok = file:write_file(filename:join(SnapDir, SnapFile), BinSnap)
        end,
        {ok, Batch} = rocksdb:batch(),
        %% store the snap as a filename
        ok = rocksdb:batch_put(Batch, SnapshotsCF, Hash, <<"file:", SnapFile/binary>>),
        %% lexiographic ordering works better with big endian
        ok = rocksdb:batch_put(Batch, SnapshotsCF, <<Height:64/integer-unsigned-big>>, Hash),
        ok = rocksdb:write_batch(DB, Batch, [])
    catch What:Why:Stack ->
            lager:warning("error adding snapshot: ~p:~p, ~p", [What, Why, Stack]),
            {error, Why}
    end.

rocksdb_gc(BytesToDrop, #blockchain{db=DB, heights=HeightsCF}=Blockchain) ->
    {ok, Height} = blockchain:height(Blockchain),
    CutoffHeight = max(2, Height - application:get_env(blockchain, blocks_to_protect_from_gc, 10000)),
    %% start at 2 here so we don't GC the genesis block
    {ok, Itr} = rocksdb:iterator(DB, HeightsCF, [{iterate_lower_bound, <<2:64/integer-unsigned-big>>}, {iterate_upper_bound, <<CutoffHeight:64/integer-unsigned-big>>}]),
    do_rocksdb_gc(BytesToDrop, Itr, Blockchain,  rocksdb:iterator_move(Itr, first)).

do_rocksdb_gc(_Bytes, _Itr, _Blockchain, {error, _}) ->
    ok;
do_rocksdb_gc(Bytes, _Itr, _Blockchain, _Res) when Bytes < 1 ->
    ok;
do_rocksdb_gc(Bytes, Itr, #blockchain{dir=Dir, db=DB, heights=HeightsCF, blocks=BlocksCF, snapshots=SnapshotsCF}=Blockchain, {ok, <<IntHeight:64/integer-unsigned-big>>=Height, Hash}) ->
    lager:info("GCing block at height ~p, ~b MB remain", [IntHeight, Bytes div ?OP_SIZE]),
    BytesRemoved0 = case rocksdb:get(DB, BlocksCF, Hash, []) of
                        {ok, Block} -> byte_size(Block);
                        _ -> 0
                    end,
    {ok, Batch} = rocksdb:batch(),
    rocksdb:batch_delete(Batch, HeightsCF, Height),
    rocksdb:batch_delete(Batch, BlocksCF, Hash),
    BytesRemoved1 = case rocksdb:get(DB, SnapshotsCF, Height, []) of
        {ok, SnapHash} ->
            lager:info("GCing snapshot at height ~p", [IntHeight]),
            rocksdb:batch_delete(Batch, SnapshotsCF, Height),
            rocksdb:batch_delete(Batch, SnapshotsCF, SnapHash),
            case rocksdb:get(DB, SnapshotsCF, SnapHash, []) of
                {ok, <<"file:", SnapFile/binary>>} ->
                    %% check if the snap is on disk
                    SnapDir = filename:join(Dir, "saved-snaps"),
                    SnapPath = filename:join(SnapDir, SnapFile),
                    case file:read_file_info(SnapPath) of
                        {ok, #file_info{size=Size}} ->
                            file:delete(SnapPath),
                            Size;
                        _ ->
                            0
                    end;
                {ok, Snap} ->
                    %% snap was in rocksdb
                    byte_size(Snap);
                _ ->
                    0
            end;
        _ ->
            0
    end,
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
    do_rocksdb_gc(Bytes - BytesRemoved0 - BytesRemoved1, Itr, Blockchain, rocksdb:iterator_move(Itr, next)).


-spec get_snapshot(blockchain_block:hash() | integer(), blockchain()) ->
                          {ok, binary()} | {ok, {file, file:filename_all()}} | {error, any()}.
get_snapshot(<<Hash/binary>>, #blockchain{db=DB, dir=Dir, snapshots=SnapshotsCF}) ->
    case rocksdb:get(DB, SnapshotsCF, Hash, []) of
        {ok, <<"__sentinel__">>} ->
            {error, sentinel};
        {ok, <<"file:", SnapFile/binary>>} ->
            SnapDir = filename:join(Dir, "saved-snaps"),
            Filename = filename:join(SnapDir, SnapFile),
            case filelib:is_file(Filename) of
                true ->
                    {ok, {file, Filename}};
                false ->
                    {error, {file_not_found, Filename}}
            end;
        {ok, Snap} ->
            {ok, Snap};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end;
get_snapshot(Height, #blockchain{db=DB, snapshots=SnapshotsCF}=Blockchain) ->
    case rocksdb:get(DB, SnapshotsCF, <<Height:64/integer-unsigned-big>>, []) of
       {ok, Hash} ->
           ?MODULE:get_snapshot(Hash, Blockchain);
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec have_snapshot(integer(), blockchain()) ->
                           boolean() | {error, any()}.
have_snapshot(Height, #blockchain{db=DB, snapshots=SnapshotsCF}) ->
    case rocksdb:get(DB, SnapshotsCF, <<Height:64/integer-unsigned-big>>, []) of
       {ok, _Hash} ->
            true;
        not_found ->
            false;
        Error ->
            Error
    end.

-spec find_last_snapshot(blockchain()) -> undefined | {non_neg_integer(), blockchain_block:hash(), binary()} | {error, any()}.
find_last_snapshot(Blockchain) ->
    case find_last_snapshots(Blockchain, 1) of
        undefined -> undefined;
        [Res] -> Res
    end.


find_last_snapshots(Blockchain, Count0) ->
    {ok, Head} = head_block(Blockchain),
    Res = fold_chain(fun(_, {0, _Acc}) ->
                             return;
                        (B, {Count, Acc}) ->
                             case blockchain_block_v1:snapshot_hash(B) of
                                 <<>> -> {Count, Acc};
                                 SnapshotHash ->
                                     Height = blockchain_block:height(B),
                                     BlockHash = blockchain_block:hash_block(B),
                                     {Count - 1, [{Height, BlockHash, SnapshotHash} | Acc]}
                             end
                     end, {Count0, []}, Head, Blockchain),
    case Res of
        {_, []} ->
            undefined;
        {_, List} ->
            lists:reverse(List)
    end.

-spec get_implicit_burn(blockchain_txn:hash(), blockchain()) -> {ok, blockchain_implicit_burn:implicit_burn()} | {error, any()}.
get_implicit_burn(TxnHash, #blockchain{db=DB, implicit_burns=ImplicitBurnsCF}) when is_binary(TxnHash) ->
    case rocksdb:get(DB, ImplicitBurnsCF, TxnHash, []) of
        {ok, Bin} ->
            {ok, blockchain_implicit_burn:deserialize(Bin)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec add_implicit_burn(blockchain_txn:hash(), blockchain_implicit_burn:implicit_burn(), blockchain()) -> ok | {error, any()}.
add_implicit_burn(TxnHash, ImplicitBurn, #blockchain{db=DB, implicit_burns=ImplicitBurnsCF}) ->
    try
        BinImp = blockchain_implicit_burn:serialize(ImplicitBurn),
        ok = rocksdb:put(DB, ImplicitBurnsCF, TxnHash, BinImp, [])
    catch What:Why:Stack ->
            lager:warning("error adding implicit burn: ~p:~p, ~p", [What, Why, Stack]),
            {error, Why}
    end.

-spec get_htlc_receipt(blockchain_htlc_receipt:address(), blockchain()) -> {ok, blockchain_htlc_receipt:htlc_receipt()} | {error, any()}.
get_htlc_receipt(Address, #blockchain{db=DB, htlc_receipts=HTLCReceiptsCF}) when is_binary(Address) ->
    case rocksdb:get(DB, HTLCReceiptsCF, Address, []) of
        {ok, Bin} ->
            {ok, blockchain_htlc_receipt:deserialize(Bin)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec add_htlc_receipt(blockchain_htlc_receipt:address(), blockchain_htlc_receipt:htlc_receipt(), blockchain()) -> ok | {error, any()}.
add_htlc_receipt(Address, HTLCReceipt, #blockchain{db=DB, htlc_receipts=HTLCReceiptsCF}) ->
    try
        BinImp = blockchain_htlc_receipt:serialize(HTLCReceipt),
        ok = rocksdb:put(DB, HTLCReceiptsCF, Address, BinImp, [{sync, true}])
    catch What:Why:Stack ->
            lager:warning("error adding htlc_receipt: ~p:~p, ~p", [What, Why, Stack]),
            {error, Why}
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
    ok = blockchain_ledger_v1:clean(?MODULE:ledger(Blockchain));
clean(Dir) when is_list(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = rocksdb:destroy(DBDir, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all(), atom()) ->
                  {blockchain(), {ok, blockchain_block:block()}
                   | {error, any()}}
                      | {error, any()}.
load(Dir, Mode) ->
    case open_db(Dir) of
        {error, _Reason}=Error ->
            Error;
        {ok, DB, [DefaultCF, BlocksCF, HeightsCF, TempBlocksCF, PlausibleBlocksCF,
                  SnapshotCF, ImplicitBurnsCF, InfoCF, HTLCReceiptsCF]} ->
            HonorQuickSync = application:get_env(blockchain, honor_quick_sync, false),
            Ledger =
                case Mode of
                    blessed_snapshot when HonorQuickSync == true ->
                        L = blockchain_ledger_v1:new(Dir, DB, BlocksCF, HeightsCF, InfoCF),
                        case blockchain_ledger_v1:current_height(L) of
                            {ok, _} ->
                                %% no longer check height here, we will check the height elsewhere
                                %% where we have better information
                                L;
                            %% if we can't open the ledger and we can
                            %% load a snapshot, does the error matter?
                            %% just reload
                            {error, _} ->
                                blockchain_ledger_v1:clean(L),
                                blockchain_ledger_v1:new(Dir, DB, BlocksCF, HeightsCF, InfoCF)
                        end;
                    _ ->
                        L = blockchain_ledger_v1:new(Dir, DB, BlocksCF, HeightsCF, InfoCF),
                        blockchain_ledger_v1:compact(L),
                        L
                end,
            Blockchain = #blockchain{
                dir=Dir,
                db=DB,
                default=DefaultCF,
                blocks=BlocksCF,
                heights=HeightsCF,
                info=InfoCF,
                temp_blocks=TempBlocksCF,
                plausible_blocks=PlausibleBlocksCF,
                implicit_burns=ImplicitBurnsCF,
                htlc_receipts=HTLCReceiptsCF,
                snapshots=SnapshotCF,
                ledger=Ledger
            },
            %% check if we need to GC some blocks to keep disk space free
            %% note that this will temporarily increase disk usage
            case os:getenv("BLOCKCHAIN_ROCKSDB_GC_BYTES") of
                false -> ok;
                ValString ->
                    try list_to_integer(ValString, 10) of
                        BytesToGC ->
                            lager:notice("System requested we free ~b MB of disk space", [BytesToGC div ?OP_SIZE]),
                            Pid = spawn(fun() -> rocksdb_gc(BytesToGC, Blockchain) end),
                            lager:info("Starting rocksdb gc on pid ~p", [Pid]),
                            ok = blockchain_worker:monitor_rocksdb_gc(Pid)
                    catch _:_ ->
                              ok
                    end
            end,
            compact(Blockchain),
            %% if this is not set, the below check will always be true
            SnapHeight = application:get_env(blockchain, blessed_snapshot_block_height, 1),
            FollowMode = follow_mode(), % no need for pre-calc when we only follow
            %% pre-calculate the missing snapshots when we're above blessed snap
            case height(Blockchain) of
                {ok, ChainHeight} when ChainHeight > 2 andalso
                                       ChainHeight > SnapHeight andalso
                                       (not FollowMode) ->
                    case ledger_at(ChainHeight - 1, Blockchain) of
                        {ok, Ld} ->
                            blockchain_ledger_v1:delete_context(Ld);
                        _ ->
                            ok
                    end;
                _ ->
                    ok
            end,
            {Blockchain, ?MODULE:genesis_block(Blockchain)}
    end.

-spec load_genesis(file:filename_all()) -> {ok, blockchain_block:block()} | {error, any()}.
load_genesis(Dir) ->
    File = filename:join(Dir, ?GEN_HASH_FILE),
    case file:read_file(File) of
        {error, _Reason}=Error ->
            Error;
        {ok, Binary} ->
            {ok, blockchain_block:deserialize(Binary)}
    end.

%% @doc Creates a signed add_gatewaytransaction with this blockchain's
%% keys as the gateway, and the given owner and payer
%%
%% NOTE: For add_gateway_txn, we need to permit the StakingFee and Fee to be supplied
%% this is to continue to allow miner to create, sign and submit add gateway txns
%% on behalf of mobile clients.  We cannot assume these clients will have the chain present
%% the supplied fee value will be calculated by the client
%% the supplied staking fee will have been derived from the API ( which will have the chain vars )
-spec add_gateway_txn(OwnerB58::string(),
                      PayerB58::string() | undefined,
                      Fee::pos_integer() | undefined,
                      StakingFee::non_neg_integer() | undefined) -> {ok, binary()}.
add_gateway_txn(OwnerB58, PayerB58, Fee0, StakingFee0) ->
    Owner = libp2p_crypto:b58_to_bin(OwnerB58),
    Payer = case PayerB58 of
                undefined -> <<>>;
                [] -> <<>>;
                _ -> libp2p_crypto:b58_to_bin(PayerB58)
            end,
    {ok, PubKey, SigFun, _ECDHFun} =  blockchain_swarm:keys(),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Txn0 = blockchain_txn_add_gateway_v1:new(Owner, PubKeyBin, Payer),
    StakingFee = case StakingFee0 of 
        undefined -> 
            blockchain_txn_add_gateway_v1:calculate_staking_fee(Txn0, blockchain_worker:blockchain());
        _ -> StakingFee0
    end,
    Txn1 = blockchain_txn_add_gateway_v1:staking_fee(Txn0, StakingFee),
    Fee = case Fee0 of
        undefined -> 
            blockchain_txn_add_gateway_v1:calculate_fee(Txn1, blockchain_worker:blockchain());
        _ ->
            Fee0
    end,
    Txn2 = blockchain_txn_add_gateway_v1:fee(Txn1, Fee),
    SignedTxn = blockchain_txn_add_gateway_v1:sign_request(Txn2, SigFun),
    {ok, blockchain_txn:serialize(SignedTxn)}.

%% @doc Creates a signed assert_location transaction using the keys of
%% this blockchain as the gateway to be asserted for the given
%% location, owner and payer.
%%
%% NOTE: For assert_loc_txn, we need to permit the StakingFee and Fee to be
%% supplied this is to continue to allow miner to create, sign and submit
%% assert_loc txns on behalf of mobile clients. We cannot assume these clients
%% will have the chain present the supplied fee value will be calculated by the
%% client the supplied staking fee will have been derived from the API ( which
%% will have the chain vars )
-spec assert_loc_txn(H3String::string(),
                     OwnerB58::string(),
                     PayerB58::string() | undefined,
                     Nonce::non_neg_integer(),
                     StakingFee::pos_integer(),
                     Fee::pos_integer()) -> {ok, binary()}.
assert_loc_txn(H3String, OwnerB58, PayerB58, Nonce, StakingFee, Fee) ->
    H3Index = h3:from_string(H3String),
    Owner = libp2p_crypto:b58_to_bin(OwnerB58),
    Payer = case PayerB58 of
                undefined -> <<>>;
                [] -> <<>>;
                _ -> libp2p_crypto:b58_to_bin(PayerB58)
            end,
    {ok, PubKey, SigFun, _ECDHFun} =  blockchain_swarm:keys(),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Txn0 = blockchain_txn_assert_location_v1:new(PubKeyBin, Owner, Payer, H3Index, Nonce),
    Txn = blockchain_txn_assert_location_v1:staking_fee(blockchain_txn_assert_location_v1:fee(Txn0, Fee), StakingFee),
    SignedTxn = blockchain_txn_assert_location_v1:sign_request(Txn, SigFun),
    {ok, blockchain_txn:serialize(SignedTxn)}.

%% @doc Creates a signed assert_location transaction using the keys of
%% this blockchain as the gateway to be asserted for the given
%% location, owner and payer.
%%
%% NOTE: This is the counter part of assert_loc_txn/6 whihch calculates
%% staking/fee for an assert location. It assumes a synced chain.
-spec assert_loc_txn(H3String::string(),
                     OwnerB58::string(),
                     PayerB58::string() | undefined,
                     Nonce::non_neg_integer()) -> {ok, binary()}.
assert_loc_txn(H3String, OwnerB58, PayerB58, Nonce) ->
    H3Index = h3:from_string(H3String),
    Owner = libp2p_crypto:b58_to_bin(OwnerB58),
    Payer = case PayerB58 of
                undefined -> <<>>;
                [] -> <<>>;
                _ -> libp2p_crypto:b58_to_bin(PayerB58)
            end,
    {ok, PubKey, SigFun, _ECDHFun} =  blockchain_swarm:keys(),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Chain = blockchain_worker:blockchain(),
    Txn0 = blockchain_txn_assert_location_v1:new(PubKeyBin, Owner, Payer, H3Index, Nonce),
    StakingFee = blockchain_txn_assert_location_v1:calculate_staking_fee(Txn0, Chain),
    Txn1 = blockchain_txn_assert_location_v1:staking_fee(Txn0, StakingFee),
    Fee = blockchain_txn_assert_location_v1:calculate_fee(Txn1, Chain),
    Txn = blockchain_txn_assert_location_v1:fee(Txn1, Fee),
    SignedTxn = blockchain_txn_assert_location_v1:sign_request(Txn, SigFun),
    {ok, blockchain_txn:serialize(SignedTxn)}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),

    case filelib:is_file(filename:join(Dir, "blockchain-open-failed")) andalso follow_mode() of
        true ->
            lager:warning("unopenable blockchain.db detected, removing"),
            ok = file:delete(filename:join(Dir, "blockchain-open-failed")),
            clean(Dir),
            blockchain_ledger_v1:clean(Dir);
        false ->
            ok
    end,


    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}, {atomic_flush, true}] ++ GlobalOpts,
    DefaultCFs = ["default", "blocks", "heights", "temp_blocks",
                  "plausible_blocks", "snapshots", "implicit_burns", "info", "htlc_receipts"],
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

    CFOpts = GlobalOpts,

    ok = file:write_file(filename:join(Dir, "blockchain-open-failed"), <<>>),
    case rocksdb:open_with_cf(DBDir, DBOptions,  [adjust_cfopts({CF, CFOpts}) || CF <- ExistingCFs]) of
        {error, _Reason}=Error ->
            ok = file:delete(filename:join(Dir, "blockchain-open-failed")),
            Error;
        {ok, DB, OpenedCFs} ->
            ok = file:delete(filename:join(Dir, "blockchain-open-failed")),
            L1 = lists:zip(ExistingCFs, OpenedCFs),
            L2 = lists:map(
                fun(CF) ->
                    {ok, CF1} = rocksdb:create_column_family(DB, CF, CFOpts),
                    {CF, CF1}
                end,
                DefaultCFs -- ExistingCFs
            ),
            L3 = L1 ++ L2,
            {ok, DB, [proplists:get_value(X, L3) || X <- DefaultCFs]}
    end.


adjust_cfopts({CF, CFOpts}) when CF == "blocks"; CF == "temp_blocks"; CF == "plausible_blocks" ->
    %% use 8mb file sizes to avoid too many compactions when storing blocks
    {CF, lists:keystore(target_file_size_base, 1, CFOpts, {target_file_size_base, 8388608})};
adjust_cfopts({CF, CFOpts}) ->
    {CF, CFOpts}.

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
save_block(Block, Batch, #blockchain{default=DefaultCF, blocks=BlocksCF, heights=HeightsCF,
                                     info=InfoCF}) ->
    Height = blockchain_block:height(Block),
    Hash = blockchain_block:hash_block(Block),
    ok = rocksdb:batch_put(Batch, BlocksCF, Hash, blockchain_block:serialize(Block)),
    ok = rocksdb:batch_put(Batch, DefaultCF, ?HEAD, Hash),
    ok = rocksdb:batch_put(Batch, DefaultCF, ?LAST_BLOCK_ADD_TIME, <<(erlang:system_time(second)):64/integer-unsigned-little>>),
    %% lexiographic ordering works better with big endian
    ok = rocksdb:batch_put(Batch, HeightsCF, Hash, <<Height:64/integer-unsigned-big>>),
    ok = rocksdb:batch_put(Batch, HeightsCF, <<Height:64/integer-unsigned-big>>, Hash),
    Info = mk_block_info(Hash, Block),
    ok = rocksdb:batch_put(Batch, InfoCF, <<Height:64/integer-unsigned-big>>, term_to_binary(Info)).

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

init_quick_sync(undefined, Blockchain, _Data) ->
    maybe_continue_resync(process_upgrades(Blockchain));
init_quick_sync(assumed_valid, Blockchain, Data) ->
    init_assumed_valid(process_upgrades(Blockchain), Data);
init_quick_sync(blessed_snapshot, Blockchain, Data) ->
    %% don't process upgrades here, check if we need to pull a snap first
    init_blessed_snapshot(Blockchain, Data).

init_assumed_valid(Blockchain, HashAndHeight={Hash, Height}) when is_binary(Hash), is_integer(Height) ->
    case ?MODULE:get_block(Hash, Blockchain) of
        {ok, _} ->
            %% already got it, chief
            maybe_continue_resync(delete_temp_blocks(Blockchain));
        _ ->
            {ok, CurrentHeight} = height(Blockchain),
            case CurrentHeight > Height of
                %% we loaded a snapshot, clean up just in case
                true ->
                    maybe_continue_resync(delete_temp_blocks(Blockchain));
                false ->
                    %% set this up here, it will get cleared if we're able to add all the assume valid blocks
                    ok = persistent_term:put(?ASSUMED_VALID, HashAndHeight),
                    #blockchain{db=DB, temp_blocks=TempBlocksCF} = Blockchain,
                    %% the chain and the ledger need to be in sync for this to have any chance of working
                    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(blockchain:ledger(Blockchain)),
                    IsInSync = CurrentHeight == LedgerHeight,
                    case rocksdb:get(DB, TempBlocksCF, Hash, []) of
                        {ok, BinBlock} when IsInSync == true ->
                            %% we already have it, try to process it
                            Block = blockchain_block:deserialize(BinBlock),
                            case blockchain_block:height(Block) == Height of
                                true ->
                                    absorb_temp_blocks(Block, Blockchain, true),
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
            end
    end;
init_assumed_valid(Blockchain, _) ->
    maybe_continue_resync(Blockchain).

init_blessed_snapshot(Blockchain, _HashAndHeight={Hash, Height0}) when is_binary(Hash), is_integer(Height0) ->
    %% the height of the snapshot is the height it hit the chain,
    %% rather than the height it leaves the ledger when it's fully
    %% loaded.  before this fix, if we crashed after loading, even if
    %% we succeeded loading, we'd redo that work.
    Height = Height0 - 1,
    case blockchain_ledger_v1:current_height(blockchain:ledger(Blockchain)) of
        %% already loaded the snapshot
        {ok, CurrHeight} when CurrHeight >= Height ->
            lager:debug("ch ~p h ~p: std sync", [CurrHeight, Height]),
            blockchain_worker:maybe_sync(),
            process_upgrades(Blockchain);
        %% chain lower than the snapshot
        {ok, CurrHeight} ->
            lager:debug("ch ~p h ~p: snap sync", [CurrHeight, Height]),
            case get_snapshot(Hash, Blockchain) of
               {ok, BinSnapOrFile} ->
                  case blockchain_ledger_snapshot_v1:deserialize(BinSnapOrFile) of
                      {ok, Snap} ->
                          lager:info("Got snapshot for height ~p - attempting install", [Height0]),
                          %% do the install in-line here vs making a blocking call to
                          %% blockchain_worker
                          OldLedger = blockchain:ledger(Blockchain),
                          HasAux = blockchain_ledger_v1:has_aux(OldLedger),
                          {ok, LaggingHeight} = blockchain_ledger_v1:current_height(blockchain_ledger_v1:mode(delayed, OldLedger)),
                          {ok, LeadingHeight} = blockchain_ledger_v1:current_height(OldLedger),
                          SnapHeight = blockchain_ledger_snapshot_v1:height(Snap),
                          lager:info("Snapshot height ~p, lagging ledger height ~p, leading ledger height ~p", [SnapHeight, LaggingHeight, LeadingHeight]),
                          case LaggingHeight == SnapHeight andalso LeadingHeight >= LaggingHeight of
                              true when HasAux == false ->
                                  lager:info("resuming interrupted snapshot block load"),
                                  %% ledger height is correct, but blockchain height is not
                                  %% so lets just try to resume loading blocks as that was likely
                                  %% interrupted
                                  %%
                                  %% snapshots are loaded atomically into the ledger, so if we have
                                  %% the right height we can be quite confident it was an interrupted
                                  %% block load and bypass re-creating the ledger
                                  blockchain_ledger_snapshot_v1:load_blocks(OldLedger, Blockchain, Snap),
                                  Blockchain;
                              _ ->
                                  blockchain_ledger_v1:clean(OldLedger),
                                  %% TODO proper error checking and recovery/retry
                                  NewLedger = blockchain_ledger_snapshot_v1:import(Blockchain, SnapHeight, Hash, Snap, BinSnapOrFile),
                                  blockchain:ledger(NewLedger, Blockchain)
                          end;
                        Other ->
                          lager:warning("failed to deserialize stored snapshot: ~p", [Other])
                  end;
                {error, not_found} ->
                    ok;
               Other ->
                  lager:error("Got ~p trying to get snapshot at height: ~p hash ~p - attempt to sync",
                              [Other, Height0, Hash])
            end,
            Blockchain;
        %% no chain at all, we need the genesis block first
        _ ->
            Blockchain
    end;
init_blessed_snapshot(Blockchain, _) ->
    process_upgrades(Blockchain).

delete_temp_blocks(Blockchain=#blockchain{db=DB, temp_blocks=TempBlocksCF, default=DefaultCF}) ->
    persistent_term:erase(?ASSUMED_VALID),
    ok = rocksdb:delete(DB, DefaultCF, ?TEMP_HEADS, [{sync, true}]),
    ok = rocksdb:drop_column_family(TempBlocksCF),
    ok = rocksdb:destroy_column_family(TempBlocksCF),
    CFOpts = application:get_env(rocksdb, global_opts, []),
    {ok, CF1} = rocksdb:create_column_family(DB, "temp_blocks", CFOpts),
    NewChain = Blockchain#blockchain{temp_blocks=CF1},
    %% do this in a spawn because we might be *in* the blockchain_worker process
    %% or the blockchain_worker might not be running (eg. tests) and
    %% this is a gen_server call which can throw an exception
    spawn(fun() -> blockchain_worker:blockchain(NewChain) end),
    NewChain.

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
                    blockchain_worker:set_resyncing(ChainHeight, LedgerHeight, Blockchain)
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
        {error, _} when LedgerHeight == 0 ->
            %% reload the genesis block
            {ok, GenesisBlock} = blockchain:genesis_block(Blockchain),
            ok = blockchain_txn:unvalidated_absorb_and_commit(GenesisBlock, Blockchain, fun(_, _) -> ok end, false),
            {ok, GenesisHash} = blockchain:genesis_hash(Blockchain),
            ok = blockchain_worker:notify({integrate_genesis_block, GenesisHash}),
            case blockchain_ledger_v1:new_snapshot(blockchain:ledger(Blockchain)) of
                {error, Reason}=Error ->
                    lager:error("Error creating snapshot, Reason: ~p", [Reason]),
                    Error;
                {ok, NewLedger} ->
                    ok = blockchain_worker:notify({add_block, GenesisHash, true, NewLedger})
            end,
            resync_fun(ChainHeight, LedgerHeight + 1, Blockchain);
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
                                              BeforeCommit = fun(FChain, FHash) ->
                                                                     ok = run_gc_hooks(FChain, FHash)
                                                             end,
                                              lager:info("absorbing block ~p", [blockchain_block:height(Block)]),
                                              case application:get_env(blockchain, force_resync_validation, false) of
                                                  true ->
                                                      ok = blockchain_txn:absorb_and_commit(Block, Blockchain, BeforeCommit, blockchain_block:is_rescue_block(Block));
                                                  false ->
                                                      ok = blockchain_txn:unvalidated_absorb_and_commit(Block, Blockchain, BeforeCommit, blockchain_block:is_rescue_block(Block))
                                              end,
                                              run_absorb_block_hooks(true, Hash, Blockchain)
                                      end, HashChain)
                    after
                        blockchain_lock:release()
                    end
            end
    end.

%% check if this block looks plausible
%% if we can validate f+1 of the signatures against our consensus group
%% that means it's probably not wildly impossible
is_block_plausible(Block, Chain) ->
    Ledger = ledger(Chain),
    BlockHeight = blockchain_block:height(Block),
    {ok, ChainHeight} = blockchain:height(Chain),
    %% check the block is higher than our chain height but not unreasonably so
    case BlockHeight > ChainHeight andalso BlockHeight < ChainHeight + 90 of
        true ->
            case blockchain_ledger_v1:consensus_members(Ledger) of
                {error, _Reason} ->
                    false;
                {ok, ConsensusAddrs} ->
                    N = length(ConsensusAddrs),
                    F = (N - 1) div 3,

                    Signees = blockchain_block:verified_signees(Block),

                    SigThreshold =
                    case blockchain:config(?election_version, blockchain:ledger(Chain)) of
                        {ok, 5} -> (2 * F) + 1;         %% much higher v5 onwards
                        _ -> F + 1                      %% maintain old behavior
                    end,

                    Received = sets:size(sets:intersection(sets:from_list(ConsensusAddrs),
                                                           sets:from_list(Signees))),

                    Received >= SigThreshold orelse blockchain_block:is_rescue_block(Block)

            end;
        false ->
            false
    end.

-spec save_plausible_blocks([{binary(), blockchain_block:block()}], blockchain()) -> blockchain_block:block() | error.
save_plausible_blocks(Blocks, #blockchain{db=DB}=Chain) ->
    %% XXX ASSUMPTION this is only called from the sync pid and thus we won't check
    %% for a blockchain lock
    {ok, Batch} = rocksdb:batch(),
    {ok, ChainHeight} = blockchain:height(Chain),
    Result = lists:foldl(fun({BinBlock, Block}, Acc) ->
                                 Hash = blockchain_block:hash_block(Block),
                                 Height = blockchain_block:height(Block),
                                 case get_block_height(Hash, Chain) of
                                     {ok, H} when H =< ChainHeight ->
                                         %% block is already in the main chain
                                         error;
                                     _ ->
                                         %% ok block is not in the main chain, check if it looks plausible
                                         case is_block_plausible(Block, Chain) of
                                             true ->
                                                 %% check if we already have stored it
                                                 case get_plausible_block(Hash, Chain) of
                                                     {ok, _} ->
                                                         %% no need to save it
                                                         ok;
                                                     _ ->
                                                         save_plausible_block(Batch, BinBlock, Height, Hash, Chain)
                                                 end,

                                                 case Acc of
                                                     error ->
                                                         %% no highest block yet, so this one wins by default
                                                         Block;
                                                     OtherBlock ->
                                                         case blockchain_block:height(Block) > blockchain_block:height(OtherBlock) of
                                                             true ->
                                                                 %% this is newer
                                                                 Block;
                                                             false ->
                                                                 %% other block is newer
                                                                 Acc
                                                         end
                                                 end;
                                             _ ->
                                                 %% block was not plausible
                                                 Acc
                                         end
                                 end
                         end, error, Blocks),
    rocksdb:write_batch(DB, Batch, [{sync, true}]),
    Result.

save_plausible_block(Block, Hash, #blockchain{db=DB}=Chain) ->
    true = blockchain_lock:check(), %% we need the lock for this
    {ok, Batch} = rocksdb:batch(),
    Height = blockchain_block:height(Block),
    save_plausible_block(Batch, blockchain_block:serialize(Block), Height, Hash, Chain),
    rocksdb:write_batch(DB, Batch, [{sync, true}]).


-spec save_plausible_block(rocksdb:batch_handle(), binary(), non_neg_integer(), blockchain_block:hash(), blockchain()) -> ok.
save_plausible_block(Batch, BinBlock, Height, Hash, #blockchain{db=DB, plausible_blocks=PlausibleBlocks}) ->
    case rocksdb:get(DB, PlausibleBlocks, Hash, []) of
        {ok, _} ->
            %% already got it, thanks
            exists;
        _ ->
            PlausiblesAtThisHeight = case rocksdb:get(DB, PlausibleBlocks, <<Height:64/integer-unsigned-big>>, []) of
                {ok, BinList} ->
                    binary_to_term(BinList);
                _ ->
                    []
            end,
            rocksdb:batch_put(Batch, PlausibleBlocks, Hash, BinBlock),
            rocksdb:batch_put(Batch, PlausibleBlocks, <<Height:64/integer-unsigned-big>>, term_to_binary([Hash|PlausiblesAtThisHeight]))
    end.

-spec remove_plausible_block(blockchain(), Batch :: rockdb:batch_handle(), Hash :: binary(), Height :: non_neg_integer()) -> ok.
remove_plausible_block(#blockchain{db=DB, plausible_blocks=CF}, Batch, Hash, Height) ->
    true = blockchain_lock:check(), %% we need the lock for this
    case rocksdb:get(DB, CF, <<Height:64/integer-unsigned-big>>, []) of
        {ok, BinList} ->
            case binary_to_term(BinList) -- [Hash] of
                [] ->
                    %% no more plausibles at this height
                    rocksdb:batch_delete(Batch, CF, <<Height:64/integer-unsigned-big>>);
                OtherHashes ->
                    rocksdb:batch_put(Batch, CF, <<Height:64/integer-unsigned-big>>, term_to_binary(OtherHashes))
            end;
        _ ->
            ok
    end,
    rocksdb:batch_delete(Batch, CF, Hash).

-spec have_plausible_block(Hash :: blockchain_block:hash(), Chain :: blockchain()) -> boolean().
have_plausible_block(Hash, #blockchain{db=DB, plausible_blocks=CF}) ->
    case rocksdb:get(DB, CF, Hash, []) of
        {ok, _} -> true;
        _ -> false
    end.

-spec get_plausible_block(Hash :: blockchain_block:hash(), Chain :: blockchain()) -> {ok, blockchain_block:block()} | {error, term()}.
get_plausible_block(Hash, #blockchain{db=DB, plausible_blocks=CF}) ->
    case rocksdb:get(DB, CF, Hash, []) of
        {ok, BinBlock} ->
            Block = blockchain_block:deserialize(BinBlock),
            {ok, Block};
        Error -> Error
    end.

-spec get_raw_plausibles(Height :: non_neg_integer(), blockchain()) -> [binary()].
get_raw_plausibles(Height, #blockchain{db=DB, plausible_blocks=CF}) ->
    case rocksdb:get(DB, CF, <<Height:64/integer-unsigned-big>>, []) of
        {ok, BinList} ->
            Hashes = binary_to_term(BinList),
            lists:foldl(fun(Hash, Acc) ->
                                case rocksdb:get(DB, CF, Hash, []) of
                                    {ok, BinBlock} ->
                                        [BinBlock|Acc];
                                    _ ->
                                        Acc
                                end
                        end, [], Hashes);
        _ ->
            []
    end.

-spec check_plausible_blocks(blockchain()) -> ok.
check_plausible_blocks(Chain) ->
    check_plausible_blocks(Chain, <<>>).

-spec check_plausible_blocks(blockchain(), binary()) -> ok.
check_plausible_blocks(#blockchain{db=DB}=Chain, GossipedHash) ->
    blockchain_lock:acquire(), %% need the lock and we can get called without holding it
    Blocks = get_plausible_blocks(Chain),
    SortedBlocks = lists:sort(fun(A, B) -> blockchain_block:height(A) =< blockchain_block:height(B) end, Blocks),
    {ok, Batch} = rocksdb:batch(),
    lists:foreach(fun(Block) ->
                          Hash = blockchain_block:hash_block(Block),
                          try can_add_block(Block, Chain) of
                              {true, _IsRescue} ->
                                  %% TODO try to retain the binary block through here and pass it into add_block to
                                  %% save on another serialize() call
                                  add_block_(Block, Chain, GossipedHash /= Hash),
                                  remove_plausible_block(Chain, Batch, Hash, blockchain_block:height(Block));
                              exists ->
                                  case is_block_plausible(Block, Chain) of
                                      true ->
                                          %% still plausible, leave it alone
                                          ok;
                                      false ->
                                          remove_plausible_block(Chain, Batch, Hash, blockchain_block:height(Block))
                                  end;
                              _Error ->
                                  remove_plausible_block(Chain, Batch, Hash, blockchain_block:height(Block))
                          catch
                              _:_ ->
                                  remove_plausible_block(Chain, Batch, Hash, blockchain_block:height(Block))
                          end
                  end, SortedBlocks),
    rocksdb:write_batch(DB, Batch, [{sync, true}]),
    blockchain_lock:release().

-spec get_plausible_blocks(blockchain()) -> [blockchain_block:block()].
get_plausible_blocks(#blockchain{db=DB, plausible_blocks=CF}) ->
    {ok, Itr} = rocksdb:iterator(DB, CF, []),
    Res = get_plausible_blocks(Itr, rocksdb:iterator_move(Itr, first), []),
    catch rocksdb:iterator_close(Itr),
    Res.

get_plausible_blocks(_Itr, {error, _}, Acc) ->
    Acc;
get_plausible_blocks(Itr, {ok, <<_Height:64/integer-unsigned-big>>, _BinBlock}, Acc) ->
    get_plausible_blocks(Itr, rocksdb:iterator_move(Itr, next), Acc);
get_plausible_blocks(Itr, {ok, _Key, BinBlock}, Acc) ->
    NewAcc = try blockchain_block:deserialize(BinBlock) of
                 Block ->
                     [Block|Acc]
             catch
                 What:Why ->
                     lager:warning("error when deserializing plausible block at key ~p: ~p ~p", [_Key, What, Why]),
                     Acc
             end,
    get_plausible_blocks(Itr, rocksdb:iterator_move(Itr, next), NewAcc).

run_gc_hooks(Blockchain, _Hash) ->
    Ledger = blockchain:ledger(Blockchain),
    try
        ok = blockchain_ledger_v1:maybe_gc_pocs(Blockchain, Ledger),

        ok = blockchain_ledger_v1:maybe_gc_scs(Blockchain, Ledger),

        ok = blockchain_ledger_v1:maybe_recalc_price(Blockchain, Ledger) %,

        %% ok = blockchain_ledger_v1:refresh_gateway_witnesses(Hash, Ledger)
    catch What:Why:Stack ->
            lager:warning("hooks failed ~p ~p ~s", [What, Why, lager:pr_stacktrace(Stack, {What, Why})]),
            {error, gc_hooks_failed}
    end.

run_absorb_block_hooks(Syncing, Hash, Blockchain) ->
    Ledger = blockchain:ledger(Blockchain),
    case blockchain_ledger_v1:new_snapshot(Ledger) of
        {error, Reason}=Error ->
            lager:error("Error creating snapshot, Reason: ~p", [Reason]),
            Error;
        {ok, NewLedger} ->
            case application:get_env(blockchain, test_mode, false) of
                false ->
                    ok = blockchain_worker:notify({add_block, Hash, Syncing, NewLedger});
                true -> ok
            end
    end.

%%--------------------------------------------------------------------
%% @doc This function is exposed so that blockchain-node and simulator
%% can access the height from the snapshot when they load it via
%% `blockchain_follower` behavior.
%% @end
%%--------------------------------------------------------------------
-spec snapshot_height(Height :: non_neg_integer()) -> non_neg_integer().
snapshot_height(Height) ->
    case application:get_env(blockchain, honor_quick_sync, false) == true andalso
         application:get_env(blockchain, quick_sync_mode, assumed_valid) == blessed_snapshot of
        true ->
            Chain = blockchain_worker:blockchain(),
            {ok, ChainHeight} = blockchain:height(Chain),
            EndHeight = case Height > ChainHeight of
                            true ->
                                %% we've rolled back
                                0;
                            false ->
                                Height - 1
                        end,
            %% find the oldest block we have that's newer than the last known height
            case find_first_height_after(EndHeight, Chain) of
                {ok, FirstHeight, _Hash} ->
                    FirstHeight;
                {error, _} ->
                    %% not sure this is the right bail strat
                    ChainHeight
            end;
        false ->
            Height
    end.

follow_mode() ->
    application:get_env(blockchain, follow_mode, false).

-spec db_handle(Chain :: blockchain()) -> rocksdb:db_handle().
db_handle(Chain) -> Chain#blockchain.db.

-spec blocks_cf(Chain :: blockchain()) -> rocksdb:cf_handle().
blocks_cf(Chain) -> Chain#blockchain.blocks.

-spec heights_cf(Chain :: blockchain()) -> rocksdb:cf_handle().
heights_cf(Chain) -> Chain#blockchain.heights.

-spec info_cf(Chain :: blockchain()) -> rocksdb:cf_handle().
info_cf(Chain) -> Chain#blockchain.info.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    BaseDir = test_utils:tmp_dir("new_test"),
    Block = blockchain_block:new_genesis_block([]),
    Hash = blockchain_block:hash_block(Block),
    {ok, Chain} = new(BaseDir, Block, undefined, undefined),
    ?assertEqual({ok, Hash}, genesis_hash(Chain)),
    ?assertEqual({ok, Block}, genesis_block(Chain)),
    ?assertEqual({ok, Hash}, head_hash(Chain)),
    ?assertEqual({ok, Block}, head_block(Chain)),
    ?assertEqual({ok, Block}, get_block(Hash, Chain)),
    ?assertEqual({ok, Block}, get_block(1, Chain)),
    test_utils:cleanup_tmp_dir(BaseDir).

% ledger_test() ->
%     Block = blockchain_block:new_genesis_block([]),
%     Chain = new(Block, test_utils:tmp_dir("ledger_test")),
%     ?assertEqual(blockchain_ledger_v1:increment_height(blockchain_ledger_v1:new()), ledger(Chain)).

blocks_test_() ->
    {timeout, 30000,
     fun() ->
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

             meck:new(blockchain_gossip_handler),
             meck:expect(blockchain_gossip_handler, regossip_block, fun(_Block, _Height, _Hash, _SwarmTID) -> ok end),

             {ok, Pid} = blockchain_lock:start_link(),

             #{secret := Priv, public := Pub} = libp2p_crypto:generate_keys(ecc_compact),
             BinPub = libp2p_crypto:pubkey_to_bin(Pub),

             Vars = #{chain_vars_version => 2},
             Txn = blockchain_txn_vars_v1:new(Vars, 1, #{master_key => BinPub}),
             Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn),
             VarTxns = [blockchain_txn_vars_v1:key_proof(Txn, Proof)],

             GenBlock = blockchain_block:new_genesis_block(VarTxns),
             GenHash = blockchain_block:hash_block(GenBlock),
             TmpDir = test_utils:tmp_dir("blocks_test"),
             {ok, Chain} = new(TmpDir, GenBlock, undefined, undefined),
             Block = blockchain_block_v1:new(#{prev_hash => GenHash,
                                               height => 2,
                                               transactions => [],
                                               signatures => [],
                                               time => 1,
                                               hbbft_round => 1,
                                               election_epoch => 1,
                                               epoch_start => 0,
                                               seen_votes => [],
                                               bba_completion => <<>>
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
             meck:unload(blockchain_swarm),
             meck:unload(blockchain_gossip_handler),
             test_utils:cleanup_tmp_dir(TmpDir)
     end}.

get_block_test_() ->
    {timeout, 30000,
     fun() ->
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
             meck:new(blockchain_gossip_handler),
             meck:expect(blockchain_gossip_handler, regossip_block, fun(_Block, _Height, _Hash, _SwarmTID) -> ok end),

             {ok, Pid} = blockchain_lock:start_link(),

             #{secret := Priv, public := Pub} = libp2p_crypto:generate_keys(ecc_compact),
             BinPub = libp2p_crypto:pubkey_to_bin(Pub),

             Vars = #{chain_vars_version => 2},
             Txn = blockchain_txn_vars_v1:new(Vars, 1, #{master_key => BinPub}),
             Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn),
             VarTxns = [blockchain_txn_vars_v1:key_proof(Txn, Proof)],

             GenBlock = blockchain_block:new_genesis_block(VarTxns),
             GenHash = blockchain_block:hash_block(GenBlock),
             TmpDir = test_utils:tmp_dir("get_block_test"),
             {ok, Chain} = new(TmpDir, GenBlock, undefined, undefined),
             Block = blockchain_block_v1:new(#{prev_hash => GenHash,
                                               height => 2,
                                               transactions => [],
                                               signatures => [],
                                               time => 1,
                                               hbbft_round => 1,
                                               election_epoch => 1,
                                               epoch_start => 0,
                                               seen_votes => [],
                                               bba_completion => <<>>
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
             meck:unload(blockchain_swarm),
             meck:unload(blockchain_gossip_handler),
             test_utils:cleanup_tmp_dir(TmpDir)
     end
    }.

block_info_upgrade_test() ->
    %% boilerplate to get a chain
    #{secret := Priv, public := Pub} = libp2p_crypto:generate_keys(ecc_compact),
    BinPub = libp2p_crypto:pubkey_to_bin(Pub),
    Vars = #{chain_vars_version => 2},
    Txn = blockchain_txn_vars_v1:new(Vars, 1, #{master_key => BinPub}),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn),
    VarTxns = [blockchain_txn_vars_v1:key_proof(Txn, Proof)],
    GenBlock = blockchain_block:new_genesis_block(VarTxns),
    TmpDir = test_utils:tmp_dir("block_info_upgrade_test"),
    {ok, Chain} = new(TmpDir, GenBlock, undefined, undefined),

    Block = blockchain_block_v1:new(#{prev_hash => <<"prev_hash">>,
                                      height => 1,
                                      transactions => [],
                                      signatures => [],
                                      time => 1,
                                      hbbft_round => 1,
                                      election_epoch => 1,
                                      epoch_start => 0,
                                      seen_votes => [],
                                      bba_completion => <<>>
                                      }),
    V1BlockInfo = #block_info{  height = 1,
                                time = 1,
                                hash = <<"blockhash">>,
                                pocs = #{}},
    ExpV2BlockInfo = #block_info_v2{height = 1,
                                    time = 1,
                                    hash = <<"blockhash">>,
                                    pocs = #{},
                                    hbbft_round = 1,
                                    election_info = {1, 0},
                                    penalties = {<<>>, []}},
    V2BlockInfo = upgrade_block_info(V1BlockInfo, Block, Chain),
    ?assertMatch(V2BlockInfo, ExpV2BlockInfo).

-endif.
