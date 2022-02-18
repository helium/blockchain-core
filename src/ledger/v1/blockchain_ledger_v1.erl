%% @doc
%% == Blockchain Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_v1).

-export([
    new/1, new/5, new/6, new/7,
    dir/1,
    mode/1, mode/2,

    has_aux/1, is_aux/1,
    maybe_load_aux/1,

    open_db/5,

    aux_heights_cf/1,
    aux_heights_md_cf/1,
    aux_heights_diff_cf/1,
    aux_heights_diffsum_cf/1,

    aux_db/1,

    check_key/2, mark_key/2, unmark_key/2,

    new_context/1, give_context/2, new_direct_context/1, delete_context/1, remove_context/1, reset_context/1, commit_context/1,
    get_context/1, context_cache/1,

    get_block/2, get_raw_block/2, get_block_info/2,

    new_snapshot/1, context_snapshot/1, has_snapshot/2, release_snapshot/1, snapshot/1,

    drop_snapshots/1,

    current_height/1, current_height/2, increment_height/2,
    consensus_members/1, consensus_members/2,
    election_height/1, election_height/2,
    election_epoch/1, election_epoch/2,
    process_delayed_actions/3,

    active_gateways/1, snapshot_gateways/1, load_gateways/2,
    entries/1,
    htlcs/1,

    master_key/1, master_key/2,
    multi_keys/1, multi_keys/2,

    vars/3,
    config/2,  % no version with default, use the set value or fail

    vars_nonce/1, vars_nonce/2,
    save_threshold_txn/2,

    bootstrap_gw_denorm/1,

    find_gateway_info/2,
    find_gateway_location/2,
    find_gateway_owner/2,
    find_gateway_last_challenge/2,
    find_gateway_mode/2,
    find_gateways_by_owner/2,
    find_gateway_gain/2,
    %% todo add more here

    add_gateway/3, add_gateway/4, add_gateway/6,
    update_gateway/4,
    fixup_neighbors/4,
    add_gateway_location/4,
    add_gateway_gain/4,
    add_gateway_elevation/4,
    insert_witnesses/3,
    add_gateway_witnesses/3,
    refresh_gateway_witnesses/2,

    cg_versions/1,

    update_gateway_score/3, gateway_score/2,
    update_gateway_oui/4,
    gateway_count/1,

    find_pocs/2,
    find_poc/3,
    request_poc/6,
    delete_poc/3,
    maybe_gc_pocs/2,
    maybe_gc_scs/2,

    upgrade_pocs/1,

    find_entry/2,
    credit_account/3, debit_account/4, debit_fee_from_account/5,
    check_balance/3,

    dc_entries/1,
    find_dc_entry/2,
    credit_dc/3,
    debit_dc/4,
    debit_fee/3, debit_fee/4, debit_fee/6,
    check_dc_balance/3,
    check_dc_or_hnt_balance/4,

    hnt_burned/1,
    add_hnt_burned/2,
    clear_hnt_burned/1,

    net_overage/1,  net_overage/2,

    token_burn_exchange_rate/1,
    token_burn_exchange_rate/2,

    securities/1,
    find_security_entry/2,
    credit_security/3, debit_security/4,
    check_security_balance/3,

    find_htlc/2,
    add_htlc/8,
    redeem_htlc/4,

    get_oui_counter/1, set_oui_counter/2, increment_oui_counter/1,
    add_oui/5,

    find_routing/2, find_routing_for_packet/2,
    find_router_ouis/2,
    find_routing_via_eui/3,
    find_routing_via_devaddr/2,
    update_routing/4,
    get_routes/1,
    routing_cf/1,

    find_state_channel/3, find_state_channel_with_mod/3,
    count_open_scs_for_owner/3, find_sc_ids_by_owner/2, find_scs_by_owner/2,
    add_state_channel/7,
    close_state_channel/6,
    is_state_channel_overpaid/2,
    get_sc_mod/2,
    get_sc_max_actors/1,
    state_channel_key/2,
    allocate_subnet/2,

    delay_vars/3,

    fingerprint/1, fingerprint/2,
    raw_fingerprint/2, %% does not use cache
    cf_fold/4,

    maybe_recalc_price/2,
    add_oracle_price/2,
    current_oracle_price/1,
    next_oracle_prices/2,
    current_oracle_price_list/1,

    apply_raw_changes/2,

    set_hexes/2, get_hexes/1, get_hexes_list/1,
    set_hex/3, get_hex/2, delete_hex/2,

    add_to_hex/3,
    remove_from_hex/3,

    clean_all_hexes/1,

    bootstrap_h3dex/1,
    get_h3dex/1, delete_h3dex/1,
    lookup_gateways_from_hex/2,
    add_gw_to_hex/3,
    remove_gw_from_hex/3,
    add_commit_hook/4, add_commit_hook/5,
    remove_commit_hook/2,

    add_validator/4,
    get_validator/2,
    deactivate_validator/3,
    update_validator/3,
    fold_validators/3,

    cooldown_stake/5,
    get_cooldown_stake/2,

    query_circulating_hnt/1,
    query_staked_hnt/1,
    query_cooldown_hnt/1,

    %% snapshot save/restore stuff
    snapshot_vars/1,
    load_vars/2,
    snapshot_pocs/1,
    load_pocs/2,
    snapshot_accounts/1,
    load_accounts/2,
    snapshot_dc_accounts/1,
    load_dc_accounts/2,
    snapshot_security_accounts/1,
    load_security_accounts/2,
    snapshot_htlcs/1,
    load_htlcs/2,
    snapshot_ouis/1,
    load_ouis/2,
    snapshot_subnets/1,
    load_subnets/2,
    snapshot_state_channels/1,
    load_state_channels/2,
    snapshot_hexes/1,
    load_hexes/2,
    snapshot_h3dex/1,
    load_h3dex/2,
    snapshot_delayed_vars/1,
    load_delayed_vars/2,
    snapshot_threshold_txns/1,
    load_threshold_txns/2,

    snapshot_raw_gateways/1,
    load_raw_gateways/2,
    snapshot_raw_pocs/1,
    load_raw_pocs/2,
    snapshot_raw_accounts/1,
    load_raw_accounts/2,
    snapshot_raw_dc_accounts/1,
    load_raw_dc_accounts/2,
    snapshot_raw_security_accounts/1,
    load_raw_security_accounts/2,

    load_oracle_price/2,
    load_oracle_price_list/2,

    snapshot_validators/1,
    load_validators/2,
    snapshot_delayed_hnt/1,
    load_delayed_hnt/2,

    clean/1, clean_aux/1, close/1,
    compact/1,

    txn_fees_active/1,
    staking_fee_txn_oui_v1/1,
    staking_fee_txn_oui_v1_per_address/1,
    staking_fee_txn_add_gateway_v1/1,
    staking_fee_txn_add_dataonly_gateway_v1/1,
    staking_fee_txn_add_light_gateway_v1/1,
    staking_fee_txn_assert_location_v1/1,
    staking_fee_txn_assert_location_dataonly_gateway_v1/1,
    staking_fee_txn_assert_location_light_gateway_v1/1,
    staking_keys/1,
    staking_keys_to_mode_mappings/1,
    txn_fee_multiplier/1,

    dc_to_hnt/2,
    hnt_to_dc/2

]).

-include("blockchain.hrl").
-include("blockchain_ledger_v1.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_caps.hrl").
-include("blockchain_txn_fees.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").
-include_lib("helium_proto/include/blockchain_txn_rewards_v2_pb.hrl").


-ifdef(TEST).
-export([median/1, checkpoint_base/1, checkpoint_dir/2, clean_checkpoints/1]).
-endif.

-type entries() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_entry_v1:entry()}.
-type dc_entries() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_data_credits_entry_v1:data_credits_entry()}.
-type active_gateways() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_gateway_v2:gateway()}.
-type htlcs() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_htlc_v1:htlc()}.
-type securities() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_security_entry_v1:entry()}.
-type hexmap() :: #{h3:h3_index() => non_neg_integer()}.
-type hexlist() :: [{h3:h3_index(), non_neg_integer()}].
-type gateway_offsets() :: [{pos_integer(), libp2p_crypto:pubkey_bin()}].
-type state_channel_map() ::  #{blockchain_state_channel_v1:id() =>
                                    blockchain_ledger_state_channel_v1:state_channel()
                                    | blockchain_ledger_state_channel_v2:state_channel_v2()}.
-type h3dex() :: #{h3:h3_index() => [libp2p_crypto:pubkey_bin()]}. %% these keys are gateway addresses
-export_type([ledger/0]).

-spec new(file:filename_all()) -> ledger().
new(Dir) ->
    new(Dir, false, undefined, undefined, undefined, undefined).

-spec new(file:filename_all(), rocksdb:db_handle(), rocksdb:cf_handle(), rocksdb:cf_handle(), rocksdb:cf_handle()) -> ledger().
new(Dir, BlocksDB, BlocksCF, HeightsCF, InfoCF) ->
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    new(Dir, false, BlocksDB, BlocksCF, HeightsCF, InfoCF, GlobalOpts).

-spec new(file:filename_all(), boolean(), rocksdb:db_handle(), rocksdb:cf_handle(), rocksdb:cf_handle(), rocksdb:cf_handle()) -> ledger().
new(Dir, ReadOnly, BlocksDB, BlocksCF, HeightsCF, InfoCF) ->
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    new(Dir, ReadOnly, BlocksDB, BlocksCF, HeightsCF, InfoCF, GlobalOpts).

-spec new(file:filename_all(), boolean(), rocksdb:db_handle(), rocksdb:cf_handle(), rocksdb:cf_handle(), rocksdb:cf_handle(), rocksdb:cf_options()) -> ledger().
new(Dir, ReadOnly, BlocksDB, BlocksCF, HeightsCF, InfoCF, Options) ->
    L = new(Dir, ReadOnly, Options),

    %% allow config-set commit hooks in case we're worried about something being racy
    Hooks = lists:foldl(fun(Hook, Acc) ->
                            case Hook of
                                {CF, Predicate, HookIncFun, HookEndFun, IncludeHeight} ->
                                    HookRecord = [#hook{cf = CF, predicate = Predicate, hook_inc_fun = HookIncFun, hook_end_fun = HookEndFun, include_height = IncludeHeight}];
                                {CF, Predicate, HookIncFun, HookEndFun} ->
                                    HookRecord = [#hook{cf = CF, predicate = Predicate, hook_inc_fun = HookIncFun, hook_end_fun = HookEndFun}]
                            end,
                            lists:append(Acc, HookRecord)
                        end,
                        [],
                        application:get_env(blockchain, commit_hook_callbacks, [])),
    Ledger = maybe_load_aux(L#ledger_v1{
        blocks_db = BlocksDB,
        blocks_cf = BlocksCF,
        heights_cf = HeightsCF,
        info_cf = InfoCF,
        commit_hooks = Hooks
    }),
    sweep_old_checkpoints(Ledger),
    Ledger.

new(Dir, ReadOnly, Options) ->
    blockchain_utils:teardown_var_cache(),
    {ok, DB, CFs} = open_db(active, Dir, true, ReadOnly, Options),

    [DefaultCF, AGwsCF, EntriesCF, DCEntriesCF, HTLCsCF, PoCsCF, SecuritiesCF, RoutingCF,
     SubnetsCF, SCsCF, H3DexCF, GwDenormCF, ValidatorsCF,
     DelayedDefaultCF, DelayedAGwsCF, DelayedEntriesCF,
     DelayedDCEntriesCF, DelayedHTLCsCF, DelayedPoCsCF, DelayedSecuritiesCF,
     DelayedRoutingCF, DelayedSubnetsCF, DelayedSCsCF, DelayedH3DexCF,
     DelayedGwDenormCF, DelayedValidatorsCF] = CFs,
    Ledger =
    #ledger_v1{
        dir=Dir,
        db=DB,
        snapshots = ets:new(snapshot_cache, [set, public, {keypos, 1}]),
        mode=active,
        active= #sub_ledger_v1{
            default=DefaultCF,
            active_gateways=AGwsCF,
            gw_denorm=GwDenormCF,
            entries=EntriesCF,
            dc_entries=DCEntriesCF,
            htlcs=HTLCsCF,
            pocs=PoCsCF,
            securities=SecuritiesCF,
            routing=RoutingCF,
            subnets=SubnetsCF,
            state_channels=SCsCF,
            h3dex=H3DexCF,
            validators=ValidatorsCF
        },
        delayed= #sub_ledger_v1{
            default=DelayedDefaultCF,
            active_gateways=DelayedAGwsCF,
            gw_denorm=DelayedGwDenormCF,
            entries=DelayedEntriesCF,
            dc_entries=DelayedDCEntriesCF,
            htlcs=DelayedHTLCsCF,
            pocs=DelayedPoCsCF,
            securities=DelayedSecuritiesCF,
            routing=DelayedRoutingCF,
            subnets=DelayedSubnetsCF,
            state_channels=DelayedSCsCF,
            h3dex=DelayedH3DexCF,
            validators=DelayedValidatorsCF
        }
    },
    Ledger.

sweep_old_checkpoints(Ledger) ->
    try
        DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
        {ok, DelayedHeight} = current_height(DelayedLedger),
        BaseDir = application:get_env(blockchain, base_dir, "data"),
        CPs = filename:join([BaseDir, "checkpoints"]),
        {ok, Subdirs} = file:list_dir(CPs),
        ToDelete =
            lists:filter(
              fun(HeightStr) ->
                      try
                          Height = list_to_integer(HeightStr),
                          Height < DelayedHeight
                      catch _:_ -> false
                      end
              end, Subdirs),
        lists:map(fun(Dir) ->
                          remove_checkpoint(filename:join([BaseDir, "checkpoints", Dir, ?DB_FILE]))
                  end,
                  ToDelete)
    catch _:_ ->
            %% this crashes on clean startup, just ignore it
            ok
    end,
    ok.

clean_checkpoints(#ledger_v1{dir = RecordDir}) ->
    clean_checkpoints(RecordDir);
clean_checkpoints(RecordDir) ->
    try
        BaseDir = checkpoint_base(RecordDir),
        CPs = filename:join([BaseDir, "checkpoints"]),
        {ok, Subdirs} = file:list_dir(CPs),
        lists:map(fun(Dir) ->
                          remove_checkpoint(filename:join([BaseDir, "checkpoints", Dir, ?DB_FILE]))
                  end,
                  Subdirs)
    catch _:_ ->
            %% this crashes on clean startup, just ignore it
            ok
    end,
    ok.

-spec maybe_load_aux(Ledger :: ledger()) -> ledger().
maybe_load_aux(Ledger) ->
    case application:get_env(blockchain, aux_ledger_dir, undefined) of
        undefined ->
            Ledger;
        Path ->
            blockchain_aux_ledger_v1:bootstrap(Path, Ledger)
    end.

-spec mode(ledger()) -> mode().
mode(Ledger) ->
    Ledger#ledger_v1.mode.

-spec has_aux(ledger()) -> boolean().
has_aux(Ledger) ->
    Ledger#ledger_v1.aux /= undefined.

-spec is_aux(ledger()) -> boolean().
is_aux(#ledger_v1{mode=aux}) ->
    true;
is_aux(_) ->
    false.

-spec mode(mode(), ledger()) -> ledger().
mode(aux, #ledger_v1{aux=undefined}) ->
    error(no_aux_ledger);
mode(Mode, Ledger) ->
    Ledger#ledger_v1{mode=Mode}.

-spec dir(ledger()) -> file:filename_all().
dir(Ledger) ->
    Ledger#ledger_v1.dir.

check_key(Key, Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, Key, []) of
        {ok, <<"true">>} ->
            true;
        not_found ->
            false;
        Error ->
            Error
    end.

mark_key(Key, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, Key, <<"true">>).

unmark_key(Key, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_delete(Ledger, DefaultCF, Key).


-spec new_context(ledger()) -> ledger().
new_context(Ledger) ->
    %% accumulate ledger changes in a read-through ETS cache
    Cache = ets:new(txn_cache, [set, protected, {keypos, 1}]),
    GwCache = ets:new(gw_cache, [set, protected, {keypos, 1}]),
    context_cache(Cache, GwCache, Ledger).


give_context(Ledger, Pid) ->
    case ?MODULE:context_cache(Ledger) of
        {undefined, undefined} ->
            ok;
        {direct, _GwCache} ->
            ok;
        {Cache, _GwCache} ->
            ets:give_away(Cache, Pid, Ledger),
            ok
    end.


-spec new_direct_context(ledger()) -> ledger().
new_direct_context(Ledger) ->
    GwCache = ets:new(gw_cache, [set, protected, {keypos, 1}]),
    context_cache(direct, GwCache, Ledger).


get_context(Ledger) ->
    case ?MODULE:context_cache(Ledger) of
        {undefined, undefined} ->
            undefined;
        Cache ->
            flatten_cache(Cache)
    end.

flatten_cache({Cache, GwCache}) ->
    {ets:tab2list(Cache), ets:tab2list(GwCache)}.

-spec delete_context(ledger()) -> ledger().
delete_context(Ledger) ->
    case ?MODULE:context_cache(Ledger) of
        {undefined, undefined} ->
            Ledger;
        {direct, GwCache} ->
            catch ets:delete(GwCache),
            context_cache(undefined, undefined, Ledger);
        {Cache, GwCache} ->
            catch ets:delete(Cache),
            catch ets:delete(GwCache),
            context_cache(undefined, undefined, Ledger)
    end.

%% @doc remove a context without deleting it, useful if you need a
%% view of the actual ledger while absorbing
-spec remove_context(ledger()) -> ledger().
remove_context(Ledger) ->
    case ?MODULE:context_cache(Ledger) of
        {undefined, undefined} ->
            Ledger;
        {_Cache, _GwCache} ->
            context_cache(undefined, undefined, Ledger)
    end.

-spec reset_context(ledger()) -> ok.
reset_context(Ledger) ->
    case ?MODULE:context_cache(Ledger) of
        {undefined, undefined} ->
            ok;
        {direct, GwCache} ->
            true = ets:delete_all_objects(GwCache),
            ok;
        {Cache, GwCache} ->
            true = ets:delete_all_objects(Cache),
            true = ets:delete_all_objects(GwCache),
            ok
    end.

-spec commit_context(ledger()) -> ok.
commit_context(Ledger) ->
    DB = db(Ledger),
    case ?MODULE:context_cache(Ledger) of
        {direct, _GwCache} ->
            ok;
        {Cache, _GwCache} ->
            {Callbacks, Batch} = batch_from_cache(Cache, Ledger),
            delete_context(Ledger),
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
            rocksdb:release_batch(Batch),
            case mode(Ledger) of
                active ->
                    Callbacks();
                _ -> ok
            end
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec context_cache(ledger()) -> {undefined | direct | ets:tid(), undefined | ets:tid()}.
context_cache(Ledger) ->
    SL = subledger(Ledger),
    #sub_ledger_v1{cache=Cache, gateway_cache=GwCache} = SL,
    {Cache, GwCache}.

-spec new_snapshot(ledger()) -> {ok, ledger()} | {error, any()}.
new_snapshot(#ledger_v1{db=DB,
                        snapshot=undefined,
                        snapshots=Cache,
                        mode=Mode,
                        active=#sub_ledger_v1{cache=undefined},
                        delayed=#sub_ledger_v1{cache=undefined}}=Ledger)
  when Mode == active; Mode == aux_load ->
    {ok, Height} = current_height(Ledger),
    Me = self(),
    Old = {Height, {pending, Me}},
    case ets:insert_new(Cache, Old) of
        false -> {ok, Ledger};
        _ ->
            %% take a real rocksdb snapshot here and put that in the cache
            %% instead of using a checkpoint ledger as it's likely faster and cheaper
            case rocksdb:snapshot(DB) of
                {ok, SnapshotHandle} ->
                    {ok, Height} = current_height(Ledger),
                    DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
                    {ok, DelayedHeight} = current_height(DelayedLedger),
                    DeleteHeight = DelayedHeight - 1,
                    case ets:lookup(Cache, DeleteHeight) of
                        [{_DeleteHeight, {snapshot, DeleteSnap}}] ->
                            rocksdb:release_snapshot(DeleteSnap);
                        _ ->
                            ok
                    end,
                    ets:delete(Cache, DeleteHeight),
                    1 = ets:select_replace(Cache, [{Old, [], [{const, {Height, {snapshot, SnapshotHandle}}}]}]),
                    %% take a checkpoint as well for use after a restart
                    %% This is treated as atomic and there are no further updates required, unlike
                    %% context_snapshot

                    %% checkpoints are not needed in follow mode, and are quite expensive to create
                    %% each block.
                    case application:get_env(blockchain, follow_mode, false) of
                        true ->
                            {ok, Ledger};
                        false ->
                            CheckpointDir = checkpoint_dir(Ledger, Height),
                            ok = filelib:ensure_dir(CheckpointDir),
                            case rocksdb:checkpoint(DB, CheckpointDir) of
                                ok ->
                                    DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
                                    {ok, DelayedHeight} = current_height(DelayedLedger),
                                    OldDir = checkpoint_dir(Ledger, DeleteHeight),
                                    remove_checkpoint(OldDir),
                                    {ok, Ledger};
                                {error, Reason1}=Error1 ->
                                    lager:error("Error creating new checkpoint for snapshot reason: ~p", [Reason1]),
                                    Error1
                            end
                    end;
                {error, Reason}=Error ->
                    lager:error("Error creating new snapshot reason: ~p", [Reason]),
                    Error
            end
    end;
new_snapshot(#ledger_v1{}) ->
    erlang:error(cannot_snapshot_delayed_ledger).

checkpoint_base(Dir) ->
    try {list_to_integer(filename:basename(Dir)), filename:basename(filename:dirname(Dir))} of
        {X, "checkpoints"} when is_integer(X) ->
            filename:dirname(filename:dirname(Dir));
        _ ->
            Dir
    catch _:_ ->
            Dir
    end.

checkpoint_dir(#ledger_v1{dir=Dir}, Height) ->
    BaseDir = checkpoint_base(Dir),
    filename:join([BaseDir, "checkpoints", integer_to_list(Height), ?DB_FILE]).

remove_checkpoint(CheckpointDir) ->
    file:delete(filename:join(CheckpointDir, "delayed")),
    rocksdb:destroy(CheckpointDir, []),
    %% remove any temp dirs that got orphaned
    [begin rocksdb:destroy(TmpDir, []), file:del_dir(filename:dirname(TmpDir)) end || TmpDir <- filelib:wildcard(CheckpointDir ++ "-[0-9]*/" ++ ?DB_FILE)],
    file:del_dir(filename:dirname(CheckpointDir)).

context_snapshot(#ledger_v1{db=DB, snapshots=Cache, mode=Mode} = Ledger) ->
    case application:get_env(blockchain, follow_mode, false) of
        true ->
            {ok, Ledger};
        false ->
            {ok, Height} = current_height(Ledger),
            CheckpointDir = checkpoint_dir(Ledger, Height),
            case ets:lookup(Cache, Height) of
                [{Height, {pending, _Pid}}] ->
                    has_snapshot(Height, Ledger);
                [{_Height, _SnapLedger}] ->
                    %% ledger already exists
                    has_snapshot(Height, Ledger);
                [] ->
                    Me = self(),
                    Old = {Height, {pending, Me}},
                    case ets:insert_new(Cache, Old) of
                        false -> has_snapshot(Height, Ledger);
                        _ ->
                            case filelib:is_dir(CheckpointDir) of
                                true ->
                                    %% this should be quite unlikely
                                    has_snapshot(Height, Ledger);
                                false ->
                                    ok = filelib:ensure_dir(CheckpointDir),
                                    %% use a temp dir and then an atomic rename so we don't accidentally
                                    %% take a checkpoint and omit writing the delayed file or the updates
                                    %% because of a crash or a restart
                                    %% note that this MUST be a subdirectory, not a sibling directory
                                    %% of the final db dir. This is because we assume the database is always called ledger.db and so there can only be one per directory
                                    TmpDir = lists:flatten(io_lib:format("~s-~p/~s", [CheckpointDir, erlang:system_time(), ?DB_FILE])),
                                    ok = filelib:ensure_dir(TmpDir),
                                    ok = rocksdb:checkpoint(DB, TmpDir),
                                    case Mode of
                                        delayed ->
                                            file:write_file(filename:join(TmpDir, "delayed"), <<>>);
                                        active ->
                                            ok
                                    end,
                                    %% open the checkpoint read-write and commit the changes in the ETS table into it
                                    Ledger2 = new(filename:dirname(TmpDir), false, []),
                                    Ledger3 = blockchain_ledger_v1:mode(Mode, Ledger2),
                                    #sub_ledger_v1{cache=ECache, gateway_cache=GwCache} = subledger(Ledger),
                                    lager:info("dumping ~p elements to checkpoint in ~p mode", [length(ets:tab2list(ECache)), Mode]),
                                    commit_context(context_cache(ECache, GwCache, Ledger3)),
                                    close(Ledger3),
                                    %% ok, we've done everything we need to do to the checkpoint, so move it into place
                                    %% now.
                                    case file:rename(TmpDir, CheckpointDir) of
                                        ok ->
                                            lager:info("renamed checkpoint from ~p to ~p", [TmpDir, CheckpointDir]),
                                            file:del_dir(filename:dirname(TmpDir)),
                                            ok;
                                        {error, Reason} ->
                                            lager:info("rename ~p to ~p failed ~p", [TmpDir, CheckpointDir, Reason]),
                                            %% someone likely beat us to it
                                            file:delete(filename:join([TmpDir, "delayed"])),
                                            rocksdb:destroy(TmpDir, []),
                                            file:del_dir(filename:dirname(TmpDir)),
                                            ets:delete(Cache, Height)
                                    end,
                                    has_snapshot(Height, Ledger)
                            end
                    end
            end
    end.

has_snapshot(Height, Ledger) ->
    has_snapshot(Height, Ledger, 120).
has_snapshot(_Height, _Ledger, 0) ->
    {error, too_many_retries};
has_snapshot(Height, #ledger_v1{snapshots=Cache} = Ledger, Retries) ->
    Me = self(),
    case ets:lookup(Cache, Height) of
        [{Height, {pending, Pid}} = OtherPend] when Pid /= Me ->
            lager:debug("other pid ~p has the snapshot lock for ~p", [Pid, Height]),
            case is_process_alive(Pid) of
                true ->
                    timer:sleep(500),
                    has_snapshot(Height, Ledger, Retries - 1);
                false ->
                    case ets:select_replace(Cache,
                                            [{OtherPend, [],
                                              [{const, {Height, {pending, Me}}}]}]) of
                        %% we grabbed the lock, proceed by restarting
                        1 -> has_snapshot(Height, Ledger, Retries);
                        0 ->
                            timer:sleep(500),
                            has_snapshot(Height, Ledger, Retries - 1)
                    end
            end;
        Res when Res == [] orelse Res == [{Height, {pending, Me}}] ->
            %% try to mark pending since this can take a bit
            Old = {Height, {pending, Me}},
            GotLock = case Res of
                [] ->
                    %% nobody has the lock, try to take it
                    ets:insert_new(Cache, Old);
                _ ->
                    %% we have the lock
                    true
            end,
            case GotLock of
                false ->
                    %% wait on whoever has the lock
                    has_snapshot(Height, Ledger);
                true ->
                    lager:debug("uncached checkpoint @ ~p", [Height]),
                    CheckpointDir = checkpoint_dir(Ledger, Height),
                    case filelib:is_dir(CheckpointDir) of
                        true ->
                            Mode = case filelib:is_regular(filename:join(CheckpointDir, "delayed")) of
                                       true ->
                                           delayed;
                                       false ->
                                           active
                                   end,
                            try
                                lager:info("loading checkpoint from disk with ledger mode ~p", [Mode]),
                                %% new/2 wants to add on the ledger.db part itself
                                NewLedger = new(filename:dirname(CheckpointDir), true, []),
                                %% share the snapshot cache with the new ledger
                                NewLedger2 = blockchain_ledger_v1:mode(Mode,
                                                                       NewLedger#ledger_v1{
                                                                         blocks_db = Ledger#ledger_v1.blocks_db,
                                                                         blocks_cf = Ledger#ledger_v1.blocks_cf,
                                                                         heights_cf = Ledger#ledger_v1.heights_cf,
                                                                         info_cf = Ledger#ledger_v1.info_cf,
                                                                         snapshots=Cache}),
                                %% sanity check
                                case current_height(NewLedger2) of
                                    {ok, Height} ->
                                        1 = ets:select_replace(Cache, [{Old, [], [{const, {Height, {ledger, NewLedger2}}}]}]),
                                        {ok, new_context(NewLedger2)};
                                    {ok, OtherHeight} ->
                                        lager:warning("expected checkpoint ledger at height ~p but got height ~p",
                                                      [Height, OtherHeight]),
                                        %% just blow it away and let it get re-calculated
                                        remove_checkpoint(CheckpointDir),
                                        ets:delete(Cache, Height),
                                        {error, snapshot_not_found}
                                end
                            catch What:Why ->
                                      lager:warning("error opening checkpoint: ~p ~p", [What, Why]),
                                      remove_checkpoint(CheckpointDir),
                                      ets:delete(Cache, Height),
                                      {error, snapshot_not_found}
                            end;
                        _ ->
                            lager:warning("couldn't find checkpoint dir? for ~p", [Height]),
                            ets:delete(Cache, Height),
                            {error, snapshot_not_found}
                    end
            end;
        [{Height, {snapshot, SnapshotHandle}}] ->
            lager:debug("snapshot @ ~p", [Height]),
            %% because the snapshot was taken as we ingested a block to the leading ledger we need to query it
            %% as an active ledger to get the right information at this desired height
            {ok, blockchain_ledger_v1:new_context(
                   blockchain_ledger_v1:mode(active,
                                             Ledger#ledger_v1{snapshot=SnapshotHandle}))
            };
        [{Height, {ledger, SnapLedger}}] ->
            lager:debug("cached checkpoint @ ~p", [Height]),
            {ok, new_context(SnapLedger)}
    end.

-spec release_snapshot(ledger()) -> ok | {error, any()}.
release_snapshot(#ledger_v1{snapshot=undefined}) ->
    {error, undefined_snapshot};
release_snapshot(#ledger_v1{snapshot=Snapshot}) ->
    rocksdb:release_snapshot(Snapshot).

-spec snapshot(ledger()) -> {ok, rocksdb:snapshot_handle()} | {error, undefined}.
snapshot(Ledger) ->
    case Ledger#ledger_v1.snapshot of
        undefined ->
            {error, undefined};
        S ->
            {ok, S}
    end.

-spec drop_snapshots(ledger()) -> true.
drop_snapshots(#ledger_v1{snapshots=Cache}=L) ->
    clean_checkpoints(L),
    ets:delete_all_objects(Cache).

-spec subledger(ledger()) -> sub_ledger().
subledger(Ledger = #ledger_v1{mode=Mode}) ->
    case Mode of
        active -> Ledger#ledger_v1.active;
        delayed -> Ledger#ledger_v1.delayed;
        T when T == aux; T == aux_load -> Ledger#ledger_v1.aux#aux_ledger_v1.aux
    end.

-spec subledger(ledger(), sub_ledger()) -> ledger().
subledger(Ledger = #ledger_v1{mode=Mode}, NewSubLedger) ->
    case Mode of
        active -> Ledger#ledger_v1{active=NewSubLedger};
        delayed -> Ledger#ledger_v1{delayed=NewSubLedger};
        T when T == aux; T == aux_load -> Ledger#ledger_v1{aux=Ledger#ledger_v1.aux#aux_ledger_v1{aux=NewSubLedger}}
    end.

-spec db(ledger()) -> rocksdb:db_handle().
db(Ledger = #ledger_v1{mode=Mode}) ->
    case Mode of
        active -> Ledger#ledger_v1.db;
        delayed -> Ledger#ledger_v1.db;
        T when T == aux; T == aux_load -> Ledger#ledger_v1.aux#aux_ledger_v1.db
    end.


atom_to_cf(Atom, Ledger) ->
    SL = subledger(Ledger),
        case Atom of
            default -> SL#sub_ledger_v1.default;
            active_gateways -> SL#sub_ledger_v1.active_gateways;
            gw_denorm -> SL#sub_ledger_v1.gw_denorm;
            entries -> SL#sub_ledger_v1.entries;
            dc_entries -> SL#sub_ledger_v1.dc_entries;
            htlcs -> SL#sub_ledger_v1.htlcs;
            pocs -> SL#sub_ledger_v1.pocs;
            securities -> SL#sub_ledger_v1.securities;
            routing -> SL#sub_ledger_v1.routing;
            subnets -> SL#sub_ledger_v1.subnets;
            state_channels -> SL#sub_ledger_v1.state_channels;
            h3dex -> SL#sub_ledger_v1.h3dex;
            validators -> SL#sub_ledger_v1.validators
        end.

apply_raw_changes(Changes, #ledger_v1{db = DB} = Ledger) ->
    {ok, Batch} = rocksdb:batch(),
    apply_raw_changes(Changes, Ledger, Batch),
    rocksdb:write_batch(DB, Batch, [{sync, true}]),
    rocksdb:release_batch(Batch).

apply_raw_changes([], _, _) ->
    ok;
apply_raw_changes([{Atom, Changes}|Tail], Ledger, Batch) ->
    CF = atom_to_cf(Atom, Ledger),
    lists:foreach(fun({changed, Key, _OldValue, Value}) ->
                          rocksdb:batch_put(Batch, CF, Key, Value);
                     ({added, Key, Value}) ->
                          rocksdb:batch_put(Batch, CF, Key, Value);
                     ({deleted, Key, _OldValue}) ->
                          rocksdb:batch_delete(Batch, CF, Key)
                  end, Changes),
    apply_raw_changes(Tail, Ledger, Batch).

cf_fold(CF, F, Acc, Ledger) ->
    try
        CFRef = atom_to_cf(CF, Ledger),
        cache_fold(Ledger, {CF, db(Ledger), CFRef}, F, Acc)
    catch C:E:S ->
            {error, {could_not_fold, C, E, S}}
    end.

fingerprint(Ledger) ->
    fingerprint(Ledger, false).

fingerprint(Ledger, Extended) ->
    {ok, Height} = current_height(Ledger),
    e2qc:cache(fp_cache, {Height, Extended},
               fun() ->
                       raw_fingerprint(Ledger, Extended)
               end).

raw_fingerprint(Ledger, Extended) ->
    try
        %% NB: remove multi_keys when they go live
        Filter = ?BC_UPGRADE_NAMES ++ [<<"transaction_fee">>, <<"multi_keys">>],
        DefaultHash0 =
            cache_fold(
              Ledger, default_cf(Ledger),
              %% if any of these are in the CF, it's a result of an
              %% old, fixed bug, they're safe to ignore.
              fun({<<"$block_", _/binary>>, _}, Acc) ->
                      Acc;
                 ({K, _} = X, Acc) ->
                      case lists:member(K, Filter) of
                          true -> Acc;
                          _ -> crypto:hash_update(Acc, term_to_binary(X))
                      end
              end, crypto:hash_init(md5)),
        DefaultHash = crypto:hash_final(DefaultHash0),
        L0 =
            [cache_fold(Ledger, CF,
                        fun({K, V}, Acc) when Mod == state_channel ->
                                {_Mod, SC} = deserialize_state_channel(V),
                                crypto:hash_update(Acc, term_to_binary({K, SC}));
                           ({K, V}, Acc) when Mod /= undefined ->
                                crypto:hash_update(Acc, term_to_binary({K, Mod:deserialize(V)}));
                           (X, Acc) ->
                                crypto:hash_update(Acc, term_to_binary(X))
                        end,
                        crypto:hash_init(md5))
             || {CF, Mod} <-
                    [{active_gateways_cf(Ledger), blockchain_ledger_gateway_v2},
                     {entries_cf(Ledger), blockchain_ledger_entry_v1},
                     {dc_entries_cf(Ledger), blockchain_ledger_data_credits_entry_v1},
                     {htlcs_cf(Ledger), blockchain_ledger_htlc_v1},
                     {pocs_cf(Ledger), blockchain_ledger_poc_v2},
                     {securities_cf(Ledger), blockchain_ledger_security_entry_v1},
                     {routing_cf(Ledger), blockchain_ledger_routing_v1},
                     {state_channels_cf(Ledger), state_channel},
                     {subnets_cf(Ledger), undefined}
                    ]],
        L = [DefaultHash | lists:map(fun crypto:hash_final/1, L0)],
        LedgerHash = crypto:hash_final(
                       lists:foldl(
                         fun(Hs, Ctx) -> crypto:hash_update(Ctx, Hs) end,
                         crypto:hash_init(md5),
                         L)),
        case Extended of
            false ->
                {ok, #{<<"ledger_fingerprint">> => LedgerHash}};
            _ ->
                [_, GWsHash, EntriesHash, DCEntriesHash, HTLCsHash,
                 PoCsHash, SecuritiesHash, RoutingsHash, StateChannelsHash, SubnetsHash] = L,
                {ok, #{<<"ledger_fingerprint">> => LedgerHash,
                       <<"gateways_fingerprint">> => GWsHash,
                       <<"core_fingerprint">> => DefaultHash,
                       <<"entries_fingerprint">> => EntriesHash,
                       <<"dc_entries_fingerprint">> => DCEntriesHash,
                       <<"htlc_fingerprint">> => HTLCsHash,
                       <<"securities_fingerprint">> => SecuritiesHash,
                       <<"routings_fingerprint">> => RoutingsHash,
                       <<"poc_fingerprint">> => PoCsHash,
                       <<"state_channels_fingerprint">> => StateChannelsHash,
                       <<"subnets_fingerprint">> => SubnetsHash
                      }}
        end
    catch C:E:S ->
            lager:warning("fp error ~p:~p ~p", [C, E, S]),
            {error, could_not_fingerprint}
    end.

-spec current_height(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
current_height(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?CURRENT_HEIGHT, []) of
        {ok, <<Height:64/integer-unsigned-big>>} ->
            {ok, Height};
        not_found ->
            {ok, 0};
        Error ->
            Error
    end.

-spec current_height(pos_integer(), ledger()) -> ok | {error, any()}.
current_height(Height, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?CURRENT_HEIGHT, <<Height:64/integer-unsigned-big>>).

-spec increment_height(blockchain_block:block(), ledger()) -> ok | {error, any()}.
increment_height(Block, Ledger) ->
    DefaultCF = default_cf(Ledger),
    BlockHeight = blockchain_block:height(Block),
    case current_height(Ledger) of
        {error, _} ->
            cache_put(Ledger, DefaultCF, ?CURRENT_HEIGHT, <<1:64/integer-unsigned-big>>);
        {ok, Height0} ->
            Height1 = erlang:max(BlockHeight, Height0),
            cache_put(Ledger, DefaultCF, ?CURRENT_HEIGHT, <<Height1:64/integer-unsigned-big>>)
    end.

-spec consensus_members(ledger()) -> {ok, [libp2p_crypto:pubkey_bin()]} | {error, any()}.
consensus_members(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?CONSENSUS_MEMBERS, []) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec consensus_members([libp2p_crypto:pubkey_bin()], ledger()) ->  ok | {error, any()}.
consensus_members(Members, Ledger) ->
    Bin = erlang:term_to_binary(Members),
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?CONSENSUS_MEMBERS, Bin).

-spec election_height(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
election_height(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?ELECTION_HEIGHT, []) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec election_height(non_neg_integer(), ledger()) -> ok | {error, any()}.
election_height(Height, Ledger) ->
    Bin = erlang:term_to_binary(Height),
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?ELECTION_HEIGHT, Bin).

-spec election_epoch(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
election_epoch(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?ELECTION_EPOCH, []) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {ok, 0};
        Error ->
            Error
    end.

-spec election_epoch(non_neg_integer(), ledger()) -> ok | {error, any()}.
election_epoch(Epoch, Ledger) ->
    Bin = erlang:term_to_binary(Epoch),
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?ELECTION_EPOCH, Bin).

process_delayed_actions(Block, Ledger, Chain) ->
    DefaultCF = default_cf(Ledger),
    ok = process_threshold_txns(DefaultCF, Ledger, Chain),
    PendingTxns =
        case cache_get(Ledger, DefaultCF, block_name(Block), []) of
            {ok, BP} ->
                binary_to_term(BP);
            not_found ->
                []
                %% function clause on error for now since this could
                %% cause a fork, should we exception here and hope an
                %% eventual retry will work?
        end,
    lists:foreach(
      fun(Hash) ->
              {ok, Bin} = cache_get(Ledger, DefaultCF, Hash, []),
              {Type, Txn} = binary_to_term(Bin),
              case Type:delayed_absorb(Txn, Ledger) of
                  ok ->
                      cache_delete(Ledger, DefaultCF, Hash);
                  {error, Reason} ->
                      lager:error("problem applying delayed txn: ~p", [Reason]),
                      error(bad_delayed_txn)
              end
      end,
      PendingTxns),
    cache_delete(Ledger, DefaultCF, block_name(Block)),

    %% do the stake cooldown work
    HeightEntries =
        case cache_get(Ledger, DefaultCF, cd_block_name(Block), []) of
            {ok, HE} ->
                binary_to_term(HE);
            not_found ->
                [];
            {error, Reason} -> error(Reason)
        end,
    lists:foreach(
      fun(#validator_stake_v1{owner = O,
                              validator = V,
                              stake = S}) ->
              ok = finalize_validator(V, Ledger),
              ok = credit_account(O, S, Ledger)
      end,
      HeightEntries),
    cache_delete(Ledger, DefaultCF, cd_block_name(Block)),
    ok.

delay_vars(Effective, Vars, Ledger) ->
    DefaultCF = default_cf(Ledger),
    %% save the vars txn to disk
    Hash = blockchain_txn_vars_v1:hash(Vars),
    cache_put(Ledger, DefaultCF, Hash, term_to_binary({blockchain_txn_vars_v1, Vars})),
    PendingTxns =
        case cache_get(Ledger, DefaultCF, block_name(Effective), []) of
            {ok, BP} ->
                binary_to_term(BP);
            not_found ->
                []
            %% Error ->  % just gonna function clause for now
            %%     %% since this could cause a fork, should we exception
            %%     %% here and hope an eventual retry will work?
            %%     []
        end,
    PendingTxns1 = PendingTxns ++ [Hash],
    cache_put(Ledger, DefaultCF, block_name(Effective),
              term_to_binary(PendingTxns1)).

block_name(Block) ->
    <<"$block_", (integer_to_binary(Block))/binary>>.

cd_block_name(Block) ->
    <<"$cd_block_", (integer_to_binary(Block))/binary>>.

-spec save_threshold_txn(blockchain_txn_vars_v1:txn_vars(), ledger()) ->  ok | {error, any()}.
save_threshold_txn(Txn, Ledger) ->
    DefaultCF = default_cf(Ledger),
    Bin = term_to_binary(Txn),
    Name = threshold_name(Txn),
    cache_put(Ledger, DefaultCF, Name, Bin).

threshold_name(Txn) ->
    Nonce = blockchain_txn_vars_v1:nonce(Txn),
    <<"$threshold_txn_", (integer_to_binary(Nonce))/binary>>.

process_threshold_txns(CF, Ledger, Chain) ->
    [case blockchain_txn_vars_v1:maybe_absorb(Txn, Ledger, Chain) of
         false -> ok;
         %% true here means we've passed the threshold and have
         %% scheduled the var to be committed in the future, so we can
         %% safely delete it from the list
         true -> cache_delete(Ledger, CF, threshold_name(Txn))
     end
     || Txn <- scan_threshold_txns(Ledger, CF)],
    ok.

scan_threshold_txns(Ledger, CF) ->
    L = cache_fold(Ledger, CF,
                   fun({_Name, BValue}, Acc) ->
                           Value = binary_to_term(BValue),
                           [Value | Acc]
                   end, [],
                   [{start, {seek, <<"$threshold_txn_">>}},
                    {iterate_upper_bound, <<"$threshold_txn`">>}]),
    lists:reverse(L).

-spec active_gateways(ledger()) -> active_gateways().
active_gateways(Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    cache_fold(
      Ledger,
      AGwsCF,
      fun({Address, Binary}, Acc) ->
              Gw = blockchain_ledger_gateway_v2:deserialize(Binary),
              maps:put(Address, Gw, Acc)
      end,
      #{}
     ).

-spec gateway_count(ledger()) -> non_neg_integer().
gateway_count(Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    cache_fold(
      Ledger,
      AGwsCF,
      fun({_Address, _Binary}, Acc) ->
              %% TODO perhaps we want to filter inactive GWs here?
              Acc + 1
      end,
      0
     ).


%% note that instead of calling lists:sort(maps:to_list(active_gateways())) here
%% we construct the list in the same order without an intermediate map to make less
%% garbage
snapshot_gateways(Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    lists:reverse(cache_fold(
                    Ledger,
                    AGwsCF,
                    fun({Address, Binary}, Acc) ->
                            Gw = blockchain_ledger_gateway_v2:deserialize(Binary),
                            [{Address, Gw}| Acc]
                    end,
                    []
                   )).

-spec snapshot_raw_gateways(ledger()) -> [{binary(), binary()}].
snapshot_raw_gateways(Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    snapshot_raw(AGwsCF, Ledger).

load_raw_gateways(Gateways, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    load_raw(Gateways, AGwsCF, Ledger).

snapshot_validators(Ledger) ->
    ValsCF = validators_cf(Ledger),
    snapshot_raw(ValsCF, Ledger).

load_validators(Gateways, Ledger) ->
    ValsCF = validators_cf(Ledger),
    load_raw(Gateways, ValsCF, Ledger).

-spec load_gateways([{libp2p_crypto:pubkey_bin(), blockchain_ledger_gateway_v2:gateway()}],
                    ledger()) -> ok | {error, _}.
load_gateways(Gws, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    maps:map(
      fun(Address, Gw) ->
              Bin = blockchain_ledger_gateway_v2:serialize(Gw),
              write_gw_denorm_values(Address, new, Gw, Ledger),
              cache_put(Ledger, AGwsCF, Address, Bin)
      end,
      maps:from_list(Gws)),
    ok.

write_gw_denorm_values(Address, new, Gw, Ledger) ->
    GwDenormCF = gw_denorm_cf(Ledger),
    Location = blockchain_ledger_gateway_v2:location(Gw),
    Mode = blockchain_ledger_gateway_v2:mode(Gw),
    Gain = blockchain_ledger_gateway_v2:gain(Gw),
    LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(Gw),
    Owner = blockchain_ledger_gateway_v2:owner_address(Gw),
    cache_put(Ledger, GwDenormCF, <<Address/binary, "-loc">>, term_to_binary(Location)),
    cache_put(Ledger, GwDenormCF, <<Address/binary, "-last-challenge">>,
              term_to_binary(LastChallenge)),
    cache_put(Ledger, GwDenormCF, <<Address/binary, "-owner">>, Owner),
    cache_put(Ledger, GwDenormCF, <<Address/binary, "-mode">>, term_to_binary(Mode)),
    cache_put(Ledger, GwDenormCF, <<Address/binary, "-gain">>, term_to_binary(Gain));
write_gw_denorm_values(Address, Old, Gw, Ledger) ->
    GwDenormCF = gw_denorm_cf(Ledger),
    OldLocation = blockchain_ledger_gateway_v2:location(Old),
    Location = blockchain_ledger_gateway_v2:location(Gw),
    OldMode = blockchain_ledger_gateway_v2:mode(Old),
    Mode = blockchain_ledger_gateway_v2:mode(Gw),
    OldGain = blockchain_ledger_gateway_v2:gain(Old),
    Gain = blockchain_ledger_gateway_v2:gain(Gw),
    OldLastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(Old),
    LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(Gw),
    OldOwner = blockchain_ledger_gateway_v2:owner_address(Old),
    Owner = blockchain_ledger_gateway_v2:owner_address(Gw),
    case Location == OldLocation of
        true -> ok;
        _ -> cache_put(Ledger, GwDenormCF, <<Address/binary, "-loc">>, term_to_binary(Location))
    end,
    case LastChallenge == OldLastChallenge of
        true -> ok;
        _ -> cache_put(Ledger, GwDenormCF, <<Address/binary, "-last-challenge">>,
                       term_to_binary(LastChallenge))
    end,
    case Owner == OldOwner of
        true -> ok;
        _ -> cache_put(Ledger, GwDenormCF, <<Address/binary, "-owner">>, Owner)
    end,
    case Mode == OldMode of
        true -> ok;
        _ -> cache_put(Ledger, GwDenormCF, <<Address/binary, "-mode">>, term_to_binary(Mode))
    end,
    case Gain == OldGain of
        true -> ok;
            _ -> cache_put(Ledger, GwDenormCF, <<Address/binary, "-gain">>, term_to_binary(Gain))
    end.

-spec entries(ledger()) -> entries().
entries(Ledger) ->
    EntriesCF = entries_cf(Ledger),
    cache_fold(
        Ledger,
        EntriesCF,
        fun({Address, Binary}, Acc) ->
            Entry = blockchain_ledger_entry_v1:deserialize(Binary),
            maps:put(Address, Entry, Acc)
        end,
        #{}
    ).

-spec dc_entries(ledger()) -> dc_entries().
dc_entries(Ledger) ->
    DCEntriesCF = dc_entries_cf(Ledger),
    cache_fold(
        Ledger,
        DCEntriesCF,
        fun({Address, Binary}, Acc) ->
            Entry = blockchain_ledger_data_credits_entry_v1:deserialize(Binary),
            maps:put(Address, Entry, Acc)
        end,
        #{}
    ).

-spec htlcs(ledger()) -> htlcs().
htlcs(Ledger) ->
    HTLCsCF = htlcs_cf(Ledger),
    cache_fold(
        Ledger,
        HTLCsCF,
        fun({Address, Binary}, Acc) ->
            Entry = blockchain_ledger_htlc_v1:deserialize(Binary),
            maps:put(Address, Entry, Acc)
        end,
        #{}
    ).

-spec master_key(ledger()) -> {ok, binary()} | {error, any()}.
master_key(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?MASTER_KEY, []) of
        {ok, MasterKey} ->
            {ok, MasterKey};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec master_key(binary(), ledger()) -> ok | {error, any()}.
master_key(NewKey, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?MASTER_KEY, NewKey).

-spec multi_keys(ledger()) -> {ok, [binary()]} | {error, any()}.
multi_keys(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?MULTI_KEYS, []) of
        {ok, []} ->
            {error, not_found};
        {ok, MultiKeysBin} ->
            {ok, blockchain_utils:bin_keys_to_list(MultiKeysBin)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec multi_keys([binary()], ledger()) -> ok | {error, any()}.
multi_keys(NewKeys, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?MULTI_KEYS, blockchain_utils:keys_list_to_bin(NewKeys)).

-spec vars(#{Key => term()}, [Key], ledger()) -> ok.
vars(Vars, Unset, Ledger) ->
    DefaultCF = default_cf(Ledger),
    maps:map(
      fun(K, V) ->
              cache_put(Ledger, DefaultCF, var_name(K), term_to_binary(V))
      end,
      Vars),
    lists:foreach(
      fun(K) ->
              cache_delete(Ledger, DefaultCF, var_name(K))
      end,
      Unset),
    ok.

-spec config(term(), ledger()) -> {ok, term()} | {error, term()}.
config(ConfigName, Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, var_name(ConfigName), []) of
        {ok, ConfigVal} ->
            {ok, binary_to_term(ConfigVal)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

vars_nonce(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?VARS_NONCE, []) of
        {ok, Nonce} ->
            {ok, binary_to_term(Nonce)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

vars_nonce(NewNonce, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?VARS_NONCE, term_to_binary(NewNonce)).

-spec find_gateway_info(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_gateway_v2:gateway()}
                                                                 | {error, any()}.
find_gateway_info(Address, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    case cache_get(Ledger, AGwsCF, Address, []) of
        {ok, BinGw} ->
            {ok, blockchain_ledger_gateway_v2:deserialize(BinGw)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

find_gateway_location(Address, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    GwDenormCF = gw_denorm_cf(Ledger),
    case cache_get(Ledger, GwDenormCF, <<Address/binary, "-loc">>, []) of
        {ok, BinLoc} ->
            {ok, binary_to_term(BinLoc)};
        _ ->
            case cache_get(Ledger, AGwsCF, Address, []) of
                {ok, BinGw} ->
                    Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
                    Location = blockchain_ledger_gateway_v2:location(Gw),
                    {ok, Location};
                not_found ->
                    {error, not_found};
                Error ->
                    Error
            end
    end.

find_gateway_owner(Address,  Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    GwDenormCF = gw_denorm_cf(Ledger),
    case cache_get(Ledger, GwDenormCF, <<Address/binary, "-owner">>, []) of
        {ok, Owner} ->
            {ok, Owner};
        _ ->
            case cache_get(Ledger, AGwsCF, Address, []) of
                {ok, BinGw} ->
                    Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
                    Owner = blockchain_ledger_gateway_v2:owner_address(Gw),
                    {ok, Owner};
                not_found ->
                    {error, not_found};
                Error ->
                    Error
            end
    end.

find_gateway_last_challenge(Address, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    GwDenormCF = gw_denorm_cf(Ledger),
    case cache_get(Ledger, GwDenormCF, <<Address/binary, "-last-challenge">>, []) of
        {ok, BinChallenge} ->
            {ok, binary_to_term(BinChallenge)};
        _ ->
            case cache_get(Ledger, AGwsCF, Address, []) of
                {ok, BinGw} ->
                    Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
                    LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(Gw),
                    {ok, LastChallenge};
                not_found ->
                    {error, not_found};
                Error ->
                    Error
            end
    end.

find_gateway_mode(Address, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    GwDenormCF = gw_denorm_cf(Ledger),
    case cache_get(Ledger, GwDenormCF, <<Address/binary, "-mode">>, []) of
        {ok, BinMode} ->
            {ok, binary_to_term(BinMode)};
        _ ->
            case cache_get(Ledger, AGwsCF, Address, []) of
                {ok, BinGw} ->
                    Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
                    Mode = blockchain_ledger_gateway_v2:mode(Gw),
                    {ok, Mode};
                not_found ->
                    {error, not_found};
                Error ->
                    Error
            end
    end.

%%===================================================================
%% NOTE: These two functions are primarily for debugging interactively.
%% DO NOT depend on these for code as they will be SLOW.
fold_gateways(Fun, InitAcc, Ledger) ->
    cache_fold(
      Ledger,
      active_gateways_cf(Ledger),
      fun({Addr, BinGW}, Acc) ->
              GW = blockchain_ledger_gateway_v2:deserialize(BinGW),
              Fun({Addr, GW}, Acc)
      end,
      InitAcc
     ).

-spec find_gateways_by_owner(OwnerPubkeyBin :: libp2p_crypto:pubkey_bin(), Ledger :: ledger()) -> [{libp2p_crypto:pubkey_bin(), blockchain_ledger_gateway_v2:gateway()}].
find_gateways_by_owner(OwnerPubkeyBin, Ledger) ->
    fold_gateways(
      fun({Addr, GW}, Acc) ->
              case blockchain_ledger_gateway_v2:owner_address(GW) == OwnerPubkeyBin of
                  true -> [{Addr, GW} | Acc];
                  false -> Acc
              end
      end, [], Ledger).
%%===================================================================

find_gateway_gain(Address, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    GwDenormCF = gw_denorm_cf(Ledger),
    case cache_get(Ledger, GwDenormCF, <<Address/binary, "-gain">>, []) of
        {ok, BinGain} ->
            {ok, binary_to_term(BinGain)};
        _ ->
            case cache_get(Ledger, AGwsCF, Address, []) of
                {ok, BinGw} ->
                    Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
                    Gain = blockchain_ledger_gateway_v2:gain(Gw),
                    {ok, Gain};
                not_found ->
                    {error, not_found};
                Error ->
                    Error
            end
    end.

-spec add_gateway(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr, GatewayAddress, Ledger) ->
    add_gateway(OwnerAddr, GatewayAddress, full, Ledger).

-spec add_gateway(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), blockchain_ledger_gateway_v2:mode(), ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr, GatewayAddress, Mode, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            Gateway = blockchain_ledger_gateway_v2:new(OwnerAddr, undefined, Mode),
            update_gateway(new, Gateway, GatewayAddress, Ledger)
    end.

%% NOTE: This should only be allowed when adding a gateway which was
%% added in an old blockchain and is being added via a special
%% genesis block transaction to a new chain.
-spec add_gateway(OwnerAddress :: libp2p_crypto:pubkey_bin(),
                  GatewayAddress :: libp2p_crypto:pubkey_bin(),
                  Location :: undefined | pos_integer(),
                  Nonce :: non_neg_integer(),
                  Mode :: blockchain_ledger_gateway_v2:mode(),
                  Ledger :: ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr,
            GatewayAddress,
            Location,
            Nonce,
            Mode,
            Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            {ok, Height} = ?MODULE:current_height(Ledger),
            Gateway = blockchain_ledger_gateway_v2:new(OwnerAddr, Location, Mode, Nonce),
            NewGw0 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(1.0, 1.0, Height, Gateway),

            NewGw =
                case ?MODULE:config(?poc_version, Ledger) of
                    {ok, V} when V > 6 ->
                        {ok, Res} = blockchain:config(?poc_target_hex_parent_res, Ledger),
                        Hex = h3:parent(Location, Res),
                        add_to_hex(Hex, GatewayAddress, Ledger),
                        NewGw0;
                    {ok, V} when V > 3 ->
                        Gateways = active_gateways(Ledger),
                        Neighbors = blockchain_poc_path:neighbors(NewGw0, Gateways, Ledger),
                        NewGw1 = blockchain_ledger_gateway_v2:neighbors(Neighbors, NewGw0),
                        fixup_neighbors(GatewayAddress, Gateways, Neighbors, Ledger),
                        NewGw1;
                    _ ->
                        Gateways = active_gateways(Ledger),
                        Neighbors = blockchain_poc_path:neighbors(NewGw0, Gateways, Ledger),
                        NewGw1 = blockchain_ledger_gateway_v2:neighbors(Neighbors, NewGw0),
                        fixup_neighbors(GatewayAddress, Gateways, Neighbors, Ledger),
                        NewGw1
                end,

            update_gateway(new, NewGw, GatewayAddress, Ledger)
    end.

fixup_neighbors(Addr, Gateways, Neighbors, Ledger) ->
    Remove = maps:filter(
               fun(A, _G) when A == Addr ->
                       false;
                  (A, G) ->
                       (not lists:member(A, Neighbors)) andalso
                           lists:member(Addr, blockchain_ledger_gateway_v2:neighbors(G))
               end,
               Gateways),
    Add = maps:filter(
            fun(A, _G) when A == Addr ->
                    false;
               (A, G) ->
                    lists:member(A, Neighbors) andalso
                        (not lists:member(Addr, blockchain_ledger_gateway_v2:neighbors(G)))
            end,
            Gateways),

    R1 = maps:map(fun(_A, G) ->
                          blockchain_ledger_gateway_v2:remove_neighbor(Addr, G)
                  end, Remove),
    A1 = maps:map(fun(_A, G) ->
                          blockchain_ledger_gateway_v2:add_neighbor(Addr, G)
                  end, Add),
    maps:map(fun(A, G) ->
                     update_gateway(new, G, A, Ledger)
             end, maps:merge(R1, A1)),
    ok.

-spec update_gateway(Old :: blockchain_ledger_gateway_v2:gateway() | new,
                     Gw :: blockchain_ledger_gateway_v2:gateway(),
                     GwAddr :: libp2p_crypto:pubkey_bin(),
                     Ledger :: ledger()) -> ok | {error, _}.
update_gateway(Old, Gw0, GwAddr, Ledger) ->
    %% we have to do this each time to make sure that we have ledger convergence for snapshots, but
    %% it feels relatively cheap in comparison to continuing to update scores.
    Gw =
        case blockchain:config(?election_version, Ledger) of
            %% election v4 removed score from consideration
            {ok, EV} when EV >= 4 ->
                blockchain_ledger_gateway_v2:set_alpha_beta_delta(0.0, 0.0, 0, Gw0);
            _ ->
                Gw0
        end,

    Bin = blockchain_ledger_gateway_v2:serialize(Gw),
    AGwsCF = active_gateways_cf(Ledger),
    cache_put(Ledger, AGwsCF, GwAddr, Bin),
    write_gw_denorm_values(GwAddr, Old, Gw, Ledger).

-spec add_gateway_location(libp2p_crypto:pubkey_bin(), non_neg_integer(), non_neg_integer(), ledger()) -> ok | {error, no_active_gateway}.
add_gateway_location(GatewayAddress, Location, Nonce, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw} ->
            {ok, Height} = ?MODULE:current_height(Ledger),
            Gw1 = blockchain_ledger_gateway_v2:location(Location, Gw),
            Gw2 = blockchain_ledger_gateway_v2:nonce(Nonce, Gw1),
            %% Disable setting the last location nonce until we build a chain var to restore witnesses
            Gw3 = Gw2, %blockchain_ledger_gateway_v2:last_location_nonce(Nonce, Gw2),
            Gw4 = blockchain_ledger_gateway_v2:set_alpha_beta_delta(1.0, 1.0, Height, Gw3),
            NewGw = blockchain_ledger_gateway_v2:clear_witnesses(Gw4),
            update_gateway(Gw, NewGw, GatewayAddress, Ledger)
    end.

-spec add_gateway_gain(libp2p_crypto:pubkey_bin(), integer(), non_neg_integer(), ledger()) -> ok | {error, no_active_gateway}.
add_gateway_gain(GatewayAddress, Gain, Nonce, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw} ->
            Gw1 = blockchain_ledger_gateway_v2:gain(Gain, Gw),
            Gw2 = blockchain_ledger_gateway_v2:nonce(Nonce, Gw1),
            update_gateway(Gw, Gw2, GatewayAddress, Ledger)
    end.

-spec add_gateway_elevation(libp2p_crypto:pubkey_bin(), integer(), non_neg_integer(), ledger()) -> ok | {error, no_active_gateway}.
add_gateway_elevation(GatewayAddress, Elevation, Nonce, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw} ->
            Gw1 = blockchain_ledger_gateway_v2:elevation(Elevation, Gw),
            Gw2 = blockchain_ledger_gateway_v2:nonce(Nonce, Gw1),
            update_gateway(Gw, Gw2, GatewayAddress, Ledger)
    end.

cg_versions(Ledger) ->
    case config(?election_version, Ledger) of
        {ok, N} when N >= 5 ->
            validator_versions(Ledger);
        _ ->
            gateway_versions(Ledger)
    end.

validator_versions(Ledger) ->
    %% reuse this var despite the name
    case config(?var_gw_inactivity_threshold, Ledger) of
        {error, _} = Err ->
            Err;
        {ok, Threshold} ->
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            ValCF = validators_cf(Ledger),
            Inc = fun(X) -> X + 1 end,
            Versions =
                cache_fold(
                  Ledger, ValCF,
                  fun({_Addr, BinVal}, Acc) ->
                          Val = blockchain_ledger_validator_v1:deserialize(BinVal),
                          Last = blockchain_ledger_validator_v1:last_heartbeat(Val),
                          Version = blockchain_ledger_validator_v1:version(Val),
                          case (Height - Last) >= Threshold of
                              true ->
                                  Acc;
                              false ->
                                  maps:update_with(Version, Inc, 1, Acc)
                          end
                  end,
                  #{}),
            L = maps:to_list(Versions),
            Tot = lists:sum([Ct || {_V, Ct} <- L]),

            %% reformat counts as percentages
            [{V, Ct / Tot} || {V, Ct} <- L]
    end.

gateway_versions(Ledger) ->
    case config(?var_gw_inactivity_threshold, Ledger) of
        {error, not_found} ->
            gateway_versions_fallback(Ledger);
        {ok, DeathThreshold} ->
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            Gateways = filter_dead(active_gateways(Ledger), Height, DeathThreshold),
            Inc = fun(X) -> X + 1 end,
            Versions =
            maps:fold(
              fun(_, Gw, Acc) ->
                      V = blockchain_ledger_gateway_v2:version(Gw),
                      maps:update_with(V, Inc, 1, Acc)
              end,
              #{},
              Gateways),
            L = maps:to_list(Versions),
            Tot = lists:sum([Ct || {_V, Ct} <- L]),

            %% reformat counts as percentages
            [{V, Ct / Tot} || {V, Ct} <- L]
    end.

gateway_versions_fallback(Ledger) ->
    Gateways = active_gateways(Ledger),
    Inc = fun(X) -> X + 1 end,
    Versions =
        maps:fold(
          fun(_, Gw, Acc) ->
                  V = blockchain_ledger_gateway_v2:version(Gw),
                  maps:update_with(V, Inc, 1, Acc)
          end,
         #{},
          Gateways),
    L = maps:to_list(Versions),
    Tot = lists:sum([Ct || {_V, Ct} <- L]),

    %% reformat counts as percentages
    [{V, Ct / Tot} || {V, Ct} <- L].

filter_dead(Gws, Height, Threshold) ->
    maps:filter(
      fun(_Addr, Gw) ->
              Last = last(blockchain_ledger_gateway_v2:last_poc_challenge(Gw)),
              %% calculate the number of blocks since we last saw a challenge
              Since = Height - Last,
              %% if since is bigger than the threshold, invert to exclude
              not (Since >= Threshold)
      end,
      Gws).

last(undefined) ->
    0;
last(N) when is_integer(N) ->
    N.

%%--------------------------------------------------------------------
%% @doc Update the score of a hotspot by looking at the updated alpha/beta values.
%% In order to ensure that old POCs don't have a drastic effect on the eventual score
%% for a gateway, we apply a constant scaled decay dependent on the delta update for the hotspot.
%%
%% Furthermore, since we don't allow scores to go negative, we scale alpha and beta values
%% back to 1.0 each, if it dips below 0 after the decay has been applied
%%
%% At the end of it, we just supply the new alpha, beta and delta values and store
%% only those in the ledger.
%%
%% @end
%%--------------------------------------------------------------------
-spec update_gateway_score(GatewayAddress :: libp2p_crypto:pubkey_bin(),
                           {Alpha :: float(), Beta :: float()},
                           Ledger :: ledger()) -> ok | {error, any()}.
update_gateway_score(GatewayAddress, {Alpha, Beta}, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Gw} ->
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            {Alpha0, Beta0, _} = blockchain_ledger_gateway_v2:score(GatewayAddress, Gw, Height, Ledger),
            NewGw = blockchain_ledger_gateway_v2:set_alpha_beta_delta(blockchain_utils:normalize_float(Alpha0 + Alpha),
                                                                      blockchain_utils:normalize_float(Beta0 + Beta),
                                                                      Height, Gw),
            update_gateway(Gw, NewGw, GatewayAddress, Ledger)
    end.

-spec gateway_score(GatewayAddress :: libp2p_crypto:pubkey_bin(), Ledger :: ledger()) -> {ok, float()} | {error, any()}.
gateway_score(GatewayAddress, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Gw} ->
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            {_Alpha, _Beta, Score} = blockchain_ledger_gateway_v2:score(GatewayAddress, Gw, Height, Ledger),
            {ok, Score}
    end.

-spec update_gateway_oui(Gateway :: libp2p_crypto:pubkey_bin(),
                         OUI :: pos_integer() | undefined,
                         Nonce :: non_neg_integer(),
                         Ledger :: ledger()) -> ok | {error, any()}.
update_gateway_oui(Gateway, OUI, Nonce, Ledger) ->
    case ?MODULE:find_gateway_info(Gateway, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Gw} ->
            NewGw0 = blockchain_ledger_gateway_v2:oui(OUI, Gw),
            NewGw = blockchain_ledger_gateway_v2:nonce(Nonce, NewGw0),
            update_gateway(Gw, NewGw, Gateway, Ledger)
    end.

-spec insert_witnesses(PubkeyBin :: libp2p_crypto:pubkey_bin(),
                       Witnesses :: [blockchain_poc_witness_v1:poc_witness() | blockchain_poc_receipt_v1:poc_receipt()],
                       Ledger :: ledger()) -> ok | {error, any()}.
insert_witnesses(_PubkeyBin, _Witnesses, _Ledger) ->
    ok.
    %% case blockchain:config(?poc_version, Ledger) of
    %%     %% only works with poc-v9 and above
    %%     {ok, V} when V >= 9 ->
    %%         case ?MODULE:find_gateway_info(PubkeyBin, Ledger) of
    %%             {error, _}=Error ->
    %%                 Error;
    %%             {ok, GW0} ->
    %%                 GW1 = lists:foldl(fun(#blockchain_poc_witness_v1_pb{}=POCWitness, GW) ->
    %%                                           WitnessPubkeyBin = blockchain_poc_witness_v1:gateway(POCWitness),
    %%                                           case ?MODULE:find_gateway_info(WitnessPubkeyBin, Ledger) of
    %%                                               {ok, WitnessGw} ->
    %%                                                   blockchain_ledger_gateway_v2:add_witness({poc_witness, WitnessPubkeyBin, WitnessGw, POCWitness, GW, PubkeyBin, Ledger});
    %%                                               {error, Reason} ->
    %%                                                   lager:warning("exiting trying to add witness", [Reason]),
    %%                                                   erlang:error({insert_witnesses_error, Reason})
    %%                                           end;
    %%                                      (#blockchain_poc_receipt_v1_pb{}=POCWitness, GW) ->
    %%                                           ReceiptPubkeyBin = blockchain_poc_receipt_v1:gateway(POCWitness),
    %%                                           case ?MODULE:find_gateway_info(ReceiptPubkeyBin, Ledger) of
    %%                                               {ok, ReceiptGw} ->
    %%                                                   blockchain_ledger_gateway_v2:add_witness({poc_receipt, ReceiptPubkeyBin, ReceiptGw, POCWitness, GW, PubkeyBin, Ledger});
    %%                                               {error, Reason} ->
    %%                                                   lager:warning("exiting trying to add witness", [Reason]),
    %%                                                   erlang:error({insert_witnesses_error, Reason})
    %%                                           end;
    %%                                      (_, _) ->
    %%                                           erlang:error({invalid, unknown_witness_type})
    %%                                   end, GW0, Witnesses),
    %%                 update_gateway(GW1, PubkeyBin, Ledger)
    %%         end;
    %%     _ ->
    %%         {error, incorrect_poc_version}
    %% end.

-spec add_gateway_witnesses(GatewayAddress :: libp2p_crypto:pubkey_bin(),
                            WitnessInfo :: [{integer(), non_neg_integer(), libp2p_crypto:pubkey_bin()}],
                            Ledger :: ledger()) -> ok | {error, any()}.
add_gateway_witnesses(_GatewayAddress, _WitnessInfo, _Ledger) ->
    ok.
    %% TODO: if we want to bring this stuff back, we need to make sure that we have a var in place
    %% to limit it more strictly.  in order to make it deterministic after this code goes in, we'll
    %% need to start the var at 0 and then raise it.

    %% case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
    %%     {error, _}=Error ->
    %%         Error;
    %%     {ok, GW0} ->
    %%         GW1 = lists:foldl(fun({RSSI, TS, WitnessAddress}, GW) ->
    %%                                   case ?MODULE:find_gateway_info(WitnessAddress, Ledger) of
    %%                                       {ok, Witness} ->
    %%                                           blockchain_ledger_gateway_v2:add_witness(WitnessAddress, Witness, RSSI, TS, GW, GatewayAddress, Ledger);
    %%                                       {error, Reason} ->
    %%                                           lager:warning("exiting trying to add witness",
    %%                                                         [Reason]),
    %%                                           erlang:error({add_gateway_error, Reason})
    %%                                   end
    %%                           end, GW0, WitnessInfo),
    %%         update_gateway(GW1, GatewayAddress, Ledger)
    %% end.

-spec remove_gateway_witness(GatewayPubkeyBin :: libp2p_crypto:pubkey_bin(),
                             Ledger :: ledger()) -> ok | {error, any()}.
remove_gateway_witness(GatewayPubkeyBin, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayPubkeyBin, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, GW0} ->
            GW1 = blockchain_ledger_gateway_v2:clear_witnesses(GW0),
            ?MODULE:update_gateway(GW0, GW1, GatewayPubkeyBin, Ledger)
    end.

-spec refresh_gateway_witnesses(blockchain_block:hash(), ledger()) -> ok | {error, any()}.
refresh_gateway_witnesses(Hash, Ledger) ->
    case ?MODULE:config(?witness_refresh_interval, Ledger) of
        {ok, RefreshInterval} when is_integer(RefreshInterval) ->
            case ?MODULE:config(?witness_refresh_rand_n, Ledger) of
                {ok, RandN} when is_integer(RandN) ->
                    %% We need to do all the calculation within this context
                    %% create a new context if we don't already have one
                    case ?MODULE:get_context(Ledger) of
                        undefined ->
                            error(refresh_out_of_context);
                        _ ->
                            ok
                    end,

                    case ?MODULE:get_hexes(Ledger) of
                        {error, not_found} ->
                            ok;
                        {error, _}=Error ->
                            Error;
                        {ok, HexMap} ->
                            ZoneList = maps:keys(HexMap),
                            GatewayPubkeyBins = zone_list_to_pubkey_bins(ZoneList, Ledger),
                            GatewayOffsets = pubkey_bins_to_offset(GatewayPubkeyBins),
                            GatewaysToRefresh = filtered_gateways_to_refresh(Hash, RefreshInterval, GatewayOffsets, RandN),
                            lager:debug("Refreshing witnesses for: ~p", [GatewaysToRefresh]),

                            Res = lists:map(fun({_, GwPubkeyBin}) ->
                                                    remove_gateway_witness(GwPubkeyBin, Ledger)
                                            end,
                                            GatewaysToRefresh),

                            case lists:all(fun(T) -> T == ok end, Res) of
                                false ->
                                    lager:warning("Witness refresh failed for: ~p", [GatewaysToRefresh]),
                                    {error, witness_refresh_failed};
                                true ->
                                    ok
                            end
                    end;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

find_poc(OnionKeyHash, Challenger, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    case cache_get(Ledger, PoCsCF, <<OnionKeyHash/binary, Challenger/binary>>, []) of
        {ok, BinPoC} ->
            {ok, blockchain_ledger_poc_v2:deserialize(BinPoC)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.


-spec find_pocs(binary(), ledger()) -> {ok, blockchain_ledger_poc_v2:pocs()} | {error, any()}.
find_pocs(OnionKeyHash, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    PoCs = cache_fold(Ledger, PoCsCF,
                     fun({_, V}, Acc) ->
                             [blockchain_ledger_poc_v2:deserialize(V)|Acc]
                     end, [], [{start, <<OnionKeyHash/binary>>}, {iterate_upper_bound, increment_bin(OnionKeyHash)}]),
    case PoCs of
        [] ->
            {error, not_found};
        _ -> {ok, PoCs}
    end.

-spec request_poc(OnionKeyHash :: binary(),
                  SecretHash :: binary(),
                  Challenger :: libp2p_crypto:pubkey_bin(),
                  BlockHash :: binary(),
                  Version :: non_neg_integer(),
                  Ledger :: ledger()) -> ok | {error, any()}.
request_poc(OnionKeyHash, SecretHash, Challenger, BlockHash, Version, Ledger) ->
    case ?MODULE:find_gateway_info(Challenger, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw0} ->
            request_poc_(OnionKeyHash, SecretHash, Challenger, BlockHash, Ledger, Gw0, Version)
    end.

request_poc_(OnionKeyHash, SecretHash, Challenger, BlockHash, Ledger, Gw0, Version) ->
    case blockchain_ledger_gateway_v2:last_poc_onion_key_hash(Gw0) of
        undefined ->
            ok;
        LastOnionKeyHash  ->
            delete_poc(LastOnionKeyHash, Challenger, Ledger)
    end,
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    Gw1 = blockchain_ledger_gateway_v2:last_poc_challenge(Height+1, Gw0),
    Gw2 = blockchain_ledger_gateway_v2:last_poc_onion_key_hash(OnionKeyHash, Gw1),
    Gw3 = blockchain_ledger_gateway_v2:version(Version, Gw2),
    ok = update_gateway(Gw0, Gw3, Challenger, Ledger),

    PoCsCF = pocs_cf(Ledger),
    PoC = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash, Challenger, BlockHash),
    PoCBin = blockchain_ledger_poc_v2:serialize(PoC),
    cache_put(Ledger, PoCsCF, <<OnionKeyHash/binary, Challenger/binary>>, PoCBin).

-spec delete_poc(binary(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, any()}.
delete_poc(OnionKeyHash, Challenger, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    cache_delete(Ledger, PoCsCF, <<OnionKeyHash/binary, Challenger/binary>>).

maybe_gc_pocs(Chain, Ledger) ->
    {ok, Height} = current_height(Ledger),
    Version = case ?MODULE:config(?poc_version, Ledger) of
                  {ok, V} -> V;
                  _ -> 1
              end,
    %% this used to be 100, but that seems like a lot to process at once, so I lowered it to make
    %% each iteration cheaper and set it off by one so it wouldn't align with so many other gc
    %% processes
    case Version > 3 andalso Height rem 51 == 0 of
        true ->
            lager:debug("gcing old pocs"),
            PoCInterval = blockchain_utils:challenge_interval(Ledger),
            PoCsCF = pocs_cf(Ledger),
            %% construct a list of recent hashes instead of fetching all the blocks by their hashes,
            %% which ends up being much much cheaper
            Hashes =
                lists:foldl(
                  fun(H, Acc) ->
                          case blockchain:get_block_hash(H, Chain) of
                              {ok, Hash} ->
                                  Acc#{Hash => H};
                              _ ->
                                  Acc
                          end
                  end, #{}, lists:seq(Height - ((PoCInterval * 2) + 1), Height)),
            cache_fold(
                Ledger,
                PoCsCF,
                fun({KeyHash, BinPoC}, Acc) ->
                        %% this CF contains all the poc request state that needs to be retained
                        %% between request and receipt validation.  however, it's possible that
                        %% both requests stop and a receipt never comes, which leads to stale (and
                        %% in some cases differing) data in the ledger.  here, we pull that data
                        %% out and delete anything that's too old, as determined by being older
                        %% than twice the request interval, which controls receipt validity.
                        PoC = blockchain_ledger_poc_v2:deserialize(BinPoC),
                        H = blockchain_ledger_poc_v2:block_hash(PoC),
                        case H of
                            <<>> ->
                                %% pre-upgrade pocs are ancient
                                cache_delete(Ledger, PoCsCF, KeyHash);
                            _ ->
                                case maps:find(H, Hashes) of
                                    {ok, BH} ->
                                        %% not sure this is even needed, it might
                                        %% always be true? but just in case
                                        case (Height - BH) < PoCInterval * 2 of
                                            false ->
                                                cache_delete(Ledger, PoCsCF, KeyHash);
                                            true ->
                                                ok
                                        end;
                                    error ->
                                        %% if it's not in the hashes map, it's too
                                        %% old by construction
                                        cache_delete(Ledger, PoCsCF, KeyHash)
                                end
                        end,
                        Acc
                end,
                []
                ),
            ok;
        _ ->
            ok
    end.

upgrade_pocs(Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    ToStore = cache_fold(
      Ledger,
      PoCsCF,
      fun({KeyHash, BinPoCs}, Acc) ->
              SPoCs = erlang:binary_to_term(BinPoCs),
              cache_delete(Ledger, PoCsCF, KeyHash),
              lists:foldl(
                fun(SPoC, A) ->
                        PoC = blockchain_ledger_poc_v2:deserialize(SPoC),
                        Challenger = blockchain_ledger_poc_v2:challenger(PoC),
                        [{<<KeyHash/binary, Challenger/binary>>, SPoC} | A]
                end, Acc, SPoCs)
      end, []),
    lists:foreach(fun({K, V}) ->
                          cache_put(Ledger, PoCsCF, K, V)
                  end, ToStore),
    ok.

-spec zone_list_to_pubkey_bins(ZoneList :: [h3:h3_index()],
                               Ledger :: ledger()) -> [libp2p_crypto:pubkey_bin()].
zone_list_to_pubkey_bins(ZoneList, Ledger) ->
    lists:flatten(lists:foldl(fun(Zone, Acc) ->
                                      {ok, ContainedPubkeyBins} = blockchain_ledger_v1:get_hex(Zone, Ledger),
                                      [ContainedPubkeyBins | Acc]
                              end,
                              [],
                              ZoneList)).

-spec pubkey_bins_to_offset(GatewayPubkeyBins :: [libp2p_crypto:pubkey_bin()]) -> gateway_offsets().
pubkey_bins_to_offset(GatewayPubkeyBins) ->
    lists:keysort(1, lists:foldl(fun(PubkeyBin, Acc) ->
                                         %% This can be 32 bytes sometimes hence in the loop
                                         S = byte_size(PubkeyBin) * 8 - 64,
                                         <<_:S, Offset:64/unsigned-little>> = PubkeyBin,
                                         [{Offset, PubkeyBin} | Acc]
                                 end,
                                 [],
                                 GatewayPubkeyBins)).

-spec filtered_gateways_to_refresh(Hash :: blockchain_block:hash(),
                                   RefreshInterval :: pos_integer(),
                                   GatewayOffsets :: gateway_offsets(),
                                   RandN :: pos_integer()) -> gateway_offsets().
filtered_gateways_to_refresh(Hash, RefreshInterval, GatewayOffsets, RandN) ->
    RandState = blockchain_utils:rand_state(Hash),
    %% NOTE: I believe this ensure that the random number gets seeded with a value
    %% higher than the RefreshInterval
    {RandVal, _NewRandState} = rand:uniform_s(RandN*RefreshInterval, RandState),
    lists:filter(fun({Offset, _PubkeyBin}) ->
                         ((Offset + RandVal) rem RefreshInterval) == 0
                 end,
                 GatewayOffsets).

-spec maybe_gc_scs(blockchain:blockchain(), ledger()) -> ok.
maybe_gc_scs(Chain, Ledger) ->
    {ok, Height} = current_height(Ledger),

    case blockchain:get_block_info(Height, Chain) of
        {ok, #block_info_v2{election_info={_Epoch, EpochStart}}} ->
            RewardVersion = case ?MODULE:config(?reward_version, Ledger) of
                                {ok, N} -> N;
                                _ -> 1
                            end,

            case ?MODULE:config(?sc_grace_blocks, Ledger) of
                {ok, Grace} ->
                    GCInterval = case ?MODULE:config(?sc_gc_interval, Ledger) of
                                     {ok, I} ->
                                         I;
                                     _ ->
                                         %% 100 was the previously hardcoded value
                                         100
                                 end,
                    case Height rem GCInterval == 0 of
                        true ->
                            lager:info("gcing old state_channels..."),
                            SCsCF = state_channels_cf(Ledger),
                            {Alters, SCIDs} = cache_fold(
                                                Ledger,
                                                SCsCF,
                                                fun({KeyHash, BinSC}, {CacheAcc, IDAcc} = Acc) ->
                                                        {Mod, SC} = deserialize_state_channel(BinSC),
                                                        ExpireAtBlock = Mod:expire_at_block(SC),
                                                        case (ExpireAtBlock + Grace) < Height of
                                                            false ->
                                                                Acc;
                                                            true ->
                                                                case Mod of
                                                                    blockchain_ledger_state_channel_v1 ->
                                                                        {[KeyHash | CacheAcc], []};
                                                                    blockchain_ledger_state_channel_v2 ->
                                                                        %% We have to protect state channels
                                                                        %% that closed during the grace blocks
                                                                        %% in the previous epoch so that
                                                                        %% we can calculate rewards for those
                                                                        %% closes.
                                                                        %%
                                                                        %% So only expire state channels that
                                                                        %% closed *before* grace in the previous
                                                                        %% epoch
                                                                        case check_sc_expire(ExpireAtBlock, Grace,
                                                                                             EpochStart,
                                                                                             RewardVersion) of
                                                                            false -> Acc;
                                                                            true ->
                                                                                ID = Mod:id(SC),
                                                                                case blockchain_ledger_state_channel_v2:close_state(SC) of
                                                                                    undefined -> ok; %% due to tests must handle
                                                                                    dispute -> ok; %% slash overcommit
                                                                                    closed -> %% refund overcommit DCs
                                                                                        SC0 = blockchain_ledger_state_channel_v2:state_channel(SC),
                                                                                        Owner = blockchain_state_channel_v1:owner(SC0),
                                                                                        Credit = calc_remaining_dcs(SC),
                                                                                        ok = credit_dc(Owner, Credit, Ledger)
                                                                                end,
                                                                                {[KeyHash | CacheAcc], [ID | IDAcc]}

                                                                        end
                                                                end
                                                        end
                                                end, {[], []}),
                            ok = blockchain_state_channels_client:gc_state_channels(SCIDs),
                            ok = blockchain_state_channels_server:gc_state_channels(SCIDs),
                            ok = lists:foreach(fun(KeyHash) ->
                                                       cache_delete(Ledger, SCsCF, KeyHash)
                                               end,
                                               Alters),
                            ok;
                        _ ->
                            ok
                    end;
                _ ->
                    ok
            end;
        _ ->
            %% We do not have the block, hence cannot gc scs
            ok
    end.


-spec check_sc_expire(ExpiresAt :: pos_integer(),
                      Grace :: pos_integer(),
                      EpochStart :: pos_integer(),
                      RewardVersion :: pos_integer()) -> boolean().
check_sc_expire(ExpiresAt, Grace, EpochStart, RewardVersion) when RewardVersion > 4 ->
    (ExpiresAt + Grace) < (EpochStart - Grace);
check_sc_expire(ExpiresAt, Grace, EpochStart, _RewardVersion) ->
    (ExpiresAt + Grace) < EpochStart.

-spec calc_remaining_dcs( blockchain_ledger_state_channel_v2:state_channel() ) -> non_neg_integer().
calc_remaining_dcs(SC) ->
    SC0 = blockchain_ledger_state_channel_v2:state_channel(SC),
    UsedDC = blockchain_state_channel_v1:total_dcs(SC0),
    ReservedDC = blockchain_ledger_state_channel_v2:amount(SC),
    max(0, ReservedDC - UsedDC).


%%--------------------------------------------------------------------
%% @doc  get staking server keys from chain var
%% @end
%%--------------------------------------------------------------------
-spec staking_keys(Ledger :: ledger()) -> not_found | [libp2p_crypto:pubkey_bin()].
staking_keys(Ledger)->
    case blockchain:config(?staking_keys, Ledger) of
        {error, not_found} -> not_found;
        {ok, V} -> blockchain_utils:bin_keys_to_list(V)
    end.

%%--------------------------------------------------------------------
%% @doc  check if txn fees are enabled on chain
%% @end
%%--------------------------------------------------------------------
-spec txn_fees_active(Ledger :: ledger()) -> boolean().
txn_fees_active(Ledger)->
    case blockchain:config(?txn_fees, Ledger) of
        {error, not_found} -> false;
        {ok, V} -> V
    end.

%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for OUI
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_oui_v1(Ledger :: ledger()) -> pos_integer().
staking_fee_txn_oui_v1(Ledger)->
    case blockchain:config(?staking_fee_txn_oui_v1, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.

%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for OUI addresses
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_oui_v1_per_address(Ledger :: ledger()) -> non_neg_integer().
staking_fee_txn_oui_v1_per_address(Ledger)->
    case blockchain:config(?staking_fee_txn_oui_v1_per_address, Ledger) of
        {error, not_found} -> 0;
        {ok, V} -> V
    end.

%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for add gateway
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_add_gateway_v1(Ledger :: ledger()) -> pos_integer().
staking_fee_txn_add_gateway_v1(Ledger)->
    case blockchain:config(?staking_fee_txn_add_gateway_v1, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.

%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for add dataonly gateway
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_add_dataonly_gateway_v1(Ledger :: ledger()) -> pos_integer().
staking_fee_txn_add_dataonly_gateway_v1(Ledger)->
    case blockchain:config(?staking_fee_txn_add_dataonly_gateway_v1, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.
%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for add light gateway
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_add_light_gateway_v1(Ledger :: ledger()) -> pos_integer().
staking_fee_txn_add_light_gateway_v1(Ledger)->
    case blockchain:config(?staking_fee_txn_add_light_gateway_v1, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.

%%--------------------------------------------------------------------
%% @doc  get txn fee multiplier
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec txn_fee_multiplier(Ledger :: ledger()) -> pos_integer().
txn_fee_multiplier(Ledger)->
    case blockchain:config(?txn_fee_multiplier, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.

%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for assert_location_v1
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_assert_location_v1(Ledger :: ledger()) -> pos_integer().
staking_fee_txn_assert_location_v1(Ledger)->
    case blockchain:config(?staking_fee_txn_assert_location_v1, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.
%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for assert_location_v1 for a dataonly gateway
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_assert_location_dataonly_gateway_v1(Ledger :: ledger()) -> pos_integer().
staking_fee_txn_assert_location_dataonly_gateway_v1(Ledger)->
    case blockchain:config(?staking_fee_txn_assert_location_dataonly_gateway_v1, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.
%%--------------------------------------------------------------------
%% @doc  get staking fee chain var value for assert_location_v1 for a light gateway
%% or return default
%% @end
%%--------------------------------------------------------------------
-spec staking_fee_txn_assert_location_light_gateway_v1(Ledger :: ledger()) -> pos_integer().
staking_fee_txn_assert_location_light_gateway_v1(Ledger)->
    case blockchain:config(?staking_fee_txn_assert_location_light_gateway_v1, Ledger) of
        {error, not_found} -> 1;
        {ok, V} -> V
    end.
%%--------------------------------------------------------------------
%% @doc  get map of staking server keys to gateway mode/type
%% @end
%%--------------------------------------------------------------------
%% TODO: come up with a better name for this
-spec staking_keys_to_mode_mappings(Ledger :: ledger()) -> not_found | [libp2p_crypto:pubkey_bin()].
staking_keys_to_mode_mappings(Ledger)->
    case blockchain:config(?staking_keys_to_mode_mappings, Ledger) of
        {error, not_found} -> not_found;
        {ok, V} -> blockchain_utils:bin_to_prop(V)
    end.

%%--------------------------------------------------------------------
%% @doc
%% converts DC to HNT bones
%% @end
%%--------------------------------------------------------------------
-spec dc_to_hnt(non_neg_integer(), ledger() | pos_integer()) -> {ok, non_neg_integer()}.
dc_to_hnt(DCAmount, OracleHNTPrice) when is_integer(OracleHNTPrice) ->
    DCInUSD = DCAmount * ?DC_TO_USD,
    %% need to put USD amount into 1/100_000_000th cents, same as oracle price
    {ok, ceil((DCInUSD * 100000000 / OracleHNTPrice) * ?BONES_PER_HNT)};
dc_to_hnt(DCAmount, Ledger)->
    case ?MODULE:current_oracle_price(Ledger) of
        {ok, 0} ->
            {ok, 0};
        {ok, OracleHNTPrice} ->
            dc_to_hnt(DCAmount, OracleHNTPrice)
    end.

%%--------------------------------------------------------------------
%% @doc
%% converts HNT bones to DC
%% @end
%%--------------------------------------------------------------------
-spec hnt_to_dc(non_neg_integer(), ledger() | pos_integer()) -> {ok, non_neg_integer()}.
hnt_to_dc(HNTAmount, OracleHNTPrice) when is_integer(OracleHNTPrice) ->
    HNTInUSD = ((HNTAmount / ?BONES_PER_HNT)  * OracleHNTPrice) / ?ORACLE_PRICE_SCALING_FACTOR,
    {ok, ceil((HNTInUSD * ?USD_TO_DC))};
hnt_to_dc(HNTAmount, Ledger)->
    case ?MODULE:current_oracle_price(Ledger) of
        {ok, 0} ->
            {ok, 0};
        {ok, OracleHNTPrice} ->
            hnt_to_dc(HNTAmount, OracleHNTPrice)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Maybe recalculate the median of the oracle prices that are valid
%% in a sliding window.
%%
%% Prices are calculated every so many blocks, frequency controlled by
%% the chain_var called `price_oracle_refresh_interval'
%%
%% Prices are considered valid if they are:
%% <ul>
%%      <li>Older than `price_oracle_scan_delay' seconds, and,</li>
%%      <li>No more than `price_oracle_scan_max' seconds old</li>
%% </ul>
%%
%% More recent prices from the same oracle replace older prices.
%%
%% Additionally there must be valid prices from a majority of the
%% oracles. Example: If there are 9 oracles, there must be valid
%% prices from <u>at least</u> 5 different oracles.
%%
%% If there are less prices than a majority, then the last calculated
%% price is reused.
%% @end
%%--------------------------------------------------------------------
-spec maybe_recalc_price( Blockchain :: blockchain:blockchain(),
                          Ledger :: ledger() ) -> ok.
maybe_recalc_price(Blockchain, Ledger) ->
    case blockchain:config(?price_oracle_refresh_interval, Ledger) of
        {error, not_found} -> ok;
        {ok, I} -> do_maybe_recalc_price(I, Blockchain, Ledger)
    end.

do_maybe_recalc_price(Interval, Blockchain, Ledger) ->
    DefaultCF = default_cf(Ledger),
    {ok, CurrentHeight} = current_height(Ledger),
    {ok, LastPrice} = current_oracle_price(Ledger),

    case CurrentHeight rem Interval == 0 of
        false -> ok;
        true ->
            {ok, #block_info_v2{time = BlockT}} = blockchain:get_block_info(CurrentHeight, Blockchain),
            {NewPrice, NewPriceList} = recalc_price(LastPrice, BlockT, DefaultCF, Ledger),
            cache_put(Ledger, DefaultCF, ?ORACLE_PRICES, term_to_binary(NewPriceList)),
            cache_put(Ledger, DefaultCF, ?CURRENT_ORACLE_PRICE, term_to_binary(NewPrice))
    end.

recalc_price(LastPrice, BlockT, _DefaultCF, Ledger) ->
    {ok, DelaySecs} = blockchain:config(?price_oracle_price_scan_delay, Ledger),
    {ok, MaxSecs} = blockchain:config(?price_oracle_price_scan_max, Ledger),
    StartScan = BlockT - DelaySecs, % typically 1 hour (in seconds)
    EndScan = BlockT - MaxSecs, % typically 1 day + 1 hour (in seconds)
    {ok, Prices} = current_oracle_price_list(Ledger),
    NewPriceList = trim_price_list(EndScan, Prices),
    {ok, RawOracleKeys} = blockchain:config(?price_oracle_public_keys, Ledger),
    Maximum = length(blockchain_utils:bin_keys_to_list(RawOracleKeys)),
    Minimum = (Maximum div 2) + 1,

    ValidPrices = lists:foldl(
                    fun(E, Acc) ->
                            select_prices_by_time(StartScan, EndScan, E, Acc)
                    end, #{},
                    %% guarantee that prices are sorted in timestamp order
                    %% so that newer prices will replace older prices
                    lists:sort(fun sort_price_entry_time/2, NewPriceList)),

    NumPrices = maps:size(ValidPrices),

    case NumPrices >= Minimum andalso NumPrices =< Maximum of
        true -> {median(maps:values(ValidPrices)), NewPriceList};
        false -> {LastPrice, NewPriceList}
    end.

sort_price_entry_time(A, B) ->
    ATime = blockchain_ledger_oracle_price_entry:timestamp(A),
    BTime = blockchain_ledger_oracle_price_entry:timestamp(B),

    ATime =< BTime.

select_prices_by_time(Start, End, Entry, Acc) ->
    T = blockchain_ledger_oracle_price_entry:timestamp(Entry),
    PK = blockchain_ledger_oracle_price_entry:public_key( Entry),
    P = blockchain_ledger_oracle_price_entry:price(Entry),

    if
        T >= End andalso T =< Start -> Acc#{ PK => P };
        T > End -> Acc;
        true -> Acc
    end.

median([P]) -> P; %% if there's only 1 entry, use it
median(L) ->
    Sorted = lists:sort(L),
    Len = length(L),
    case Len rem 2 of
        0 -> lists:nth(Len div 2, Sorted);
        1 ->
            Div = Len div 2,
            Div1 = Div+1,
            (lists:nth(Div, Sorted) + lists:nth(Div1, Sorted)) div 2 %% no floats
    end.

trim_price_list(LastTime, PriceEntries) ->
    lists:filter(fun(Entry) ->
                            case blockchain_ledger_oracle_price_entry:timestamp(Entry) of
                                T when T > LastTime -> true;
                                _ -> false
                            end
                    end, PriceEntries).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_entry(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_entry_v1:entry()}
                                                          | {error, any()}.
find_entry(Address, Ledger) ->
    EntriesCF = entries_cf(Ledger),
    case cache_get(Ledger, EntriesCF, Address, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_entry_v1:deserialize(BinEntry)};
        not_found ->
            {error, address_entry_not_found};
        Error ->
            Error
    end.

-spec credit_account(libp2p_crypto:pubkey_bin(), integer(), ledger()) -> ok | {error, any()}.
credit_account(Address, Amount, Ledger) ->
    EntriesCF = entries_cf(Ledger),
    case ?MODULE:find_entry(Address, Ledger) of
        {error, address_entry_not_found} ->
            Entry = blockchain_ledger_entry_v1:new(0, Amount),
            Bin = blockchain_ledger_entry_v1:serialize(Entry),
            cache_put(Ledger, EntriesCF, Address, Bin);
        {ok, Entry} ->
            Entry1 = blockchain_ledger_entry_v1:new(
                blockchain_ledger_entry_v1:nonce(Entry),
                blockchain_ledger_entry_v1:balance(Entry) + Amount
            ),
            Bin = blockchain_ledger_entry_v1:serialize(Entry1),
            cache_put(Ledger, EntriesCF, Address, Bin);
        {error, _}=Error ->
            Error
    end.

-spec debit_account(libp2p_crypto:pubkey_bin(), integer(), integer(), ledger()) -> ok | {error, any()}.
debit_account(Address, Amount, Nonce, Ledger) ->
    case ?MODULE:find_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            case Nonce =:= blockchain_ledger_entry_v1:nonce(Entry) + 1 of
                true ->
                    Balance = blockchain_ledger_entry_v1:balance(Entry),
                    case (Balance - Amount) >= 0 of
                        true ->
                            Entry1 = blockchain_ledger_entry_v1:new(
                                Nonce,
                                (Balance - Amount)
                            ),
                            Bin = blockchain_ledger_entry_v1:serialize(Entry1),
                            EntriesCF = entries_cf(Ledger),
                            cache_put(Ledger, EntriesCF, Address, Bin);
                        false ->
                            {error, {insufficient_balance, {Amount, Balance}}}
                    end;
                false ->
                    {error, {bad_nonce, {payment, Nonce, blockchain_ledger_entry_v1:nonce(Entry)}}}
            end
    end.

-spec debit_fee_from_account(libp2p_crypto:pubkey_bin(), integer(), ledger(), blockchain_txn:hash(), blockchain:blockchain()) -> ok | {error, any()}.
debit_fee_from_account(Address, Fee, Ledger, TxnHash, Chain) ->
    case ?MODULE:find_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            case (Balance - Fee) >= 0 of
                true ->
                    case application:get_env(blockchain, store_implicit_burns, false) of
                        true when TxnHash =/= undefined, Chain =/= undefined ->
                            ImplicitBurn = blockchain_implicit_burn:new(
                                Fee,
                                Address
                            ),
                            blockchain:add_implicit_burn(TxnHash, ImplicitBurn, Chain);
                        false ->
                            ok
                    end,
                    Entry1 = blockchain_ledger_entry_v1:new(
                        blockchain_ledger_entry_v1:nonce(Entry),
                        (Balance - Fee)
                    ),
                    EntryBin = blockchain_ledger_entry_v1:serialize(Entry1),
                    EntriesCF = entries_cf(Ledger),
                    cache_put(Ledger, EntriesCF, Address, EntryBin);
                false ->
                    {error, {insufficient_balance_for_fee, {Fee, Balance}}}
            end
    end.

-spec check_balance(Address :: libp2p_crypto:pubkey_bin(), Amount :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
check_balance(Address, Amount, Ledger) ->
    case ?MODULE:find_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            case (Balance - Amount) >= 0 of
                false ->
                    {error, {insufficient_balance, {Amount, Balance}}};
                true ->
                    ok
            end
    end.

-spec find_dc_entry(libp2p_crypto:pubkey_bin(), ledger()) ->
    {ok, blockchain_ledger_data_credits_entry_v1:data_credits_entry()}
    | {error, any()}.
find_dc_entry(Address, Ledger) ->
    EntriesCF = dc_entries_cf(Ledger),
    case cache_get(Ledger, EntriesCF, Address, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_data_credits_entry_v1:deserialize(BinEntry)};
        not_found ->
            {error, dc_entry_not_found};
        Error ->
            Error
    end.

-spec credit_dc(libp2p_crypto:pubkey_bin(), integer(), ledger()) -> ok | {error, any()}.
credit_dc(Address, Amount, Ledger) ->
    EntriesCF = dc_entries_cf(Ledger),
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, dc_entry_not_found} ->
            Entry = blockchain_ledger_data_credits_entry_v1:new(0, Amount),
            Bin = blockchain_ledger_data_credits_entry_v1:serialize(Entry),
            cache_put(Ledger, EntriesCF, Address, Bin);
        {ok, Entry} ->
            Entry1 = blockchain_ledger_data_credits_entry_v1:new(
                blockchain_ledger_data_credits_entry_v1:nonce(Entry),
                blockchain_ledger_data_credits_entry_v1:balance(Entry) + Amount
            ),
            Bin = blockchain_ledger_data_credits_entry_v1:serialize(Entry1),
            cache_put(Ledger, EntriesCF, Address, Bin);
        {error, _}=Error ->
            Error
    end.

-spec debit_dc(Address :: libp2p_crypto:pubkey_bin(),
               Nonce :: non_neg_integer(),
               Amount :: non_neg_integer(),
               Ledger :: ledger()) -> ok | {error, any()}.
debit_dc(Address, Nonce, Amount, Ledger) ->
    DebitFun =
        fun(Entry) ->
            Balance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            %% NOTE: If fee = 0, this should still work..
            case (Balance - Amount) >= 0 of
                true ->
                    Entry1 = blockchain_ledger_data_credits_entry_v1:new(Nonce, (Balance - Amount)),
                    Bin = blockchain_ledger_data_credits_entry_v1:serialize(Entry1),
                    EntriesCF = dc_entries_cf(Ledger),
                    cache_put(Ledger, EntriesCF, Address, Bin);
                false ->
                    {error, {insufficient_dc_balance, {Amount, Balance}}}
            end
        end,

    case ?MODULE:find_dc_entry(Address, Ledger) of
        {ok, Entry0} ->
            case Nonce =:= blockchain_ledger_data_credits_entry_v1:nonce(Entry0) + 1 of
                false ->
                    {error, {bad_nonce, {data_credit, Nonce, blockchain_ledger_data_credits_entry_v1:nonce(Entry0)}}};
                true ->
                    DebitFun(Entry0)
            end;
        {error, dc_entry_not_found} ->
            %% Just create a blank entry if dc_entry_not_found
            Entry0 = blockchain_ledger_data_credits_entry_v1:new(0, 0),
            DebitFun(Entry0);
        {error, _}=Error ->
            Error
    end.

-spec debit_fee(
    Address :: libp2p_crypto:pubkey_bin(),
    Fee :: non_neg_integer(),
    Ledger :: ledger()
) -> ok | {error, any()}.
debit_fee(_Address, Fee, _Ledger) ->
    debit_fee(_Address, Fee, _Ledger, false, undefined, undefined).

-spec debit_fee(
    Address :: libp2p_crypto:pubkey_bin(),
    Fee :: non_neg_integer(),
    Ledger :: ledger(),
    MaybeTryImplicitBurn :: boolean()
) -> ok | {error, any()}.
debit_fee(_Address, Fee, _Ledger, _MaybeTryImplicitBurn) ->
    debit_fee(_Address, Fee, _Ledger, _MaybeTryImplicitBurn, undefined, undefined).

-spec debit_fee(
    Address :: libp2p_crypto:pubkey_bin(),
    Fee :: non_neg_integer(),
    Ledger :: ledger(),
    MaybeTryImplicitBurn :: boolean(),
    TxnHash :: blockchain_txn:hash() | undefined,
    Chain :: blockchain:blockchain() | undefined
) -> ok | {error, any()}.
debit_fee(_Address, 0, _Ledger, _MaybeTryImplicitBurn, _TxnHash, _Chain) ->
    ok;
debit_fee(Address, Fee, Ledger, MaybeTryImplicitBurn, TxnHash, Chain) ->
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, dc_entry_not_found} when MaybeTryImplicitBurn == true ->
            {ok, FeeInHNT} = ?MODULE:dc_to_hnt(Fee, Ledger),
            ok = add_hnt_burned(FeeInHNT, Ledger),
            ?MODULE:debit_fee_from_account(Address, FeeInHNT, Ledger, TxnHash, Chain);
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            case {(Balance - Fee) >= 0, MaybeTryImplicitBurn} of
                {true, _} ->
                    Entry1 = blockchain_ledger_data_credits_entry_v1:new(
                        blockchain_ledger_data_credits_entry_v1:nonce(Entry),
                        (Balance - Fee)
                    ),
                    Bin = blockchain_ledger_data_credits_entry_v1:serialize(Entry1),
                    EntriesCF = dc_entries_cf(Ledger),
                    cache_put(Ledger, EntriesCF, Address, Bin);
                {false, true} ->
                    %% user does not have sufficient DC balance, try to do an implicit hnt burn instead
                    {ok, FeeInHNT} = ?MODULE:dc_to_hnt(Fee, Ledger),
                    ok = add_hnt_burned(FeeInHNT, Ledger),
                    ?MODULE:debit_fee_from_account(Address, FeeInHNT, Ledger, TxnHash, Chain);
                {false, false} ->
                    {error, {insufficient_dc_balance, {Fee, Balance}}}
            end
    end.

-spec check_dc_balance(Address :: libp2p_crypto:pubkey_bin(), Amount :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
check_dc_balance(_Address, 0, _Ledger) ->
    ok;
check_dc_balance(Address, Amount, Ledger) ->
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            case (Balance - Amount) >= 0 of
                false ->
                    {error, {insufficient_dc_balance, {Amount, Balance}}};
                true ->
                    ok
            end
    end.

-spec check_dc_or_hnt_balance(Address :: libp2p_crypto:pubkey_bin(), Amount :: non_neg_integer(), Ledger :: ledger(), boolean()) -> ok | {error, any()}.
check_dc_or_hnt_balance(_Address, 0, _Ledger, _IsFeesEnabled) ->
    ok;
check_dc_or_hnt_balance(Address, Amount, Ledger, IsFeesEnabled) ->
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, dc_entry_not_found} ->
            {ok, AmountInHNT} = ?MODULE:dc_to_hnt(Amount, Ledger),
            case ?MODULE:check_balance(Address, AmountInHNT, Ledger) of
                {error, address_entry_not_found} ->
                    %% if the entry is not found, return insufficient balance error
                    {error, insufficient_dc_and_hnt_balance};
                Res ->
                    %% pass thru all other responses unaltered
                    Res
            end;
        {ok, Entry} ->
            Balance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            case {(Balance - Amount) >= 0, IsFeesEnabled}  of
                {true, _} ->
                    ok;
                {false, false} ->
                    {error, {insufficient_dc_balance, {Amount, Balance}}};
                {false, true} ->
                    {ok, AmountInHNT} = ?MODULE:dc_to_hnt(Amount, Ledger),
                    case ?MODULE:check_balance(Address, AmountInHNT, Ledger) of
                        {error, address_entry_not_found} ->
                            %% if the entry is not found, return insufficient balance error
                            {error, insufficient_dc_and_hnt_balance};
                        Res ->
                            %% pass thru all other responses unaltered
                            Res
                    end
            end
    end.

-spec hnt_burned(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
hnt_burned(Ledger) ->
    case blockchain:config(?net_emissions_enabled, Ledger) of
        {ok, true} ->
            DefaultCF = default_cf(Ledger),
            case cache_get(Ledger, DefaultCF, ?HNT_BURNED, []) of
                {ok, <<Burned:64/integer-unsigned-native>>} ->
                    {ok, Burned};
                not_found ->
                    {ok, 0};
                Error ->
                    Error
            end;
        _ -> {ok, 0}
    end.

-spec add_hnt_burned(non_neg_integer(), ledger()) -> ok.
add_hnt_burned(Burned, Ledger) ->
    case blockchain:config(?net_emissions_enabled, Ledger) of
        {ok, true} ->
            DefaultCF = default_cf(Ledger),
            Prev =
                case cache_get(Ledger, DefaultCF, ?HNT_BURNED, []) of
                    {ok, <<P:64/integer-unsigned-native>>} -> P;
                    not_found -> 0
                end,
            New = Prev + Burned,
            cache_put(Ledger, DefaultCF, ?HNT_BURNED, <<New:64/integer-unsigned-native>>);
        _ -> ok
    end.

-spec clear_hnt_burned(ledger()) -> ok.
clear_hnt_burned(Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?HNT_BURNED, <<0:64/integer-unsigned-native>>).

-spec net_overage(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
net_overage(Ledger) ->
    case blockchain:config(?net_emissions_enabled, Ledger) of
        {ok, true} ->
            DefaultCF = default_cf(Ledger),
            case cache_get(Ledger, DefaultCF, ?NET_OVERAGE, []) of
                {ok, <<Overage:64/integer-unsigned-native>>} ->
                    {ok, Overage};
                not_found ->
                    {ok, 0};
                Error ->
                    Error
            end;
        _ -> {ok, 0}
    end.

-spec net_overage(non_neg_integer(), ledger()) -> ok.
net_overage(Overage, Ledger) ->
    case blockchain:config(?net_emissions_enabled, Ledger) of
        {ok, true} ->
            DefaultCF = default_cf(Ledger),
            cache_put(Ledger, DefaultCF, ?NET_OVERAGE, <<Overage:64/integer-unsigned-native>>);
        _ -> ok
    end.

-spec token_burn_exchange_rate(ledger()) -> {ok, integer()} | {error, any()}.
token_burn_exchange_rate(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?BURN_RATE, []) of
        {ok, <<Rate:64/integer-unsigned-big>>} ->
            {ok, Rate};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec token_burn_exchange_rate(non_neg_integer(), ledger()) -> ok.
token_burn_exchange_rate(Rate, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?BURN_RATE, <<Rate:64/integer-unsigned-big>>).

-spec securities(ledger()) -> securities().
securities(Ledger) ->
    SecuritiesCF = securities_cf(Ledger),
    cache_fold(
        Ledger,
        SecuritiesCF,
        fun({Address, Binary}, Acc) ->
            Entry = blockchain_ledger_security_entry_v1:deserialize(Binary),
            maps:put(Address, Entry, Acc)
        end,
        #{}
    ).

-spec find_security_entry(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_security_entry_v1:entry()}
                                                                   | {error, any()}.
find_security_entry(Address, Ledger) ->
    SecuritiesCF = securities_cf(Ledger),
    case cache_get(Ledger, SecuritiesCF, Address, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_security_entry_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec credit_security(libp2p_crypto:pubkey_bin(), integer(), ledger()) -> ok | {error, any()}.
credit_security(Address, Amount, Ledger) ->
    SecuritiesCF = securities_cf(Ledger),
    case ?MODULE:find_security_entry(Address, Ledger) of
        {error, not_found} ->
            Entry = blockchain_ledger_security_entry_v1:new(0, Amount),
            Bin = blockchain_ledger_security_entry_v1:serialize(Entry),
            cache_put(Ledger, SecuritiesCF, Address, Bin);
        {ok, Entry} ->
            Entry1 = blockchain_ledger_security_entry_v1:new(
                blockchain_ledger_security_entry_v1:nonce(Entry),
                blockchain_ledger_security_entry_v1:balance(Entry) + Amount
            ),
            Bin = blockchain_ledger_security_entry_v1:serialize(Entry1),
            cache_put(Ledger, SecuritiesCF, Address, Bin);
        {error, _}=Error ->
            Error
    end.

-spec debit_security(libp2p_crypto:pubkey_bin(), integer(), integer(), ledger()) -> ok | {error, any()}.
debit_security(Address, Amount, Nonce, Ledger) ->
    case ?MODULE:find_security_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            case Nonce =:= blockchain_ledger_security_entry_v1:nonce(Entry) + 1 of
                true ->
                    Balance = blockchain_ledger_security_entry_v1:balance(Entry),
                    case (Balance - Amount) >= 0 of
                        true ->
                            Entry1 = blockchain_ledger_security_entry_v1:new(
                                Nonce,
                                (Balance - Amount)
                            ),
                            Bin = blockchain_ledger_security_entry_v1:serialize(Entry1),
                            SecuritiesCF = securities_cf(Ledger),
                            cache_put(Ledger, SecuritiesCF, Address, Bin);
                        false ->
                            {error, {insufficient_security_balance, {Amount, Balance}}}
                    end;
                false ->
                    {error, {bad_nonce, {payment, Nonce, blockchain_ledger_security_entry_v1:nonce(Entry)}}}
            end
    end.

-spec check_security_balance(Address :: libp2p_crypto:pubkey_bin(), Amount :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
check_security_balance(Address, Amount, Ledger) ->
    case ?MODULE:find_security_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_security_entry_v1:balance(Entry),
            case (Balance - Amount) >= 0 of
                false ->
                    {error, {insufficient_security_balance, {Amount, Balance}}};
                true ->
                    ok
            end
    end.

-spec find_htlc(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_htlc_v1:htlc()}
                                                         | {error, any()}.
find_htlc(Address, Ledger) ->
    HTLCsCF = htlcs_cf(Ledger),
    case cache_get(Ledger, HTLCsCF, Address, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_htlc_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec add_htlc(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
               non_neg_integer(), non_neg_integer(),  binary(), non_neg_integer(), ledger()) -> ok | {error, any()}.
add_htlc(Address, Payer, Payee, Amount, Nonce, Hashlock, Timelock, Ledger) ->
    HTLCsCF = htlcs_cf(Ledger),
    case ?MODULE:find_htlc(Address, Ledger) of
        {ok, _} ->
            {error, address_already_exists};
        {error, _} ->
            HTLC = blockchain_ledger_htlc_v1:new(Payer, Payee, Amount, Nonce, Hashlock, Timelock),
            Bin = blockchain_ledger_htlc_v1:serialize(HTLC),
            cache_put(Ledger, HTLCsCF, Address, Bin)
    end.

-spec redeem_htlc(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), ledger(), blockchain:blockchain()) -> ok | {error, any()}.
redeem_htlc(Address, Payee, Ledger, Chain) ->
    case ?MODULE:find_htlc(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, HTLC} ->
            Amount = blockchain_ledger_htlc_v1:balance(HTLC),
            case ?MODULE:credit_account(Payee, Amount, Ledger) of
                {error, _}=Error -> Error;
                ok ->
                    %% Store receipt if env requries it
                    case application:get_env(blockchain, store_htlc_receipts, false) of
                        true when Address =/= undefined, Chain =/= undefined ->
                            {ok, RedeemedAt} = current_height(Ledger),
                            Payer = blockchain_ledger_htlc_v1:payer(HTLC),
                            Hashlock = blockchain_ledger_htlc_v1:hashlock(HTLC),
                            Timelock = blockchain_ledger_htlc_v1:timelock(HTLC),
                            HTLCReceipt = blockchain_htlc_receipt:new(
                                Payer,
                                Payee,
                                Address,
                                Amount,
                                Hashlock,
                                Timelock,
                                RedeemedAt
                            ),
                            blockchain:add_htlc_receipt(Address, HTLCReceipt, Chain);
                        false ->
                            ok
                    end,

                    %% Delete redeemed HTLC from DB
                    HTLCsCF = htlcs_cf(Ledger),
                    cache_delete(Ledger, HTLCsCF, Address)
            end
    end.


-spec get_oui_counter(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
get_oui_counter(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?OUI_COUNTER, []) of
        {ok, <<OUI:32/little-unsigned-integer>>} ->
            {ok, OUI};
        not_found ->
            {ok, 0};
        Error ->
            Error
    end.

-spec set_oui_counter(pos_integer(), ledger()) -> ok | {error, _}.
set_oui_counter(Count, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?OUI_COUNTER, <<Count:32/little-unsigned-integer>>).

-spec increment_oui_counter(ledger()) -> {ok, pos_integer()} | {error, any()}.
increment_oui_counter(Ledger) ->
    case ?MODULE:get_oui_counter(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, OUICounter} ->
            DefaultCF = default_cf(Ledger),
            ok = cache_put(Ledger, DefaultCF, ?OUI_COUNTER, <<(OUICounter+1):32/little-unsigned-integer>>),
            {ok, OUICounter+1}
    end.

-spec add_oui(binary(), [binary()], binary(), <<_:48>>, ledger()) -> ok | {error, any()}.
add_oui(Owner, Addresses, Filter, Subnet, Ledger) ->
    case ?MODULE:increment_oui_counter(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, OUI} ->
            RoutingCF = routing_cf(Ledger),
            SubnetCF = subnets_cf(Ledger),
            Routing = blockchain_ledger_routing_v1:new(OUI, Owner, Addresses, Filter, Subnet, 0),
            Bin = blockchain_ledger_routing_v1:serialize(Routing),
            ok = cache_put(Ledger, RoutingCF, <<OUI:32/integer-unsigned-big>>, Bin),
            ok = cache_put(Ledger, SubnetCF, Subnet, <<OUI:32/little-unsigned-integer>>)
    end.

-spec get_routes(ledger()) -> {ok, [blockchain_ledger_routing_v1:routing()]}.
get_routes(Ledger) ->
    RoutingCF = routing_cf(Ledger),
    {ok, cache_fold(Ledger, RoutingCF,
                     fun({<<_OUI:32/integer-unsigned-big>>, V}, Acc) ->
                             Route = blockchain_ledger_routing_v1:deserialize(V),
                             [Route | Acc];
                        ({_K, _V}, Acc) ->
                             Acc
                     end, [], [{start, <<0:32/integer-unsigned-big>>}, {iterate_upper_bound, <<4294967295:32/integer-unsigned-big>>}])}.

-spec find_routing(non_neg_integer(), ledger()) -> {ok, blockchain_ledger_routing_v1:routing()}
                                                   | {error, any()}.
find_routing(OUI, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    case cache_get(Ledger, RoutingCF, <<OUI:32/integer-unsigned-big>>, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_routing_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec find_routing_for_packet(blockchain_helium_packet_v1:packet(), ledger()) -> {ok, [blockchain_ledger_routing_v1:routing(), ...]}
                                                                                 | {error, any()}.
find_routing_for_packet(Packet, Ledger) ->
    case blockchain_helium_packet_v1:routing_info(Packet) of
        {eui, DevEUI, AppEUI} ->
            find_routing_via_eui(DevEUI, AppEUI, Ledger);
        {devaddr, DevAddr0} ->
            find_routing_via_devaddr(DevAddr0, Ledger)
    end.

-spec find_routing_via_eui(DevEUI :: non_neg_integer(),
                           AppEUI :: non_neg_integer(),
                           Ledger :: ledger()) -> {ok, [blockchain_ledger_routing_v1:routing(), ...]} | {error, any()}.
find_routing_via_eui(DevEUI, AppEUI, Ledger) ->
    %% ok, search the xor filters
    Key = <<DevEUI:64/integer-unsigned-little, AppEUI:64/integer-unsigned-little>>,
    RoutingCF = routing_cf(Ledger),
    Res = cache_fold(Ledger, RoutingCF,
                     fun({<<_OUI:32/integer-unsigned-big>>, V}, Acc) ->
                             Route = blockchain_ledger_routing_v1:deserialize(V),
                             case lists:any(fun(Filter) ->
                                                    xor16:contain({Filter, fun xxhash:hash64/1}, Key)
                                            end, blockchain_ledger_routing_v1:filters(Route)) of
                                 true ->
                                     [Route | Acc];
                                 false ->
                                     Acc
                             end;
                        ({_K, _V}, Acc) ->
                             Acc
                     end, [], [{start, <<0:32/integer-unsigned-big>>}, {iterate_upper_bound, <<4294967295:32/integer-unsigned-big>>}]),
    case Res of
        [] ->
            {error, eui_not_matched};
        _ ->
            {ok, Res}
    end.

-spec find_routing_via_devaddr(DevAddr0 :: non_neg_integer(),
                               Ledger :: ledger()) -> {ok, [blockchain_ledger_routing_v1:routing(), ...]} | {error, any()}.
find_routing_via_devaddr(DevAddr0, Ledger) ->
    DevAddrPrefix = application:get_env(blockchain, devaddr_prefix, $H),
    case <<DevAddr0:32/integer-unsigned-little>> of
        <<DevAddr:25/integer-unsigned-little, DevAddrPrefix:7/integer>> ->
            %% use the subnets
            {_Name, DB, SubnetCF} = subnets_cf(Ledger),
            {ok, Itr} = rocksdb:iterator(DB, SubnetCF, []),
            Dest = subnet_lookup(Itr, DevAddr, rocksdb:iterator_move(Itr, {seek_for_prev, <<DevAddr:25/integer-unsigned-big, ?BITS_23:23/integer>>})),
            catch rocksdb:iterator_close(Itr),
            case Dest of
                error ->
                    {error, subnet_not_found};
                _ ->
                    case find_routing(Dest, Ledger) of
                        {ok, Route} ->
                            {ok, [Route]};
                        Error ->
                            Error
                    end
            end;
        <<_:25/integer, Prefix:7/integer>> ->
            {error, {unknown_devaddr_prefix, Prefix}}
    end.

-spec find_router_ouis(RouterPubkeyBin :: libp2p_crypto:pubkey_bin(),
                       Ledger :: ledger()) -> [non_neg_integer()].
find_router_ouis(RouterPubkeyBin, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    cache_fold(
      Ledger,
      RoutingCF,
      fun({<<OUI:32/integer-unsigned-big>>, Bin}, Acc) ->
              Routing = blockchain_ledger_routing_v1:deserialize(Bin),
              Addresses = blockchain_ledger_routing_v1:addresses(Routing),
              case lists:member(RouterPubkeyBin, Addresses) of
                  false ->
                      Acc;
                  true ->
                      [OUI | Acc]
              end
      end,
      []
     ).

-spec update_routing(non_neg_integer(), blockchain_txn_routing_v1:action(), non_neg_integer(), ledger()) -> ok | {error, any()}.
update_routing(OUI, Action, Nonce, Ledger) ->
    case find_routing(OUI, Ledger) of
        {ok, Routing} ->
            RoutingCF = routing_cf(Ledger),
            Bin = blockchain_ledger_routing_v1:serialize(blockchain_ledger_routing_v1:update(Routing, Action, Nonce)),
            cache_put(Ledger, RoutingCF, <<OUI:32/integer-unsigned-big>>, Bin);
        Error ->
            Error
    end.

-spec find_state_channel_with_mod(ID :: binary(),
                         Owner :: libp2p_crypto:pubkey_bin(),
                         Ledger :: ledger()) ->
    {ok, blockchain_ledger_state_channel_v1, blockchain_ledger_state_channel_v1:state_channel()}
    | {ok, blockchain_ledger_state_channel_v2, blockchain_ledger_state_channel_v2:state_channel()}
    | {error, any()}.
find_state_channel_with_mod(ID, Owner, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    Key = state_channel_key(ID, Owner),
    case cache_get(Ledger, SCsCF, Key, []) of
        {ok, BinEntry} ->
            {Mod, SC} = deserialize_state_channel(BinEntry),
            {ok, Mod, SC};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec find_state_channel(ID :: binary(),
                         Owner :: libp2p_crypto:pubkey_bin(),
                         Ledger :: ledger()) ->
    {ok, blockchain_ledger_state_channel_v1:state_channel()}
    | {ok, blockchain_ledger_state_channel_v2:state_channel()}
    | {error, any()}.
find_state_channel(ID, Owner, Ledger) ->
    case find_state_channel_with_mod(ID, Owner, Ledger) of
        {ok, _Mod, SC} -> {ok, SC};
        Error -> Error
    end.

-spec count_open_scs_for_owner(BinIds :: [binary()],
                               Owner :: libp2p_crypto:pubkey_bin(),
                               Ledger :: ledger()) -> non_neg_integer().
count_open_scs_for_owner(BinIds, Owner, Ledger) ->
    lists:foldl(
      fun(Id, Acc) ->
              case find_state_channel(Id, Owner, Ledger) of
                  {ok, SC} ->
                      case blockchain_ledger_state_channel_v2:is_v2(SC) of
                          true ->
                              case blockchain_ledger_state_channel_v2:close_state(SC) of
                                  undefined -> Acc + 1;
                                  _ -> Acc
                              end;
                          false ->
                              %% this is a v1 state channel, ignore
                              %% shouldn't get here ever
                              Acc
                      end;
                  _ -> Acc
              end
      end, 0, BinIds).

-spec find_sc_ids_by_owner(Owner :: libp2p_crypto:pubkey_bin(),
                           Ledger :: ledger()) -> {ok, [binary()]}.
find_sc_ids_by_owner(Owner, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    OwnerLength = byte_size(Owner),
    %% find all the state channels where the key begins with the owner
    %% and return the list of IDs (the second part of the key)
    {ok, cache_fold(Ledger, SCsCF,
               fun({K, _V}, Acc) when erlang:binary_part(K, {0, OwnerLength}) == Owner ->
                       [binary:part(K, OwnerLength, byte_size(K) - OwnerLength)|Acc];
                  (_, Acc) ->
                       Acc
               end, [], [{start, Owner}, {iterate_upper_bound, increment_bin(Owner)}])}.

-spec find_scs_by_owner(Owner :: libp2p_crypto:pubkey_bin(),
                        Ledger :: blockchain_ledger_v1:ledger()) -> {ok, state_channel_map()}.
find_scs_by_owner(Owner, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    OwnerLength = byte_size(Owner),
    %% find all the state channels where the key begins with the owner,
    %% extract the ID from the second half of the key and deserialize the value
    {ok, cache_fold(Ledger, SCsCF,
               fun({K, V}, Acc) when erlang:binary_part(K, {0, OwnerLength}) == Owner ->
                       ID = binary:part(K, OwnerLength, byte_size(K) - OwnerLength),
                       {_Mod, SC} = deserialize_state_channel(V),
                       maps:put(ID, SC, Acc);
                  (_, Acc) ->
                       Acc
               end, #{}, [{start, Owner}, {iterate_upper_bound, increment_bin(Owner)}])}.

-spec add_state_channel(ID :: binary(),
                        Owner :: libp2p_crypto:pubkey_bin(),
                        ExpireWithin :: pos_integer(),
                        Nonce :: non_neg_integer(),
                        Original :: non_neg_integer(),
                        Amount :: non_neg_integer(),
                        Ledger :: ledger()) -> ok | {error, any()}.
add_state_channel(ID, Owner, ExpireWithin, Nonce, Original, Amount, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    {ok, CurrHeight} = ?MODULE:current_height(Ledger),
    Key = state_channel_key(ID, Owner),
    Bin = case blockchain:config(?sc_version, Ledger) of
        {ok, 2} ->
            Routing = blockchain_ledger_state_channel_v2:new(ID, Owner,
                                                             CurrHeight+ExpireWithin,
                                                             Original, Amount, Nonce),
            blockchain_ledger_state_channel_v2:serialize(Routing);
        _ ->
            Routing = blockchain_ledger_state_channel_v1:new(ID, Owner,
                                                             CurrHeight+ExpireWithin, Nonce),
            blockchain_ledger_state_channel_v1:serialize(Routing)
          end,
    cache_put(Ledger, SCsCF, Key, Bin).

-spec is_state_channel_overpaid(blockchain_state_channel_v1:state_channel(), ledger()) -> boolean().
is_state_channel_overpaid(SC, Ledger) ->
    %% assume we've checked this channel is active, etc
    {ok, LedgerSC} = find_state_channel(blockchain_state_channel_v1:id(SC), blockchain_state_channel_v1:owner(SC), Ledger),
    case blockchain_ledger_state_channel_v2:is_v2(LedgerSC) of
        true ->
            blockchain_ledger_state_channel_v2:original(LedgerSC) < blockchain_state_channel_v1:total_dcs(SC);
        false ->
            false
    end.

-spec close_state_channel(Owner :: libp2p_crypto:pubkey_bin(),
                          Closer :: libp2p_crypto:pubkey_bin(),
                          SC :: blockchain_state_channel_v1:state_channel(),
                          SCID :: blockchain_state_channel_v1:id(),
                          HadConflict :: boolean(),
                          Ledger :: ledger()) -> ok.
close_state_channel(Owner, Closer, SC, SCID, HadConflict, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    Key = state_channel_key(SCID, Owner),
    case ?MODULE:config(?sc_version, Ledger) of
        {ok, 2} ->
            %% DC overcommits are returned during SC garbage collection
            %% because it's possible another actor might submit a
            %% SC close txn with an updated nonce/state
            %%
            %% The internal close state of a SC would then be updated
            %% by the `close_proposal' function. So we just record
            %% it here and deal with overcommit during GC.
            {ok, PrevSCE} = find_state_channel(SCID, Owner, Ledger),
            case blockchain_ledger_state_channel_v2:is_v2(PrevSCE) of
                true ->
                    ConsiderEffectOf = case blockchain:config(?sc_causality_fix, Ledger) of
                                           {ok, N} when N > 0 ->
                                               true;
                                           _ ->
                                               false
                                       end,
                    MaxActorsAllowed = blockchain_state_channel_v1:max_actors_allowed(Ledger),
                    NewSCE = blockchain_ledger_state_channel_v2:close_proposal(Closer, SC, HadConflict, PrevSCE, ConsiderEffectOf, MaxActorsAllowed),
                    Bin = blockchain_ledger_state_channel_v2:serialize(NewSCE),
                    cache_put(Ledger, SCsCF, Key, Bin);
                false ->
                    %% holdover v1 from before upgrade
                    cache_delete(Ledger, SCsCF, Key)
            end;
        _ ->
            cache_delete(Ledger, SCsCF, Key)
    end.

-spec allocate_subnet(pos_integer(), ledger()) -> {ok, <<_:48>>} | {error, any()}.
allocate_subnet(Size, Ledger) ->
    {_, DB, SubnetCF} = subnets_cf(Ledger),
    {ok, Itr} = rocksdb:iterator(DB, SubnetCF, []),
    Result = allocate_subnet(Size, Itr, rocksdb:iterator_move(Itr, first), none),
    catch rocksdb:iterator_close(Itr),
    Result.

allocate_subnet(Size, _Itr, {error, invalid_iterator}, none) ->
    %% we don't have any allocations at all
    Mask = blockchain_ledger_routing_v1:subnet_size_to_mask(Size),
    {ok, <<0:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>};
allocate_subnet(Size, Itr, {ok, <<ABase:25/integer-unsigned-big, AMask:23/integer-unsigned-big>>, _}, none) ->
    %% just record the actual 'last' allocation and continue
    allocate_subnet(Size, Itr, rocksdb:iterator_move(Itr, next), {ABase, blockchain_ledger_routing_v1:subnet_mask_to_size(AMask)});
allocate_subnet(Size, Itr, {ok, <<ABase:25/integer-unsigned-big, AMask:23/integer-unsigned-big>>, _}, {LastBase, LastSize}) ->
    %% check if the last allocation was contiguous with this one
    case LastBase + LastSize == ABase of
        true ->
            %% ok, no gaps here, keep on truckin'
            allocate_subnet(Size, Itr, rocksdb:iterator_move(Itr, next), {ABase, blockchain_ledger_routing_v1:subnet_mask_to_size(AMask)});
        false ->
            %% check if there's enough room
            case ABase - (LastBase + LastSize) >= Size of
                false ->
                    %% no room at the inn, sorry
                    allocate_subnet(Size, Itr, rocksdb:iterator_move(Itr, next), {ABase, blockchain_ledger_routing_v1:subnet_mask_to_size(AMask)});
                true ->
                    %% compute the base of the new allocation
                    Mask = blockchain_ledger_routing_v1:subnet_size_to_mask(Size),
                    NewBase = case ((LastBase + LastSize) band (Mask bsl 2)) == (LastBase + Size) of
                                  true ->
                                      %% we're on the right alignment boundary
                                      LastBase + LastSize;
                                  false ->
                                      %% compute the next allowed boundary
                                      (LastBase band (Mask bsl 2)) + Size
                              end,
                    %% assert there's room
                    true = NewBase + Size =< ABase,
                    {ok, <<NewBase:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>}
            end
    end;
allocate_subnet(Size, _Itr, {error, invalid_iterator}, {LastBase, LastSize}) ->
    %% we're at the end of the allocation list
    %% check if we have room at the end for this allocation
    case LastBase + LastSize + Size =< ?BITS_25 of
        false ->
            {error, no_space};
        true ->
            %% still room
            Mask = blockchain_ledger_routing_v1:subnet_size_to_mask(Size),
            NewBase = case ((LastBase + LastSize) band (Mask bsl 2)) == (LastBase + LastSize) of
                          true ->
                              %% we're on the right alignment boundary
                              LastBase + LastSize;
                          false ->
                              %% compute the next allowed boundary
                              (LastBase band (Mask bsl 2)) + Size
                      end,
            {ok, <<NewBase:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>}
    end.

-spec add_oracle_price(
        Entry :: blockchain_ledger_oracle_price_entry:oracle_price_entry(),
        Ledger :: ledger()) -> ok.
add_oracle_price(PriceEntry, Ledger) ->
    DefaultCF = default_cf(Ledger),
    Prices = case cache_get(Ledger, DefaultCF, ?ORACLE_PRICES, []) of
                 {ok, BinPrices} ->
                     binary_to_term(BinPrices);
                 not_found ->
                     []
             end,
    cache_put(Ledger, DefaultCF, ?ORACLE_PRICES, term_to_binary([ PriceEntry | Prices ])).

-spec load_oracle_price(Price :: non_neg_integer(), Ledger :: ledger()) -> ok.
load_oracle_price(Price, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?CURRENT_ORACLE_PRICE, term_to_binary(Price)).

-spec load_oracle_price_list(
        PriceEntries :: [blockchain_ledger_oracle_price_entry:oracle_price_entry()],
        Ledger :: ledger()) -> ok.
load_oracle_price_list(PriceEntries, Ledger) ->
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?ORACLE_PRICES, term_to_binary(PriceEntries)).

-spec current_oracle_price(ledger()) -> {ok, Price :: non_neg_integer()} | {error, any()}.
current_oracle_price(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?CURRENT_ORACLE_PRICE, []) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        not_found ->
            {ok, 0};
        Other ->
            Other
    end.

-spec next_oracle_prices(blockchain:blockchain(), ledger()) -> [{NextPrice :: non_neg_integer(), AtTime :: pos_integer()}].
next_oracle_prices(Blockchain, Ledger) ->
    DefaultCF = default_cf(Ledger),
    {ok, CurrentHeight} = current_height(Ledger),
    {ok, Interval} = blockchain:config(?price_oracle_refresh_interval, Ledger),
    {ok, DelaySecs} = blockchain:config(?price_oracle_price_scan_delay, Ledger),
    {ok, MaxSecs} = blockchain:config(?price_oracle_price_scan_max, Ledger),

    LastUpdate = CurrentHeight - (CurrentHeight rem Interval),

    {ok, #block_info_v2{time = BlockT}} = blockchain:get_block_info(LastUpdate, Blockchain),
    {ok, LastPrice} = current_oracle_price(Ledger),

    StartScan = BlockT - DelaySecs, % typically 1 hour (in seconds)
    EndScan = (BlockT - MaxSecs) + DelaySecs, % typically 1 day (in seconds)
    {ok, Prices} = current_oracle_price_list(Ledger),

    %% get the prices currently too new to be considered
    PendingPrices = trim_price_list(StartScan, Prices),

    %% get the prices expiring in the next DelaySecs
    ExpiringPrices = [ P || P <- Prices, blockchain_ledger_oracle_price_entry:timestamp(P) < EndScan],

    %% walk the times a price will mature or expire and check if that causes a price change
    %% note we take the tail of the reversed result here as the first element in the accumulator is the current price/time
    tl(lists:reverse(lists:foldl(
      fun(T, [{Price, _Time}|_]=Acc) ->
              %% calculate what the price would be at time T, which is either a time
              %% a price is expiring or a time a price has matured past DelaySecs
              {NewPrice, _} = recalc_price(LastPrice, T, DefaultCF, Ledger),
              %% if the price changes, add it to the accumulator
              case NewPrice /= Price of
                  true ->
                      [{NewPrice, T}|Acc];
                  false ->
                      Acc
              end
      end,
      %% supply the current price and current block time as the initial values of the accumulator
      [{LastPrice, BlockT}],
      %% calculate both the times when an old price report will expire, and a pending report will become active
      lists:usort([ blockchain_ledger_oracle_price_entry:timestamp(P) + MaxSecs || P <- ExpiringPrices] ++
                 [ blockchain_ledger_oracle_price_entry:timestamp(P) + DelaySecs || P <- PendingPrices])))).

-spec current_oracle_price_list(ledger()) ->
    {ok, [ blockchain_ledger_oracle_price_entry:oracle_price_entry() ]}
    | {error, any()}.
current_oracle_price_list(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?ORACLE_PRICES, []) of
        {ok, BinPrices} ->
            {ok, binary_to_term(BinPrices)};
        not_found ->
            {ok, []};
        Other ->
            Other
    end.

clean(#ledger_v1{dir=Dir, db=DB}=L) ->
    delete_context(L),
    DBDir = filename:join(Dir, ?DB_FILE),
    catch ok = rocksdb:close(DB),
    rocksdb:destroy(DBDir, []),
    clean_checkpoints(L),
    clean_aux(L);
clean(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    rocksdb:destroy(DBDir, []).

clean_aux(L) ->
    case has_aux(L) of
        true ->
            catch ok = rocksdb:close(L#ledger_v1.aux#aux_ledger_v1.db),
            DBDir = filename:join(L#ledger_v1.aux#aux_ledger_v1.dir, ?DB_FILE),
            rocksdb:destroy(DBDir, []);
        false ->
            ok
    end.

close(#ledger_v1{db=DB}=L) ->
    rocksdb:close(DB),
    case has_aux(L) of
        true ->
            rocksdb:close(L#ledger_v1.aux#aux_ledger_v1.db);
        false ->
            ok
    end.


compact(#ledger_v1{db=DB, active=Active, delayed=Delayed}) ->
    rocksdb:compact_range(DB, undefined, undefined, []),
    compact_ledger(DB, Active),
    compact_ledger(DB, Delayed),
    ok.

-spec state_channel_key(libp2p_crypto:pubkey_bin(), binary()) -> binary().
state_channel_key(ID, Owner) ->
    <<Owner/binary, ID/binary>>.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

compact_ledger(DB, #sub_ledger_v1{default=Default,
                                  active_gateways=Gateways,
                                  entries=Entries,
                                  dc_entries=DCEntries,
                                  htlcs=HTLCs,
                                  pocs=PoCs,
                                  securities=Securities,
                                  routing=Routing}) ->
    rocksdb:compact_range(DB, Default, undefined, undefined, []),
    rocksdb:compact_range(DB, Gateways, undefined, undefined, []),
    rocksdb:compact_range(DB, Entries, undefined, undefined, []),
    rocksdb:compact_range(DB, DCEntries, undefined, undefined, []),
    rocksdb:compact_range(DB, HTLCs, undefined, undefined, []),
    rocksdb:compact_range(DB, PoCs, undefined, undefined, []),
    rocksdb:compact_range(DB, Securities, undefined, undefined, []),
    rocksdb:compact_range(DB, Routing, undefined, undefined, []),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% need to prefix to keep people from messing with existing names on accident
%% @end
%%--------------------------------------------------------------------
var_name(Name) when is_atom(Name) ->
    <<"$var_", (atom_to_binary(Name, utf8))/binary>>;
%% binary clause for snapshot import
var_name(Name) when is_binary(Name) ->
    <<"$var_", Name/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec context_cache(undefined | direct | ets:tid(), undefined | ets:tid(), ledger()) -> ledger().
context_cache(Cache, GwCache, Ledger) ->
    SL = subledger(Ledger),
    subledger(Ledger, SL#sub_ledger_v1{cache=Cache, gateway_cache=GwCache}).

%% these ledger types don't allow fetching by hash
-spec get_block(integer(), ledger()) ->
          {ok, blockchain_block:block()} | {error, any()}.
get_block(Height, #ledger_v1{blocks_db = DB,
                             heights_cf = HeightsCF} = Ledger) when is_integer(Height) ->
    {ok, LedgerHeight} = current_height(Ledger),
    case Height > LedgerHeight of
        true -> {error, too_new};
        _ ->
            case rocksdb:get(DB, HeightsCF, <<Height:64/integer-unsigned-big>>, []) of
                {ok, Hash} ->
                    get_block(Hash, Ledger);
                not_found ->
                    {error, not_found};
                Error ->
                    Error
            end
    end;
get_block(Hash, Ledger) when is_binary(Hash) ->
    case get_raw_block(Hash, Ledger) of
        {ok, BinBlock} ->
            Block = blockchain_block:deserialize(BinBlock),
            case {ok, blockchain_block:height(Block)} > current_height(Ledger) of
                true ->
                    {error, too_new};
                false ->
                    {ok, Block}
            end;
        Other ->
            Other
    end.

get_raw_block(Hash, #ledger_v1{blocks_db = DB,
                               blocks_cf = BlocksCF}) ->
    case rocksdb:get(DB, BlocksCF, Hash, []) of
        {ok, BinBlock} ->
            {ok, BinBlock};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.


get_block_info(Height, #ledger_v1{blocks_db = DB,
                                  info_cf = InfoCF} = Ledger) ->
    case Height > current_height(Ledger) of
        true -> {error, too_new};
        _ ->
            case rocksdb:get(DB, InfoCF, <<Height:64/integer-unsigned-big>>, []) of
                {ok, BinInfo} ->
                    case binary_to_term(BinInfo) of
                        BI = #block_info_v2{} ->
                            {ok, BI};
                        _ ->
                            %% probably a stale one
                            case get_block(Height, Ledger) of
                                {ok, Block} ->
                                    NewInfo = blockchain:mk_block_info(blockchain_block:hash_block(Block), Block),
                                    rocksdb:put(DB, InfoCF, <<Height:64/integer-unsigned-big>>, term_to_binary(NewInfo), []),
                                    {ok, NewInfo};
                                Error ->
                                    Error
                            end
                    end;
                Error ->
                    Error
            end
    end.

-spec default_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
default_cf(Ledger) ->
    SL = subledger(Ledger),
    {default, db(Ledger), SL#sub_ledger_v1.default}.

-spec active_gateways_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
active_gateways_cf(Ledger) ->
    SL = subledger(Ledger),
    {active_gateways, db(Ledger), SL#sub_ledger_v1.active_gateways}.

-spec gw_denorm_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
gw_denorm_cf(Ledger) ->
    SL = subledger(Ledger),
    {gw_denorm, db(Ledger), SL#sub_ledger_v1.gw_denorm}.

-spec entries_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
entries_cf(Ledger) ->
    SL = subledger(Ledger),
    {entries, db(Ledger), SL#sub_ledger_v1.entries}.

-spec dc_entries_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
dc_entries_cf(Ledger) ->
    SL = subledger(Ledger),
    {dc_entries, db(Ledger), SL#sub_ledger_v1.dc_entries}.

-spec htlcs_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
htlcs_cf(Ledger) ->
    SL = subledger(Ledger),
    {htlcs, db(Ledger), SL#sub_ledger_v1.htlcs}.

-spec pocs_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
pocs_cf(Ledger) ->
    SL = subledger(Ledger),
    {pocs, db(Ledger), SL#sub_ledger_v1.pocs}.

-spec securities_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
securities_cf(Ledger) ->
    SL = subledger(Ledger),
    {securities, db(Ledger), SL#sub_ledger_v1.securities}.


-spec routing_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
routing_cf(Ledger) ->
    SL = subledger(Ledger),
    {routing, db(Ledger), SL#sub_ledger_v1.routing}.

-spec subnets_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
subnets_cf(Ledger) ->
    SL = subledger(Ledger),
    {subnets, db(Ledger), SL#sub_ledger_v1.subnets}.

-spec state_channels_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
state_channels_cf(Ledger) ->
    SL = subledger(Ledger),
    {state_channels, db(Ledger), SL#sub_ledger_v1.state_channels}.

-spec h3dex_cf(ledger()) -> {atom(), rocksdb:db_handle(), rocksdb:cf_handle()}.
h3dex_cf(Ledger) ->
    SL = subledger(Ledger),
    {h3dex, db(Ledger), SL#sub_ledger_v1.h3dex}.

-spec aux_heights_cf(ledger()) -> undefined | rocksdb:cf_handle().
aux_heights_cf(Ledger) ->
    case has_aux(Ledger) of
        false -> undefined;
        true -> Ledger#ledger_v1.aux#aux_ledger_v1.aux_heights
    end.

-spec aux_heights_md_cf(ledger()) -> undefined | rocksdb:cf_handle().
aux_heights_md_cf(Ledger) ->
    case has_aux(Ledger) of
        false -> undefined;
        true -> Ledger#ledger_v1.aux#aux_ledger_v1.aux_heights_md
    end.

-spec aux_heights_diff_cf(ledger()) -> undefined | rocksdb:cf_handle().
aux_heights_diff_cf(Ledger) ->
    case has_aux(Ledger) of
        false -> undefined;
        true -> Ledger#ledger_v1.aux#aux_ledger_v1.aux_heights_diff
    end.

-spec aux_heights_diffsum_cf(ledger()) -> undefined | rocksdb:cf_handle().
aux_heights_diffsum_cf(Ledger) ->
    case has_aux(Ledger) of
        false -> undefined;
        true -> Ledger#ledger_v1.aux#aux_ledger_v1.aux_heights_diffsum
    end.

-spec aux_db(ledger()) -> undefined | rocksdb:db_handle().
aux_db(Ledger) ->
    case has_aux(Ledger) of
        false -> undefined;
        true -> Ledger#ledger_v1.aux#aux_ledger_v1.db
    end.

-spec validators_cf(ledger()) -> rocksdb:cf_handle().
validators_cf(Ledger) ->
    SL = subledger(Ledger),
    {validators, db(Ledger), SL#sub_ledger_v1.validators}.

-spec cache_put(ledger(), rocksdb:cf_handle(), binary(), binary()) -> ok.
cache_put(Ledger, {Name, _DB, _CF}, Key, Value) ->
    case context_cache(Ledger) of
        {direct, _GwCache} ->
            rocksdb:put(db(Ledger), _CF, Key, Value, [{disable_wal, true}, {sync, false}]);
        {Cache, _GwCache} ->
            true = ets:insert(Cache, {{Name, Key}, Value})
    end,
    ok.

-spec cache_get(ledger(), rocksdb:cf_handle(), any(), [any()]) -> {ok, any()} | {error, any()} | not_found.
cache_get(Ledger, {Name, DB, CF}, Key, Options) ->
    case context_cache(Ledger) of
        {C, _GwCache} when C == undefined; C == direct ->
            rocksdb:get(DB, CF, Key, maybe_use_snapshot(Ledger, Options));
        {Cache, _GwCache} ->
            %% don't do anything smart here with the cache yet,
            %% otherwise the semantics get all confused.
            case ets:lookup(Cache, {Name, Key}) of
                [] ->
                    case rocksdb:get(DB, CF, Key, maybe_use_snapshot(Ledger, Options)) of
                        {ok, Value} ->
                            %% check if we should cache this in the context.
                            %% Currently 4 things are cached:
                            %% * Chain Vars
                            %% * Var Nonce
                            %% * Ledger Height
                            %% * the hex list for targeting
                            case {Name, Key} of
                                {default, ?hex_list} ->
                                    catch ets:insert(Cache, {{Name, Key}, {'__cached', Value}});
                                {default, ?CURRENT_HEIGHT} ->
                                    catch ets:insert(Cache, {{Name, Key}, {'__cached', Value}});
                                {default, ?VARS_NONCE} ->
                                    catch ets:insert(Cache, {{Name, Key}, {'__cached', Value}});
                                {default, <<"$var_", _/binary>>} ->
                                    catch ets:insert(Cache, {{Name, Key}, {'__cached', Value}});
                                _ ->
                                    ok
                            end,
                            {ok, Value};
                        Other ->
                            Other
                    end;
                [{_, ?CACHE_TOMBSTONE}] ->
                    %% deleted in the cache
                    not_found;
                [{_, {'__cached', Value}}] ->
                    {ok, Value};
                [{_, Value}] ->
                    {ok, Value}
            end
    end.

-spec cache_delete(ledger(), rocksdb:cf_handle(), binary()) -> ok.
cache_delete(Ledger, {Name, _DB, _CF}, Key) ->
    %% TODO: check if we're a gateway and delete that cache too, but
    %% we never delete gateways now
    case context_cache(Ledger) of
        {direct, _GWCache} ->
            rocksdb:delete(db(Ledger), _CF, Key, []);
        {Cache, _GwCache} ->
            true = ets:insert(Cache, {{Name, Key}, ?CACHE_TOMBSTONE})
    end,
    ok.

-spec cache_fold(Ledger :: ledger(),
                 CF :: rocksdb:cf_handle(),
                 Fun0 :: fun(({Key::binary(), Value::binary()}, Acc::any()) -> NewAcc::any()),
                 OriginalAcc :: any()) -> FinalAcc::any().
cache_fold(Ledger, CF, Fun0, OriginalAcc) ->
    cache_fold(Ledger, CF, Fun0, OriginalAcc, []).

cache_fold(Ledger, {CFName, DB, CF}, Fun0, OriginalAcc, Opts) ->
    Start0 = proplists:get_value(start, Opts, first),
    Start =
        case Start0 of
            {seek, Val} ->
                Val;
            _ ->
                Start0
        end,
    End = proplists:get_value(iterate_upper_bound, Opts, undefined),
    case context_cache(Ledger) of
        {C, _} when C == undefined; C == direct ->
            %% fold rocks directly
            rocks_fold(Ledger, DB, CF, Opts, Fun0, OriginalAcc);
        {Cache, _GwCache} ->
            %% fold using the cache wrapper
            Fun = mk_cache_fold_fun(Cache, CFName, Start, End, Fun0),
            Keys = lists:sort(ets:select(Cache, [{{{'$1','$2'},'_'},[{'==','$1', CFName}],['$2']}])),
            {TrailingKeys, Res0} = rocks_fold(Ledger, DB, CF, Opts, Fun, {Keys, OriginalAcc}),
            process_fun(TrailingKeys, Cache, CFName, Start, End, Fun0, Res0)
    end.

rocks_fold(Ledger, DB, CF, Opts0, Fun, Acc) ->
    Start = proplists:get_value(start, Opts0, first),
    Opts = proplists:delete(start, Opts0),
    {ok, Itr} = rocksdb:iterator(DB, CF, maybe_use_snapshot(Ledger, Opts)),
    Init = rocksdb:iterator_move(Itr, Start),
    Loop = fun L({error, invalid_iterator}, A) ->
                   A;
               L({error, _}, _A) ->
                   throw(iterator_error);
               L({ok, K} , A) ->
                   L(rocksdb:iterator_move(Itr, next),
                     Fun(K, A));
               L({ok, K, V}, A) ->
                   L(rocksdb:iterator_move(Itr, next),
                     Fun({K, V}, A))
           end,
    try
        Loop(Init, Acc)
    %% catch _:_ ->
    %%         Acc
    after
        catch rocksdb:iterator_close(Itr)
    end.

mk_cache_fold_fun(Cache, CF, Start, End, Fun) ->
    %% we want to preserve rocksdb order, but we assume it's normal lexiographic order
    fun ({Key, Value}, {CacheKeys, Acc0}) ->
            {NewCacheKeys, Acc} = process_cache_only_keys(CacheKeys, Cache, CF, Key,
                                                          Start, End,
                                                          Fun, Acc0),
            case ets:lookup(Cache, {CF, Key}) of
                [{_, ?CACHE_TOMBSTONE}] ->
                    {NewCacheKeys, Acc};
                [{_, {'__cached', CacheValue}}] ->
                    {NewCacheKeys, Fun({Key, CacheValue}, Acc)};
                [{_, CacheValue}] ->
                    {NewCacheKeys, Fun({Key, CacheValue}, Acc)};
                [] when Value /= cacheonly ->
                    {NewCacheKeys, Fun({Key, Value}, Acc)};
                [] ->
                    {NewCacheKeys, Acc}
            end
    end.

process_cache_only_keys(CacheKeys, Cache, CF, Key,
                        Start, End,
                        Fun, Acc) ->
    case lists:splitwith(fun(E) -> E < Key end, CacheKeys) of
        {ToProcess, [Key|Remaining]} -> ok;
        {ToProcess, Remaining} -> ok
    end,
    {Remaining, process_fun(ToProcess, Cache, CF, Start, End, Fun, Acc)}.

process_fun(ToProcess, Cache, CF,
            Start, End,
            Fun, Acc) ->
    lists:foldl(
      fun(K, A) when Start /= first andalso K < Start ->
              A;
         (K, A) when End /= undefined andalso K >= End ->
              A;
         (K, A) ->
              case ets:lookup(Cache, {CF, K}) of
                  [{_, ?CACHE_TOMBSTONE}] ->
                      A;
                  [{_Key, {'__cached', CacheValue}}] ->
                      Fun({K, CacheValue}, A);
                  [{_Key, CacheValue}] ->
                      Fun({K, CacheValue}, A);
                  [] ->
                      A
              end
      end, Acc, ToProcess).

-spec open_db(Mode :: mode(),
              Dir :: file:filename_all(),
              HasDelayed :: boolean(), ReadOnly :: boolean(), Options :: rocksdb:cf_options()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(active, Dir, true, ReadOnly, Options) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    DBOptions = lists:keymerge(1, lists:ukeysort(1, Options), lists:ukeysort(1, [{create_if_missing, true}, {atomic_flush, true}])),
    DefaultCFs = default_cfs() ++ delayed_cfs(),
    open_db_(DBDir, DBOptions, DefaultCFs, Options, ReadOnly, false);
open_db(aux, Dir, false, ReadOnly, Options) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    DBOptions = lists:keymerge(1, lists:ukeysort(1, Options), lists:ukeysort(1, [{create_if_missing, true}, {atomic_flush, true}])),
    DefaultCFs = default_cfs() ++ aux_cfs(),
    open_db_(DBDir, DBOptions, DefaultCFs, Options, ReadOnly, false);
open_db(active, _Dir, false, _, _) ->
    {error, not_opening_active_without_delayed};
open_db(aux, _Dir, true, _, _) ->
    {error, not_opening_aux_with_delayed}.

open_db_(DBDir, DBOptions, DefaultCFs, CFOpts, ReadOnly, Retry) ->
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

     OpenResult = case ReadOnly of
                              true ->
                                  rocksdb:open_with_cf_readonly(DBDir, DBOptions,  [{CF, CFOpts} || CF <- ExistingCFs]);
                              false ->
                                  rocksdb:open_with_cf(DBDir, DBOptions,  [{CF, CFOpts} || CF <- ExistingCFs])
                          end,
     case OpenResult of
         {error, {db_open,"Corruption:" ++ Reason}} when Retry == false ->
             lager:warning("deleting corrupted ledger: ~p", [Reason]),
             BaseDir = filename:dirname(DBDir),
             rocksdb:destroy(DBDir, []),
             clean_checkpoints(BaseDir),
             open_db_(DBDir, DBOptions, DefaultCFs, CFOpts, ReadOnly, true);
         {error, Reason} ->
             error(Reason);
         {ok, DB, OpenedCFs} ->
             L1 = lists:zip(ExistingCFs, OpenedCFs),
             L2 = lists:map(
                    fun(CF) ->
                            case ReadOnly of
                                true ->
                                    {CF, undefined};
                                false ->
                                    {ok, CF1} = rocksdb:create_column_family(DB, CF, CFOpts),
                                    {CF, CF1}
                            end
                    end,
                    DefaultCFs -- ExistingCFs
                   ),
             L3 = L1 ++ L2,
             {ok, DB, [proplists:get_value(X, L3) || X <- DefaultCFs]}
     end.

-spec default_cfs() -> list().
default_cfs() ->
    ["default", "active_gateways", "entries", "dc_entries", "htlcs",
     "pocs", "securities", "routing", "subnets", "state_channels",
     "h3dex", "gw_denorm", "validators"].

-spec delayed_cfs() -> list().
delayed_cfs() ->
    ["delayed_" ++ I || I <- default_cfs()].

-spec aux_cfs() -> list().
aux_cfs() ->
    ["aux_heights", "aux_heights_md", "aux_heights_diff", "aux_heights_diffsum"].

-spec maybe_use_snapshot(ledger(), list()) -> list().
maybe_use_snapshot(#ledger_v1{snapshot=Snapshot}, Options) ->
    case Snapshot of
        undefined ->
            Options;
        S ->
            [{snapshot, S} | Options]
    end.

-spec set_hexes(HexMap :: hexmap(), Ledger :: ledger()) -> ok | {error, any()}.
set_hexes(HexMap, Ledger) ->
    HexList = maps:to_list(HexMap),
    set_hexes_list(HexList, Ledger).

-spec set_hexes_list(HexList :: hexlist(), Ledger :: ledger()) -> ok | {error, any()}.
set_hexes_list(HexList, Ledger) ->
    L = lists:sort(HexList),
    CF = default_cf(Ledger),
    cache_put(Ledger, CF, ?hex_list, term_to_binary(L, [compressed])).

-spec get_hexes(Ledger :: ledger()) -> {ok, hexmap()} | {error, any()}.
get_hexes(Ledger) ->
    CF = default_cf(Ledger),
    case cache_get(Ledger, CF, ?hex_list, []) of
        {ok, BinList} ->
            {ok, maps:from_list(binary_to_term(BinList))};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec get_hexes_list(Ledger :: ledger()) -> {ok, []} | {error, any()}.
get_hexes_list(Ledger) ->
    CF = default_cf(Ledger),
    case cache_get(Ledger, CF, ?hex_list, []) of
        {ok, BinList} ->
            {ok, binary_to_term(BinList)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec set_hex(Hex :: h3:h3_index(),
              GwPubkeyBins :: [libp2p_crypto:pubkey_bin()],
              Ledger :: ledger()) -> ok | {error, any()}.
set_hex(Hex, GwPubkeyBins, Ledger) ->
    L = lists:sort(GwPubkeyBins),
    CF = default_cf(Ledger),
    cache_put(Ledger, CF, hex_name(Hex), term_to_binary(L, [compressed])).

-spec get_hex(Hex :: h3:h3_index(), Ledger :: ledger()) -> {ok, term()} | {error, any()}.
get_hex(Hex, Ledger) ->
    CF = default_cf(Ledger),
    case cache_get(Ledger, CF, hex_name(Hex), []) of
        {ok, BinList} ->
            {ok, binary_to_term(BinList)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec delete_hex(Hex :: h3:h3_index(), Ledger :: ledger()) -> ok | {error, any()}.
delete_hex(Hex, Ledger) ->
    CF = default_cf(Ledger),
    cache_delete(Ledger, CF, hex_name(Hex)).

hex_name(Hex) ->
    <<?hex_prefix, (integer_to_binary(Hex))/binary>>.


add_to_hex(Hex, Gateway, Ledger) ->
    Hexes = case get_hexes(Ledger) of
                {ok, Hs} ->
                    Hs;
                {error, not_found} ->
                    #{}
            end,
    Hexes1 = maps:update_with(Hex, fun(X) -> X + 1 end, 1, Hexes),
    ok = set_hexes(Hexes1, Ledger),

    case get_hex(Hex, Ledger) of
        {ok, OldAddrs} ->
            ok = set_hex(Hex, [Gateway | OldAddrs], Ledger);
        {error, not_found} ->
            ok = set_hex(Hex, [Gateway], Ledger)
    end.

remove_from_hex(Hex, Gateway, Ledger) ->
    {ok, Hexes} = get_hexes(Ledger),
    Hexes1 =
        case maps:get(Hex, Hexes) of
            1 ->
                ok = delete_hex(Hex, Ledger),
                maps:remove(Hex, Hexes);
            N ->
                {ok, OldAddrs} = get_hex(Hex, Ledger),
                ok = set_hex(Hex, lists:delete(Gateway, OldAddrs), Ledger),
                Hexes#{Hex => N - 1}
        end,
    ok = set_hexes(Hexes1, Ledger).

clean_all_hexes(Ledger) ->
    CF1 = default_cf(Ledger),
    case get_hexes(Ledger) of
        {ok, Hexes} ->
            L1 = new_context(Ledger),
            cache_delete(L1, CF1, ?hex_list),
            %% undo the upgrade marker, too, so we automatically re-upgrade
            %% next restart
            cache_delete(L1, CF1, <<"hex_targets">>),
            maps:map(fun(Hex, _) ->
                             cache_delete(L1, CF1, hex_name(Hex))
                     end, Hexes),
            commit_context(L1);
        _ -> ok
    end,

    DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
    CF2 = default_cf(DelayedLedger),
    L2 = new_context(DelayedLedger),
    case get_hexes(DelayedLedger) of
        {ok, Hexes2} ->
            cache_delete(L2, CF2, ?hex_list),
            maps:map(fun(Hex, _) ->
                             cache_delete(L2, CF2, hex_name(Hex))
                     end, Hexes2),
            commit_context(L2);
        _ -> ok
    end.

-spec bootstrap_h3dex(ledger()) -> ok.
bootstrap_h3dex(Ledger) ->
    ok = delete_h3dex(Ledger),
    AGwsCF = active_gateways_cf(Ledger),
    H3Dex = cache_fold(
              Ledger,
              AGwsCF,
              fun({GwAddr, Binary}, Acc) ->
                      Gw = blockchain_ledger_gateway_v2:deserialize(Binary),
                      case blockchain_ledger_gateway_v2:location(Gw) of
                          undefined ->
                              Acc;
                          Location ->
                              maps:update_with(Location, fun(V) -> [GwAddr | V] end, [GwAddr], Acc)
                      end
              end,
              #{}),
    set_h3dex(H3Dex, Ledger).

-spec set_h3dex(h3dex(), ledger()) -> ok.
set_h3dex(H3Dex, Ledger) ->
    H3CF = h3dex_cf(Ledger),
    _ = maps:map(fun(Loc, Gateways) ->
                         BinLoc = h3_to_key(Loc),
                         BinGWs = term_to_binary(lists:sort(Gateways), [compressed]),
                         cache_put(Ledger, H3CF, BinLoc, BinGWs)
                 end, H3Dex),
    ok.

-spec get_h3dex(ledger()) -> h3dex().
get_h3dex(Ledger) ->
    H3CF = h3dex_cf(Ledger),
    Res = cache_fold(Ledger, H3CF,
                     fun({Key, GWs}, Acc) ->
                             maps:put(key_to_h3(Key), binary_to_term(GWs), Acc)
                     end, #{}, []),
    Res.

-spec delete_h3dex(ledger()) -> ok.
delete_h3dex(Ledger) ->
    H3CF = h3dex_cf(Ledger),
    _ = maps:map(fun(H3Index, _) ->
                         cache_delete(Ledger, H3CF, h3_to_key(H3Index))
                 end, get_h3dex(Ledger)),
    ok.

-spec lookup_gateways_from_hex(Hex :: [non_neg_integer()] | non_neg_integer(),
                               Ledger :: ledger()) -> Results :: h3dex().
%% @doc Given a hex find candidate gateways in the span to the next adjacent
%% hex. N.B. May return an empty map.
lookup_gateways_from_hex(Hexes, Ledger) when is_list(Hexes) ->
    lists:foldl(fun(Hex, Acc) ->
                        maps:merge(Acc, lookup_gateways_from_hex(Hex, Ledger))
                end, #{}, Hexes);
lookup_gateways_from_hex(Hex, Ledger) when is_integer(Hex) ->
    H3CF = h3dex_cf(Ledger),
    cache_fold(Ledger, H3CF,
               fun({Key, GWs}, Acc) ->
                       maps:put(key_to_h3(Key), binary_to_term(GWs), Acc)
               end, #{}, [
                          {start, {seek, find_lower_bound_hex(Hex)}},
                          {iterate_upper_bound, increment_bin(h3_to_key(Hex))}
                         ]
              ).

-spec find_lower_bound_hex(Hex :: non_neg_integer()) -> binary().
%% @doc Let's find the nearest set of k neighbors for this hex at the
%% same resolution and return the "lowest" one. Since these numbers
%% are actually packed binaries, we will destructure them to sort better
%% lexically.
find_lower_bound_hex(Hex) ->
    %% both reserved fields must be 0 and Mode must be 1 for this to be a h3 cell
    <<0:1, 1:4/integer-unsigned-big, 0:3, Resolution:4/integer-unsigned-big, BaseCell:7/integer-unsigned-big, Digits/bitstring>> = <<Hex:64/integer-unsigned-big>>,
    ActualDigitCount = Resolution * 3,
    %% pull out the actual digits used and dump the rest
    <<ActualDigits:ActualDigitCount/integer-unsigned-big, _/bitstring>> = Digits,
    Padding = 45 - ActualDigitCount,
    %% store the resolution inverted (15 - 15) = 0 so it sorts earlier
    %% pad the actual digits used with 0s on the end
    <<BaseCell:7/integer-unsigned-big, ActualDigits:ActualDigitCount/integer-unsigned-big, 0:Padding, 0:4/integer-unsigned-big>>.

h3_to_key(H3) ->
    %% both reserved fields must be 0 and Mode must be 1 for this to be a h3 cell
    <<0:1/integer-unsigned-big, 1:4/integer-unsigned-big, 0:3/integer-unsigned-big, Resolution:4/integer-unsigned-big, BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big>> = <<H3:64/integer-unsigned-big>>,
    %% store the resolution inverted (15 - Resolution) so it sorts later
    <<BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big, (15 - Resolution):4/integer-unsigned-big>>.

key_to_h3(Key) ->
    <<BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big, InverseResolution:4/integer-unsigned-big>> = Key,
    <<H3:64/integer-unsigned-big>> = <<0:1, 1:4/integer-unsigned-big, 0:3, (15 - InverseResolution):4/integer-unsigned-big, BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big>>,
    H3.


-spec add_gw_to_hex(Hex :: non_neg_integer(),
                    GWAddr :: libp2p_crypto:pubkey_bin(),
                    Ledger :: ledger()) -> ok | {error, any()}.
%% @doc During an assert, this function will add a gateway address to a hex
add_gw_to_hex(Hex, GWAddr, Ledger) ->
    H3CF = h3dex_cf(Ledger),
    BinHex = h3_to_key(Hex),
    case cache_get(Ledger, H3CF, BinHex, []) of
        not_found ->
            cache_put(Ledger, H3CF, BinHex, term_to_binary([GWAddr], [compressed]));
        {ok, BinGws} ->
            GWs = binary_to_term(BinGws),
            cache_put(Ledger, H3CF, BinHex, term_to_binary(lists:sort([GWAddr | GWs]), [compressed]));
        Error -> Error
    end.

-spec remove_gw_from_hex(Hex :: non_neg_integer(),
                         GWAddr :: libp2p_crypto:pubkey_bin(),
                         Ledger :: ledger()) -> ok | {error, any()}.
%% @doc During an assert, if a gateway already had an asserted location
%% (and has been reasserted), this function will remove a gateway
%% address from a hex
remove_gw_from_hex(Hex, GWAddr, Ledger) ->
    H3CF = h3dex_cf(Ledger),
    BinHex = h3_to_key(Hex),
    case cache_get(Ledger, H3CF, BinHex, []) of
        not_found -> ok;
        {ok, BinGws} ->
            case lists:delete(GWAddr, binary_to_term(BinGws)) of
                [] ->
                    cache_delete(Ledger, H3CF, BinHex);
                NewGWs ->
                    cache_put(Ledger, H3CF, BinHex, term_to_binary(lists:sort(NewGWs), [compressed]))
            end;
        Error -> Error
    end.

-spec bootstrap_gw_denorm(ledger()) -> ok.
bootstrap_gw_denorm(Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    cache_fold(
      Ledger,
      AGwsCF,
      fun({GwAddr, Binary}, _) ->
              Gw = blockchain_ledger_gateway_v2:deserialize(Binary),
              write_gw_denorm_values(GwAddr, new, Gw, Ledger)
      end,
      ignore).

%% do not call this except via the blockchain_worker wrapper
add_commit_hook(CF, HookIncFun, HookEndFun, Ledger) ->
    add_commit_hook(CF, HookIncFun, HookEndFun, undefined, Ledger).

add_commit_hook(CF, HookIncFun, HookEndFun, Pred, #ledger_v1{commit_hooks = Hooks} = Ledger) ->
    Ref = make_ref(),
    NewHook = #hook{cf = CF,
                    predicate = Pred,
                    hook_inc_fun = HookIncFun,
                    hook_end_fun = HookEndFun,
                    ref = Ref},
    Hooks1 = [NewHook | Hooks],
    {Ref, Ledger#ledger_v1{commit_hooks = Hooks1}}.

%% do not call this either except via the blockchain_worker wrapper
remove_commit_hook(Ref, #ledger_v1{commit_hooks = Hooks} = Ledger) when is_reference(Ref) ->
    Hooks1 = lists:keydelete(Ref, #hook.ref, Hooks),
    Ledger#ledger_v1{commit_hooks = Hooks1};
remove_commit_hook(Atom, #ledger_v1{commit_hooks = Hooks} = Ledger) when is_atom(Atom) ->
    Hooks1 = lists:keydelete(Atom, #hook.cf, Hooks),
    Ledger#ledger_v1{commit_hooks = Hooks1}.

-spec add_validator(ValidatorAddress :: libp2p_crypto:pubkey_bin(),
                    OwnerAddress :: libp2p_crypto:pubkey_bin(),
                    Stake :: pos_integer(),
                    Ledger :: ledger()) -> ok | {error, validator_already_added}.
add_validator(Address,
              OwnerAddr,
              Stake,
              Ledger) ->
    case ?MODULE:get_validator(Address, Ledger) of
        {ok, _} ->
            {error, validator_already_added};
        _ ->
            Val = blockchain_ledger_validator_v1:new(Address, OwnerAddr, Stake),
            update_validator(Address, Val, Ledger)
    end.

-spec update_validator(Addr :: libp2p_crypto:pubkey_bin(),
                       Gw :: blockchain_ledger_validator_v1:validator(),
                       Ledger :: ledger()) -> ok | {error, _}.
update_validator(Addr, Val, Ledger) ->
    Bin = blockchain_ledger_validator_v1:serialize(Val),
    ValsCF = validators_cf(Ledger),
    cache_put(Ledger, ValsCF, Addr, Bin).

-spec get_validator(libp2p_crypto:pubkey_bin(), ledger()) ->
          {ok, blockchain_ledger_validator_v1:validator()}
              | {error, any()}.
get_validator(Address, Ledger) ->
    ValsCF = validators_cf(Ledger),
    case cache_get(Ledger, ValsCF, Address, []) of
        {ok, BinVal} ->
            {ok, blockchain_ledger_validator_v1:deserialize(BinVal)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec deactivate_validator(libp2p_crypto:pubkey_bin(), pos_integer(), ledger()) ->
          ok | {error, any()}.
deactivate_validator(Address, StakeReleaseHeight, Ledger) ->
    case get_validator(Address, Ledger) of
        {ok, Val} ->
            Stake = blockchain_ledger_validator_v1:stake(Val),
            Owner = blockchain_ledger_validator_v1:owner_address(Val),

            %% set status to cooldown
            Val1 = blockchain_ledger_validator_v1:status(cooldown, Val),
            Val2 = blockchain_ledger_validator_v1:release_height(StakeReleaseHeight, Val1),
            %% put the stake HNT into cooldown
            ok = cooldown_stake(Owner, Address, Stake, StakeReleaseHeight, Ledger),
            update_validator(Address, Val2, Ledger);
        Error -> Error
    end.

finalize_validator(Address, Ledger) ->
    case get_validator(Address, Ledger) of
        {ok, Val} ->
            %% set status to unstaked
            Val1 = blockchain_ledger_validator_v1:status(unstaked, Val),
            %% zero out the stake
            Val2 = blockchain_ledger_validator_v1:stake(0, Val1),
            update_validator(Address, Val2, Ledger);
        Error -> Error
    end.


-spec cooldown_stake(Owner :: libp2p_crypto:pubkey_bin(),
                     Validator :: libp2p_crypto:pubkey_bin(),
                     Stake :: non_neg_integer(),
                     StakeReleaseHeight :: pos_integer(),
                     Ledger :: ledger()) ->
          ok | {error, any()}.
cooldown_stake(Owner, Validator, Stake, StakeReleaseHeight, Ledger) ->
    DefaultCF = default_cf(Ledger),

    NewStakeRec = new_stake_record(Owner, Validator, Stake, StakeReleaseHeight),

    %% add a callback at stake release height block
    HeightEntry =
        case cache_get(Ledger, DefaultCF, cd_block_name(StakeReleaseHeight), []) of
            {ok, HE} ->
                binary_to_term(HE);
            not_found ->
                []
            %% just gonna function clause for now
        end,
    HeightEntry1 = [ NewStakeRec | HeightEntry ],
    cache_put(Ledger, DefaultCF, cd_block_name(StakeReleaseHeight),
              term_to_binary(HeightEntry1)).

new_stake_record(Owner, Validator, Stake, SRH) ->
    #validator_stake_v1{owner = Owner,
                        validator = Validator,
                        stake = Stake,
                        stake_release_height = SRH}.

get_cooldown_stake(Val, Ledger) ->
    Address = blockchain_ledger_validator_v1:address(Val),
    ReleaseHeight = blockchain_ledger_validator_v1:release_height(Val),

    CF = default_cf(Ledger),
    case cache_get(Ledger, CF, cd_block_name(ReleaseHeight), []) of
        {ok, StakeList} ->
            Stakes = binary_to_term(StakeList),
            case lists:keyfind(Address, #validator_stake_v1.validator, Stakes) of
                false -> {error, not_found};
                #validator_stake_v1{stake = S} -> {ok, S}
            end;
        not_found ->
            {error, not_found};
        Err -> Err
    end.

-spec query_circulating_hnt(Ledger :: ledger()) -> non_neg_integer().
query_circulating_hnt(Ledger) ->
    cache_fold(
      Ledger,
      entries_cf(Ledger),
      fun({_Addr, BinEnt}, Acc) ->
              Ent = blockchain_ledger_entry_v1:deserialize(BinEnt),
              Acc + blockchain_ledger_entry_v1:balance(Ent)
      end,
      0
     ).

-spec query_staked_hnt(Ledger :: ledger()) -> non_neg_integer().
query_staked_hnt(Ledger) ->
    query_hnt_by_status(staked, Ledger).

-spec query_cooldown_hnt(Ledger :: ledger()) -> non_neg_integer().
query_cooldown_hnt(Ledger) ->
    query_hnt_by_status(cooldown, Ledger).

fold_validators(Fun, InitAcc, Ledger) ->
    cache_fold(
      Ledger,
      validators_cf(Ledger),
      fun({_Addr, BinVal}, Acc) ->
              Val = blockchain_ledger_validator_v1:deserialize(BinVal),
              Fun(Val, Acc)
      end,
      InitAcc
     ).

query_hnt_by_status(Status, Ledger) ->
    fold_validators(fun(Val, Acc) ->
        case blockchain_ledger_validator_v1:status(Val) of
            Status ->
                Acc + blockchain_ledger_validator_v1:stake(Val);
            _ ->
                Acc
        end
    end, 0, Ledger).

batch_from_cache(ETS, #ledger_v1{commit_hooks = Hooks, mode = Mode} = Ledger) ->
    {ok, Batch} = rocksdb:batch(),
    Filters =
        case Mode of
            active ->
                lists:foldl(
                  fun(#hook{cf = C} = H, A) ->
                          K = atom_to_cf(C, Ledger),
                          maps:update_with(K, fun(L) -> [H | L] end, [H], A)
                  end,
                  #{},
                  Hooks);
            %% don't do any of this work on the delayed or aux ledgers
            _ -> #{}
        end,
    {Batch, FilteredChanges} =
        ets:foldl(fun({{CFName, Key}, ?CACHE_TOMBSTONE}, {B, Changes}) ->
                          CF = atom_to_cf(CFName, Ledger),
                          rocksdb:batch_delete(B, CF, Key),
                          Changes1 = case maps:is_key(CF, Filters) of
                                         true ->
                                             case apply_filters(CF, Filters, Key, deleted) of
                                                 true ->
                                                     [{CF, delete, Key} | Changes];
                                                 false ->
                                                     Changes
                                             end;
                                         false ->
                                             Changes
                                     end,
                          {B, Changes1};
                     ({{_CFName, _Key}, {'__cached', _Value}}, {B, Changes}) ->
                          %% cached value which by definition has not changed
                          {B, Changes};
                     ({{CFName, Key}, Value}, {B, Changes}) ->
                          CF = atom_to_cf(CFName, Ledger),
                          rocksdb:batch_put(B, CF, Key, Value),
                          Changes1 = case maps:is_key(CF, Filters) of
                                         true ->
                                             case apply_filters(CF, Filters, Key, Value) of
                                                 true ->
                                                     [{CF, put, Key, Value} | Changes];
                                                 false ->
                                                     Changes
                                             end;
                                         false ->
                                             Changes
                                     end,
                          {B, Changes1}
                  end, {Batch, []}, ETS),
    %% we don't actually want to invoke this here, but passing back the arguments is kind of clunky
    {ok, Height} = current_height(Ledger),
    {fun() -> invoke_commit_hooks(FilteredChanges, Filters, Height) end, Batch}.

apply_filters(CF, Filters, Key, Value) ->
    %% pre-tested for existence
    Preds0 = [Pred || #hook{predicate = Pred} <- maps:get(CF, Filters)],
    Preds = lists:filter(fun(V) -> V /= undefined end, Preds0),
    case Preds of
        %% if we don't define any predicates, just send all the updates
        [] ->
            true;
        _ ->
            %% note that predicates can take an atom 'delete' instead of a value
            lists:any(fun(P) -> P(Key, Value) end, Preds)
    end.

invoke_commit_hooks([] = _Changes, _Filters, _Height) ->
    %% if no changes then do nothing
    ok;
invoke_commit_hooks(Changes, Filters, Height) ->
    %% best effort async delivery
    FiltersMap = maps:fold(fun(CF, HookList, Acc) ->
                                   #hook{cf = CFAtom} = hd(HookList),
                                   Acc#{CF => CFAtom}
                          end,
                          #{},
                          Filters),
    spawn(
      fun() ->
              %% process the changes into CF groups
              Groups = lists:foldl(
                         fun(Change, Grps) ->
                                 CF = element(1, Change),
                                 Atom = maps:get(CF, FiltersMap),
                                 maps:update_with(Atom, fun(L) -> [Change | L] end,
                                                  [Change], Grps)
                         end,
                         #{},
                         Changes),
              %% call each incremental hook on each group
              maps:map(
                fun(CF, HookList) ->
                        HookAtom = maps:get(CF, FiltersMap),
                        case maps:get(HookAtom, Groups, undefined) of
                            undefined ->
                                %% if there are no changes for this CF, do nothing
                                noop;
                            HookChanges ->
                                lists:foreach(
                                  fun(#hook{hook_inc_fun = HookFun, predicate = undefined, include_height = IncludeHeight}) ->
                                          case IncludeHeight of
                                              true ->
                                                HookFun(HookChanges, Height);
                                              _ ->
                                                HookFun(HookChanges)
                                          end;
                                     (#hook{hook_inc_fun = HookFun, predicate = Pred, include_height = IncludeHeight}) ->
                                          FilteredHookChanges =
                                              lists:filter(fun({_, _, K, V}) ->
                                                                   Pred(K, V)
                                                           end, HookChanges),
                                          case IncludeHeight of
                                              true ->
                                                HookFun(FilteredHookChanges, Height);
                                              _ ->
                                                HookFun(FilteredHookChanges)
                                          end
                                  end, HookList)
                        end
                end,
                Filters),

              %% accumulate a list of all CFs which have updates
              %% and for each updated CF accumulate a list of keys which have been modified
              %% call the end funs for each CF with the CF atom and the list of modified keys
              maps:map(
                fun(CF, HookList) ->
                        CFAtom = maps:get(CF, FiltersMap),
                        %% check if any changes for each CF, if true fire the end fun, it false do nothing
                        case maps:get(CFAtom, Groups, undefined) of
                            undefined ->
                                noop;
                            CFChanges ->
                                %% iterate over the changes for this group and collect the keys which have been modified
                                CFChangedKeys = [{Action, Key} || {_CF, Action, Key, _Value} <- CFChanges],
                                lists:foreach(
                                  fun(#hook{hook_end_fun = HookFun, include_height = IncludeHeight}) ->
                                          %% call the end fun, pass it the updated CF and the list of modified keys
                                          case IncludeHeight of
                                              true ->
                                                %% include the ledger commit height if requested
                                                HookFun(CFAtom, CFChangedKeys, Height);
                                              _ ->
                                                HookFun(CFAtom, CFChangedKeys)
                                          end
                                  end, HookList)
                        end
                end,
                Filters)
      end).

%% @doc Increment a binary for the purposes of lexical sorting
-spec increment_bin(binary()) -> binary().
increment_bin(Binary) ->
    Size = byte_size(Binary) * 8,
    <<BinAsInt:Size/integer-unsigned-big>> = Binary,
    BitsNeeded0 = ceil(math:log2(BinAsInt+2)),
    BitsNeeded = case BitsNeeded0 rem 8 of
                     0 -> BitsNeeded0;
                     N ->
                         BitsNeeded0 + (8 - N)
                 end,
    NewSize = max(Size, BitsNeeded),
    <<(BinAsInt+1):NewSize/integer-unsigned-big>>.

subnet_lookup(Itr, DevAddr, {ok, <<Base:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>, <<Dest:32/integer-unsigned-little>>}) ->
    case (DevAddr band (Mask bsl 2)) == Base of
        true ->
            Dest;
        false ->
            subnet_lookup(Itr, DevAddr, rocksdb:iterator_move(Itr, prev))
    end;
subnet_lookup(_, _, _) ->
    error.

%% extract and load section for snapshots.  note that for determinism
%% reasons, we need to not use maps, but sorted lists

-spec snapshot_vars(ledger()) -> [{binary(), binary()}].
snapshot_vars(Ledger) ->
    CF = default_cf(Ledger),
    lists:sort(
      maps:to_list(
        cache_fold(
          Ledger, CF,
          fun({<<"$var_", Name/binary>>, BValue}, Acc) ->
                  Value = binary_to_term(BValue),
                  maps:put(Name, Value, Acc)
          end, #{},
          [{start, {seek, <<"$var_">>}},
           {iterate_upper_bound, <<"$var`">>}]))).

load_vars(Vars, Ledger) ->
    vars(maps:from_list(Vars), [], Ledger),
    ok.

-spec snapshot_delayed_vars(ledger()) ->
    [{integer(), [{TODO :: term(), TODO :: term()}]}].
snapshot_delayed_vars(Ledger) ->
    CF = default_cf(Ledger),
    {ok, Height} = current_height(Ledger),
    lists:sort(
      maps:to_list(
        cache_fold(
          Ledger, CF,
          fun({<<"$block_", HashHeightBin/binary>>, BP}, Acc) ->
                  %% there is a long standing bug not deleting
                  %% these lists once processed, just fixed as
                  %% this code was written, so we need to ignore
                  %% old ones till we're past all of this
                  HashHeight = binary_to_integer(HashHeightBin),
                  case HashHeight >= Height of
                      true ->
                          Hashes = binary_to_term(BP),
                          Val = lists:sort(
                                  lists:map(
                                    fun(Hash) ->
                                            {ok, Bin} = cache_get(Ledger, CF, Hash, []),
                                            {Hash, binary_to_term(Bin)}
                                    end,
                                    Hashes)),
                          maps:put(HashHeight, Val, Acc);
                      false ->
                          Acc
                  end
          end, #{},
          %% we could iterate from the correct block if I had
          %% encoded the blocks correctly, but I didn't
          [{start, {seek, <<"$block_">>}},
           {iterate_upper_bound, <<"$block`">>}]))).

load_delayed_vars(DVars, Ledger) ->
    CF = default_cf(Ledger),
    maps:map(
      fun(Height, HashesAndVars) ->
              {Hashes, _Vars} = lists:unzip(HashesAndVars),
              BHashes = term_to_binary(Hashes),
              ok = cache_put(Ledger, CF, block_name(Height), BHashes),
              lists:foreach(
                fun({Hash, Vars}) ->
                        cache_put(Ledger, CF, Hash, term_to_binary(Vars))
                end, HashesAndVars)
      end, maps:from_list(DVars)),
    ok.

-spec snapshot_delayed_hnt(ledger()) -> [{binary(), binary()}].
snapshot_delayed_hnt(Ledger) ->
    CF = default_cf(Ledger),
    %% grab everything related to looking up HNT during cooldown
    lists:reverse(
      lists:append(
        %% height entries
        cache_fold(
          Ledger, CF,
          fun(KV, Acc) ->
                  [KV | Acc]
          end,
          [],
          [{start, {seek, <<"$cd_block_">>}},
           {iterate_upper_bound, <<"$cd_block`">>}]),
        %% owner entries
        cache_fold(
          Ledger, CF,
          fun(KV, Acc) ->
                  [KV | Acc]
          end,
          [],
          [{start, {seek, <<"$owner_">>}},
           {iterate_upper_bound, <<"$owner`">>}]))).

load_delayed_hnt(DHNT, Ledger) ->
    CF = default_cf(Ledger),
    load_raw(DHNT, CF, Ledger).

-spec snapshot_threshold_txns(ledger()) -> [{binary(), binary()}].
snapshot_threshold_txns(Ledger) ->
    CF = default_cf(Ledger),
    lists:sort(scan_threshold_txns(Ledger, CF)).

load_threshold_txns(Txns, Ledger) ->
    lists:map(fun(T) -> save_threshold_txn(T, Ledger) end, Txns),
    ok.

-spec snapshot_pocs(ledger()) -> [{binary(), binary()}].
snapshot_pocs(Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    lists:sort(
      maps:to_list(
        cache_fold(
          Ledger, PoCsCF,
          fun({OnionKeyHash, BValue}, Acc) ->
                  List = binary_to_term(BValue),
                  Value = lists:map(fun blockchain_ledger_poc_v2:deserialize/1, List),
                  maps:put(OnionKeyHash, Value, Acc)
          end, #{},
          []))).

load_pocs(PoCs, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    maps:map(
      fun(OnionHash, P) ->
              BPoC = term_to_binary(lists:map(fun blockchain_ledger_poc_v2:serialize/1, P)),
              cache_put(Ledger, PoCsCF, OnionHash, BPoC)
      end,
      maps:from_list(PoCs)),
    ok.

-spec snapshot_raw(CFSpec, ledger()) ->
    [{binary(), binary()}] when
    CFSpec ::
        {
            CFName :: atom(),
            DB :: rocksdb:db_handle(),
            CF :: rocksdb:cf_handle()
        }.
snapshot_raw(CF, L) ->
    %% XXX Preserve order to ensure that snapshots hash deterministically.
    %% Since rocks folds are lexicographic - we can just reverse:
    lists:reverse(cache_fold(L, CF, fun({_, _}=KV, KVs) -> [KV | KVs] end, [])).

-spec load_raw([{binary(), binary()}] | function(), rocksdb:cf_handle(), ledger()) -> ok.
load_raw(Iter, {Name, DB, CF}, Ledger) when is_function(Iter, 0) ->
    case Iter() of
        {K, V, NewIter} ->
            rocksdb:put(DB, CF, K, V, []),
            load_raw(NewIter, {Name, DB, CF}, Ledger);
        ok ->
            ok
    end;
load_raw(KVL, {_Name, DB, CF}, _Ledger) ->
    %% you can probably make this much larger on larger machines
    BatchSize = application:get_env(blockchain, snapshot_load_batch_size, 100),
    {ok, Batch0} = rocksdb:batch(),
    FinalBatch = lists:foldl(fun({K, V}, Batch) ->
                        rocksdb:batch_put(Batch, CF, K, V),
                        case rocksdb:batch_count(Batch) > BatchSize of
                            true ->
                                rocksdb:write_batch(DB, Batch, []),
                                {ok, NewBatch} = rocksdb:batch(),
                                NewBatch;
                            false ->
                                Batch
                        end
                end, Batch0, KVL),
    rocksdb:write_batch(DB, FinalBatch, []),
    ok.

-spec snapshot_raw_pocs(ledger()) -> [{binary(), binary()}].
snapshot_raw_pocs(Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    snapshot_raw(PoCsCF, Ledger).

load_raw_pocs(PoCs, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    load_raw(PoCs, PoCsCF, Ledger).

-spec snapshot_accounts(ledger()) -> [{binary(), binary()}].
snapshot_accounts(Ledger) ->
    lists:sort(maps:to_list(entries(Ledger))).

load_accounts(Accounts, Ledger) ->
    EntriesCF = entries_cf(Ledger),
    maps:map(
      fun(Address, Entry) ->
              BEntry = blockchain_ledger_entry_v1:serialize(Entry),
              cache_put(Ledger, EntriesCF, Address, BEntry)
      end,
      maps:from_list(Accounts)),
    ok.

-spec snapshot_raw_accounts(ledger()) -> [{binary(), binary()}].
snapshot_raw_accounts(Ledger) ->
    EntriesCF = entries_cf(Ledger),
    snapshot_raw(EntriesCF, Ledger).

load_raw_accounts(Accounts, Ledger) ->
    EntriesCF = entries_cf(Ledger),
    load_raw(Accounts, EntriesCF, Ledger).

-spec snapshot_dc_accounts(ledger()) -> [{binary(), binary()}].
snapshot_dc_accounts(Ledger) ->
    lists:sort(maps:to_list(dc_entries(Ledger))).

load_dc_accounts(DCAccounts, Ledger) ->
    EntriesCF = dc_entries_cf(Ledger),
    maps:map(
      fun(Address, Entry) ->
              BEntry = blockchain_ledger_data_credits_entry_v1:serialize(Entry),
              cache_put(Ledger, EntriesCF, Address, BEntry)
      end,
      maps:from_list(DCAccounts)),
    ok.

-spec snapshot_raw_dc_accounts(ledger()) -> [{binary(), binary()}].
snapshot_raw_dc_accounts(Ledger) ->
    EntriesCF = dc_entries_cf(Ledger),
    snapshot_raw(EntriesCF, Ledger).

load_raw_dc_accounts(Accounts, Ledger) ->
    EntriesCF = dc_entries_cf(Ledger),
    load_raw(Accounts, EntriesCF, Ledger).

-spec snapshot_security_accounts(ledger()) -> [{binary(), binary()}].
snapshot_security_accounts(Ledger) ->
    lists:sort(maps:to_list(securities(Ledger))).

load_security_accounts(SecAccounts, Ledger) ->
    EntriesCF = securities_cf(Ledger),
    maps:map(
      fun(Address, Entry) ->
              BEntry = blockchain_ledger_security_entry_v1:serialize(Entry),
              cache_put(Ledger, EntriesCF, Address, BEntry)
      end,
      maps:from_list(SecAccounts)),
    ok.

-spec snapshot_raw_security_accounts(ledger()) -> [{binary(), binary()}].
snapshot_raw_security_accounts(Ledger) ->
    EntriesCF = securities_cf(Ledger),
    snapshot_raw(EntriesCF, Ledger).

load_raw_security_accounts(Accounts, Ledger) ->
    EntriesCF = securities_cf(Ledger),
    load_raw(Accounts, EntriesCF, Ledger).

-spec snapshot_htlcs(ledger()) -> [{binary(), binary()}].
snapshot_htlcs(Ledger) ->
    lists:sort(maps:to_list(htlcs(Ledger))).

load_htlcs(HTLCs, Ledger) ->
    HTLCsCF = htlcs_cf(Ledger),
    maps:map(
      fun(Address, Entry) ->
              BEntry = blockchain_ledger_htlc_v1:serialize(Entry),
              cache_put(Ledger, HTLCsCF, Address, BEntry)
      end,
      maps:from_list(HTLCs)),
    ok.

-spec snapshot_ouis(ledger()) -> [{binary(), binary()}].
snapshot_ouis(Ledger) ->
    RoutingCF = routing_cf(Ledger),
    lists:sort(
      maps:to_list(
        cache_fold(
          Ledger, RoutingCF,
          fun({OUI0, BValue}, Acc) ->
                  <<OUI:32/integer-unsigned-big>> = OUI0,
                  Value = blockchain_ledger_routing_v1:deserialize(BValue),
                  maps:put(OUI, Value, Acc)
          end, #{},
          []))).

load_ouis(OUIs, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    maps:map(
      fun(OUI, Routing) ->
              BRouting = blockchain_ledger_routing_v1:serialize(Routing),
              cache_put(Ledger, RoutingCF, <<OUI:32/integer-unsigned-big>>, BRouting)
      end,
      maps:from_list(OUIs)),
    ok.

-spec snapshot_subnets(ledger()) -> [{binary(), binary()}].
snapshot_subnets(Ledger) ->
    SubnetsCF = subnets_cf(Ledger),
    lists:sort(
      maps:to_list(
        cache_fold(
          Ledger, SubnetsCF,
          fun({Subnet, OUI0}, Acc) ->
                  <<OUI:32/little-unsigned-integer>> = OUI0,
                  maps:put(Subnet, OUI, Acc)
          end, #{},
          []))).

load_subnets(Subnets, Ledger) ->
    SubnetsCF = subnets_cf(Ledger),
    maps:map(
      fun(Subnet, OUI) ->
              cache_put(Ledger, SubnetsCF, Subnet, <<OUI:32/little-unsigned-integer>>)
      end,
      maps:from_list(Subnets)),
    ok.

-spec snapshot_state_channels(ledger()) ->
    [{ID, SC}] when
    ID :: binary(),
    SC ::
        {blockchain_ledger_state_channel_v1, V1}
        | {blockchain_ledger_state_channel_v2, V2},
    V1 :: blockchain_ledger_state_channel_v1:state_channel(),
    V2 :: blockchain_ledger_state_channel_v2:state_channel_v2().
snapshot_state_channels(Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    lists:sort(
      maps:to_list(
        cache_fold(
          Ledger, SCsCF,
          fun({ID, V}, Acc) ->
                  %% do we need to decompose the ID here into Key and Owner?
                  {_Mod, SC} = deserialize_state_channel(V),
                  maps:put(ID, SC, Acc)
          end, #{},
          []))).

-spec load_state_channels([tuple()], ledger()) -> ok.
%% @doc Loads the cache with state channels from a snapshot
load_state_channels(SCs, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    maps:map(
      fun(ID, Channel) ->
              SCMod = get_sc_mod(Channel, Ledger),
              BChannel = SCMod:serialize(Channel),
              cache_put(Ledger, SCsCF, ID, BChannel)
      end,
      maps:from_list(SCs)),
    ok.

-spec snapshot_hexes(ledger()) -> [{non_neg_integer(), [binary()]} | {list, #{non_neg_integer() => pos_integer()}}].
snapshot_hexes(Ledger) ->
    case blockchain_ledger_v1:get_hexes_list(Ledger) of
        {ok, Hexes} ->
            lists:foldl(
              fun({HexAddr, _Ct}, Acc) ->
                      {ok, Hex} = get_hex(HexAddr, Ledger),
                      [{HexAddr, Hex} | Acc]
              end,
              [{list, Hexes}],
              Hexes);
        {error, not_found} ->
            []
    end.

load_hexes(Hexes0, Ledger) ->
    lists:foreach(fun({list, Hexes}) when is_list(Hexes) ->
                          ok = set_hexes_list(Hexes, Ledger);
                     ({list, Hexes}) when is_map(Hexes) ->
                          ok = set_hexes(Hexes, Ledger);
                     ({HexAddr, Hex}) ->
                      set_hex(HexAddr, Hex, Ledger)
                  end, Hexes0).

-spec snapshot_h3dex(ledger()) -> [{binary(), binary()}].
snapshot_h3dex(Ledger) ->
    lists:sort(
      maps:to_list(
        get_h3dex(Ledger))).

-spec load_h3dex([{binary(), binary()}], ledger()) -> ok.
load_h3dex(H3DexList, Ledger) ->
    {_Name, DB, H3CF} = h3dex_cf(Ledger),
    {ok, Batch0} = rocksdb:batch(),
    BatchSize = application:get_env(blockchain, snapshot_load_batch_size, 100),
    FinalBatch = lists:foldl(fun({Loc, Gateways}, Batch) ->
                         BinLoc = h3_to_key(Loc),
                         BinGWs = term_to_binary(lists:sort(Gateways), [compressed]),
                         rocksdb:batch_put(Batch, H3CF, BinLoc, BinGWs),
                         case rocksdb:batch_count(Batch) > BatchSize of
                             true ->
                                 rocksdb:write_batch(DB, Batch, []),
                                 {ok, NewBatch} = rocksdb:batch(),
                                 NewBatch;
                             false ->
                                 Batch
                         end
                 end, Batch0, H3DexList),

    rocksdb:write_batch(DB, FinalBatch, []),
    ok.

-spec get_sc_mod( Entry :: blockchain_ledger_state_channel_v1:state_channel() |
                           blockchain_ledger_state_channel_v2:state_channel_v2(),
                  Ledger :: ledger() ) -> blockchain_ledger_state_channel_v1
                                          | blockchain_ledger_state_channel_v2.
get_sc_mod(Channel, Ledger) ->
    case ?MODULE:config(?sc_version, Ledger) of
        {ok, 2} ->
            case blockchain_ledger_state_channel_v2:is_v2(Channel) of
                true -> blockchain_ledger_state_channel_v2;
                false -> blockchain_ledger_state_channel_v1
            end;
        _ -> blockchain_ledger_state_channel_v1
    end.

%%--------------------------------------------------------------------
%% @doc
%% This function allows us to get a lower sc_max_actors for testing
%% It should still pass any chain validation as it can only be lower
%% @end
%%--------------------------------------------------------------------
-spec get_sc_max_actors(Ledger :: ledger()) -> pos_integer().
get_sc_max_actors(Ledger) ->
    MaxActorsAllowed = blockchain_state_channel_v1:max_actors_allowed(Ledger),
    case application:get_env(blockchain, sc_max_actors, MaxActorsAllowed) of
        Str when is_list(Str) ->
            try erlang:list_to_integer(Str) of
                TooHigh when TooHigh > MaxActorsAllowed ->
                    MaxActorsAllowed;
                Max ->
                    Max
            catch What:Why ->
                lager:info("failed to convert sc_max_actors to int ~p", [{What, Why}]),
                MaxActorsAllowed
            end;
        TooHigh when TooHigh > MaxActorsAllowed ->
            MaxActorsAllowed;
        Max ->
            Max
    end.

-spec deserialize_state_channel( <<_:8, _:_*8>> ) ->
    { blockchain_ledger_state_channel_v1, blockchain_ledger_state_channel_v1:state_channel() } |
    { blockchain_ledger_state_channel_v2, blockchain_ledger_state_channel_v2:state_channel_v2() }.
deserialize_state_channel(<<1, _/binary>> = SC) ->
    {blockchain_ledger_state_channel_v1, blockchain_ledger_state_channel_v1:deserialize(SC)};
deserialize_state_channel(<<2, _/binary>> = SC) ->
    {blockchain_ledger_state_channel_v2, blockchain_ledger_state_channel_v2:deserialize(SC)}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

find_entry_test() ->
    BaseDir = test_utils:tmp_dir("find_entry_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, address_entry_not_found}, find_entry(<<"test">>, Ledger)),
    test_utils:cleanup_tmp_dir(BaseDir).

find_gateway_info_test() ->
    BaseDir = test_utils:tmp_dir("find_gateway_info_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, find_gateway_info(<<"address">>, Ledger)),
    test_utils:cleanup_tmp_dir(BaseDir).

mode_test() ->
    BaseDir = test_utils:tmp_dir("mode_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, consensus_members(Ledger)),
    Ledger1 = new_context(Ledger),
    ok = consensus_members([1, 2, 3], Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({ok, [1, 2, 3]}, consensus_members(Ledger)),
    Ledger2 = mode(delayed, Ledger1),
    Ledger3 = new_context(Ledger2),
    ?assertEqual({error, not_found}, consensus_members(Ledger3)),
    test_utils:cleanup_tmp_dir(BaseDir).

consensus_members_1_test() ->
    BaseDir = test_utils:tmp_dir("consensus_members_1_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, consensus_members(Ledger)),
    test_utils:cleanup_tmp_dir(BaseDir).

consensus_members_2_test() ->
    BaseDir = test_utils:tmp_dir("consensus_members_2_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = consensus_members([1, 2, 3], Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({ok, [1, 2, 3]}, consensus_members(Ledger)),
    test_utils:cleanup_tmp_dir(BaseDir).

active_gateways_test() ->
    BaseDir = test_utils:tmp_dir("active_gateways_test"),
    Ledger = new(BaseDir),
    ?assertEqual(#{}, active_gateways(Ledger)),
    test_utils:cleanup_tmp_dir(BaseDir).

add_gateway_test() ->
    BaseDir = test_utils:tmp_dir("add_gateway_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    meck:new(blockchain, [passthrough]),
    meck:expect(blockchain, config, fun(?election_version, _) -> {ok, 4} end),

    ok = add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger1),
    ok = commit_context(Ledger1),
    ?assertMatch(
        {ok, _},
        find_gateway_info(<<"gw_address">>, Ledger)
    ),
    ?assertEqual({error, gateway_already_active}, add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger)),
    meck:unload(blockchain),
    test_utils:cleanup_tmp_dir(BaseDir).

add_gateway_location_test() ->
    BaseDir = test_utils:tmp_dir("add_gateway_location_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger1),
    ok = commit_context(Ledger1),
    Ledger2 = new_context(Ledger),
    ?assertEqual(
       ok,
       add_gateway_location(<<"gw_address">>, 1, 1, Ledger2)
    ),
    test_utils:cleanup_tmp_dir(BaseDir).

credit_account_test() ->
    BaseDir = test_utils:tmp_dir("credit_account_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_account(<<"address">>, 1000, Ledger1),
    ok = commit_context(Ledger1),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(1000, blockchain_ledger_entry_v1:balance(Entry)),
    test_utils:cleanup_tmp_dir(BaseDir).

debit_account_test() ->
    BaseDir = test_utils:tmp_dir("debit_account_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_account(<<"address">>, 1000, Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({error, {bad_nonce, {payment, 0, 0}}}, debit_account(<<"address">>, 1000, 0, Ledger)),
    ?assertEqual({error, {bad_nonce, {payment, 12, 0}}}, debit_account(<<"address">>, 1000, 12, Ledger)),
    ?assertEqual({error, {insufficient_balance, {9999, 1000}}}, debit_account(<<"address">>, 9999, 1, Ledger)),
    Ledger2 = new_context(Ledger),
    ok = debit_account(<<"address">>, 500, 1, Ledger2),
    ok = commit_context(Ledger2),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(500, blockchain_ledger_entry_v1:balance(Entry)),
    ?assertEqual(1, blockchain_ledger_entry_v1:nonce(Entry)),
    test_utils:cleanup_tmp_dir(BaseDir).

credit_dc_test() ->
    BaseDir = test_utils:tmp_dir("credit_dc_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_dc(<<"address">>, 1000, Ledger1),
    ok = commit_context(Ledger1),
    {ok, Entry} = find_dc_entry(<<"address">>, Ledger),
    ?assertEqual(1000, blockchain_ledger_data_credits_entry_v1:balance(Entry)),
    test_utils:cleanup_tmp_dir(BaseDir).

debit_fee_test() ->
    BaseDir = test_utils:tmp_dir("debit_fee_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_dc(<<"address">>, 1000, Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({error, {insufficient_dc_balance, {9999, 1000}}}, debit_fee(<<"address">>, 9999, Ledger)),
    Ledger2 = new_context(Ledger),
    ok = debit_fee(<<"address">>, 500, Ledger2),
    ok = commit_context(Ledger2),
    {ok, Entry} = find_dc_entry(<<"address">>, Ledger),
    ?assertEqual(500, blockchain_ledger_data_credits_entry_v1:balance(Entry)),
    ?assertEqual(0, blockchain_ledger_data_credits_entry_v1:nonce(Entry)),
    test_utils:cleanup_tmp_dir(BaseDir).

credit_security_test() ->
    BaseDir = test_utils:tmp_dir("credit_security_test"),
    Ledger = new(BaseDir),
    commit(
        fun(L) ->
            ok = credit_security(<<"address">>, 1000, L)
        end,
        Ledger
    ),
    {ok, Entry} = find_security_entry(<<"address">>, Ledger),
    ?assertEqual(#{<<"address">> => Entry}, securities(Ledger)),
    ?assertEqual(1000, blockchain_ledger_security_entry_v1:balance(Entry)),
    test_utils:cleanup_tmp_dir(BaseDir).

debit_security_test() ->
    BaseDir = test_utils:tmp_dir("debit_security_test"),
    Ledger = new(BaseDir),
    commit(
        fun(L) ->
            ok = credit_security(<<"address">>, 1000, L)
        end,
        Ledger
    ),
    ?assertEqual({error, {bad_nonce, {payment, 0, 0}}}, debit_security(<<"address">>, 1000, 0, Ledger)),
    ?assertEqual({error, {bad_nonce, {payment, 12, 0}}}, debit_security(<<"address">>, 1000, 12, Ledger)),
    ?assertEqual({error, {insufficient_security_balance, {9999, 1000}}}, debit_security(<<"address">>, 9999, 1, Ledger)),
    commit(
        fun(L) ->
            ok = debit_security(<<"address">>, 500, 1, L)
        end,
        Ledger
    ),
    {ok, Entry} = find_security_entry(<<"address">>, Ledger),
    ?assertEqual(500, blockchain_ledger_security_entry_v1:balance(Entry)),
    ?assertEqual(1, blockchain_ledger_security_entry_v1:nonce(Entry)),
    test_utils:cleanup_tmp_dir(BaseDir).

fold_test() ->
    BaseDir = test_utils:tmp_dir("fold_test"),
    Ledger = new(BaseDir),
    commit(fun(L) ->
                   CF = default_cf(L),
                   [begin
                        K = <<"key_", (integer_to_binary(N))/binary>>,
                        V = <<"asdlkjasdlkjasd">>,
                        cache_put(L, CF, K, V)
                    end || N <- lists:seq(1, 20)]
           end,
           Ledger),

    DCF = default_cf(Ledger),
    F = cache_fold(Ledger, DCF, fun({K, _V}, A) -> [K | A] end, []),
    ?assertEqual([<<"key_1">>,<<"key_10">>,<<"key_11">>,<<"key_12">>,
                  <<"key_13">>,<<"key_14">>,<<"key_15">>,<<"key_16">>,
                  <<"key_17">>,<<"key_18">>,<<"key_19">>,<<"key_2">>,
                  <<"key_20">>,<<"key_3">>,<<"key_4">>,<<"key_5">>,
                  <<"key_6">>,<<"key_7">>,<<"key_8">>,<<"key_9">>],
                 lists:sort(F)),

    Ledger1 = new_context(Ledger),

    [cache_delete(Ledger1, DCF, K1)
     || K1 <- [<<"key_13">>,<<"key_14">>,<<"key_15">>,<<"key_16">>]],

    cache_put(Ledger1, DCF, <<"aaa">>, <<"bbb">>),
    cache_put(Ledger1, DCF, <<"key_1">>, <<"bbb">>),

    %% check cache fold
    F1 = cache_fold(Ledger1, DCF, fun({K, _V}, A) -> [K | A] end, []),
    ?assertEqual([<<"aaa">>,
                  <<"key_1">>,<<"key_10">>,<<"key_11">>,<<"key_12">>,
                  <<"key_17">>,<<"key_18">>,<<"key_19">>,<<"key_2">>,
                  <<"key_20">>,<<"key_3">>,<<"key_4">>,<<"key_5">>,
                  <<"key_6">>,<<"key_7">>,<<"key_8">>,<<"key_9">>],
                 lists:sort(F1)),

    %% check cached and uncached reads
    ?assertEqual(not_found, cache_get(Ledger, DCF, <<"aaa">>, [])),
    ?assertEqual({ok, <<"bbb">>}, cache_get(Ledger1, DCF, <<"aaa">>, [])),

    ?assertEqual({ok, <<"asdlkjasdlkjasd">>}, cache_get(Ledger, DCF, <<"key_1">>, [])),
    ?assertEqual({ok, <<"bbb">>}, cache_get(Ledger1, DCF, <<"key_1">>, [])),

    %% check uncached fold
    F2 = cache_fold(Ledger, DCF, fun({K, _V}, A) -> [K | A] end, []),
    ?assertEqual([<<"key_1">>,<<"key_10">>,<<"key_11">>,<<"key_12">>,
                  <<"key_13">>,<<"key_14">>,<<"key_15">>,<<"key_16">>,
                  <<"key_17">>,<<"key_18">>,<<"key_19">>,<<"key_2">>,
                  <<"key_20">>,<<"key_3">>,<<"key_4">>,<<"key_5">>,
                  <<"key_6">>,<<"key_7">>,<<"key_8">>,<<"key_9">>],
                 lists:sort(F2)),

    %% commit, recheck
    commit_context(Ledger1),

    F3 = cache_fold(Ledger, DCF, fun({K, _V}, A) -> [K | A] end, []),
    ?assertEqual([<<"aaa">>,
                  <<"key_1">>,<<"key_10">>,<<"key_11">>,<<"key_12">>,
                  <<"key_17">>,<<"key_18">>,<<"key_19">>,<<"key_2">>,
                  <<"key_20">>,<<"key_3">>,<<"key_4">>,<<"key_5">>,
                  <<"key_6">>,<<"key_7">>,<<"key_8">>,<<"key_9">>],
                 lists:sort(F3)),

    %% check cached and uncached reads
    ?assertEqual({ok, <<"bbb">>}, cache_get(Ledger, DCF, <<"aaa">>, [])),

    ?assertEqual({ok, <<"bbb">>}, cache_get(Ledger, DCF, <<"key_1">>, [])),
    test_utils:cleanup_tmp_dir(BaseDir).


poc_test() ->
    BaseDir = test_utils:tmp_dir("poc_test"),
    Ledger = new(BaseDir),

    Challenger0 = <<"challenger0">>,
    Challenger1 = <<"challenger1">>,

    OnionKeyHash0 = <<"onion_key_hash0">>,
    OnionKeyHash1 = <<"onion_key_hash1">>,

    BlockHash = <<"block_hash">>,

    OwnerAddr = <<"owner_address">>,
    Location = h3:from_geo({37.78101, -122.465372}, 12),
    Nonce = 1,

    SecretHash = <<"secret_hash">>,


    meck:new(blockchain_swarm, [passthrough]),
    meck:expect(blockchain,
                config,
                fun(min_score, _) ->
                        {ok, 0.2};
                   (h3_exclusion_ring_dist, _) ->
                        {ok, 3};
                   (h3_max_grid_distance, _) ->
                        {ok, 60};
                   (h3_neighbor_res, _) ->
                        {ok, 12};
                   (election_version, _) ->
                        {ok, 4};
                    (full_gateway_capabilities_mask, _) ->
                        {ok, ?GW_CAPABILITIES_FULL_GATEWAY_V1}
                end),

    ?assertEqual({error, not_found}, find_pocs(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = add_gateway(OwnerAddr, Challenger0, Location, Nonce, full, L),
            ok = add_gateway(OwnerAddr, Challenger1, Location, Nonce, full, L),
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger0, BlockHash, 0, L)
        end,
        Ledger
    ),
    PoC0 = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash0, Challenger0, BlockHash),
    ?assertEqual({ok, [PoC0]} ,find_pocs(OnionKeyHash0, Ledger)),
    ?assertEqual({ok, PoC0} ,find_poc(OnionKeyHash0, Challenger0, Ledger)),
    {ok, GwInfo0} = find_gateway_info(Challenger0, Ledger),
    ?assertEqual(1, blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo0)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GwInfo0)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger1, BlockHash, 0, L)
        end,
        Ledger
    ),
    PoC1 = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash0, Challenger1, BlockHash),
    ?assertEqual({ok, [PoC1, PoC0]}, find_pocs(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = delete_poc(OnionKeyHash0, Challenger0, L)
        end,
        Ledger
    ),
    ?assertEqual({ok, [PoC1]} ,find_pocs(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = delete_poc(OnionKeyHash0, Challenger1, L)
        end,
        Ledger
    ),
    ?assertEqual({error, not_found} ,find_pocs(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger0, BlockHash, 0, L)
        end,
        Ledger
    ),
    ?assertEqual({ok, [PoC0]} ,find_pocs(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash1, SecretHash, Challenger0, BlockHash, 0, L)
        end,
        Ledger
    ),
    ?assertEqual({error, not_found} ,find_pocs(OnionKeyHash0, Ledger)),
    PoC2 = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash1, Challenger0, BlockHash),
    ?assertEqual({ok, [PoC2]}, find_pocs(OnionKeyHash1, Ledger)),
    {ok, GwInfo1} = find_gateway_info(Challenger0, Ledger),
    ?assertEqual(1, blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo1)),
    ?assertEqual(OnionKeyHash1, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GwInfo1)),
    meck:unload(blockchain_swarm),
    meck:unload(blockchain),
    test_utils:cleanup_tmp_dir(BaseDir),
    ok.

commit(Fun, Ledger0) ->
    Ledger1 = new_context(Ledger0),
    _ = Fun(Ledger1),
    commit_context(Ledger1).

-define(KEY1, <<0,105,110,41,229,175,44,3,221,73,181,25,27,184,120,84,
               138,51,136,194,72,161,94,225,240,73,70,45,135,23,41,96,78>>).
-define(KEY2, <<1,72,253,248,131,224,194,165,164,79,5,144,254,1,168,254,
                111,243,225,61,41,178,207,35,23,54,166,116,128,38,164,87,212>>).
-define(KEY3, <<1,124,37,189,223,186,125,185,240,228,150,61,9,164,28,75,
                44,232,76,6,121,96,24,24,249,85,177,48,246,236,14,49,80>>).

routing_test() ->
    BaseDir = test_utils:tmp_dir("routing_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ?assertEqual({error, not_found}, find_routing(1, Ledger1)),
    ?assertEqual({ok, 0}, get_oui_counter(Ledger1)),
    ?assertEqual([], ?MODULE:find_router_ouis(?KEY1, Ledger1)),

    Ledger2 = new_context(Ledger),
    ok = add_oui(<<"owner">>, [?KEY1], <<>>, <<>>, Ledger2),
    ok = commit_context(Ledger2),
    {ok, Routing0} = find_routing(1, Ledger),
    ?assertEqual(<<"owner">>, blockchain_ledger_routing_v1:owner(Routing0)),
    ?assertEqual(1, blockchain_ledger_routing_v1:oui(Routing0)),
    ?assertEqual([1], ?MODULE:find_router_ouis(?KEY1, Ledger)),
    ?assertEqual([?KEY1], blockchain_ledger_routing_v1:addresses(Routing0)),
    ?assertEqual(0, blockchain_ledger_routing_v1:nonce(Routing0)),

    Ledger3 = new_context(Ledger),
    ok = add_oui(<<"owner2">>, [?KEY2], <<>>, <<>>, Ledger3),
    ok = commit_context(Ledger3),
    {ok, Routing1} = find_routing(2, Ledger),
    ?assertEqual(<<"owner2">>, blockchain_ledger_routing_v1:owner(Routing1)),
    ?assertEqual(2, blockchain_ledger_routing_v1:oui(Routing1)),
    ?assertEqual([2], ?MODULE:find_router_ouis(?KEY2, Ledger)),
    ?assertEqual([?KEY2], blockchain_ledger_routing_v1:addresses(Routing1)),
    ?assertEqual(0, blockchain_ledger_routing_v1:nonce(Routing1)),

    Ledger4 = new_context(Ledger),
    ok = update_routing(2, {update_routers, [?KEY3]}, 1, Ledger4),
    ok = commit_context(Ledger4),
    {ok, Routing2} = find_routing(2, Ledger),
    ?assertEqual(<<"owner2">>, blockchain_ledger_routing_v1:owner(Routing2)),
    ?assertEqual(2, blockchain_ledger_routing_v1:oui(Routing2)),
    ?assertEqual([?KEY3], blockchain_ledger_routing_v1:addresses(Routing2)),
    ?assertEqual([2], ?MODULE:find_router_ouis(?KEY3, Ledger)),
    ?assertEqual(1, blockchain_ledger_routing_v1:nonce(Routing2)),
    ?assertEqual([], ?MODULE:find_router_ouis(?KEY2, Ledger)),
    ?assertEqual([1], ?MODULE:find_router_ouis(?KEY1, Ledger)),

    test_utils:cleanup_tmp_dir(BaseDir),
    ok.

state_channels_test() ->
    BaseDir = test_utils:tmp_dir("state_channels_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ID = crypto:strong_rand_bytes(32),
    Owner = <<"owner">>,
    Nonce = 1,

    ?assertEqual({error, not_found}, find_state_channel(ID, Owner, Ledger1)),
    ?assertEqual({ok, []}, find_sc_ids_by_owner(Owner, Ledger1)),

    Ledger2 = new_context(Ledger),
    ok = add_state_channel(ID, Owner, 10, Nonce, 0, 0, Ledger2),
    ok = commit_context(Ledger2),
    {ok, SC} = find_state_channel(ID, Owner, Ledger),
    ?assertEqual(ID, blockchain_ledger_state_channel_v1:id(SC)),
    ?assertEqual(Owner, blockchain_ledger_state_channel_v1:owner(SC)),
    ?assertEqual(Nonce, blockchain_ledger_state_channel_v1:nonce(SC)),
    ?assertEqual({ok, [ID]}, find_sc_ids_by_owner(Owner, Ledger)),

    Ledger3 = new_context(Ledger),
    ok = close_state_channel(Owner, Owner, SC, ID, false, Ledger3),
    ok = commit_context(Ledger3),
    ?assertEqual({error, not_found}, find_state_channel(ID, Owner, Ledger)),
    ?assertEqual({ok, []}, find_sc_ids_by_owner(Owner, Ledger)),
    test_utils:cleanup_tmp_dir(BaseDir),

    ok.

state_channels_v2_test() ->
    {timeout, 30000,
     fun() ->
             BaseDir = test_utils:tmp_dir("state_channels_v2_test"),
             Ledger = ?MODULE:new(BaseDir),
             Ledger1 = ?MODULE:new_context(Ledger),
             ID = crypto:strong_rand_bytes(32),
             Owner = <<"owner">>,
             Nonce = 1,

             ?assertEqual({error, not_found}, ?MODULE:find_state_channel(ID, Owner, Ledger1)),
             ?assertEqual({ok, []}, ?MODULE:find_sc_ids_by_owner(Owner, Ledger1)),

             meck:new(blockchain, [passthrough]),
             meck:expect(blockchain, config, fun(?sc_version, _) -> {ok, 2} end),

             Ledger2 = ?MODULE:new_context(Ledger),
             ok = ?MODULE:add_state_channel(ID, Owner, 10, Nonce, 0, 0, Ledger2),
             ok = ?MODULE:commit_context(Ledger2),
             {ok, SC} = ?MODULE:find_state_channel(ID, Owner, Ledger),
             ?assertEqual(ID, blockchain_ledger_state_channel_v2:id(SC)),
             ?assertEqual(Owner, blockchain_ledger_state_channel_v2:owner(SC)),
             ?assertEqual(Nonce, blockchain_ledger_state_channel_v2:nonce(SC)),
             ?assertEqual({ok, [ID]}, ?MODULE:find_sc_ids_by_owner(Owner, Ledger)),

             Ledger3 = ?MODULE:new_context(Ledger),
             ok = ?MODULE:close_state_channel(Owner, Owner, SC, ID, false, Ledger3),
             ok = ?MODULE:commit_context(Ledger3),
             ?assertEqual({error, not_found}, ?MODULE:find_state_channel(ID, Owner, Ledger)),
             ?assertEqual({ok, []}, ?MODULE:find_sc_ids_by_owner(Owner, Ledger)),
             test_utils:cleanup_tmp_dir(BaseDir),
             ?assert(meck:validate(blockchain)),
             meck:unload(blockchain),
             ok
     end}.

increment_bin_test() ->
    ?assertEqual(<<2>>, increment_bin(<<1>>)),
    ?assertEqual(<<1, 0>>, increment_bin(<<255>>)).

find_scs_by_owner_test() ->
    BaseDir = test_utils:tmp_dir("find_scs_by_owner_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ID1 = crypto:strong_rand_bytes(32),
    ID2 = crypto:strong_rand_bytes(32),
    IDs = [ID1, ID2],
    Owner = <<"owner">>,
    Nonce = 1,

    ?assertEqual({error, not_found}, find_state_channel(ID1, Owner, Ledger1)),
    ?assertEqual({error, not_found}, find_state_channel(ID2, Owner, Ledger1)),
    ?assertEqual({ok, []}, find_sc_ids_by_owner(Owner, Ledger1)),
    ?assertEqual({ok, #{}}, find_scs_by_owner(Owner, Ledger1)),

    Ledger2 = new_context(Ledger),
    %% Add two state channels for this owner
    ok = add_state_channel(ID1, Owner, 10, Nonce, 0, 0, Ledger2),
    ok = add_state_channel(ID2, Owner, 10, Nonce, 0, 0, Ledger2),
    ok = commit_context(Ledger2),

    {ok, FoundIDs} = find_sc_ids_by_owner(Owner, Ledger),
    ?assertEqual(lists:sort(FoundIDs), lists:sort(IDs)),

    {ok, SCs} = find_scs_by_owner(Owner, Ledger),
    ?assertEqual(lists:sort(maps:keys(SCs)), lists:sort(IDs)),
    test_utils:cleanup_tmp_dir(BaseDir),
    ok.

subnet_allocation_test() ->
    BaseDir = test_utils:tmp_dir("subnet_allocation_test"),
    Ledger = new(BaseDir),
    {subnets, _DB, SubnetCF} = subnets_cf(Ledger),
    Mask8 = blockchain_ledger_routing_v1:subnet_size_to_mask(8),
    Mask16 = blockchain_ledger_routing_v1:subnet_size_to_mask(16),
    Mask32 = blockchain_ledger_routing_v1:subnet_size_to_mask(32),
    Mask64 = blockchain_ledger_routing_v1:subnet_size_to_mask(64),
    {ok, Subnet} = allocate_subnet(8, Ledger),
    ?assertEqual(<<0:25/integer-unsigned-big, Mask8:23/integer-unsigned-big>>, Subnet),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet, <<1:32/little-unsigned-integer>>, []),

    {ok, Subnet2} = allocate_subnet(8, Ledger),
    ?assertEqual(<<8:25/integer-unsigned-big, Mask8:23/integer-unsigned-big>>, Subnet2),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet2, <<2:32/little-unsigned-integer>>, []),

    {ok, Subnet3} = allocate_subnet(32, Ledger),
    ?assertEqual(<<32:25/integer-unsigned-big, Mask32:23/integer-unsigned-big>>, Subnet3),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet3, <<3:32/little-unsigned-integer>>, []),

    {ok, Subnet4} = allocate_subnet(8, Ledger),
    ?assertEqual(<<16:25/integer-unsigned-big, Mask8:23/integer-unsigned-big>>, Subnet4),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet4, <<4:32/little-unsigned-integer>>, []),

    {ok, Subnet5} = allocate_subnet(16, Ledger),
    ?assertEqual(<<64:25/integer-unsigned-big, Mask16:23/integer-unsigned-big>>, Subnet5),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet5, <<5:32/little-unsigned-integer>>, []),

    {ok, Subnet6} = allocate_subnet(8, Ledger),
    ?assertEqual(<<24:25/integer-unsigned-big, Mask8:23/integer-unsigned-big>>, Subnet6),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet6, <<6:32/little-unsigned-integer>>, []),

    {ok, Subnet7} = allocate_subnet(16, Ledger),
    ?assertEqual(<<80:25/integer-unsigned-big, Mask16:23/integer-unsigned-big>>, Subnet7),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet7, <<7:32/little-unsigned-integer>>, []),

    {ok, Subnet8} = allocate_subnet(64, Ledger),
    ?assertEqual(<<128:25/integer-unsigned-big, Mask64:23/integer-unsigned-big>>, Subnet8),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet8, <<8:32/little-unsigned-integer>>, []),

    {ok, Subnet9} = allocate_subnet(32, Ledger),
    ?assertEqual(<<96:25/integer-unsigned-big, Mask32:23/integer-unsigned-big>>, Subnet9),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet9, <<9:32/little-unsigned-integer>>, []),
    test_utils:cleanup_tmp_dir(BaseDir),
    ok.

subnet_allocation2_test() ->
    BaseDir = test_utils:tmp_dir("subnet_allocation2_test"),
    Ledger = new(BaseDir),
    {subnets, _DB, SubnetCF} = subnets_cf(Ledger),
    Mask8 = blockchain_ledger_routing_v1:subnet_size_to_mask(8),
    Mask32 = blockchain_ledger_routing_v1:subnet_size_to_mask(32),
    {ok, Subnet} = allocate_subnet(8, Ledger),
    ?assertEqual(<<0:25/integer-unsigned-big, Mask8:23/integer-unsigned-big>>, Subnet),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet, <<1:32/little-unsigned-integer>>, []),
    {ok, Subnet2} = allocate_subnet(32, Ledger),
    ?assertEqual(<<32:25/integer-unsigned-big, Mask32:23/integer-unsigned-big>>, Subnet2),
    ok = rocksdb:put(Ledger#ledger_v1.db, SubnetCF, Subnet2, <<3:32/little-unsigned-integer>>, []),
    test_utils:cleanup_tmp_dir(BaseDir),
    ok.

debit_dc_test() ->
    BaseDir = test_utils:tmp_dir("debit_dc_test"),
    Ledger = new(BaseDir),
    %% check no dc entry initially
    {error, dc_entry_not_found} = ?MODULE:find_dc_entry(<<"address">>, Ledger),

    %% debit dc, note: no dc entry here still
    Ledger2 = new_context(Ledger),
    ok = ?MODULE:debit_dc(<<"address">>, 1, 0, Ledger2),
    ok = commit_context(Ledger2),

    %% blank dc entry should pop up here
    {ok, Entry0} = ?MODULE:find_dc_entry(<<"address">>, Ledger),
    ?assertEqual(0, blockchain_ledger_data_credits_entry_v1:balance(Entry0)),
    ?assertEqual(1, blockchain_ledger_data_credits_entry_v1:nonce(Entry0)),

    %% credit some dc to this
    Ledger3 = new_context(Ledger),
    ok = credit_dc(<<"address">>, 1000, Ledger3),
    ok = commit_context(Ledger3),

    %% check updated dcs
    {ok, Entry} = find_dc_entry(<<"address">>, Ledger),
    ?assertEqual(1000, blockchain_ledger_data_credits_entry_v1:balance(Entry)),
    test_utils:cleanup_tmp_dir(BaseDir).

hnt_to_dc_test() ->
    ?assertEqual({ok, 30000}, hnt_to_dc(1 * ?BONES_PER_HNT, trunc(0.3 * ?ORACLE_PRICE_SCALING_FACTOR))),
    ok.

dc_to_hnt_test() ->
    %% NOTE +1 below as dc_to_hnt uses ceil and thus we need to bump our expected figure
    ?assertEqual({ok, (1 * ?BONES_PER_HNT) + 1} , dc_to_hnt(30000, trunc(0.3 * ?ORACLE_PRICE_SCALING_FACTOR))),
    ok.

commit_hooks_test() ->
    BaseDir = test_utils:tmp_dir("commit_hooks_test"),
    Me = self(),
    %% check that config-set hooks work
    %% {CF, Predicate, HookIncFun, HookEndFun} <- application:get_env(blockchain, commit_hook_callbacks, [])],
    %% {CF, Predicate, HookIncFun, HookEndFun, IncludeHeight} <- application:get_env(blockchain, commit_hook_callbacks, [])],
    application:set_env(blockchain, commit_hook_callbacks,
                        [{active_gateways,
                            undefined,
                            fun(Changes) -> Me ! {hook1, Changes} end,
                            fun(_CF, ChangedKeys) -> Me ! {hook1, changes_complete, ChangedKeys} end                            
                        },
                        {active_gateways,
                            undefined,
                            fun(Changes, Height) -> Me ! {hook2, Changes, Height} end,
                            fun(_CF, ChangedKeys, Height) -> Me ! {hook2, changes_complete, ChangedKeys, Height} end,
                            true                            
                        }]),

    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    {ok, Ledger1Height} = current_height(Ledger1),
    ok = add_gateway(<<"owner_address 1">>, <<"gw_address">>, Ledger1),
    ok = commit_context(Ledger1),

    receive
        {hook1, _} -> ok
    after 200 ->
            error(hook1_timeout)
    end,

    receive
        {hook1, changes_complete, _ReceivedChangedKeys1} -> ok
    after 200 ->
            error(config_set_timeout)
    end,

    receive
        {hook2, _, ReceivedHeight1} -> 
            ?assertMatch(Ledger1Height,
                         ReceivedHeight1)
    after 200 ->
            error(hook2_timeout)
    end,

    receive
        {hook2, changes_complete, _, ReceivedHeight2} ->
            ?assertMatch(Ledger1Height,
                         ReceivedHeight2)
    after 200 ->
            error(config_set_timeout)
    end,

    %% check that multiple hooks fire
    {_Ref, Ledger2} = add_commit_hook(entries,
                                        fun(Changes) -> Me ! {hook3, Changes} end,
                                        fun(_CF, ChangedKeys) -> Me ! {hook3, changes_complete, ChangedKeys} end,
                                        fun(K, _) -> K == <<"my_address">> end, Ledger1),
    Ledger3 = new_context(Ledger2),
    ok = add_gateway(<<"owner_address 2">>, <<"gw_address 2">>, Ledger3),
    ok = credit_account(<<"your_address">>, 4000, Ledger3),
    ok = credit_account(<<"my_address">>, 4000, Ledger3),

    ok = commit_context(Ledger3),

    %% confirm we get two msgs from the 1st active gateways CF hook
    receive
        {hook1, _} -> ok
    after 200 ->
            error(hook1_timeout)
    end,

    receive
        {hook1, changes_complete, _ReceivedChangedKeys2} -> ok
    after 200 ->
            error(hook1_timeout)
    end,

    %% confirm we get expected msgs from the 2nd hook
    receive
        {hook3, Changes} ->
            ?assertMatch([{_, put, <<"my_address">>, _}],
                         Changes)
    after 200 ->
            error(hook3_timeout)
    end,

    receive
        {hook3, changes_complete, _ReceivedChangedKeys3} -> ok
    after 200 ->
            error(hook3_timeout)
    end,


    %% check that multiple hooks for multiple CFs work

    %% check that removal works

    test_utils:cleanup_tmp_dir(BaseDir).

-endif.
