%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_v1).

-export([
    new/1,
    mode/1, mode/2,
    dir/1,

    check_key/2, mark_key/2,

    new_context/1, delete_context/1, reset_context/1, commit_context/1,
    get_context/1, context_cache/1,

    new_snapshot/1, context_snapshot/2, has_snapshot/2, release_snapshot/1, snapshot/1,

    current_height/1, increment_height/2,
    transaction_fee/1, update_transaction_fee/1,
    consensus_members/1, consensus_members/2,
    election_height/1, election_height/2,
    election_epoch/1, election_epoch/2,
    process_delayed_txns/3,

    active_gateways/1,
    entries/1,
    htlcs/1,

    master_key/1, master_key/2,
    all_vars/1,
    vars/3,
    config/2,  % no version with default, use the set value or fail
    vars_nonce/1, vars_nonce/2,
    save_threshold_txn/2,

    find_gateway_info/2,
    add_gateway/3, add_gateway/5,
    update_gateway/3,
    fixup_neighbors/4,
    add_gateway_location/4,
    add_gateway_witnesses/3,

    gateway_versions/1,

    update_gateway_score/3, gateway_score/2,
    update_gateway_oui/3,

    find_poc/2,
    request_poc/5,
    delete_poc/3, delete_pocs/2,
    maybe_gc_pocs/1,

    find_entry/2,
    credit_account/3, debit_account/4,
    check_balance/3,

    dc_entries/1,
    find_dc_entry/2,
    credit_dc/3,
    debit_dc/3,
    debit_fee/3,
    check_dc_balance/3,

    token_burn_exchange_rate/1,
    token_burn_exchange_rate/2,

    securities/1,
    find_security_entry/2,
    credit_security/3, debit_security/4,
    check_security_balance/3,

    find_htlc/2,
    add_htlc/8,
    redeem_htlc/3,

    get_oui_counter/1, increment_oui_counter/2,
    find_ouis/2, add_oui/4,
    find_routing/2,  add_routing/5,

    find_state_channel/3, find_state_channels_by_owner/2,
    add_state_channel/5,
    close_state_channel/3,

    delay_vars/3,

    fingerprint/1, fingerprint/2,
    raw_fingerprint/2, %% does not use cache
    cf_fold/4,

    apply_raw_changes/2,

    set_hexes/2, get_hexes/1,
    set_hex/3, get_hex/2, delete_hex/2,

    add_to_hex/3,
    remove_from_hex/3,

    clean_all_hexes/1,

    clean/1, close/1,
    compact/1
]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_v1, {
    dir :: file:filename_all(),
    db :: rocksdb:db_handle(),
    snapshots :: ets:tid(),
    mode = active :: active | delayed,
    active :: sub_ledger(),
    delayed :: sub_ledger(),
    snapshot :: undefined | rocksdb:snapshot_handle()
}).

-record(sub_ledger_v1, {
    default :: rocksdb:cf_handle(),
    active_gateways :: rocksdb:cf_handle(),
    entries :: rocksdb:cf_handle(),
    dc_entries :: rocksdb:cf_handle(),
    htlcs :: rocksdb:cf_handle(),
    pocs :: rocksdb:cf_handle(),
    securities :: rocksdb:cf_handle(),
    routing :: rocksdb:cf_handle(),
    cache :: undefined | ets:tid()
}).

-define(DB_FILE, "ledger.db").
-define(CURRENT_HEIGHT, <<"current_height">>).
-define(TRANSACTION_FEE, <<"transaction_fee">>).
-define(CONSENSUS_MEMBERS, <<"consensus_members">>).
-define(ELECTION_HEIGHT, <<"election_height">>).
-define(ELECTION_EPOCH, <<"election_epoch">>).
-define(OUI_COUNTER, <<"oui_counter">>).
-define(MASTER_KEY, <<"master_key">>).
-define(VARS_NONCE, <<"vars_nonce">>).
-define(BURN_RATE, <<"token_burn_exchange_rate">>).
-define(hex_list, <<"$hex_list">>).
-define(hex_prefix, "$hex_").

-define(CACHE_TOMBSTONE, '____ledger_cache_tombstone____').

-type ledger() :: #ledger_v1{}.
-type sub_ledger() :: #sub_ledger_v1{}.
-type entries() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_entry_v1:entry()}.
-type dc_entries() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_data_credits_entry_v1:data_credits_entry()}.
-type active_gateways() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_gateway_v2:gateway()}.
-type htlcs() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_htlc_v1:htlc()}.
-type securities() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_security_entry_v1:entry()}.
-type hexmap() :: #{h3:h3_index() => non_neg_integer()}.

-export_type([ledger/0]).

-spec new(file:filename_all()) -> ledger().
new(Dir) ->
    {ok, DB, CFs} = open_db(Dir),
    [DefaultCF, AGwsCF, EntriesCF, DCEntriesCF, HTLCsCF, PoCsCF, SecuritiesCF, RoutingCF, SCsCF,
     DelayedDefaultCF, DelayedAGwsCF, DelayedEntriesCF, DelayedDCEntriesCF, DelayedHTLCsCF,
     DelayedPoCsCF, DelayedSecuritiesCF, DelayedRoutingCF, DelayedSCsCF] = CFs,
    #ledger_v1{
        dir=Dir,
        db=DB,
        mode=active,
        snapshots = ets:new(snapshot_cache, [set, public, {keypos, 1}]),
        active= #sub_ledger_v1{
            default=DefaultCF,
            active_gateways=AGwsCF,
            entries=EntriesCF,
            dc_entries=DCEntriesCF,
            htlcs=HTLCsCF,
            pocs=PoCsCF,
            securities=SecuritiesCF,
            routing=RoutingCF,
            state_channels=SCsCF
        },
        delayed= #sub_ledger_v1{
            default=DelayedDefaultCF,
            active_gateways=DelayedAGwsCF,
            entries=DelayedEntriesCF,
            dc_entries=DelayedDCEntriesCF,
            htlcs=DelayedHTLCsCF,
            pocs=DelayedPoCsCF,
            securities=DelayedSecuritiesCF,
            routing=DelayedRoutingCF,
            state_channels=DelayedSCsCF
        }
    }.

-spec mode(ledger()) -> active | delayed.
mode(Ledger) ->
    Ledger#ledger_v1.mode.

-spec mode(active | delayed, ledger()) -> ledger().
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

-spec new_context(ledger()) -> ledger().
new_context(Ledger) ->
    %% accumulate ledger changes in a read-through ETS cache
    Cache = ets:new(txn_cache, [set, protected, {keypos, 1}]),
    context_cache(Cache, Ledger).

get_context(Ledger) ->
    case ?MODULE:context_cache(Ledger) of
        undefined ->
            undefined;
        Cache ->
            flatten_cache(Cache)
    end.

flatten_cache(Cache) ->
    ets:tab2list(Cache).

install_context(FlatCache, Ledger) ->
    Cache = ets:new(txn_cache, [set, protected, {keypos, 1}]),
    ets:insert(Cache, FlatCache),
    context_cache(Cache, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delete_context(ledger()) -> ledger().
delete_context(Ledger) ->
    case ?MODULE:context_cache(Ledger) of
        undefined ->
            Ledger;
        Cache ->
            ets:delete(Cache),
            context_cache(undefined, Ledger)
    end.

-spec reset_context(ledger()) -> ok.
reset_context(Ledger) ->
    case ?MODULE:context_cache(Ledger) of
        undefined ->
            ok;
        Cache ->
            true = ets:delete_all_objects(Cache),
            ok
    end.

-spec commit_context(ledger()) -> ok.
commit_context(#ledger_v1{db=DB}=Ledger) ->
    Cache = ?MODULE:context_cache(Ledger),
    Context = batch_from_cache(Cache),
    ok = rocksdb:write_batch(DB, Context, [{sync, true}]),
    rocksdb:release_batch(Context),
    delete_context(Ledger),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec context_cache(ledger()) -> undefined | ets:tid().
context_cache(#ledger_v1{mode=active, active=#sub_ledger_v1{cache=Cache}}) ->
    Cache;
context_cache(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{cache=Cache}}) ->
    Cache.

-spec new_snapshot(ledger()) -> {ok, ledger()} | {error, any()}.
new_snapshot(#ledger_v1{db=DB,
                        snapshot=undefined,
                        snapshots=Cache,
                        mode=active,
                        active=#sub_ledger_v1{cache=undefined},
                        delayed=#sub_ledger_v1{cache=undefined}}=Ledger) ->
    case rocksdb:snapshot(DB) of
        {ok, SnapshotHandle} ->
            {ok, Height} = current_height(Ledger),
            DelayedLedger = blockchain_ledger_v1:mode(delayed, Ledger),
            {ok, DelayedHeight} = current_height(DelayedLedger),
            ets:delete(Cache, DelayedHeight - 1),
            ets:insert(Cache, {Height, {snapshot, SnapshotHandle}}),
            {ok, Ledger#ledger_v1{snapshot=SnapshotHandle}};
        {error, Reason}=Error ->
            lager:error("Error creating new snapshot, reason: ~p", [Reason]),
            Error
    end;
new_snapshot(#ledger_v1{}) ->
    erlang:error(cannot_snapshot_delayed_ledger).

context_snapshot(Context, #ledger_v1{db=DB, snapshots=Cache} = Ledger) ->
    {ok, Height} = current_height(Ledger),
    case ets:lookup(Cache, Height) of
        [{_Height, _Snapshot}] ->
            ok;
        _ ->
            case rocksdb:snapshot(DB) of
                {ok, SnapshotHandle} ->
                    ets:insert_new(Cache, {Height, {context, SnapshotHandle, Context}});
                {error, Reason} = Error ->
                    lager:error("Error creating new snapshot, reason: ~p", [Reason]),
                    Error
            end
    end.

has_snapshot(Height, #ledger_v1{snapshots=Cache}=Ledger) ->
    case ets:lookup(Cache, Height) of
        [{Height, {snapshot, SnapshotHandle}}] ->
            %% because the snapshot was taken as we ingested a block to the leading ledger we need to query it
            %% as an active ledger to get the right information at this desired height
            {ok, blockchain_ledger_v1:new_context(blockchain_ledger_v1:mode(active, Ledger#ledger_v1{snapshot=SnapshotHandle}))};
        [{Height, {context, SnapshotHandle, Context}}] ->
            %% context ledgers are always a lagging ledger snapshot with a set of overlay data in an ETS table
            %% and therefore must be in delayed ledger mode
            {ok, install_context(Context, blockchain_ledger_v1:mode(delayed, Ledger#ledger_v1{snapshot=SnapshotHandle}))};
        _ ->
            {error, snapshot_not_found}
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

atom_to_cf(Atom, #ledger_v1{mode = Mode} = Ledger) ->
        SL = case Mode of
                 active -> Ledger#ledger_v1.active;
                 delayed -> Ledger#ledger_v1.delayed
             end,
        case Atom of
            default -> SL#sub_ledger_v1.default;
            active_gateways -> SL#sub_ledger_v1.active_gateways;
            entries -> SL#sub_ledger_v1.entries;
            dc_entries -> SL#sub_ledger_v1.dc_entries;
            htlcs -> SL#sub_ledger_v1.htlcs;
            pocs -> SL#sub_ledger_v1.pocs;
            securities -> SL#sub_ledger_v1.securities;
            routing -> SL#sub_ledger_v1.routing;
            state_channels -> SL#sub_ledger_v1.state_channels
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
        cache_fold(Ledger, CFRef, F, Acc)
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

raw_fingerprint(#ledger_v1{mode = Mode} = Ledger, Extended) ->
    try
        SubLedger =
        case Mode of
            active ->
                Ledger#ledger_v1.active;
            delayed ->
                Ledger#ledger_v1.delayed
        end,
        #sub_ledger_v1{
           default = DefaultCF,
           active_gateways = AGwsCF,
           entries = EntriesCF,
           dc_entries = DCEntriesCF,
           htlcs = HTLCsCF,
           pocs = PoCsCF,
           securities = SecuritiesCF,
           routing = RoutingCF,
           state_channels = SCsCF
          } = SubLedger,
        %% NB: keep in sync with upgrades macro in blockchain.erl
        Filter = [<<"gateway_v2">>],
        DefaultVals = cache_fold(
                        Ledger, DefaultCF,
                        fun({K, _} = X, Acc) ->
                                case lists:member(K, Filter) of
                                    true -> Acc;
                                    _ -> [X | Acc]
                                end
                        end, []),
        L0 = [GWsVals, EntriesVals, DCEntriesVals, HTLCs,
              PoCs, Securities, Routings, StateChannels]
        = [cache_fold(Ledger, CF, fun(X, Acc) -> [X | Acc] end, [])
           || CF <- [AGwsCF, EntriesCF, DCEntriesCF, HTLCsCF,
                     PoCsCF, SecuritiesCF, RoutingCF, SCsCF]],
        L = lists:append(L0, DefaultVals),
        case Extended of
            false ->
                {ok, #{<<"ledger_fingerprint">> => fp(lists:flatten(L))}};
            true ->
                {ok, #{<<"ledger_fingerprint">> => fp(lists:flatten(L)),
                       <<"gateways_fingerprint">> => fp(GWsVals),
                       <<"core_fingerprint">> => fp(DefaultVals),
                       <<"entries_fingerprint">> => fp(EntriesVals),
                       <<"dc_entries_fingerprint">> => fp(DCEntriesVals),
                       <<"htlc_fingerprint">> => fp(HTLCs),
                       <<"securities_fingerprint">> => fp(Securities),
                       <<"routings_fingerprint">> => fp(Routings),
                       <<"poc_fingerprint">> => fp(PoCs),
                       <<"state_channels_fingerprint">> => fp(StateChannels)
                      }}
        end
    catch _:_ ->
              {error, could_not_fingerprint}
    end.

fp(L) ->
    erlang:phash2(lists:sort(L)).

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


-spec transaction_fee(ledger()) -> {ok, pos_integer()} | {error, any()}.
transaction_fee(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?TRANSACTION_FEE, []) of
        {ok, <<Height:64/integer-unsigned-big>>} ->
            {ok, Height};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec update_transaction_fee(ledger()) -> ok.
update_transaction_fee(Ledger) ->
    %% TODO - this should calculate a new transaction fee for the network
    %% TODO - based on the average of usage fees
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?TRANSACTION_FEE, <<0:64/integer-unsigned-big>>).

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

process_delayed_txns(Block, Ledger, Chain) ->
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

all_vars(Ledger) ->
    CF = default_cf(Ledger),
    cache_fold(Ledger, CF,
               fun({<<"$var_", Name/binary>>, BValue}, Acc) ->
                       Value = binary_to_term(BValue),
                       maps:put(Name, Value, Acc)
               end, #{},
               [{start, {seek, <<"$var_">>}},
                {iterate_upper_bound, <<"$var`">>}]).

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

-spec add_gateway(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr, GatewayAddress, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            Gateway = blockchain_ledger_gateway_v2:new(OwnerAddr, undefined),
            Bin = blockchain_ledger_gateway_v2:serialize(Gateway),
            cache_put(Ledger, AGwsCF, GatewayAddress, Bin)
    end.

%% NOTE: This should only be allowed when adding a gateway which was
%% added in an old blockchain and is being added via a special
%% genesis block transaction to a new chain.
-spec add_gateway(OwnerAddress :: libp2p_crypto:pubkey_bin(),
                  GatewayAddress :: libp2p_crypto:pubkey_bin(),
                  Location :: undefined | pos_integer(),
                  Nonce :: non_neg_integer(),
                  Ledger :: ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr,
            GatewayAddress,
            Location,
            Nonce,
            Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            {ok, Height} = ?MODULE:current_height(Ledger),
            Gateway = blockchain_ledger_gateway_v2:new(OwnerAddr, Location, Nonce),

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

            Bin = blockchain_ledger_gateway_v2:serialize(NewGw),
            AGwsCF = active_gateways_cf(Ledger),
            ok = cache_put(Ledger, AGwsCF, GatewayAddress, Bin)
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
                     update_gateway(G, A, Ledger)
             end, maps:merge(R1, A1)),
    ok.

-spec update_gateway(Gw :: blockchain_ledger_gateway_v2:gateway(),
                     GwAddr :: libp2p_crypto:pubkey_bin(),
                     Ledger :: ledger()) -> ok | {error, _}.
update_gateway(Gw, GwAddr, Ledger) ->
    Bin = blockchain_ledger_gateway_v2:serialize(Gw),
    AGwsCF = active_gateways_cf(Ledger),
    cache_put(Ledger, AGwsCF, GwAddr, Bin).

-spec add_gateway_location(libp2p_crypto:pubkey_bin(), non_neg_integer(), non_neg_integer(), ledger()) -> ok | {error, no_active_gateway}.
add_gateway_location(GatewayAddress, Location, Nonce, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw} ->
            {ok, Height} = ?MODULE:current_height(Ledger),
            Gw1 = blockchain_ledger_gateway_v2:location(Location, Gw),
            Gw2 = blockchain_ledger_gateway_v2:nonce(Nonce, Gw1),
            NewGw = blockchain_ledger_gateway_v2:set_alpha_beta_delta(1.0, 1.0, Height, Gw2),
            %% we need to clear all our old witnesses out
            Bin = blockchain_ledger_gateway_v2:serialize(blockchain_ledger_gateway_v2:clear_witnesses(NewGw)),
            AGwsCF = active_gateways_cf(Ledger),
            cache_put(Ledger, AGwsCF, GatewayAddress, Bin),
            %% this is only needed if the gateway previously had a location
            case Nonce > 1 of
                true ->
                    %% we need to also remove any old witness links for this device's previous location on other gateways
                    lists:foreach(fun({Addr, GW}) ->
                                          case blockchain_ledger_gateway_v2:has_witness(GW, GatewayAddress) of
                                              true ->
                                                  GW1 = blockchain_ledger_gateway_v2:remove_witness(GW, GatewayAddress),
                                                  cache_put(Ledger, AGwsCF, Addr, blockchain_ledger_gateway_v2:serialize(GW1));
                                              false ->
                                                  ok
                                          end
                                  end, maps:to_list(active_gateways(Ledger)));
                false ->
                    ok
            end
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
            Bin = blockchain_ledger_gateway_v2:serialize(NewGw),
            AGwsCF = active_gateways_cf(Ledger),
            cache_put(Ledger, AGwsCF, GatewayAddress, Bin)
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
                         Ledger :: ledger()) -> ok | {error, any()}.
update_gateway_oui(Gateway, OUI, Ledger) ->
    case ?MODULE:find_gateway_info(Gateway, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Gw} ->
            NewGw = blockchain_ledger_gateway_v2:oui(OUI, Gw),
            Bin = blockchain_ledger_gateway_v2:serialize(NewGw),
            AGwsCF = active_gateways_cf(Ledger),
            cache_put(Ledger, AGwsCF, Gateway, Bin)
    end.

-spec add_gateway_witnesses(GatewayAddress :: libp2p_crypto:pubkey_bin(),
                            WitnessInfo :: [{integer(), non_neg_integer(), libp2p_crypto:pubkey_bin()}],
                            Ledger :: ledger()) -> ok | {error, any()}.
add_gateway_witnesses(GatewayAddress, WitnessInfo, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, GW0} ->
            GW1 = lists:foldl(fun({RSSI, TS, WitnessAddress}, GW) ->
                                      case ?MODULE:find_gateway_info(WitnessAddress, Ledger) of
                                          {ok, Witness} ->
                                              blockchain_ledger_gateway_v2:add_witness(WitnessAddress, Witness, RSSI, TS, GW);
                                          {error, _} ->
                                              GW
                                      end
                              end, GW0, WitnessInfo),
            AGwsCF = active_gateways_cf(Ledger),
            cache_put(Ledger, AGwsCF, GatewayAddress, blockchain_ledger_gateway_v2:serialize(GW1))
    end.

-spec find_poc(binary(), ledger()) -> {ok, blockchain_ledger_poc_v2:pocs()} | {error, any()}.
find_poc(OnionKeyHash, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    case cache_get(Ledger, PoCsCF, OnionKeyHash, []) of
        {ok, BinPoCs} ->
            PoCs = erlang:binary_to_term(BinPoCs),
            {ok, lists:map(fun blockchain_ledger_poc_v2:deserialize/1, PoCs)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec request_poc(OnionKeyHash :: binary(),
                  SecretHash :: binary(),
                  Challenger :: libp2p_crypto:pubkey_bin(),
                  BlockHash :: binary(),
                  Ledger :: ledger()) -> ok | {error, any()}.
request_poc(OnionKeyHash, SecretHash, Challenger, BlockHash, Ledger) ->
    case ?MODULE:find_gateway_info(Challenger, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw0} ->
            case ?MODULE:find_poc(OnionKeyHash, Ledger) of
                {error, not_found} ->
                    request_poc_(OnionKeyHash, SecretHash, Challenger, BlockHash, Ledger, Gw0, []);
                {error, _} ->
                    {error, fail_getting_poc};
                {ok, PoCs} ->
                    request_poc_(OnionKeyHash, SecretHash, Challenger, BlockHash, Ledger, Gw0, PoCs)
            end
    end.

request_poc_(OnionKeyHash, SecretHash, Challenger, BlockHash, Ledger, Gw0, PoCs) ->
    case blockchain_ledger_gateway_v2:last_poc_onion_key_hash(Gw0) of
        undefined ->
            ok;
        LastOnionKeyHash  ->
            case delete_poc(LastOnionKeyHash, Challenger, Ledger) of
                {error, _}=Error ->
                    Error;
                ok -> ok
            end
    end,
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    Gw1 = blockchain_ledger_gateway_v2:last_poc_challenge(Height+1, Gw0),
    Gw2 = blockchain_ledger_gateway_v2:last_poc_onion_key_hash(OnionKeyHash, Gw1),
    GwBin = blockchain_ledger_gateway_v2:serialize(Gw2),
    AGwsCF = active_gateways_cf(Ledger),
    ok = cache_put(Ledger, AGwsCF, Challenger, GwBin),

    PoC = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash, Challenger, BlockHash),
    PoCBin = blockchain_ledger_poc_v2:serialize(PoC),
    BinPoCs = erlang:term_to_binary([PoCBin|lists:map(fun blockchain_ledger_poc_v2:serialize/1, PoCs)]),
    PoCsCF = pocs_cf(Ledger),
    cache_put(Ledger, PoCsCF, OnionKeyHash, BinPoCs).

-spec delete_poc(binary(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, any()}.
delete_poc(OnionKeyHash, Challenger, Ledger) ->
    case ?MODULE:find_poc(OnionKeyHash, Ledger) of
        {error, not_found} ->
            ok;
        {error, _}=Error ->
            Error;
        {ok, PoCs} ->
            FilteredPoCs = lists:filter(
                fun(PoC) ->
                    blockchain_ledger_poc_v2:challenger(PoC) =/= Challenger
                end,
                PoCs
            ),
            case FilteredPoCs of
                [] ->
                    ?MODULE:delete_pocs(OnionKeyHash, Ledger);
                _ ->
                    BinPoCs = erlang:term_to_binary(lists:map(fun blockchain_ledger_poc_v2:serialize/1, FilteredPoCs)),
                    PoCsCF = pocs_cf(Ledger),
                    cache_put(Ledger, PoCsCF, OnionKeyHash, BinPoCs)
            end
    end.

-spec delete_pocs(binary(), ledger()) -> ok | {error, any()}.
delete_pocs(OnionKeyHash, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    cache_delete(Ledger, PoCsCF, OnionKeyHash).

maybe_gc_pocs(Chain) ->
    Ledger0 = blockchain:ledger(Chain),
    {ok, Height} = current_height(Ledger0),
    Version = case ?MODULE:config(?poc_version, Ledger0) of
                  {ok, V} -> V;
                  _ -> 1
              end,
    case Version > 3 andalso Height rem 100 == 0 of
        true ->
            lager:debug("gcing old pocs"),
            PoCInterval = blockchain_utils:challenge_interval(Ledger0),
            Ledger = new_context(Ledger0),
            PoCsCF = pocs_cf(Ledger),
            Alters =
                cache_fold(
                  Ledger,
                  PoCsCF,
                  fun({KeyHash, BinPoCs}, Acc) ->
                          %% this CF contains all the poc request state that needs to be retained
                          %% between request and receipt validation.  however, it's possible that
                          %% both requests stop and a receipt never comes, which leads to stale (and
                          %% in some cases differing) data in the ledger.  here, we pull that data
                          %% out and delete anything that's too old, as determined by being older
                          %% than twice the request interval, which controls receipt validity.
                          SPoCs = erlang:binary_to_term(BinPoCs),
                          PoCs = lists:map(fun blockchain_ledger_poc_v2:deserialize/1, SPoCs),
                          FPoCs =
                              lists:filter(
                                fun(PoC) ->
                                        H = blockchain_ledger_poc_v2:block_hash(PoC),
                                        case H of
                                            <<>> ->
                                                %% pre-upgrade pocs are ancient
                                                false;
                                            _ ->
                                                {ok, B} = blockchain:get_block(H, Chain),
                                                BH = blockchain_block:height(B),
                                                (Height - BH) < PoCInterval * 2
                                        end
                                end, PoCs),
                          case FPoCs == PoCs of
                              true ->
                                  Acc;
                              _ ->
                                  [{KeyHash, FPoCs} | Acc]
                          end
                  end,
                  []
                 ),
            lager:debug("Alterations ~p", [Alters]),
            %% here we have two clauses, so we don't uselessly store a [] in the ledger, as that
            %% might cause drift, depending on the timing of the GC and a few other factors.
            lists:foreach(
              fun({KeyHash, []}) ->
                      cache_delete(Ledger, PoCsCF, KeyHash);
                 ({KeyHash, NewPoCs}) ->
                      BinPoCs = erlang:term_to_binary(
                                  lists:map(fun blockchain_ledger_poc_v2:serialize/1,
                                            NewPoCs)),
                      cache_put(Ledger, PoCsCF, KeyHash, BinPoCs)
              end,
              Alters),
            commit_context(Ledger),
            ok;
        _ ->
            ok
    end.

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
            {error, not_found};
        Error ->
            Error
    end.

-spec credit_account(libp2p_crypto:pubkey_bin(), integer(), ledger()) -> ok | {error, any()}.
credit_account(Address, Amount, Ledger) ->
    EntriesCF = entries_cf(Ledger),
    case ?MODULE:find_entry(Address, Ledger) of
        {error, not_found} ->
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_dc(Address :: libp2p_crypto:pubkey_bin(), Fee :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
debit_dc(_Address, 0,_Ledger) ->
    ok;
debit_dc(Address, Fee, Ledger) ->
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            case (Balance - Fee) >= 0 of
                true ->
                    Entry1 = blockchain_ledger_data_credits_entry_v1:new(
                        blockchain_ledger_data_credits_entry_v1:nonce(Entry),
                        (Balance - Fee)
                    ),
                    Bin = blockchain_ledger_data_credits_entry_v1:serialize(Entry1),
                    EntriesCF = dc_entries_cf(Ledger),
                    cache_put(Ledger, EntriesCF, Address, Bin);
                false ->
                    {error, {insufficient_balance, Fee, Balance}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
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
                            {error, {insufficient_balance, Amount, Balance}}
                    end;
                false ->
                    {error, {bad_nonce, {payment, Nonce, blockchain_ledger_entry_v1:nonce(Entry)}}}
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
                    {error, {insufficient_balance, Amount, Balance}};
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
            {error, not_found};
        Error ->
            Error
    end.

-spec credit_dc(libp2p_crypto:pubkey_bin(), integer(), ledger()) -> ok | {error, any()}.
credit_dc(Address, Amount, Ledger) ->
    EntriesCF = dc_entries_cf(Ledger),
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, not_found} ->
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

-spec debit_dc(Address :: libp2p_crypto:pubkey_bin(), Fee :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
debit_dc(_Address, 0,_Ledger) ->
    ok;
debit_dc(Address, Fee, Ledger) ->
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            case (Balance - Fee) >= 0 of
                true ->
                    Entry1 = blockchain_ledger_data_credits_entry_v1:new(
                        blockchain_ledger_data_credits_entry_v1:nonce(Entry),
                        (Balance - Fee)
                    ),
                    Bin = blockchain_ledger_data_credits_entry_v1:serialize(Entry1),
                    EntriesCF = dc_entries_cf(Ledger),
                    cache_put(Ledger, EntriesCF, Address, Bin);
                false ->
                    {error, {insufficient_balance, Fee, Balance}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec credit_dc(libp2p_crypto:pubkey_bin(), integer(), ledger()) -> ok | {error, any()}.
credit_dc(Address, Amount, Ledger) ->
    EntriesCF = dc_entries_cf(Ledger),
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, not_found} ->
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_fee(Address :: libp2p_crypto:pubkey_bin(), Fee :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
debit_fee(_Address, 0,_Ledger) ->
    ok;
debit_fee(Address, Fee, Ledger) ->
    case ?MODULE:find_dc_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_data_credits_entry_v1:balance(Entry),
            case (Balance - Fee) >= 0 of
                true ->
                    Entry1 = blockchain_ledger_data_credits_entry_v1:new(
                        blockchain_ledger_data_credits_entry_v1:nonce(Entry),
                        (Balance - Fee)
                    ),
                    Bin = blockchain_ledger_data_credits_entry_v1:serialize(Entry1),
                    EntriesCF = dc_entries_cf(Ledger),
                    cache_put(Ledger, EntriesCF, Address, Bin);
                false ->
                    {error, {insufficient_balance, Fee, Balance}}
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
                    {error, {insufficient_balance, Amount, Balance}};
                true ->
                    ok
            end
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
                            {error, {insufficient_balance, Amount, Balance}}
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
                    {error, {insufficient_balance, Amount, Balance}};
                true ->
                    ok
            end
    end.

-spec find_ouis(binary(), ledger()) -> {ok, [non_neg_integer()]} | {error, any()}.
find_ouis(Owner, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    case cache_get(Ledger, RoutingCF, Owner, []) of
        {ok, Bin} -> {ok, erlang:binary_to_term(Bin)};
        not_found -> {ok, []};
        Error -> Error

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

-spec redeem_htlc(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, any()}.
redeem_htlc(Address, Payee, Ledger) ->
    case ?MODULE:find_htlc(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, HTLC} ->
            Amount = blockchain_ledger_htlc_v1:balance(HTLC),
            case ?MODULE:credit_account(Payee, Amount, Ledger) of
                {error, _}=Error -> Error;
                ok ->
                    HTLCsCF = htlcs_cf(Ledger),
                    cache_delete(Ledger, HTLCsCF, Address)
            end
    end.


-spec get_oui_counter(ledger()) -> {ok, pos_integer()} | {error, any()}.
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

-spec increment_oui_counter(pos_integer(), ledger()) -> {ok, pos_integer()} | {error, any()}.
increment_oui_counter(OUI, Ledger) ->
    case ?MODULE:get_oui_counter(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, OUICounter} when OUICounter + 1 == OUI ->
            DefaultCF = default_cf(Ledger),
            ok = cache_put(Ledger, DefaultCF, ?OUI_COUNTER, <<OUI:32/little-unsigned-integer>>),
            {ok, OUI};
        {ok, OUICounter} ->
            {error, {invalid_oui, OUI, OUICounter+1}}
    end.

-spec add_oui(binary(), [binary()], pos_integer(), ledger()) -> ok | {error, any()}.
add_oui(Owner, Addresses, OUI, Ledger) ->
    case ?MODULE:increment_oui_counter(OUI, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, OUI} ->
            RoutingCF = routing_cf(Ledger),
            Routing = blockchain_ledger_routing_v1:new(OUI, Owner, Addresses, 0),
            Bin = blockchain_ledger_routing_v1:serialize(Routing),
            case ?MODULE:find_ouis(Owner, Ledger) of
                {error, _}=Error ->
                    Error;
                {ok, OUIs} ->
                    ok = cache_put(Ledger, RoutingCF, <<OUI:32/little-unsigned-integer>>, Bin),
                    cache_put(Ledger, RoutingCF, Owner, erlang:term_to_binary([OUI|OUIs]))
            end
    end.

-spec find_routing(non_neg_integer(), ledger()) -> {ok, blockchain_ledger_routing_v1:routing()}
                                                   | {error, any()}.
find_routing(OUI, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    case cache_get(Ledger, RoutingCF, <<OUI:32/little-unsigned-integer>>, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_routing_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec add_routing(binary(), non_neg_integer(), [binary()], non_neg_integer(), ledger()) -> ok | {error, any()}.
add_routing(Owner, OUI, Addresses, Nonce, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    Routing = blockchain_ledger_routing_v1:new(OUI, Owner, Addresses, Nonce),
    Bin = blockchain_ledger_routing_v1:serialize(Routing),
    cache_put(Ledger, RoutingCF, <<OUI:32/little-unsigned-integer>>, Bin).

-spec find_state_channel(binary(), libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_state_channel_v1:state_channel()} | {error, any()}.
find_state_channel(ID, Owner, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    Key = state_channel_key(ID, Owner),
    case cache_get(Ledger, SCsCF, Key, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_state_channel_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec find_state_channels_by_owner(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, [binary()]} | {error, any()}.
find_state_channels_by_owner(Owner, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    case cache_get(Ledger, SCsCF, Owner, []) of
        {ok, BinEntry} ->
            {ok, erlang:binary_to_term(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec add_state_channel(binary(), libp2p_crypto:pubkey_bin(), non_neg_integer(), pos_integer(), ledger()) -> ok | {error, any()}.
add_state_channel(ID, Owner, Amount, Timer, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    {ok, CurrHeight} = ?MODULE:current_height(Ledger),
    Routing = blockchain_ledger_state_channel_v1:new(ID, Owner, Amount, CurrHeight+Timer),
    Bin = blockchain_ledger_state_channel_v1:serialize(Routing),
    Key = state_channel_key(ID, Owner),
    ok = cache_put(Ledger, SCsCF, Key, Bin),
    case ?MODULE:find_state_channels_by_owner(Owner, Ledger) of
        {error, not_found} ->
            cache_put(Ledger, SCsCF, Owner, erlang:term_to_binary([ID]));
        {error, _}=Error ->
            Error;
        {ok, SCIDs} ->
            cache_put(Ledger, SCsCF, Owner, erlang:term_to_binary([ID|SCIDs]))
    end.

-spec close_state_channel(binary(), libp2p_crypto:pubkey_bin(), ledger()) -> ok.
close_state_channel(ID, Owner, Ledger) ->
    SCsCF = state_channels_cf(Ledger),
    Key = state_channel_key(ID, Owner),
    ok = cache_delete(Ledger, SCsCF, Key),
    case ?MODULE:find_state_channels_by_owner(Owner, Ledger) of
        {error, not_found} ->
            ok;
        {error, _}=Error ->
            Error;
        {ok, SCIDs} ->
            cache_put(Ledger, SCsCF, Owner, erlang:term_to_binary(SCIDs -- [ID]))
    end.

clean(#ledger_v1{dir=Dir, db=DB}=L) ->
    delete_context(L),
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = rocksdb:close(DB),
    rocksdb:destroy(DBDir, []).

close(#ledger_v1{db=DB}) ->
    rocksdb:close(DB).

compact(#ledger_v1{db=DB, active=Active, delayed=Delayed}) ->
    rocksdb:compact_range(DB, undefined, undefined, []),
    compact_ledger(DB, Active),
    compact_ledger(DB, Delayed),
    ok.

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

-spec state_channel_key(libp2p_crypto:pubkey_bin(), binary()) -> binary().
state_channel_key(ID, Owner) ->
    <<Owner/binary, ID/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% need to prefix to keep people from messing with existing names on accident
%% @end
%%--------------------------------------------------------------------
var_name(Name) ->
    <<"$var_", (atom_to_binary(Name, utf8))/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec context_cache(undefined | ets:tid(), ledger()) -> ledger().
context_cache(Cache, #ledger_v1{mode=active, active=Active}=Ledger) ->
    Ledger#ledger_v1{active=Active#sub_ledger_v1{cache=Cache}};
context_cache(Cache, #ledger_v1{mode=delayed, delayed=Delayed}=Ledger) ->
    Ledger#ledger_v1{delayed=Delayed#sub_ledger_v1{cache=Cache}}.

-spec default_cf(ledger()) -> rocksdb:cf_handle().
default_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{default=DefaultCF}}) ->
    DefaultCF;
default_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{default=DefaultCF}}) ->
    DefaultCF.

-spec active_gateways_cf(ledger()) -> rocksdb:cf_handle().
active_gateways_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{active_gateways=AGCF}}) ->
    AGCF;
active_gateways_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{active_gateways=AGCF}}) ->
    AGCF.

-spec entries_cf(ledger()) -> rocksdb:cf_handle().
entries_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{entries=EntriesCF}}) ->
    EntriesCF;
entries_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{entries=EntriesCF}}) ->
    EntriesCF.

-spec dc_entries_cf(ledger()) -> rocksdb:cf_handle().
dc_entries_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{dc_entries=EntriesCF}}) ->
    EntriesCF;
dc_entries_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{dc_entries=EntriesCF}}) ->
    EntriesCF.

-spec htlcs_cf(ledger()) -> rocksdb:cf_handle().
htlcs_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{htlcs=HTLCsCF}}) ->
    HTLCsCF;
htlcs_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{htlcs=HTLCsCF}}) ->
    HTLCsCF.

-spec pocs_cf(ledger()) -> rocksdb:cf_handle().
pocs_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{pocs=PoCsCF}}) ->
    PoCsCF;
pocs_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{pocs=PoCsCF}}) ->
    PoCsCF.

-spec securities_cf(ledger()) -> rocksdb:cf_handle().
securities_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{securities=SecuritiesCF}}) ->
    SecuritiesCF;
securities_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{securities=SecuritiesCF}}) ->
    SecuritiesCF.

-spec routing_cf(ledger()) -> rocksdb:cf_handle().
routing_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{routing=RoutingCF}}) ->
    RoutingCF;
routing_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{routing=RoutingCF}}) ->
    RoutingCF.

-spec state_channels_cf(ledger()) -> rocksdb:cf_handle().
state_channels_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{state_channels=SCsCF}}) ->
    SCsCF;
state_channels_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{state_channels=SCsCF}}) ->
    SCsCF.

-spec cache_put(ledger(), rocksdb:cf_handle(), binary(), binary()) -> ok.
cache_put(Ledger, CF, Key, Value) ->
    Cache = context_cache(Ledger),
    true = ets:insert(Cache, {{CF, Key}, Value}),
    ok.

-spec cache_get(ledger(), rocksdb:cf_handle(), any(), [any()]) -> {ok, any()} | {error, any()} | not_found.
cache_get(#ledger_v1{db=DB}=Ledger, CF, Key, Options) ->
    case context_cache(Ledger) of
        undefined ->
            rocksdb:get(DB, CF, Key, maybe_use_snapshot(Ledger, Options));
        Cache ->
            case ets:lookup(Cache, {CF, Key}) of
                [] ->
                    cache_get(context_cache(undefined, Ledger), CF, Key, Options);
                [{_, ?CACHE_TOMBSTONE}] ->
                    %% deleted in the cache
                    not_found;
                [{_, Value}] ->
                    {ok, Value}
            end
    end.

-spec cache_delete(ledger(), rocksdb:cf_handle(), any()) -> ok.
cache_delete(Ledger, CF, Key) ->
    Cache = context_cache(Ledger),
    true = ets:insert(Cache, {{CF, Key}, ?CACHE_TOMBSTONE}),
    ok.

cache_fold(Ledger, CF, Fun0, Acc) ->
    cache_fold(Ledger, CF, Fun0, Acc, []).

cache_fold(Ledger, CF, Fun0, Acc, Opts) ->
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
        undefined ->
            %% fold rocks directly
            rocks_fold(Ledger, CF, Opts, Fun0, Acc);
        Cache ->
            %% fold using the cache wrapper
            Fun = mk_cache_fold_fun(Cache, CF, Start, End, Fun0),
            Keys = lists:usort(lists:flatten(ets:match(Cache, {{'_', '$1'}, '_'}))),
            {TrailingKeys, Res0} = rocks_fold(Ledger, CF, Opts, Fun, {Keys, Acc}),
            process_fun(TrailingKeys, Cache, CF, Start, End, Fun0, Res0)
    end.

rocks_fold(Ledger = #ledger_v1{db=DB}, CF, Opts0, Fun, Acc) ->
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
                  [{_Key, CacheValue}] ->
                      Fun({K, CacheValue}, A);
                  [] ->
                      A
              end
      end, Acc, ToProcess).

-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),

    GlobalOpts = application:get_env(rocksdb, global_opts, []),

    DBOptions = [{create_if_missing, true}, {atomic_flush, true}] ++ GlobalOpts,

    CFOpts = GlobalOpts,

    DefaultCFs = ["default", "active_gateways", "entries", "dc_entries", "htlcs", "pocs", "securities", "routing", "state_channels",
                  "delayed_default", "delayed_active_gateways", "delayed_entries", "delayed_dc_entries", "delayed_htlcs",
                  "delayed_pocs", "delayed_securities", "delayed_routing", "delayed_state_channels"],
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

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

batch_from_cache(ETS) ->
    {ok, Batch} = rocksdb:batch(),
    ets:foldl(fun({{CF, Key}, ?CACHE_TOMBSTONE}, Acc) ->
                      rocksdb:batch_delete(Acc, CF, Key),
                      Acc;
                 ({{CF, Key}, Value}, Acc) ->
                      rocksdb:batch_put(Acc, CF, Key, Value),
                      Acc
              end, Batch, ETS).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

find_entry_test() ->
    BaseDir = test_utils:tmp_dir("find_entry_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, find_entry(<<"test">>, Ledger)),
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
    ?assertEqual(#{}, active_gateways(Ledger)).

add_gateway_test() ->
    BaseDir = test_utils:tmp_dir("add_gateway_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger1),
    ok = commit_context(Ledger1),
    ?assertMatch(
        {ok, _},
        find_gateway_info(<<"gw_address">>, Ledger)
    ),
    ?assertEqual({error, gateway_already_active}, add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger)),
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
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_account(<<"address">>, 9999, 1, Ledger)),
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
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_fee(<<"address">>, 9999, Ledger)),
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
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_security(<<"address">>, 9999, 1, Ledger)),
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
    BaseDir = test_utils:tmp_dir("poc_test"),
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
                        {ok, 12}
                end),

    ?assertEqual({error, not_found}, find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = add_gateway(OwnerAddr, Challenger0, Location, Nonce, L),
            ok = add_gateway(OwnerAddr, Challenger1, Location, Nonce, L),
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger0, BlockHash, L)
        end,
        Ledger
    ),
    PoC0 = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash0, Challenger0, BlockHash),
    ?assertEqual({ok, [PoC0]} ,find_poc(OnionKeyHash0, Ledger)),
    {ok, GwInfo0} = find_gateway_info(Challenger0, Ledger),
    ?assertEqual(1, blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo0)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GwInfo0)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger1, BlockHash, L)
        end,
        Ledger
    ),
    PoC1 = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash0, Challenger1, BlockHash),
    ?assertEqual({ok, [PoC1, PoC0]}, find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = delete_poc(OnionKeyHash0, Challenger0, L)
        end,
        Ledger
    ),
    ?assertEqual({ok, [PoC1]} ,find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = delete_poc(OnionKeyHash0, Challenger1, L)
        end,
        Ledger
    ),
    ?assertEqual({error, not_found} ,find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger0, BlockHash, L)
        end,
        Ledger
    ),
    ?assertEqual({ok, [PoC0]} ,find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash1, SecretHash, Challenger0, BlockHash, L)
        end,
        Ledger
    ),
    ?assertEqual({error, not_found} ,find_poc(OnionKeyHash0, Ledger)),
    PoC2 = blockchain_ledger_poc_v2:new(SecretHash, OnionKeyHash1, Challenger0, BlockHash),
    ?assertEqual({ok, [PoC2]}, find_poc(OnionKeyHash1, Ledger)),
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

routing_test() ->
    BaseDir = test_utils:tmp_dir("routing_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ?assertEqual({error, not_found}, find_routing(1, Ledger1)),
    ?assertEqual({ok, 0}, get_oui_counter(Ledger1)),

    Ledger2 = new_context(Ledger),
    ok = add_oui(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, Ledger2),
    ok = commit_context(Ledger2),
    {ok, Routing0} = find_routing(1, Ledger),
    ?assertEqual(<<"owner">>, blockchain_ledger_routing_v1:owner(Routing0)),
    ?assertEqual(1, blockchain_ledger_routing_v1:oui(Routing0)),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], blockchain_ledger_routing_v1:addresses(Routing0)),
    ?assertEqual(0, blockchain_ledger_routing_v1:nonce(Routing0)),

    Ledger3 = new_context(Ledger),
    ok = add_oui(<<"owner2">>, [<<"/p2p/random">>], 2, Ledger3),
    ok = commit_context(Ledger3),
    {ok, Routing1} = find_routing(2, Ledger),
    ?assertEqual(<<"owner2">>, blockchain_ledger_routing_v1:owner(Routing1)),
    ?assertEqual(2, blockchain_ledger_routing_v1:oui(Routing1)),
    ?assertEqual([<<"/p2p/random">>], blockchain_ledger_routing_v1:addresses(Routing1)),
    ?assertEqual(0, blockchain_ledger_routing_v1:nonce(Routing1)),

    Ledger4 = new_context(Ledger),
    ok = add_routing(<<"owner2">>, 2, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, Ledger4),
    ok = commit_context(Ledger4),
    {ok, Routing2} = find_routing(2, Ledger),
    ?assertEqual(<<"owner2">>, blockchain_ledger_routing_v1:owner(Routing2)),
    ?assertEqual(2, blockchain_ledger_routing_v1:oui(Routing2)),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], blockchain_ledger_routing_v1:addresses(Routing2)),
    ?assertEqual(1, blockchain_ledger_routing_v1:nonce(Routing2)),

    ?assertEqual({ok, [1]}, blockchain_ledger_v1:find_ouis(<<"owner">>, Ledger)),
    ?assertEqual({ok, [2]}, blockchain_ledger_v1:find_ouis(<<"owner2">>, Ledger)),
    test_utils:cleanup_tmp_dir(BaseDir),
    ok.

state_channels_test() ->
    BaseDir = test_utils:tmp_dir("state_channels_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ID = crypto:strong_rand_bytes(32),
    Owner = <<"owner">>,

    ?assertEqual({error, not_found}, find_state_channel(ID, Owner, Ledger1)),
    ?assertEqual({error, not_found}, find_state_channels_by_owner(Owner, Ledger1)),

    Ledger2 = new_context(Ledger),
    ok = add_state_channel(ID, Owner, 12, 10, Ledger2),
    ok = commit_context(Ledger2),
    {ok, SC} = find_state_channel(ID, Owner, Ledger),
    ?assertEqual(ID, blockchain_ledger_state_channel_v1:id(SC)),
    ?assertEqual(Owner, blockchain_ledger_state_channel_v1:owner(SC)),
    ?assertEqual(12, blockchain_ledger_state_channel_v1:amount(SC)),
    ?assertEqual({ok, [ID]}, find_state_channels_by_owner(Owner, Ledger)),

    Ledger3 = new_context(Ledger),
    ok = close_state_channel(ID, Owner, Ledger3),
    ok = commit_context(Ledger3),
    ?assertEqual({error, not_found}, find_state_channel(ID, Owner, Ledger)),
    ?assertEqual({ok, []}, find_state_channels_by_owner(Owner, Ledger)),

    ok.

-endif.
