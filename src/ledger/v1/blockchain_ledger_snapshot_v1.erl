-module(blockchain_ledger_snapshot_v1).

-include("blockchain_ledger_snapshot_v1.hrl").
-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-export([
         serialize/1,
         deserialize/1,
         deserialize/2,

         is_v6/1,
         version/1,

         snapshot/3,
         snapshot/4,
         import/5,
         load_into_ledger/3,
         load_blocks/3,

         get_blocks/1,
         get_infos/1,
         get_h3dex/1,

         height/1,
         hash/1,
         blocks_info/1,

         diff/2
        ]).

-ifdef(TEST).
-export([
         deserialize_field/2
        ]).
-endif.

-export_type([
    snapshot/0,
    snapshot_error/0
]).

-type state_channel() ::
      {blockchain_ledger_state_channel_v1, blockchain_ledger_state_channel_v1:state_channel()}
    | {blockchain_ledger_state_channel_v2, blockchain_ledger_state_channel_v2:state_channel_v2()}.

%% this assumes that everything will have loaded the genesis block
%% already.  I'm not sure that's totally safe in all cases, but it's
%% the right thing for the spots and easy to work around elsewhere.
-define(min_height, 2).

-type kv_stream() ::
    kv_stream(binary(), binary()).

%% TODO Should be: -type stream(A) :: fun(() -> none | {some, {A, t(A)}}).
-type kv_stream(K, V) ::
    fun(() -> ok | {K, V, kv_stream()}).

%% this is temporary, something to work with easily while we nail the
%% format and functionality down.  once it's final we can move on to a
%% more permanent and less flexible format, like protobufs, or
%% cauterize.
-type snapshot_v5() ::
    #{
        version           => v5,
        current_height    => non_neg_integer(),
        transaction_fee   => non_neg_integer(),
        consensus_members => [libp2p_crypto:pubkey_bin()],
        election_height   => non_neg_integer(),
        election_epoch    => non_neg_integer(),
        delayed_vars      => [{Height :: integer(), [{Hash :: binary(), Vars :: term()}]}], % TODO Type of Vars
        threshold_txns    => [{binary(), binary()}], % According to spec of blockchain_ledger_v1:snapshot_threshold_txns
        master_key        => binary(),
        multi_keys        => [binary()],
        vars_nonce        => pos_integer(),
        vars              => [{Name :: binary(), Val :: term()}], % XXX Val can be anything.
        htlcs             => [{Address :: binary(), blockchain_ledger_htlc_v1:htlc()}],
        ouis              => [{non_neg_integer(), blockchain_ledger_routing_v1:routing()}],
        subnets           => [{Subnet :: binary(), OUI :: non_neg_integer()}],
        oui_counter       => pos_integer(),
        hexes             => [{list, blockchain_ledger_v1:hexmap()} | {h3:h3_index(), [libp2p_crypto:pubkey_bin()]}],
        h3dex             => [{integer(), [binary()]}],
        state_channels    => [{binary(), state_channel()}],
        blocks            => [blockchain_block:block()],
        oracle_price      => non_neg_integer(),
        oracle_price_list => [blockchain_ledger_oracle_price_entry:oracle_price_entry()],
        key_raw()         => [{binary(), binary()}]
    }.

-type file_position() ::
    {
        file:fd(),
        Pos :: non_neg_integer(),
        Len :: pos_integer()
    }.

-type snapshot_v6() ::
    #{
        version                   => v6,
        key_non_version_non_raw() => binary() | file_position(),
        key_raw()                 => kv_stream()
    }.

-type key() ::
    version | key_non_version_non_raw().

-type key_non_version_non_raw() ::
      current_height
    | transaction_fee
    | consensus_members
    | election_height
    | election_epoch
    | delayed_vars
    | threshold_txns
    | master_key
    | multi_keys
    | vars_nonce
    | vars
    | htlcs
    | ouis
    | subnets
    | oui_counter
    | hexes
    | h3dex
    | state_channels
    | blocks
    | infos
    | oracle_price
    | oracle_price_list
    | upgrades
    | net_overage
    | hnt_burned
    .

-type key_raw() ::
      gateways
    | pocs
    | accounts
    | dc_accounts
    | security_accounts
    .

-type snapshot_of_any_version() ::
      #blockchain_snapshot_v1{}
    | #blockchain_snapshot_v2{}
    | #blockchain_snapshot_v3{}
    | #blockchain_snapshot_v4{}
    | snapshot_v5()
    | snapshot_v6()
    .

-type snapshot() :: snapshot_v6().

-type snapshot_error() ::
    {
        error_taking_snapshot,
        Class :: error | exit | throw,
        Reason :: term(),
        Stack :: term()
    }.

-spec snapshot(blockchain_ledger_v1:ledger(), [binary()], [binary()]) ->
    {ok, snapshot()}
    | {error, killed | snapshot_error()}.
snapshot(Ledger0, Blocks, Infos) ->
    snapshot(Ledger0, Blocks, Infos, delayed).

-spec snapshot(
    blockchain_ledger_v1:ledger(),
    [binary()],
    [binary()],
    blockchain_ledger_v1:mode()
) ->
    {ok, snapshot()} | {error, killed | snapshot_error()}.
snapshot(Ledger0, Blocks, Infos, Mode) ->
    Parent = self(),
    Ref = make_ref(),
    {_Pid, MonitorRef} =
        spawn_opt(
            fun Retry() ->
                Ledger = blockchain_ledger_v1:mode(Mode, Ledger0),
                {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),
                %% this should not leak a huge amount of atoms
                Regname = list_to_atom("snapshot_"++integer_to_list(CurrHeight)),
                try register(Regname, self()) of
                    true ->
                        Res = generate_snapshot(Ledger0, Blocks, Infos, Mode),
                        %% deliver to the caller
                        Parent ! {Ref, Res},
                        %% deliver to anyone else blocking
                        deliver(Res)
                catch error:badarg ->
                    %% already a snapshot generation running, just attach to that
                    IntMonitorRef = erlang:monitor(process, Regname),
                    whereis(Regname) ! {deliver, self(), Ref},
                    %% wait for the result from the already-running process
                    receive
                        {Ref, Res} ->
                            Parent ! {Ref, Res};
                        {'DOWN', IntMonitorRef, process, _, noproc} ->
                            %% we were unable to attach to an existing process
                            %% before it terminated
                            Retry();
                        {'DOWN', IntMonitorRef, process, _, killed} ->
                            %% the already running process OOMed
                            Parent ! {Ref, {error, killed}}
                    end
                end
            end,
            [
                {
                    max_heap_size,
                    (fun() ->
                        Mb = application:get_env(blockchain, snapshot_memory_limit, 200),
                        Mb * 1024 * 1024 div erlang:system_info(wordsize)
                    end)()
                },
                {fullsweep_after, 0},
                monitor
            ]
        ),
    receive
        {Ref, Res} ->
            Res;
        {'DOWN', MonitorRef, process, _, killed} ->
            {error, killed}
    end.

deliver(Res) ->
    receive
        {deliver, Pid, Ref} ->
            Pid ! {Ref, Res},
            deliver(Res)
    after 0 ->
              ok
    end.

-spec generate_snapshot(
    blockchain_ledger_v1:ledger(),
    [binary()],
    [binary()],
    blockchain_ledger_v1:mode()
) ->
    {ok, snapshot()} | {error, snapshot_error()}.
generate_snapshot(Ledger, Blocks, Infos, Mode) ->
    case generate_snapshot_v5(Ledger, Blocks, Infos, Mode) of
        {ok, #{version := v5}=Snap} ->
            {ok, v5_to_v6(Snap)};
        {error, _}=Err ->
            Err
    end.

-spec generate_snapshot_v5(
    blockchain_ledger_v1:ledger(),
    [binary()],
    [binary()],
    blockchain_ledger_v1:mode()
) ->
    {ok, snapshot_v5()} | {error, snapshot_error()}.
generate_snapshot_v5(Ledger0, Blocks, Infos, Mode) ->
    try
        Ledger = blockchain_ledger_v1:mode(Mode, Ledger0),
        {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),
        {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(Ledger),
        {ok, ElectionHeight} = blockchain_ledger_v1:election_height(Ledger),
        {ok, ElectionEpoch} = blockchain_ledger_v1:election_epoch(Ledger),
        {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
        MultiKeys = case blockchain_ledger_v1:multi_keys(Ledger) of
                        {ok, Keys} -> Keys;
                        _ -> []
                    end,
        DelayedVars = blockchain_ledger_v1:snapshot_delayed_vars(Ledger),
        ThresholdTxns = blockchain_ledger_v1:snapshot_threshold_txns(Ledger),
        {ok, VarsNonce} = blockchain_ledger_v1:vars_nonce(Ledger),
        Vars = blockchain_ledger_v1:snapshot_vars(Ledger),
        Gateways = blockchain_ledger_v1:snapshot_raw_gateways(Ledger),
        Validators = blockchain_ledger_v1:snapshot_validators(Ledger),
        DelayedHNT = blockchain_ledger_v1:snapshot_delayed_hnt(Ledger),
        PoCs = blockchain_ledger_v1:snapshot_raw_pocs(Ledger),
        Accounts = blockchain_ledger_v1:snapshot_raw_accounts(Ledger),
        DCAccounts = blockchain_ledger_v1:snapshot_raw_dc_accounts(Ledger),
        SecurityAccounts = blockchain_ledger_v1:snapshot_raw_security_accounts(Ledger),

        HTLCs = blockchain_ledger_v1:snapshot_htlcs(Ledger),

        OUIs = blockchain_ledger_v1:snapshot_ouis(Ledger),
        Subnets = blockchain_ledger_v1:snapshot_subnets(Ledger),
        {ok, OUICounter} = blockchain_ledger_v1:get_oui_counter(Ledger),

        Hexes = blockchain_ledger_v1:snapshot_hexes(Ledger),
        H3dex = blockchain_ledger_v1:snapshot_h3dex(Ledger),

        StateChannels = blockchain_ledger_v1:snapshot_state_channels(Ledger),

        {ok, OraclePrice} = blockchain_ledger_v1:current_oracle_price(Ledger),
        {ok, OraclePriceList} = blockchain_ledger_v1:current_oracle_price_list(Ledger),

        {ok, NetOverage} = blockchain_ledger_v1:net_overage(Ledger),
        {ok, HntBurned} = blockchain_ledger_v1:hnt_burned(Ledger),

        %% use the active ledger here because that's where upgrades are marked
        Upgrades = blockchain:get_upgrades(blockchain_ledger_v1:mode(active, Ledger0)),
        Pairs =
            [
                {version          , v5},
                {current_height   , CurrHeight},
                {transaction_fee  , 0},
                {consensus_members, ConsensusMembers},
                {election_height  , ElectionHeight},
                {election_epoch   , ElectionEpoch},
                {delayed_vars     , DelayedVars},
                {threshold_txns   , ThresholdTxns},
                {master_key       , MasterKey},
                {multi_keys       , MultiKeys},
                {vars_nonce       , VarsNonce},
                {vars             , Vars},
                {gateways         , Gateways},
                {validators       , Validators},
                {delayed_hnt      , DelayedHNT},
                {pocs             , PoCs},
                {accounts         , Accounts},
                {dc_accounts      , DCAccounts},
                {security_accounts, SecurityAccounts},
                {htlcs            , HTLCs},
                {ouis             , OUIs},
                {subnets          , Subnets},
                {oui_counter      , OUICounter},
                {hexes            , Hexes},
                {h3dex            , H3dex},
                {state_channels   , StateChannels},
                {blocks           , Blocks},
                {infos            , Infos},
                {oracle_price     , OraclePrice},
                {oracle_price_list, OraclePriceList},
                {upgrades         , Upgrades},
                {net_overage      , NetOverage},
                {hnt_burned       , HntBurned}
             ],
        Snap = maps:from_list(Pairs),
        {ok, Snap}
    catch C:E:S ->
        {error, {error_taking_snapshot, C, E, S}}
    end.

%% simple framing with version, size, & snap
-spec frame(pos_integer(), iolist() | binary()) -> iolist().
frame(Vsn, Data) ->
    Siz = iolist_size(Data),
    [<<Vsn:8/integer>>, <<Siz:32/little-unsigned-integer>>, Data].

-spec frame_bin(pos_integer(), binary()) -> binary().
frame_bin(Vsn, Data) ->
    iolist_to_binary(frame(Vsn, Data)).

-spec serialize(snapshot_of_any_version()) ->
    iolist() | binary().
serialize(Snapshot) ->
    serialize(Snapshot, blocks).

-spec serialize(snapshot_of_any_version(), blocks | noblocks) ->
    iolist() | binary().
serialize(Snapshot, BlocksOrNoBlocks) ->
    Serialize =
        case version(Snapshot) of
            v6 -> fun serialize_v6/2;
            v5 -> fun serialize_v5/2;
            v4 -> fun serialize_v4/2;
            v3 -> fun serialize_v3/2;
            v2 -> fun serialize_v2/2;
            v1 -> fun serialize_v1/2
        end,
    Serialize(Snapshot, BlocksOrNoBlocks).

-spec serialize_v6(snapshot_v6(), blocks | noblocks) -> iolist().
serialize_v6(#{version := v6}=Snapshot0, BlocksOrNoBlocks) ->
    EmptyListBin = term_to_binary([]),
    Blocks =
        case BlocksOrNoBlocks of
            blocks ->
                maps:get(blocks, Snapshot0, EmptyListBin);
            noblocks ->
                EmptyListBin
        end,
    Infos =
        case BlocksOrNoBlocks of
            blocks ->
                maps:get(infos, Snapshot0, EmptyListBin);
            noblocks ->
                EmptyListBin
        end,

    Snapshot1 = maps:put(blocks, Blocks, Snapshot0),
    Snapshot2 = maps:put(infos, Infos, Snapshot1),

    Pairs = lists:keysort(1, maps:to_list(Snapshot2)),
    frame(6, serialize_pairs(Pairs)).

-spec serialize_v5(snapshot_v5(), noblocks) -> binary().
serialize_v5(Snapshot, noblocks) ->
    %% XXX serialize_v5 only gets called with noblocks
    Snapshot1 = Snapshot#{blocks => []},
    frame_bin(5, term_to_binary(Snapshot1, [{compressed, 9}])).

-spec serialize_v4(#blockchain_snapshot_v4{}, noblocks) -> binary().
serialize_v4(Snapshot, noblocks) ->
    %% XXX serialize_v4 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v4{blocks = []},
    frame_bin(4, term_to_binary(Snapshot1, [{compressed, 9}])).

-spec serialize_v3(#blockchain_snapshot_v3{}, noblocks) -> binary().
serialize_v3(Snapshot, noblocks) ->
    %% XXX serialize_v3 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v3{blocks = []},
    frame_bin(3, term_to_binary(Snapshot1, [{compressed, 9}])).

-spec serialize_v2(#blockchain_snapshot_v2{}, noblocks) -> binary().
serialize_v2(Snapshot, noblocks) ->
    %% XXX serialize_v2 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v2{blocks = []},
    frame_bin(2, term_to_binary(Snapshot1, [{compressed, 9}])).

-spec serialize_v1(#blockchain_snapshot_v1{}, noblocks) -> binary().
serialize_v1(Snapshot, noblocks) ->
    %% XXX serialize_v1 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v1{blocks = []},
    frame_bin(1, term_to_binary(Snapshot1, [{compressed, 9}])).

-spec deserialize(binary() | {file, filename:filename_all()}) ->
      {ok, snapshot()}
    | {error, bad_snapshot_hash}
    | {error, bad_snapshot_binary}.
deserialize(BinOrFile) ->
    deserialize(none, BinOrFile).

-spec deserialize(DigestOpt :: none | binary(), binary() | {file, filename:filename_all()}) ->
      {ok, snapshot()}
    | {error, bad_snapshot_hash}
    | {error, bad_snapshot_binary}.
deserialize(DigestOpt, {file, Filename}) ->
    {ok, FD} = file:open(Filename, [raw, read, binary, compressed]),
    {ok, <<Vsn:8/integer, Siz:32/little-unsigned-integer>>} = file:read(FD, 5),
    case Vsn of
        V when V < 6 ->
            %% these old versions cannot be read off disk, must be loaded
            file:close(FD),
            {ok, Bin} = file:read_file(Filename),
            deserialize(DigestOpt, Bin);
        6 ->
            Pairs = find_pairs_in_file(FD, 5, 5+Siz),
            {ok, upgrade(maps:from_list(Pairs ++ [{version, v6}]))}
    end;
deserialize(DigestOpt, <<Bin0/binary>>) ->
    try
        <<Vsn:8/integer, Siz:32/little-unsigned-integer, Bin:Siz/binary>> = Bin0,
        Snapshot =
            case Vsn of
                V when (V >= 1) and (V < 5) ->
                    binary_to_term(Bin);
                5 ->
                    #{version := v5} = S = maps:from_list(binary_to_term(Bin)),
                    S;
                6 ->
                    Pairs = deserialize_pairs(Bin),
                    maps:from_list(Pairs)
            end,
        case DigestOpt of
            %% if we don't care what the hash is,
            %% don't bother to compute it
            none -> {ok, upgrade(Snapshot)};
            Digest when is_binary(Digest)->
                case hash(Snapshot) of
                    Digest -> {ok, upgrade(Snapshot)};
                    _Other -> {error, bad_snapshot_hash}
                end
        end
    catch C:E:St ->
            lager:warning("deserialize failed: ~p:~p:~p", [C, E, St]),
            {error, bad_snapshot_binary}
    end.

%% sha will be stored externally
-spec import(blockchain:blockchain(), pos_integer(), binary(), snapshot(), binary() | {file, filename:filename_all()}) ->
    blockchain_ledger_v1:ledger().
import(Chain, Height, SHA, #{version := v6}=Snapshot, BinSnap) ->
    print_memory(),
    CLedger = blockchain:ledger(Chain),
    Dir = blockchain:dir(Chain),

    %% potentially open ledger with compaction disabled so
    %% we can bulk load

    %% clean the ledger in case we had a partial snapshot load
    blockchain_ledger_v1:clean(CLedger),

    Ledger0 = blockchain_ledger_v1:new(
      Dir,
      false,
      blockchain:db_handle(Chain),
      blockchain:blocks_cf(Chain),
      blockchain:heights_cf(Chain),
      blockchain:info_cf(Chain),
      %% these options taken from rocksdb's PrepareForBulkLoad()
      %% and are only used if allow_bulk_snapshot_loads is true
      lists:flatten([
                     [
                      {disable_auto_compactions, true},
                      {num_levels, 2},
                      {max_write_buffer_number, 10},
                      {min_write_buffer_number_to_merge, 1},
                      {max_background_flushes, 4},
                      {level0_file_num_compaction_trigger, 1 bsl 30},
                      {level0_slowdown_writes_trigger, 1 bsl 30},
                      {level0_stop_writes_trigger, 1 bsl 30},
                      {max_compaction_bytes, 1 bsl 60},
                      {target_file_size_base, 8388608},
                      {atomic_flush, false},
                      {write_buffer_size, 8388608}
                     ] || application:get_env(blockchain, allow_bulk_snapshot_loads, true) ])
     ),

    %% we load up both with the same snapshot here, then sync the next N
    %% blocks and check that we're valid.
    [load_into_ledger(Snapshot, Ledger0, Mode)
     || Mode <- [delayed, active]],
    load_blocks(Ledger0, Chain, Snapshot),
    case blockchain_ledger_v1:has_aux(Ledger0) of
        true ->
            load_into_ledger(Snapshot, Ledger0, aux),
            load_blocks(blockchain_ledger_v1:mode(aux_load, Ledger0), Chain, Snapshot);
        false ->
            ok
    end,
    {ok, Curr3} = blockchain_ledger_v1:current_height(Ledger0),
    lager:info("ledger height is ~p after absorbing blocks", [Curr3]),
    print_memory(),

    %% store the snapshot if we don't have it already
    case blockchain:get_snapshot(SHA, Chain) of
        {ok, _Snap} ->
            ok;
        {error, _} ->
            blockchain:add_bin_snapshot(BinSnap, Height, SHA, Chain)
    end,
    %% re-open the ledger with the normal options
    blockchain_ledger_v1:close(Ledger0),
    Ledger1 = blockchain_ledger_v1:new(
      Dir,
      blockchain:db_handle(Chain),
      blockchain:blocks_cf(Chain),
      blockchain:heights_cf(Chain),
      blockchain:info_cf(Chain)
     ),
    %% and then compact the ledger
    blockchain_ledger_v1:compact(Ledger1),
    Ledger1.


-spec load_into_ledger(snapshot(), L, M) -> ok when
    L :: blockchain_ledger_v1:ledger(),
    M :: blockchain_ledger_v1:mode().
load_into_ledger(Snapshot, L0, Mode) ->
    lager:info("loading snapshot into ~p ledger", [Mode]),
    print_memory(),
    Get = fun (K) -> deserialize_field(K, maps:get(K, Snapshot)) end,
    L1 = blockchain_ledger_v1:mode(Mode, L0),
    %% don't cache the writes to this context, do direct rocksdb writes
    %% for performance and to save memory
    L = blockchain_ledger_v1:new_direct_context(L1),

    %% list of snapshot keys and the ledger functions used to load them
    %% because they're not all the same, for reasons?
    %% format is [Key, {Key, Function} or {Key, Module, Function}].
    %% Stop adding new weird kinds of loads, please
    load_into_ledger_([consensus_members,
                       election_height,
                       election_epoch,
                       {delayed_vars, load_delayed_vars},
                       {threshold_txns, load_threshold_txns},
                        master_key,
                        multi_keys,
                        vars_nonce,
                        {vars, load_vars},
                        {gateways, load_raw_gateways},
                        {pocs, load_raw_pocs},
                        {accounts, load_raw_accounts},
                        {dc_accounts, load_raw_dc_accounts},
                        {security_accounts, load_raw_security_accounts},
                        {htlcs, load_htlcs},
                        {ouis, load_ouis},
                        {subnets, load_subnets},
                        {oui_counter, set_oui_counter},
                        {hexes, load_hexes},
                        {h3dex, load_h3dex},
                        {state_channels, load_state_channels},
                        {oracle_price, load_oracle_price},
                        {oracle_price_list, load_oracle_price_list}] ++
                      [{validators, load_validators} || maps:is_key(validators, Snapshot)] ++
                      [{delayed_hnt, load_delayed_hnt} || maps:is_key(delayed_hnt, Snapshot)] ++
                      [{upgrades, blockchain, mark_upgrades} || maps:is_key(upgrades, Snapshot)] ++
                      [net_overage || maps:is_key(net_overage, Snapshot)] ++
                      [begin
                           ok = blockchain_ledger_v1:clear_hnt_burned(L),
                           {hnt_burned, add_hnt_burned}
                       end || maps:is_key(hnt_burned, Snapshot)] ++
                      %% keep this last so incomplete loads are obvious
                      [current_height], Get, L),
    case blockchain_ledger_v1:check_key(<<"poc_upgrade">>, L) of
        true -> ok;
        _ ->
            %% have to do this here, otherwise it'll break block loads
            blockchain_ledger_v1:upgrade_pocs(L),
            blockchain_ledger_v1:mark_key(<<"poc_upgrade">>, L)
    end,
    blockchain_ledger_v1:commit_context(L).


load_into_ledger_([], _, _) ->
    ok;
load_into_ledger_([{K,F}|T], Get, L) ->
    Start = erlang:monotonic_time(millisecond),
    ok = blockchain_ledger_v1:F(Get(K), L),
    End = erlang:monotonic_time(millisecond),
    lager:info("loaded ~p from snapshot in ~p ms", [K, End - Start]),
    print_memory(),
    load_into_ledger_(T, Get, L);
load_into_ledger_([{K,M,F}|T], Get, L) ->
    Start = erlang:monotonic_time(millisecond),
    ok = M:F(Get(K), L),
    End = erlang:monotonic_time(millisecond),
    lager:info("loaded ~p from snapshot in ~p ms", [K, End - Start]),
    print_memory(),
    load_into_ledger_(T, Get, L);
load_into_ledger_([K|T], Get, L) ->
    Start = erlang:monotonic_time(millisecond),
    ok = blockchain_ledger_v1:K(Get(K), L),
    End = erlang:monotonic_time(millisecond),
    lager:info("loaded ~p from snapshot in ~p ms", [K, End - Start]),
    print_memory(),
    load_into_ledger_(T, Get, L).

print_memory() ->
    lager:info("memory ~p ~p", [erlang:process_info(self(), total_heap_size), erlang:memory(binary)]).

-spec load_blocks(blockchain_ledger_v1:ledger(), blockchain:blockchain(), snapshot()) ->
    ok.
load_blocks(Ledger0, Chain, Snapshot) ->
    print_memory(),
    lager:info("loading block info"),
    Infos =
        case maps:find(infos, Snapshot) of
            {ok, Is} when is_binary(Is) ->
                stream_from_list(binary_to_term(Is));
            {ok, {_, _, _}=InfoFileHandle} ->
                blockchain_term:from_file_stream_bin_list(InfoFileHandle);
            error ->
                stream_from_list([])
        end,
    stream_iter(
      fun(Bin) ->
              case binary_to_term(Bin) of
                  ({Ht, #block_info{hash = Hash} = Info}) ->
                      ok = blockchain:put_block_height(Hash, Ht, Chain),
                      ok = blockchain:put_block_info(Ht, Info, Chain);
                  ({Ht, #block_info_v2{hash = Hash} = Info}) ->
                      ok = blockchain:put_block_height(Hash, Ht, Chain),
                      ok = blockchain:put_block_info(Ht, Info, Chain)
              end
      end,
      Infos),
    print_memory(),
    lager:info("loading blocks"),
    BlockStream =
        case maps:find(blocks, Snapshot) of
            {ok, <<Bs/binary>>} ->
                lager:info("blocks binary is ~p", [byte_size(Bs)]),
                print_memory(),
                %% use a custom decoder here to preserve sub binary references
                {ok, Blocks0} = blockchain_term:from_bin(Bs),
                stream_from_list(Blocks0);
            {ok, {_, _, _}=FileHandle} ->
                blockchain_term:from_file_stream_bin_list(FileHandle);
            error ->
                stream_from_list([])
        end,

    print_memory(),
    {ok, Curr2} = blockchain_ledger_v1:current_height(Ledger0),

    lager:info("ledger height is ~p before absorbing snapshot", [Curr2]),

    stream_iter(
      fun(Res) ->
            Block0 =
                case Res of
                    {ok, <<B0/binary>>} -> B0;
                    <<B0/binary>> -> B0
                end,
            Block = blockchain_block:deserialize(Block0),

              Ht = blockchain_block:height(Block),
              %% since hash and block are written at the same time, just getting the
              %% hash from the height is an acceptable presence check, and much cheaper
              case blockchain:get_block_hash(Ht, Chain, false) of
                  {ok, _Hash} ->
                      lager:info("skipping block ~p", [Ht]),
                      %% already have it, don't need to store it again.
                      ok;
                  _ ->
                      lager:info("saving block ~p", [Ht]),
                      ok = blockchain:save_block(Block, Chain)
              end,
              print_memory(),
              case Ht > Curr2 of
                  %% we need some blocks before for history, only absorb if they're
                  %% not on the ledger already
                  true ->
                      lager:info("loading block ~p", [Ht]),
                      Ledger2 = blockchain_ledger_v1:new_context(Ledger0),
                      Chain1 = blockchain:ledger(Ledger2, Chain),
                      Rescue = blockchain_block:is_rescue_block(Block),
                      {ok, _Chain} = blockchain_txn:absorb_block(Block, Rescue, Chain1),
                      %% Hash = blockchain_block:hash_block(Block),
                      ok = blockchain_ledger_v1:maybe_gc_pocs(Chain1, Ledger2),
                      ok = blockchain_ledger_v1:maybe_gc_scs(Chain1, Ledger2),
                      %% ok = blockchain_ledger_v1:refresh_gateway_witnesses(Hash, Ledger2),
                      ok = blockchain_ledger_v1:maybe_recalc_price(Chain1, Ledger2),
                      %% TODO Q: Why no match result?
                      blockchain_ledger_v1:commit_context(Ledger2),
                      blockchain_ledger_v1:new_snapshot(Ledger0),
                      print_memory();
                  _ ->
                      ok
              end
      end,
      BlockStream).

-spec stream_iter(fun((A) -> ok), blockchain_term:stream(A)) -> ok.
stream_iter(F, S0) ->
    case S0() of
        none ->
            ok;
        {some, {X, S1}} ->
            F(X),
            stream_iter(F, S1)
    end.

-spec stream_from_list([A]) -> blockchain_term:stream(A).
stream_from_list([]) ->
    fun () -> none end;
stream_from_list([X | Xs]) ->
    fun () -> {some, {X, stream_from_list(Xs)}} end.

-spec get_infos(blockchain:blockchain()) ->
    [binary()].
get_infos(Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    {ok, POCChallengeInterval} = blockchain:config(?poc_challenge_interval, Ledger),

    LoadInfoStart = Height - (POCChallengeInterval * 2) + 1,

    [begin
         {ok, B} = blockchain:get_block_info(N, Chain),
         term_to_binary({N, B})
     end
     || N <- lists:seq(max(?min_height, LoadInfoStart), Height)].

-spec get_blocks(blockchain:blockchain()) ->
    {ok, [binary()]} | {error, encountered_a_rescue_block}.
get_blocks(Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    %% this is for rewards calculation when an epoch ends
    %% see https://github.com/helium/blockchain-core/pull/627
    #{election_height := ElectionHeight} = blockchain_election:election_info(Ledger),
    GraceBlocks =
        case blockchain:config(?sc_grace_blocks, Ledger) of
            {ok, GBs} ->
                GBs;
            %% hard matching on this config makes it impossible to snapshot before we hit state
            %% channels
            {error, not_found} ->
                0
        end,

    DLedger = blockchain_ledger_v1:mode(delayed, Ledger),
    {ok, DHeight0} = blockchain_ledger_v1:current_height(DLedger),

    {ok, #block_info_v2{election_info={_, DHeight}}} = blockchain:get_block_info(DHeight0, Chain),

    %% We need _at least_ the grace blocks before current election or the delayed ledger height,
    %% whichever is lower.
    LoadBlockStart = min(DHeight, ElectionHeight - GraceBlocks),

    BlockHeightRange = lists:seq(max(?min_height, LoadBlockStart), Height),
    Error = encountered_a_rescue_block,
    try
        FetchBlockAtHeight =
            fun (H) ->
                {ok, <<BlockRaw/binary>>} = blockchain:get_raw_block(H, Chain),
                Block = blockchain_block:deserialize(BlockRaw),
                case blockchain_block:is_rescue_block(Block) of
                    true -> throw(Error);
                    false -> BlockRaw
                end
            end,
        Blocks = lists:map(FetchBlockAtHeight, BlockHeightRange),
        {ok, Blocks}
    catch throw:Error ->
        {error, Error}
    end.

is_v6(#{version := v6}) -> true;
is_v6(_) -> false.

-spec get_h3dex(snapshot()) -> [{integer(), [binary()]}].
get_h3dex(#{h3dex := {FD, Pos, Len}}) ->
    {ok, Pos} = file:position(FD, {bof, Pos}),
    {ok, H3DexBin} = file:read(FD, Len),
    binary_to_term(H3DexBin);
get_h3dex(#{h3dex := H3DexBin}) when is_binary(H3DexBin) ->
    binary_to_term(H3DexBin).

-spec height(snapshot()) -> non_neg_integer().
height(#{current_height := {FD, Pos, Len}}) ->
    {ok, Pos} = file:position(FD, {bof, Pos}),
    {ok, HeightBin} = file:read(FD, Len),
    binary_to_term(HeightBin);
height(#{current_height := HeightBin}) when is_binary(HeightBin) ->
    binary_to_term(HeightBin).

-spec hash(snapshot_of_any_version()) -> binary().
hash(Snap) ->
    case version(Snap) of
        v6 ->
            %% attempt to incrementally hash the snapshot without building up a big binary
            Ctx0 = crypto:hash_init(sha256),
            Size = snapshot_size(Snap#{blocks => <<>>, infos => <<>>}),
            Ctx1 = crypto:hash_update(Ctx0, <<6, Size:32/integer-unsigned-little>>),
            FinalCtx = lists:foldl(fun({blocks, _}, Acc) ->
                              Key = term_to_binary(blocks),
                              KeyLen = byte_size(Key),
                              crypto:hash_update(Acc, <<KeyLen:32/integer-unsigned-little, Key/binary, 0:32/integer-unsigned-little>>);
                          ({infos, _}, Acc) ->
                              Key = term_to_binary(infos),
                              KeyLen = byte_size(Key),
                              crypto:hash_update(Acc, <<KeyLen:32/integer-unsigned-little, Key/binary, 0:32/integer-unsigned-little>>);
                          ({version, Version}, Acc) ->
                              Key = term_to_binary(version),
                              KeyLen = byte_size(Key),
                              Value = term_to_binary(Version),
                              ValueLen = byte_size(Value),
                              crypto:hash_update(Acc, <<KeyLen:32/integer-unsigned-little, Key/binary, ValueLen:32/integer-unsigned-little, Value/binary>>);
                          ({K, V}, Acc) when is_binary(V) ->
                              Key = term_to_binary(K),
                              KeyLen = byte_size(Key),
                              ValueLen = byte_size(V),
                              TmpCtx = crypto:hash_update(Acc, <<KeyLen:32/integer-unsigned-little, Key/binary, ValueLen:32/integer-unsigned-little>>),
                              crypto:hash_update(TmpCtx, V);
                         ({K, {FD, Pos, Len}}, Acc) ->
                              Key = term_to_binary(K),
                              KeyLen = byte_size(Key),
                              ValueLen = Len,
                              TmpCtx = crypto:hash_update(Acc, <<KeyLen:32/integer-unsigned-little, Key/binary, ValueLen:32/integer-unsigned-little>>),
                              {ok, Pos} = file:position(FD, {bof, Pos}),
                              hash_bytes(TmpCtx, FD, Len)
                      end, Ctx1, lists:keysort(1, maps:to_list(Snap))),
            crypto:hash_final(FinalCtx);
        _ ->
            crypto:hash(sha256, serialize(Snap, noblocks))
    end.

hash_bytes(Ctx, FD, Len) ->
    case Len > 1024 of
        true ->
            {ok, Bin} = file:read(FD, 1024),
            NewCtx = crypto:hash_update(Ctx, Bin),
            hash_bytes(NewCtx, FD, Len - 1024);
        false ->
            {ok, Bin} = file:read(FD, Len),
            crypto:hash_update(Ctx, Bin)
    end.


snapshot_size(Snap) ->
    maps:fold(fun(version, Version, Acc) ->
                      byte_size(term_to_binary(version)) + byte_size(term_to_binary(Version)) + 8 + Acc;
                  (K, V, Acc) when is_binary(V) ->
                      byte_size(term_to_binary(K)) + byte_size(V) + 8 + Acc;
                 (K, {_FD, _Pos, Len}, Acc) ->
                      byte_size(term_to_binary(K)) + Len + 8 + Acc
              end, 0, Snap).

v1_to_v2(#blockchain_snapshot_v1{
            previous_snapshot_hash = <<>>,
            leading_hash = <<>>,

            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            %%token_burn_rate = TokenBurnRate,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks
           }) ->
    #blockchain_snapshot_v2{
       previous_snapshot_hash = <<>>,
       leading_hash = <<>>,

       current_height = CurrHeight,
       transaction_fee = 0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       gateways = Gateways,
       pocs = PoCs,

       accounts = Accounts,
       dc_accounts = DCAccounts,

       %%token_burn_rate = TokenBurnRate,
       token_burn_rate = 0,

       security_accounts = SecurityAccounts,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks
      }.

v2_to_v3(#blockchain_snapshot_v2{
            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->
    #blockchain_snapshot_v3{
       current_height = CurrHeight,
       transaction_fee = 0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       %% these need to be re-serialized for v3

       gateways = kvl_map_vals(fun blockchain_ledger_gateway_v2:serialize/1, Gateways),
       pocs =
            lists:map(
                fun({K, V}) ->
                    List = lists:map(fun blockchain_ledger_poc_v2:serialize/1, V),
                    Value = term_to_binary(List),
                    {K, Value}
                end,
                PoCs),

       accounts = kvl_map_vals(fun blockchain_ledger_entry_v1:serialize/1, Accounts),
       dc_accounts = kvl_map_vals(fun blockchain_ledger_data_credits_entry_v1:serialize/1, DCAccounts),

       security_accounts = kvl_map_vals(fun blockchain_ledger_security_entry_v1:serialize/1, SecurityAccounts),

       %% end re-serialization

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks,

       oracle_price = OraclePrice,
       oracle_price_list = OraclePriceList
      }.

v3_to_v4(#blockchain_snapshot_v3{
            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->
    #blockchain_snapshot_v4{
       current_height = CurrHeight,
       transaction_fee = 0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       multi_keys = [],
       vars_nonce = VarsNonce,
       vars = Vars,

       gateways = Gateways,
       pocs = PoCs,

       accounts = Accounts,
       dc_accounts = DCAccounts,

       security_accounts = SecurityAccounts,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks,

       oracle_price = OraclePrice,
       oracle_price_list = OraclePriceList
      }.

v4_to_v5(#blockchain_snapshot_v4{
            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            multi_keys = MultiKeys,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->
    #{
      version => v5,
      current_height => CurrHeight,
      transaction_fee => 0,
      consensus_members => ConsensusMembers,

      election_height => ElectionHeight,
      election_epoch => ElectionEpoch,

      delayed_vars => DelayedVars,
      threshold_txns => ThresholdTxns,

      master_key => MasterKey,
      multi_keys => MultiKeys,
      vars_nonce => VarsNonce,
      vars => Vars,

      gateways => Gateways,
      pocs => PoCs,

      accounts => Accounts,
      dc_accounts => DCAccounts,

      security_accounts => SecurityAccounts,

      htlcs => HTLCs,

      ouis => OUIs,
      subnets => Subnets,
      oui_counter => OUICounter,

      hexes => Hexes,
      h3dex => [],

      state_channels => StateChannels,

      blocks => Blocks,

      oracle_price => OraclePrice,
      oracle_price_list => OraclePriceList
     }.

-spec v5_to_v6(snapshot_v5()) -> snapshot_v6().
v5_to_v6(#{version := v5}=V5) ->
    maps:map(
        fun (version, v5) ->
                v6;
            (K, V) ->
                iolist_to_binary(serialize_field(K, V))
        end,
        V5
    ).

-spec upgrade(snapshot_of_any_version()) -> snapshot().
upgrade(S) ->
    case version(S) of
        v6 -> S;
        v5 -> v5_to_v6(S);
        v4 -> v5_to_v6(v4_to_v5(S));
        v3 -> v5_to_v6(v4_to_v5(v3_to_v4(S)));
        v2 -> v5_to_v6(v4_to_v5(v3_to_v4(v2_to_v3(S))));
        v1 -> v5_to_v6(v4_to_v5(v3_to_v4(v2_to_v3(v1_to_v2(S)))))
    end.

-spec version(snapshot_of_any_version()) -> v1 | v2 | v3 | v4 | v5 | v6.
version(#{version := V}          ) -> V;
version(#blockchain_snapshot_v4{}) -> v4;
version(#blockchain_snapshot_v3{}) -> v3;
version(#blockchain_snapshot_v2{}) -> v2;
version(#blockchain_snapshot_v1{}) -> v1.

-spec kv_stream_to_list(kv_stream(K, V)) -> [{K, V}].
kv_stream_to_list(Next0) when is_function(Next0) ->
    case Next0() of
        ok -> [];
        {K, V, Next1} -> [{K, V} | kv_stream_to_list(Next1)]
    end.

-spec blocks_info(snapshot()) -> {ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}}.
blocks_info(Snap) ->
    BlocksContained = case maps:get(blocks, Snap) of 
                          BinData = <<131, _/binary>> -> 
                              binary_to_term(BinData);
                          {FD, Offset, Size} ->
                              file:position(FD, Offset),
                              {ok, BinData} = file:read(FD, Size),
                              binary_to_term(BinData)
                      end,
    NumBlocks = length(BlocksContained),
    StartBlockHt = blockchain_block:height(blockchain_block:deserialize(hd(BlocksContained))),
    EndBlockHt = blockchain_block:height(blockchain_block:deserialize(lists:last(BlocksContained))),
    {ok, {NumBlocks, StartBlockHt, EndBlockHt}}.

diff(#{}=A0, #{}=B0) ->
    A = maps:from_list(
          lists:map(fun({version, V}) ->
                            {version, V};
                       ({K, V0}) ->
                            V =
                                case {deserialize_field(K, V0), is_raw_field(K)} of
                                    {V1, true} -> kv_stream_to_list(V1);
                                    {V1, false} -> V1
                                end,
                            {K, V}
                    end, maps:to_list(A0))),
    B = maps:from_list(
          lists:map(fun({version, V}) ->
                            {version, V};
                       ({K, V0}) ->
                            V =
                                case {deserialize_field(K, V0), is_raw_field(K)} of
                                    {V1, true} -> kv_stream_to_list(V1);
                                    {V1, false} -> V1
                                end,
                            {K, V}
                    end, maps:to_list(B0))),
    lists:foldl(
      fun({Field, AI, BI}, Acc) ->
              case AI == BI of
                  true ->
                      Acc;
                  _ ->
                      case Field of
                          F when F == vars; F == security_accounts ->
                              [{Field, AI, BI} | Acc];
                          %% we experience the most drift here, so
                          %% it's worth some effort.
                          accounts ->
                              AUniq = AI -- BI,
                              BUniq = BI -- AI,
                              [{Field, {libp2p_crypto:bin_to_b58(K), blockchain_ledger_entry_v1:deserialize(V), case proplists:get_value(K, BI) of undefined -> undefined; V2 -> blockchain_ledger_entry_v1:deserialize(V2) end}} || {K,V} <- AUniq ] ++
                              [{Field, {libp2p_crypto:bin_to_b58(K), blockchain_ledger_entry_v1:deserialize(V), undefined}} || {K,V} <- BUniq, not lists:keymember(K, 1, AI) ] ++ Acc;
                          gateways ->
                              AUniq = AI -- BI,
                              BUniq = BI -- AI,
                              case diff_gateways(AUniq, BUniq, []) of
                                  [] ->
                                      Acc;
                                  Diff ->
                                      [{gateways, Diff} | Acc]
                              end;
                          upgrades ->
                              [{Field, AI, BI}|Acc];
                          blocks ->
                              AHeightAndHash = [ begin
                                                     Block = blockchain_block:deserialize(Block0),
                                                     {blockchain_block:height(Block),
                                                      blockchain_block:hash_block(Block)}
                                                 end
                                                 || Block0 <- AI],
                              BHeightAndHash = [ begin
                                                     Block = blockchain_block:deserialize(Block0),
                                                     {blockchain_block:height(Block),
                                                      blockchain_block:hash_block(Block)}
                                                 end || Block0 <- BI],
                              case {AHeightAndHash -- BHeightAndHash, BHeightAndHash -- AHeightAndHash} of
                                  {[], []} ->
                                      Acc;
                                  {ADiffs, BDiffs} ->
                                      [{Field, [Height || {Height, _Hash} <- ADiffs], [Height || {Height, _Hash} <- BDiffs]} | Acc]
                              end;
                          h3dex ->
                              [{Field, length(AI), length(BI)} | Acc];
                          _ ->
                              [Field | Acc]
                      end
              end
      end,
      [],
      [{K, V, maps:get(K, B, undefined)} || {K, V} <- maps:to_list(A)]).

diff_gateways([] , [], Acc) ->
    Acc;
diff_gateways(AList , [], Acc) ->
    [lists:map(fun({Addr, _}) -> {Addr, b_missing} end, AList)
     | Acc];
diff_gateways([] , BList, Acc) ->
    [lists:map(fun({Addr, _}) -> {Addr, a_missing} end, BList)
     | Acc];
diff_gateways([{Addr, A} | T] , BList, Acc) ->
    case gwget(Addr, BList) of
        missing ->
            diff_gateways(T, lists:keydelete(Addr, 1, BList),
                          [{Addr, b_missing} | Acc]);
        B ->
            %% sometimes map encoding lies to us
            case minimize_gw(A, B) of
                [] ->
                    diff_gateways(T, lists:keydelete(Addr, 1, BList),
                                  Acc);
                MiniGw ->
                    diff_gateways(T, lists:keydelete(Addr, 1, BList),
                                  [{Addr, MiniGw} | Acc])
            end
    end.

gwget(Addr, L) ->
    case lists:keyfind(Addr, 1, L) of
        {_, GW} ->
            GW;
        false ->
            missing
    end.

minimize_gw(A0, B0) ->
    A = blockchain_ledger_gateway_v2:deserialize(A0),
    B = blockchain_ledger_gateway_v2:deserialize(B0),
    %% We can directly compare some fields
    Compare =
        lists:flatmap(
          fun(Fn) ->
                  AVal = blockchain_ledger_gateway_v2:Fn(A),
                  BVal = blockchain_ledger_gateway_v2:Fn(B),
                  case AVal == BVal of
                      true ->
                          [];
                      false ->
                          [{Fn, AVal, BVal}]
                  end
          end,
          [owner_address, location, version, last_poc_challenge, last_poc_onion_key_hash,
           nonce, alpha, beta, delta, oui, gain, elevation, mode, last_location_nonce]),
    %% but for witnesses, we want to do additional minimization
    AWits = blockchain_ledger_gateway_v2:witnesses(A),
    BWits = blockchain_ledger_gateway_v2:witnesses(B),
    %% we do a more detailed comparison here, which can sometimes
    %% reveal encoding differences :/
    case minimize_witnesses(AWits, BWits) of
        [] -> Compare;
        MiniWit ->
            [{witnesses, MiniWit} | Compare]
    end.

minimize_witnesses(A, B) ->
    Compare =
        maps:fold(
          fun(Addr, AWit, Acc) ->
                  case maps:get(Addr, B, missing) of
                      missing ->
                          [{Addr, b_missing} | Acc];
                      BWit ->
                          case BWit == AWit of
                              true ->
                                  Acc;
                              false ->
                                  %% we could probably do more here,
                                  %% narrowing down to counts/histograms/whatever
                                  [{Addr, AWit, BWit} | Acc]
                          end
                  end
          end,
          [], A),
    AKeys = maps:keys(A),
    B1 = maps:without(AKeys, B),
    case maps:size(B1) of
        0 ->
            Compare;
        _ ->
            AMissing =
                maps:fold(fun(K, _V, Acc) ->
                                  [{K, a_missing} | Acc]
                          end, [], B1),
            [AMissing | Compare]
    end.

-spec kvl_map_vals(fun((V1) -> V2), [{K, V1}]) -> [{K, V2}].
kvl_map_vals(F, KVL) ->
    [{K, F(V)} || {K, V} <- KVL].

-spec serialize_pairs([{key(), term()}]) -> iolist().
serialize_pairs(Pairs) ->
    [case K of
         version ->
             bin_pair_to_iolist({term_to_binary(K), term_to_binary(V)});
         _ ->
             bin_pair_to_iolist({term_to_binary(K), V})
     end
     || {K, V} <- Pairs].

find_pairs_in_file(FD, Pos, MaxPos) ->
    find_pairs_in_file(FD, Pos, MaxPos, []).

find_pairs_in_file(_FD, Pos, Pos, Acc) ->
    %% position is at the end of the file, all done
    Acc;
find_pairs_in_file(FD, Pos, MaxPos, Acc) when Pos < MaxPos ->
    {ok, <<SizK:32/integer-unsigned-little>>} = file:read(FD, 4),
    {ok, <<Key:SizK/binary, SizV:32/integer-unsigned-little>>} = file:read(FD, SizK + 4),
    {ok, NewPosition} = file:position(FD, {cur, SizV}),
    find_pairs_in_file(FD, NewPosition, MaxPos, [{binary_to_term(Key), {FD, Pos + SizK + 8, SizV}} | Acc]).

deserialize_pairs(<<Bin/binary>>) ->
    lists:map(
        fun({K0, V}) ->
            K = binary_to_term(K0),
            case K of
                version -> {K, binary_to_term(V)};
                _  -> {K, V}
            end
        end,
        bin_pairs_from_bin(Bin)
    ).

-spec deserialize_field(key(), binary()) -> term().
deserialize_field(hexes, Bin) when is_binary(Bin) ->
    %% hexes are encoded as a term_to_binary of a big
    %% key/value list of {h3() -> [binary()]},
    %% and a single 'list' key which points to a map of
    %% #{h3() -> pos_integer()}.
    %% binary_to_term, however, does not create sub binary
    %% references and so on larger snapshots this blows the binary
    %% heap. This function is a hand rolled term_to_binary decoder
    %% that decodes all that is needed to deserialize the
    %% hexes structure while preserving sub binaries.
    %% We do the deseraialize in a try/catch in case
    %% there are bugs or the structure of hexes changes in the
    %% future.
    try
        {ok, Term} = blockchain_term:from_bin(Bin),
        Term
    catch What:Why ->
        lager:warning("deserializing hexes from snapshot failed ~p ~p, falling back to binary_to_term", [What, Why]),
        binary_to_term(Bin)
    end;
deserialize_field(K, {FD, Pos, Len}) ->
    case is_raw_field(K) of
        true ->
            mk_file_iterator(FD, Pos, Pos + Len);
        false ->
            {ok, Pos} = file:position(FD, {bof, Pos}),
            {ok, Bin} = file:read(FD, Len),
            deserialize_field(K, Bin)
    end;
deserialize_field(K, <<Bin/binary>>) ->
    case is_raw_field(K) of
        true -> mk_bin_iterator(Bin);
        false -> binary_to_term(Bin)
    end.

-spec serialize_field(key(), term()) -> iolist().
serialize_field(K, V) ->
    case is_raw_field(K) of
        true -> lists:map(fun bin_pair_to_iolist/1, V);
        false -> term_to_binary(V)
    end.

-spec is_raw_field(key()) -> boolean().
is_raw_field(Key) ->
    lists:member(Key, [gateways, pocs, accounts, dc_accounts, security_accounts]).

-spec bin_pair_to_iolist({binary(), binary()}) -> iolist().
bin_pair_to_iolist({<<K/binary>>, V}) ->
    [
        <<(byte_size(K)):32/little-unsigned-integer>>,
        K,
        <<(iolist_size(V)):32/little-unsigned-integer>>,
        V
    ].

mk_bin_iterator(<<>>) ->
    fun() -> ok end;
mk_bin_iterator(<<SizK:32/little-unsigned-integer, K:SizK/binary,
                  SizV:32/little-unsigned-integer, V:SizV/binary,
                  Rest/binary>>) ->
    fun() ->
            {K, V, mk_bin_iterator(Rest)}
    end.

mk_file_iterator(_FD, End, End) ->
    fun() -> ok end;
mk_file_iterator(FD, Pos, End) when Pos < End ->
    fun() ->
            {ok, Pos} = file:position(FD, {bof, Pos}),
            {ok, <<SizK:32/integer-unsigned-little>>} = file:read(FD, 4),
            {ok, <<K:SizK/binary, SizV:32/integer-unsigned-little>>} = file:read(FD, SizK + 4),
            {ok, V} = file:read(FD, SizV),
            lager:debug("read key of size ~p and value of size ~p", [SizK, SizV]),
            {K, V, mk_file_iterator(FD, Pos + 4 + SizK + 4 + SizV, End)}
    end.

-spec bin_pairs_from_bin(binary()) -> [{binary(), binary()}].
bin_pairs_from_bin(<<Bin/binary>>) ->
    bin_pairs_from_bin(Bin, []).

bin_pairs_from_bin(<<>>, Pairs) ->
    lists:reverse(Pairs);
bin_pairs_from_bin(
    <<
        SizK:32/little-unsigned-integer, K:SizK/binary,
        SizV:32/little-unsigned-integer, V:SizV/binary,
        Rest/binary
    >>,
    Pairs
) ->
    bin_pairs_from_bin(Rest, [{K, V} | Pairs]).
