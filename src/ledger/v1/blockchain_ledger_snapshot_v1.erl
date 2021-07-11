-module(blockchain_ledger_snapshot_v1).

-include("blockchain_ledger_snapshot_v1.hrl").
-include("blockchain_vars.hrl").

-export([
         serialize/1,
         deserialize/1,
         deserialize/2,

         is_v6/1,
         version/1,

         snapshot/2,
         snapshot/3,
         import/3,
         load_into_ledger/3,
         load_blocks/3,

         get_blocks/1,
         get_h3dex/1,

         height/1,
         hash/1,

         diff/2
        ]).

-ifdef(TEST).
-export([
         deserialize_field/2
        ]).
-endif.

-export_type([
    snapshot/0,
    snapshot_v5/0
]).

%% -type state_channel() ::
%%       {blockchain_ledger_state_channel_v1, blockchain_ledger_state_channel_v1:state_channel()}
%%     | {blockchain_ledger_state_channel_v2, blockchain_ledger_state_channel_v2:state_channel_v2()}.

%% this assumes that everything will have loaded the genesis block
%% already.  I'm not sure that's totally safe in all cases, but it's
%% the right thing for the spots and easy to work around elsewhere.
-define(min_height, 2).

%% this is temporary, something to work with easily while we nail the
%% format and functionality down.  once it's final we can move on to a
%% more permanent and less flexible format, like protobufs, or
%% cauterize.
-type snapshot_v5_or_v6(Version) :: #{version => Version, atom() => binary()}.
    %% #{
    %%     version           => Version,
    %%     current_height    => non_neg_integer(),
    %%     transaction_fee   =>  non_neg_integer(),
    %%     consensus_members => [libp2p_crypto:pubkey_bin()],
    %%     election_height   => non_neg_integer(),
    %%     election_epoch    => non_neg_integer(),
    %%     delayed_vars      => [{integer(), [{Hash :: term(), TODO :: term()}]}], % TODO More specific
    %%     threshold_txns    => [{binary(), binary()}], % According to spec of blockchain_ledger_v1:snapshot_threshold_txns
    %%     master_key        => binary(),
    %%     multi_keys        => [binary()],
    %%     vars_nonce        => pos_integer(),
    %%     vars              => [{binary(), term()}], % TODO What is the term()?
    %%     htlcs             => [{Address :: binary(), blockchain_ledger_htlc_v1:htlc()}],
    %%     ouis              => [term()], % TODO Be more specific
    %%     subnets           => [term()], % TODO Be more specific
    %%     oui_counter       => pos_integer(),
    %%     hexes             => [term()], % TODO Be more specific
    %%     h3dex             => [{integer(), [binary()]}],
    %%     state_channels    => [{binary(), state_channel()}],
    %%     blocks            => [blockchain_block:block()],
    %%     oracle_price      => non_neg_integer(),
    %%     oracle_price_list => [blockchain_ledger_oracle_price_entry:oracle_price_entry()],

    %%     %% Raw
    %%     gateways          => [{binary(), binary()}],
    %%     pocs              => [{binary(), binary()}],
    %%     accounts          => [{binary(), binary()}],
    %%     dc_accounts       => [{binary(), binary()}],
    %%     security_accounts => [{binary(), binary()}]
    %% }.

%% v5 and v6 differ only in serialization format.
-type snapshot_v5() :: snapshot_v5_or_v6(v5).
-type snapshot_v6() :: snapshot_v5_or_v6(v6).

-type key() :: atom().

-type snapshot_of_any_version() ::
    #blockchain_snapshot_v1{}
    | #blockchain_snapshot_v2{}
    | #blockchain_snapshot_v3{}
    | #blockchain_snapshot_v4{}
    | snapshot_v5()
    | snapshot_v6().

-type snapshot() :: snapshot_v6().

-spec snapshot(blockchain_ledger_v1:ledger(), [binary()]) ->
    {ok, snapshot()}
    | {error, term()}.  % TODO More-specific than just term()
snapshot(Ledger0, Blocks) ->
    snapshot(Ledger0, Blocks, delayed).

-spec snapshot(
    blockchain_ledger_v1:ledger(),
    [binary()],
    blockchain_ledger_v1:mode()
) ->
    {ok, snapshot()} | {error, term()}.  % TODO More-specific than just term()
snapshot(Ledger0, Blocks, Mode) ->
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
                        Res = generate_snapshot(Ledger0, Blocks, Mode),
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
    blockchain_ledger_v1:mode()
) ->
    {ok, snapshot()} | {error, term()}.  % TODO More-specific than just term()
generate_snapshot(Ledger0, Blocks, Mode) ->
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

        %% use the active ledger here because that's where upgrades are marked
        Upgrades = blockchain:get_upgrades(blockchain_ledger_v1:mode(active, Ledger0)),
        Pairs =
            [
                {version          , v6},
                {current_height   , CurrHeight},
                {transaction_fee  ,  0},
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
                {oracle_price     , OraclePrice},
                {oracle_price_list, OraclePriceList},
                {upgrades         , Upgrades}
             ],
        M = maps:from_list(Pairs),
        M1 = maps:map(fun(version, V) ->
                              V;
                         (K, V) ->
                              iolist_to_binary(serialize_field(K, V))
                      end, M),
        {ok, M1}
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
    Key = blocks,
    Blocks =
        case BlocksOrNoBlocks of
            blocks ->
                term_to_binary(
                  lists:map(
                    fun (B) when is_tuple(B) ->
                            blockchain_block:serialize(B);
                        (B) -> B
                    end,
                    deserialize_field(Key, maps:get(Key, Snapshot0, []))
                   ));
            noblocks ->
                term_to_binary([])
        end,
    Snapshot1 = maps:put(Key, Blocks, Snapshot0),
    Pairs = lists:keysort(1, maps:to_list(Snapshot1)),
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

-spec deserialize(binary()) ->
      {ok, snapshot()}
    | {error, bad_snapshot_hash}
    | {error, bad_snapshot_binary}.
deserialize(<<Bin0/binary>>) ->
    deserialize(none, <<Bin0/binary>>).

-spec deserialize(DigestOpt :: none | binary(), binary()) ->
      {ok, snapshot()}
    | {error, bad_snapshot_hash}
    | {error, bad_snapshot_binary}.
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
-spec import(blockchain:blockchain(), binary(), snapshot()) ->
    blockchain_ledger_v1:ledger().
import(Chain, SHA, #{version := v6}=Snapshot) ->
    CLedger = blockchain:ledger(Chain),
    Dir = blockchain:dir(Chain),
    Ledger0 =
        case catch blockchain_ledger_v1:current_height(CLedger) of
            %% nothing in there, proceed
            {ok, 1} ->
                CLedger;
            _ ->
                blockchain_ledger_v1:clean(CLedger),
                blockchain_ledger_v1:new(
                    Dir,
                    blockchain:db_handle(Chain),
                    blockchain:blocks_cf(Chain),
                    blockchain:heights_cf(Chain)
                )
        end,
    %% we load up both with the same snapshot here, then sync the next N
    %% blocks and check that we're valid.
    [load_into_ledger(Snapshot, Ledger0, Mode)
     || Mode <- [delayed, active]],
    load_blocks(Ledger0, Chain, Snapshot),
    case blockchain_ledger_v1:has_aux(Ledger0) of
        true ->
            load_into_ledger(Snapshot, Ledger0, aux),
            load_blocks(blockchain_ledger_v1:mode(aux, Ledger0), Chain, Snapshot);
        false ->
            ok
    end,
    {ok, Curr3} = blockchain_ledger_v1:current_height(Ledger0),
    lager:info("ledger height is ~p after absorbing blocks", [Curr3]),

    %% store the snapshot if we don't have it already
    case blockchain:get_snapshot(SHA, Chain) of
        {ok, _Snap} ->
            ok;
        {error, _} ->
            blockchain:add_snapshot(Snapshot, Chain)
    end,
    Ledger0.

-spec load_into_ledger(snapshot(), L, M) -> ok when
    L :: blockchain_ledger_v1:ledger(),
    M :: blockchain_ledger_v1:mode().
load_into_ledger(Snapshot, L0, Mode) ->
    Get = fun (K) -> deserialize_field(K, maps:get(K, Snapshot)) end,
    L1 = blockchain_ledger_v1:mode(Mode, L0),
    L = blockchain_ledger_v1:new_context(L1),
    ok = blockchain_ledger_v1:current_height(Get(current_height), L),
    ok = blockchain_ledger_v1:consensus_members(Get(consensus_members), L),
    ok = blockchain_ledger_v1:election_height(Get(election_height), L),
    ok = blockchain_ledger_v1:election_epoch(Get(election_epoch), L),
    ok = blockchain_ledger_v1:load_delayed_vars(Get(delayed_vars), L),
    ok = blockchain_ledger_v1:load_threshold_txns(Get(threshold_txns), L),
    ok = blockchain_ledger_v1:master_key(Get(master_key), L),
    ok = blockchain_ledger_v1:multi_keys(Get(multi_keys), L),
    ok = blockchain_ledger_v1:vars_nonce(Get(vars_nonce), L),
    ok = blockchain_ledger_v1:load_vars(Get(vars), L),

    ok = blockchain_ledger_v1:load_raw_gateways(Get(gateways), L),

    %% optional validator era stuff will be missing in pre validator snaps
    case maps:find(validators, Snapshot) of
        error ->
            ok;
        {ok, Validators} ->
            ok = blockchain_ledger_v1:load_validators(deserialize_field(validators, Validators), L)
    end,
    case maps:find(delayed_hnt, Snapshot) of
        error ->
            ok;
        {ok, DelayedHNT} ->
            ok = blockchain_ledger_v1:load_delayed_hnt(deserialize_field(delayed_hnt, DelayedHNT), L)
    end,

    case maps:find(upgrades, Snapshot) of
        error ->
            ok;
        {ok, Upgrades} ->
            ok = blockchain:mark_upgrades(deserialize_field(upgrades, Upgrades), L)
    end,

    ok = blockchain_ledger_v1:load_raw_pocs(Get(pocs), L),
    ok = blockchain_ledger_v1:load_raw_accounts(Get(accounts), L),
    ok = blockchain_ledger_v1:load_raw_dc_accounts(Get(dc_accounts), L),
    ok = blockchain_ledger_v1:load_raw_security_accounts(Get(security_accounts), L),

    ok = blockchain_ledger_v1:load_htlcs(Get(htlcs), L),

    ok = blockchain_ledger_v1:load_ouis(Get(ouis), L),
    ok = blockchain_ledger_v1:load_subnets(Get(subnets), L),
    ok = blockchain_ledger_v1:set_oui_counter(Get(oui_counter), L),

    ok = blockchain_ledger_v1:load_hexes(Get(hexes), L),
    ok = blockchain_ledger_v1:load_h3dex(Get(h3dex), L),

    ok = blockchain_ledger_v1:load_state_channels(Get(state_channels), L),

    ok = blockchain_ledger_v1:load_oracle_price(Get(oracle_price), L),
    ok = blockchain_ledger_v1:load_oracle_price_list(Get(oracle_price_list), L),
    blockchain_ledger_v1:commit_context(L).

-spec load_blocks(blockchain_ledger_v1:ledger(), blockchain:blockchain(), snapshot()) ->
    ok.
load_blocks(Ledger0, Chain, Snapshot) ->
    Blocks =
        case maps:find(blocks, Snapshot) of
            {ok, Bs} ->
                binary_to_term(Bs);
            error ->
                []
        end,
    {ok, Curr2} = blockchain_ledger_v1:current_height(Ledger0),

    lager:info("ledger height is ~p before absorbing snapshot", [Curr2]),
    lager:info("snapshot contains ~p blocks", [length(Blocks)]),

    case Blocks of
        [] ->
            %% ignore blocks in testing
            ok;
        [_|_] ->
            %% just store the head, we'll need it sometimes
            lists:foreach(
              fun(Block0) ->
                      Block =
                      case Block0 of
                          B when is_binary(B) ->
                              blockchain_block:deserialize(B);
                          B -> B
                      end,

                      Ht = blockchain_block:height(Block),
                      %% since hash and block are written at the same time, just getting the
                      %% hash from the height is an acceptable presence check, and much cheaper
                      case blockchain:get_block_hash(Ht, Chain) of
                          {ok, _Hash} ->
                              lager:info("skipping block ~p", [Ht]),
                              %% already have it, don't need to store it again.
                              ok;
                          _ ->
                              lager:info("saving block ~p", [Ht]),
                              ok = blockchain:save_block(Block, Chain)
                      end,
                      case Ht > Curr2 of
                          %% we need some blocks before for history, only absorb if they're
                          %% not on the ledger already
                          true ->
                              lager:info("loading block ~p", [Ht]),
                              Ledger2 = blockchain_ledger_v1:new_context(Ledger0),
                              Chain1 = blockchain:ledger(Ledger2, Chain),
                              Rescue = blockchain_block:is_rescue_block(Block),
                              {ok, _Chain} = blockchain_txn:absorb_block(Block, Rescue, Chain1),
                              Hash = blockchain_block:hash_block(Block),
                              ok = blockchain_ledger_v1:maybe_gc_pocs(Chain1, Ledger2),
                              ok = blockchain_ledger_v1:maybe_gc_scs(Chain1, Ledger2),
                              ok = blockchain_ledger_v1:refresh_gateway_witnesses(Hash, Ledger2),
                              ok = blockchain_ledger_v1:maybe_recalc_price(Chain1, Ledger2),
                              %% TODO Q: Why no match result?
                              blockchain_ledger_v1:commit_context(Ledger2),
                              blockchain_ledger_v1:new_snapshot(Ledger0);
                          _ ->
                              ok
                      end
              end,
              Blocks)
    end.

-spec get_blocks(blockchain:blockchain()) ->
    [binary()].
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
    {ok, POCChallengeInterval} = blockchain:config(?poc_challenge_interval, Ledger),

    DLedger = blockchain_ledger_v1:mode(delayed, Ledger),
    {ok, DHeight} = blockchain_ledger_v1:current_height(DLedger),

    %% We need _at least_ the grace blocks before current election
    %% or the delayed ledger height less than last poc_challenge_interval blocks, whichever is
    %% lower.
    LoadBlockStart = min(DHeight - (POCChallengeInterval + 1), ElectionHeight - GraceBlocks),

    [begin
         {ok, B} = blockchain:get_raw_block(N, Chain),
         B
     end
     || N <- lists:seq(max(?min_height, LoadBlockStart), Height)].

is_v6(#{version := v6}) -> true;
is_v6(_) -> false.

-spec get_h3dex(snapshot()) -> [{integer(), [binary()]}].
get_h3dex(#{h3dex := H3DexBin}) ->
    binary_to_term(H3DexBin).

-spec height(snapshot()) -> non_neg_integer().
height(#{current_height := HeightBin}) ->
    binary_to_term(HeightBin).

-spec hash(snapshot_of_any_version()) -> binary().
hash(Snap) ->
    crypto:hash(sha256, serialize(Snap, noblocks)).

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

-dialyzer([
    {nowarn_function, upgrade/1}
]).

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
      %% TODO these need to be reserialized to please dialyzer (and probably to work?)
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
    maps:put(version, v6, V5).

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

diff(#{}=A0, #{}=B0) ->
    A = maps:from_list(
          lists:map(fun({version, V}) ->
                            {version, V};
                       ({K, V}) ->
                            {K, deserialize_field(K, V)}
                    end, maps:to_list(A0))),
    B = maps:from_list(
          lists:map(fun({version, V}) ->
                            {version, V};
                       ({K, V}) ->
                            {K, deserialize_field(K, V)}
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
          [location, version, last_poc_challenge, last_poc_onion_key_hash,
           nonce, alpha, beta, delta, oui]),
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
deserialize_field(master_key, <<Bin/binary>>) ->
    Bin;
deserialize_field(K, <<Bin/binary>>) ->
    case is_raw_field(K) of
        true -> bin_pairs_from_bin(Bin);
        false -> binary_to_term(Bin)
    end.

-spec serialize_field(key(), term()) -> iolist().
serialize_field(_K, V) when is_binary(V) ->
    V;
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
