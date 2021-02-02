-module(blockchain_ledger_snapshot_v1).

-include("blockchain_ledger_snapshot_v1.hrl").
-include("blockchain_vars.hrl").

-export([
         serialize/1,
         deserialize/1,

         snapshot/2,
         import/3,

         get_blocks/1,

         height/1,
         hash/1,

         diff/2
        ]).

%% this assumes that everything will have loaded the genesis block
%% already.  I'm not sure that's totally safe in all cases, but it's
%% the right thing for the spots and easy to work around elsewhere.
-define(min_height, 2).

%% this is temporary, something to work with easily while we nail the
%% format and functionality down.  once it's final we can move on to a
%% more permanent and less flexible format, like protobufs, or
%% cauterize.

snapshot(Ledger0, Blocks) ->
    Parent = self(),
    Ref = make_ref(),
    {_Pid, MonitorRef} =
        spawn_opt(fun ThisFun() ->
                      Ledger = blockchain_ledger_v1:mode(delayed, Ledger0),
                      {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),
                      %% this should not leak a huge amount of atoms
                      Regname = list_to_atom("snapshot_"++integer_to_list(CurrHeight)),
                      try register(Regname, self()) of
                          true ->
                              Res = generate_snapshot(Ledger0, Blocks),
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
                                        %% we were unable to attach to an existing process before it terminated
                                        ThisFun();
                                    {'DOWN', IntMonitorRef, process, _, killed} ->
                                        %% the already running process OOMed
                                        Parent ! {Ref, {error, killed}}
                                end
                      end
              end,
              [{max_heap_size, application:get_env(blockchain, snapshot_memory_limit, 75) * 1024 * 1024 div erlang:system_info(wordsize)}, {fullsweep_after, 0}, monitor]),
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

generate_snapshot(Ledger0, Blocks) ->
    try
        %% TODO: actually verify we're delayed here instead of
        %% changing modes?
        Ledger = blockchain_ledger_v1:mode(delayed, Ledger0),
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

        Snapshot =
            #{
              version => v5,
              current_height => CurrHeight,
              transaction_fee =>  0,
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
              validators => Validators,
              delayed_hnt => DelayedHNT,
              pocs => PoCs,

              accounts => Accounts,
              dc_accounts => DCAccounts,

              security_accounts => SecurityAccounts,

              htlcs => HTLCs,

              ouis => OUIs,
              subnets => Subnets,
              oui_counter => OUICounter,

              hexes => Hexes,
              h3dex => H3dex,

              state_channels => StateChannels,

              blocks => Blocks,

              oracle_price => OraclePrice,
              oracle_price_list => OraclePriceList
             },

        {ok, Snapshot}
    catch C:E:S ->
            {error, {error_taking_snapshot, C, E, S}}
    end.

serialize(Snapshot) ->
    serialize(Snapshot, blocks).

serialize(Snapshot, BlocksP) ->
    Snapshot1 = case BlocksP of
                    blocks ->
                        Blocks = lists:map(fun(B) when is_tuple(B) ->
                                                   blockchain_block:serialize(B);
                                              (B) -> B
                                           end, maps:get(blocks, Snapshot, [])),
                        lists:sort(maps:to_list(Snapshot#{blocks => Blocks}));
                    noblocks -> Snapshot#{blocks => []}
                end,
    Bin = term_to_binary(Snapshot1, [{compressed, 9}]),
    BinSz = byte_size(Bin),

    %% do some simple framing with version, size, & snap
    Snap = <<5, %% version
             BinSz:32/little-unsigned-integer, Bin/binary>>,
    {ok, Snap}.

serialize_v1(Snapshot, noblocks) ->
    %% NOTE: serialize_v1 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v1{blocks = []},
    Bin = term_to_binary(Snapshot1, [{compressed, 9}]),
    BinSz = byte_size(Bin),

    %% do some simple framing with version, size, & snap
    Snap = <<1, %% version
             BinSz:32/little-unsigned-integer, Bin/binary>>,
    {ok, Snap}.

serialize_v2(Snapshot, noblocks) ->
    %% NOTE: serialize_v2 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v2{blocks = []},
    Bin = term_to_binary(Snapshot1, [{compressed, 9}]),
    BinSz = byte_size(Bin),

    %% do some simple framing with version, size, & snap
    Snap = <<2, %% version
             BinSz:32/little-unsigned-integer, Bin/binary>>,
    {ok, Snap}.

serialize_v3(Snapshot, noblocks) ->
    %% NOTE: serialize_v3 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v3{blocks = []},
    Bin = term_to_binary(Snapshot1, [{compressed, 9}]),
    BinSz = byte_size(Bin),

    %% do some simple framing with version, size, & snap
    Snap = <<3, %% version
             BinSz:32/little-unsigned-integer, Bin/binary>>,
    {ok, Snap}.

serialize_v4(Snapshot, noblocks) ->
    %% NOTE: serialize_v4 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v4{blocks = []},
    Bin = term_to_binary(Snapshot1, [{compressed, 9}]),
    BinSz = byte_size(Bin),

    %% do some simple framing with version, size, & snap
    Snap = <<4, %% version
             BinSz:32/little-unsigned-integer, Bin/binary>>,
    {ok, Snap}.


deserialize(<<1,
              %%SHASz:16/little-unsigned-integer, SHA:SHASz/binary,
              BinSz:32/little-unsigned-integer, BinSnap:BinSz/binary>>) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(v3_to_v4(v2_to_v3(v1_to_v2(OldSnapshot)))),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize(<<2,
              %%SHASz:16/little-unsigned-integer, SHA:SHASz/binary,
              BinSz:32/little-unsigned-integer, BinSnap:BinSz/binary>>) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(v3_to_v4(v2_to_v3(OldSnapshot))),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize(<<3,
              %%SHASz:16/little-unsigned-integer, SHA:SHASz/binary,
              BinSz:32/little-unsigned-integer, BinSnap:BinSz/binary>>) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(v3_to_v4(OldSnapshot)),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize(<<4,
              %%SHASz:16/little-unsigned-integer, SHA:SHASz/binary,
              BinSz:32/little-unsigned-integer, BinSnap:BinSz/binary>>) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(OldSnapshot),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize(<<5,
              %%SHASz:16/little-unsigned-integer, SHA:SHASz/binary,
              BinSz:32/little-unsigned-integer, BinSnap:BinSz/binary>>) ->
    try maps:from_list(binary_to_term(BinSnap)) of
        #{version := v5} = Snapshot ->
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end.

%% sha will be stored externally
import(Chain, SHA,
       #{
         current_height := CurrHeight,
         transaction_fee :=  _TxnFee,
         consensus_members := ConsensusMembers,

         election_height := ElectionHeight,
         election_epoch := ElectionEpoch,

         delayed_vars := DelayedVars,
         threshold_txns := ThresholdTxns,

         master_key := MasterKey,
         multi_keys := MultiKeys,
         vars_nonce := VarsNonce,
         vars := Vars,

         gateways := Gateways,
         pocs := PoCs,

         accounts := Accounts,
         dc_accounts := DCAccounts,

         security_accounts := SecurityAccounts,

         htlcs := HTLCs,

         ouis := OUIs,
         subnets := Subnets,
         oui_counter := OUICounter,

         hexes := Hexes,
         h3dex := H3dex,

         state_channels := StateChannels,

         blocks := Blocks,

         oracle_price := OraclePrice,
         oracle_price_list := OraclePriceList
         } = Snapshot) ->
    Dir = blockchain:dir(Chain),
    case hash(Snapshot) == SHA orelse
        hash_v4(v5_to_v4(Snapshot)) == SHA orelse
        hash_v3(v4_to_v3(v5_to_v4(Snapshot))) == SHA orelse
        hash_v2(v3_to_v2(v4_to_v3(v5_to_v4(Snapshot)))) == SHA orelse
        hash_v1(v2_to_v1(v3_to_v2(v4_to_v3(v5_to_v4(Snapshot))))) == SHA of
        true ->
            CLedger = blockchain:ledger(Chain),
            Ledger0 =
                case catch blockchain_ledger_v1:current_height(CLedger) of
                    %% nothing in there, proceed
                    {ok, 1} ->
                        CLedger;
                    _ ->
                        blockchain_ledger_v1:clean(CLedger),
                        blockchain_ledger_v1:new(Dir)
                end,

            %% we load up both with the same snapshot here, then sync the next N
            %% blocks and check that we're valid.
            [begin
                 Ledger1 = blockchain_ledger_v1:mode(Mode, Ledger0),
                 Ledger = blockchain_ledger_v1:new_context(Ledger1),
                 ok = blockchain_ledger_v1:current_height(CurrHeight, Ledger),
                 ok = blockchain_ledger_v1:consensus_members(ConsensusMembers, Ledger),
                 ok = blockchain_ledger_v1:election_height(ElectionHeight, Ledger),
                 ok = blockchain_ledger_v1:election_epoch(ElectionEpoch, Ledger),
                 ok = blockchain_ledger_v1:load_delayed_vars(DelayedVars, Ledger),
                 ok = blockchain_ledger_v1:load_threshold_txns(ThresholdTxns, Ledger),
                 ok = blockchain_ledger_v1:master_key(MasterKey, Ledger),
                 ok = blockchain_ledger_v1:multi_keys(MultiKeys, Ledger),
                 ok = blockchain_ledger_v1:vars_nonce(VarsNonce, Ledger),
                 ok = blockchain_ledger_v1:load_vars(Vars, Ledger),

                 ok = blockchain_ledger_v1:load_raw_gateways(Gateways, Ledger),
                 %% optional validator era stuff will be missing in pre validator snaps
                 case maps:find(validators, Snapshot) of
                     error -> ok;
                     {ok, Validators} ->
                         ok = blockchain_ledger_v1:load_validators(Validators, Ledger)
                 end, 
                 case maps:find(delayed_hnt, Snapshot) of
                     error -> ok;
                     {ok, DelayedHNT} ->
                         ok = blockchain_ledger_v1:load_delayed_hnt(DelayedHNT, Ledger)
                 end, 
                 ok = blockchain_ledger_v1:load_raw_pocs(PoCs, Ledger),
                 ok = blockchain_ledger_v1:load_raw_accounts(Accounts, Ledger),
                 ok = blockchain_ledger_v1:load_raw_dc_accounts(DCAccounts, Ledger),
                 ok = blockchain_ledger_v1:load_raw_security_accounts(SecurityAccounts, Ledger),

                 ok = blockchain_ledger_v1:load_htlcs(HTLCs, Ledger),

                 ok = blockchain_ledger_v1:load_ouis(OUIs, Ledger),
                 ok = blockchain_ledger_v1:load_subnets(Subnets, Ledger),
                 ok = blockchain_ledger_v1:set_oui_counter(OUICounter, Ledger),

                 ok = blockchain_ledger_v1:load_hexes(Hexes, Ledger),
                 ok = blockchain_ledger_v1:load_h3dex(H3dex, Ledger),

                 ok = blockchain_ledger_v1:load_state_channels(StateChannels, Ledger),

                 ok = blockchain_ledger_v1:load_oracle_price(OraclePrice, Ledger),
                 ok = blockchain_ledger_v1:load_oracle_price_list(OraclePriceList, Ledger),

                 blockchain_ledger_v1:commit_context(Ledger)
             end
             || Mode <- [delayed, active]],
            Ledger2 = blockchain_ledger_v1:new_context(Ledger0),
            Chain1 = blockchain:ledger(Ledger2, Chain),
            {ok, Curr2} = blockchain_ledger_v1:current_height(Ledger2),
            lager:info("ledger height is ~p after absorbing snapshot", [Curr2]),
            lager:info("snapshot contains ~p blocks", [length(Blocks)]),

            case Blocks of
                [] ->
                    %% ignore blocks in testing
                    ok;
                Blocks ->
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
                              case blockchain:get_block(Ht, Chain) of
                                  {ok, _Block} ->
                                      %% already have it, don't need to store it again.
                                      ok;
                                  _ ->
                                      ok = blockchain:save_block(Block, Chain)
                              end,
                              case Ht > Curr2 of
                                  %% we need some blocks before for history, only absorb if they're
                                  %% not on the ledger already
                                  true ->
                                      lager:info("loading block ~p", [Ht]),
                                      Rescue = blockchain_block:is_rescue_block(Block),
                                      {ok, _Chain} = blockchain_txn:absorb_block(Block, Rescue, Chain1),
                                      Hash = blockchain_block:hash_block(Block),
                                      ok = blockchain_ledger_v1:maybe_gc_pocs(Chain1, Ledger2),

                                      ok = blockchain_ledger_v1:maybe_gc_scs(Chain1),

                                      ok = blockchain_ledger_v1:refresh_gateway_witnesses(Hash, Ledger2),
                                      ok = blockchain_ledger_v1:maybe_recalc_price(Chain1, Ledger2);
                                  _ ->
                                      ok
                              end
                      end,
                      Blocks)
            end,
            blockchain_ledger_v1:commit_context(Ledger2),
            {ok, Curr3} = blockchain_ledger_v1:current_height(Ledger0),
            lager:info("ledger height is ~p after absorbing blocks", [Curr3]),

            %% store the snapshot if we don't have it already
            case blockchain:get_snapshot(SHA, Chain) of
                {ok, _Snap} -> ok;
                {error, not_found} ->
                    blockchain:add_snapshot(Snapshot, Chain)
            end,

            {ok, Ledger0};
        _ ->
            {error, bad_snapshot_checksum}
    end.

get_blocks(Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    %% this is for rewards calculation when an epoch ends
    %% see https://github.com/helium/blockchain-core/pull/627
    #{ election_height := ElectionHeight } = blockchain_election:election_info(Ledger, Chain),
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
    {ok, DHeight} = blockchain_ledger_v1:current_height(DLedger),

    %% We need _at least_ the grace blocks before current election
    %% or the delayed ledger height less 181 blocks, whichever is
    %% lower.
    LoadBlockStart = min(DHeight - 181, ElectionHeight - GraceBlocks),

    [begin
         {ok, B} = blockchain:get_raw_block(N, Chain),
         B
     end
     || N <- lists:seq(max(?min_height, LoadBlockStart), Height)].

height(#{current_height := Height}) ->
    Height.

hash(#{version := v5} = Snap) ->
    {ok, BinSnap} = serialize(Snap, noblocks),
    crypto:hash(sha256, BinSnap).

hash_v1(#blockchain_snapshot_v1{} = Snap) ->
    {ok, BinSnap} = serialize_v1(Snap, noblocks),
    crypto:hash(sha256, BinSnap).

hash_v2(#blockchain_snapshot_v2{} = Snap) ->
    {ok, BinSnap} = serialize_v2(Snap, noblocks),
    crypto:hash(sha256, BinSnap).

hash_v3(#blockchain_snapshot_v3{} = Snap) ->
    {ok, BinSnap} = serialize_v3(Snap, noblocks),
    crypto:hash(sha256, BinSnap).

hash_v4(#blockchain_snapshot_v4{} = Snap) ->
    {ok, BinSnap} = serialize_v4(Snap, noblocks),
    crypto:hash(sha256, BinSnap).

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

v2_to_v1(#blockchain_snapshot_v2{
            previous_snapshot_hash = <<>>,
            leading_hash = <<>>,

            current_height = CurrHeight,
            transaction_fee =  _TxnFee,
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
    #blockchain_snapshot_v1{
       previous_snapshot_hash = <<>>,
       leading_hash = <<>>,

       current_height = CurrHeight,
       transaction_fee =  0,
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

       gateways = reserialize(fun blockchain_ledger_gateway_v2:serialize/1, Gateways),
       pocs = reserialize_pocs(PoCs),

       accounts = reserialize(fun blockchain_ledger_entry_v1:serialize/1, Accounts),
       dc_accounts = reserialize(fun blockchain_ledger_data_credits_entry_v1:serialize/1, DCAccounts),

       security_accounts = reserialize(fun blockchain_ledger_security_entry_v1:serialize/1, SecurityAccounts),

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


reserialize(Fun, Values) ->
    lists:map(fun({K, V}) ->
                      {K, Fun(V)}
              end,
              Values).

reserialize_pocs(Values) ->
    lists:map(fun({K, V}) ->
                      List = lists:map(fun blockchain_ledger_poc_v2:serialize/1, V),
                      Value = term_to_binary(List),
                      {K, Value}
              end,
              Values).

v3_to_v2(#blockchain_snapshot_v3{
            current_height = CurrHeight,
            transaction_fee =  _TxnFee,
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
    #blockchain_snapshot_v2{
       previous_snapshot_hash = <<>>,
       leading_hash = <<>>,

       current_height = CurrHeight,
       transaction_fee =  0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       %% these need to be deserialized

       gateways = deserialize(fun blockchain_ledger_gateway_v2:deserialize/1, Gateways),
       pocs = deserialize_pocs(PoCs),

       accounts = deserialize(fun blockchain_ledger_entry_v1:deserialize/1, Accounts),
       dc_accounts = deserialize(fun blockchain_ledger_data_credits_entry_v1:deserialize/1, DCAccounts),

       security_accounts = deserialize(fun blockchain_ledger_security_entry_v1:deserialize/1, SecurityAccounts),

       %% end deserialize

       token_burn_rate = 0,

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

v4_to_v3(#blockchain_snapshot_v4{
            current_height = CurrHeight,
            transaction_fee =  _TxnFee,
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
       transaction_fee =  _TxnFee,
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
      }.

v5_to_v4(#{
           version := v5,
           current_height := CurrHeight,
           consensus_members := ConsensusMembers,

           election_height := ElectionHeight,
           election_epoch := ElectionEpoch,

           delayed_vars := DelayedVars,
           threshold_txns := ThresholdTxns,

           master_key := MasterKey,
           multi_keys := MultiKeys,
           vars_nonce := VarsNonce,
           vars := Vars,

           gateways := Gateways,
           pocs := PoCs,

           accounts := Accounts,
           dc_accounts := DCAccounts,

           security_accounts := SecurityAccounts,

           htlcs := HTLCs,

           ouis := OUIs,
           subnets := Subnets,
           oui_counter := OUICounter,

           hexes := Hexes,
           h3dex := _H3dex,

           state_channels := StateChannels,

           blocks := Blocks,

           oracle_price := OraclePrice,
           oracle_price_list := OraclePriceList}) ->
    #blockchain_snapshot_v4{
       current_height = CurrHeight,
       consensus_members = ConsensusMembers,

       transaction_fee =  0,

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
      }.

deserialize(Fun, Values) ->
    lists:map(fun({K, V}) ->
                      {K, Fun(V)}
              end,
              Values).

deserialize_pocs(Values) ->
    lists:map(fun({K, V}) ->
                      List = binary_to_term(V),
                      Value = lists:map(fun blockchain_ledger_poc_v2:deserialize/1, List),
                      {K, Value}
              end,
              Values).

diff(A, B) ->
    Fields = lists:sort(maps:keys(A)),
    Comp = lists:foldl(
             fun(Field, Acc) ->
                     AV = maps:get(Field, A),
                     BV = maps:get(Field, B),
                     [{Field, AV, BV} | Acc]
             end,
             [],
             Fields),
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
                          gateways ->
                              AUniq = AI -- BI,
                              BUniq = BI -- AI,
                              case diff_gateways(AUniq, BUniq, []) of
                                  [] ->
                                      Acc;
                                  Diff ->
                                      [{gateways, Diff} | Acc]
                              end;
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
      Comp).

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
