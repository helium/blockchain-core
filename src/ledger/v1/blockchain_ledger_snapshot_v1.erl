-module(blockchain_ledger_snapshot_v1).

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

-record(blockchain_snapshot_v1,
        {
         %% meta stuff here
         previous_snapshot_hash :: binary(),
         leading_hash :: binary(),

         %% ledger stuff here
         current_height :: pos_integer(),
         transaction_fee :: non_neg_integer(),
         consensus_members :: [libp2p_crypto:pubkey_bin()],

         %% these are updated but never used.  Not sure what to do!
         election_height :: pos_integer(),
         election_epoch :: pos_integer(),

         delayed_vars :: [any()],
         threshold_txns :: [any()],

         master_key :: binary(),
         vars_nonce :: pos_integer(),
         vars :: [any()],

         gateways :: [any()],
         pocs :: [any()],

         accounts :: [any()],
         dc_accounts :: [any()],

         token_burn_rate :: non_neg_integer(),

         security_accounts :: [any()],

         htlcs :: [any()],

         ouis :: [any()],
         subnets :: [any()],
         oui_counter :: pos_integer(),

         hexes :: [any()],

         state_channels :: [any()],

         blocks :: [blockchain_block:block()]
        }).

-record(blockchain_snapshot_v2,
        {
         %% meta stuff here
         previous_snapshot_hash :: binary(),
         leading_hash :: binary(),

         %% ledger stuff here
         current_height :: pos_integer(),
         transaction_fee :: non_neg_integer(),
         consensus_members :: [libp2p_crypto:pubkey_bin()],

         %% these are updated but never used.  Not sure what to do!
         election_height :: pos_integer(),
         election_epoch :: pos_integer(),

         delayed_vars :: [any()],
         threshold_txns :: [any()],

         master_key :: binary(),
         vars_nonce :: pos_integer(),
         vars :: [any()],

         gateways :: [any()],
         pocs :: [any()],

         accounts :: [any()],
         dc_accounts :: [any()],

         token_burn_rate :: non_neg_integer(),

         security_accounts :: [any()],

         htlcs :: [any()],

         ouis :: [any()],
         subnets :: [any()],
         oui_counter :: pos_integer(),

         hexes :: [any()],

         state_channels :: [any()],

         blocks :: [blockchain_block:block()],

         oracle_price = 0 :: non_neg_integer(),
         oracle_price_list = [] :: [any()]
        }).

snapshot(Ledger0, Blocks) ->
    try
        %% TODO: actually verify we're delayed here instead of
        %% changing modes?
        Ledger = blockchain_ledger_v1:mode(delayed, Ledger0),
        {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),
        {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(Ledger),
        {ok, ElectionHeight} = blockchain_ledger_v1:election_height(Ledger),
        {ok, ElectionEpoch} = blockchain_ledger_v1:election_epoch(Ledger),
        {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
        DelayedVars = blockchain_ledger_v1:snapshot_delayed_vars(Ledger),
        ThresholdTxns = blockchain_ledger_v1:snapshot_threshold_txns(Ledger),
        {ok, VarsNonce} = blockchain_ledger_v1:vars_nonce(Ledger),
        Vars = blockchain_ledger_v1:snapshot_vars(Ledger),
        Gateways = blockchain_ledger_v1:snapshot_gateways(Ledger),
        %% need to write these on the ledger side
        PoCs = blockchain_ledger_v1:snapshot_pocs(Ledger),
        Accounts = blockchain_ledger_v1:snapshot_accounts(Ledger),
        DCAccounts = blockchain_ledger_v1:snapshot_dc_accounts(Ledger),
        SecurityAccounts = blockchain_ledger_v1:snapshot_security_accounts(Ledger),

        %%{ok, TokenBurnRate} = blockchain_ledger_v1:token_burn_exchange_rate(Ledger),

        HTLCs = blockchain_ledger_v1:snapshot_htlcs(Ledger),

        OUIs = blockchain_ledger_v1:snapshot_ouis(Ledger),
        Subnets = blockchain_ledger_v1:snapshot_subnets(Ledger),
        {ok, OUICounter} = blockchain_ledger_v1:get_oui_counter(Ledger),

        Hexes = blockchain_ledger_v1:snapshot_hexes(Ledger),

        StateChannels = blockchain_ledger_v1:snapshot_state_channels(Ledger),

        {ok, OraclePrice} = blockchain_ledger_v1:current_oracle_price(Ledger),
        {ok, OraclePriceList} = blockchain_ledger_v1:current_oracle_price_list(Ledger),

        Snapshot =
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

               blocks = Blocks,

               oracle_price = OraclePrice,
               oracle_price_list = OraclePriceList
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
                                           end, Snapshot#blockchain_snapshot_v2.blocks),
                        Snapshot#blockchain_snapshot_v2{blocks = Blocks};
                    noblocks -> Snapshot#blockchain_snapshot_v2{blocks = []}
                end,
    Bin = term_to_binary(Snapshot1, [{compressed, 9}]),
    BinSz = byte_size(Bin),

    %% do some simple framing with version, size, & snap
    Snap = <<2, %% version
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

deserialize(<<1,
              %%SHASz:16/little-unsigned-integer, SHA:SHASz/binary,
              BinSz:32/little-unsigned-integer, BinSnap:BinSz/binary>>) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v1_to_v2(OldSnapshot),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize(<<2,
              %%SHASz:16/little-unsigned-integer, SHA:SHASz/binary,
              BinSz:32/little-unsigned-integer, BinSnap:BinSz/binary>>) ->
    try binary_to_term(BinSnap) of
        #blockchain_snapshot_v2{} = Snapshot ->
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end.

%% sha will be stored externally
import(Chain, SHA,
       #blockchain_snapshot_v2{
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

          blocks = Blocks0,

          oracle_price = OraclePrice,
          oracle_price_list = OraclePriceList
         } = Snapshot) ->
    Dir = blockchain:dir(Chain),
    case hash(Snapshot) == SHA orelse
        hash_v1(v2_to_v1(Snapshot)) == SHA of
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
                 ok = blockchain_ledger_v1:vars_nonce(VarsNonce, Ledger),
                 ok = blockchain_ledger_v1:load_vars(Vars, Ledger),
                 ok = blockchain_ledger_v1:load_gateways(Gateways, Ledger),
                 %% need to write these on the ledger side
                 ok = blockchain_ledger_v1:load_pocs(PoCs, Ledger),
                 ok = blockchain_ledger_v1:load_accounts(Accounts, Ledger),
                 ok = blockchain_ledger_v1:load_dc_accounts(DCAccounts, Ledger),
                 ok = blockchain_ledger_v1:load_security_accounts(SecurityAccounts, Ledger),

                 %% ok = blockchain_ledger_v1:token_burn_exchange_rate(TokenBurnRate, Ledger),

                 ok = blockchain_ledger_v1:load_htlcs(HTLCs, Ledger),
                 %% add once this is added

                 ok = blockchain_ledger_v1:load_ouis(OUIs, Ledger),
                 ok = blockchain_ledger_v1:load_subnets(Subnets, Ledger),
                 ok = blockchain_ledger_v1:set_oui_counter(OUICounter, Ledger),

                 ok = blockchain_ledger_v1:load_hexes(Hexes, Ledger),

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
            lager:info("snapshot contains ~p blocks", [length(Blocks0)]),

            Blocks = lists:map(fun(B) when is_binary(B) ->
                                       blockchain_block:deserialize(B);
                                  (B) -> B
                               end, Blocks0),

            case Blocks of
                [] ->
                    %% ignore blocks in testing
                    ok;
                Blocks ->
                    %% just store the head, we'll need it sometimes
                    lists:foreach(
                      fun(Block) ->
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
                                      ok = blockchain_ledger_v1:maybe_gc_pocs(Chain, Ledger2),

                                      ok = blockchain_ledger_v1:maybe_gc_scs(Chain),

                                      ok = blockchain_ledger_v1:refresh_gateway_witnesses(Hash, Ledger2);
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
    DLedger = blockchain_ledger_v1:mode(delayed, Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    {ok, DHeight} = blockchain_ledger_v1:current_height(DLedger),
    [begin
         {ok, B} = blockchain:get_block(N, Chain),
         B
     end
     || N <- lists:seq(max(?min_height, DHeight - 181), Height)].

height(#blockchain_snapshot_v2{current_height = Height}) ->
    Height.

hash(#blockchain_snapshot_v2{} = Snap) ->
    {ok, BinSnap} = serialize(Snap, noblocks),
    crypto:hash(sha256, BinSnap).

hash_v1(#blockchain_snapshot_v1{} = Snap) ->
    {ok, BinSnap} = serialize_v1(Snap, noblocks),
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

diff(A, B) ->
    Fields = record_info(fields, blockchain_snapshot_v2),
    [_ | AL] = tuple_to_list(A),
    [_ | BL] = tuple_to_list(B),
    Comp = lists:zip3(Fields, AL, BL),
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
                              Diff = diff_gateways(AUniq, BUniq, []),
                              [{gateways, Diff} | Acc];
                          blocks ->
                              AHeightAndHash = [ {blockchain_block:height(Block), blockchain_block:hash_block(Block)} || Block <- AI],
                              BHeightAndHash = [ {blockchain_block:height(Block), blockchain_block:hash_block(Block)} || Block <- BI],
                              case {AHeightAndHash -- BHeightAndHash, BHeightAndHash -- AHeightAndHash} of
                                  {[], []} ->
                                      Acc;
                                  {ADiffs, BDiffs} ->
                                      [{Field, [Height || {Height, _Hash} <- ADiffs], [Height || {Height, _Hash} <- BDiffs]} | Acc]
                              end;
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
            diff_gateways(T, lists:keydelete(Addr, 1, BList),
                          [{Addr, minimize_gw(A, B)} | Acc])
    end.

gwget(Addr, L) ->
    case lists:keyfind(Addr, 1, L) of
        {_, GW} ->
            GW;
        false ->
            missing
    end.

minimize_gw(A, B) ->
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
    [{witnesses, minimize_witnesses(AWits, BWits)} | Compare].

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
