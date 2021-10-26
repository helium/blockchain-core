-module(blockchain_aux_ledger_v1).

-export([
    new/1,
    bootstrap/2,

    set_vars/2,
    get_rewards_at/2,
    set_rewards/4,
    get_rewards/1,
    diff_rewards_for/2,
    diff_rewards/1,
    diff_reward_sums/1,

    set_rewards_md/4,
    get_rewards_md/1,
    get_rewards_md_for/3,
    get_rewards_md_at/2,

    get_rewards_md_sums/1,
    get_rewards_md_sums_at/2,

    get_rewards_md_diff/1,
    get_rewards_md_diff_at/2,

    diff_rewards_md_sums/1,
    overall_diff_rewards_md_sums/1
]).

-include("blockchain_vars.hrl").
-include("blockchain_ledger_v1.hrl").
-include_lib("helium_proto/include/blockchain_txn_rewards_v2_pb.hrl").

%% ==================================================================
%% Type definitions
%% ==================================================================

-type reward_diff() :: {
    ActualRewards :: blockchain_txn_reward_v1:rewards(),
    AuxRewards :: blockchain_txn_reward_v1:rewards()
}.
-type reward_md_diff() :: {
    ActualRewardsMD :: blockchain_txn_reward_v1:rewards_metadata(),
    AuxRewardsMD :: blockchain_txn_reward_v1:rewards_metadata()
}.
-type reward_diff_sum() :: #{
    Key ::
        binary() => {Orig :: #{amount => non_neg_integer()}, Aux :: #{amount => non_neg_integer()}}
}.
-type reward_diff_map() :: #{Height :: non_neg_integer() => reward_diff_sum()}.
-type aux_rewards() :: #{Height :: non_neg_integer() => reward_diff()}.
-type aux_rewards_md() :: #{Height :: non_neg_integer() => reward_md_diff()}.
-type gw_rewards_md() :: #{Ht :: non_neg_integer() => {non_neg_integer(), non_neg_integer()}}.

%% ==================================================================
%% aux ledger key prefixes
%% ==================================================================

aux_height(Height) ->
    <<?aux_height_prefix, (integer_to_binary(Height))/binary>>.

aux_height_md(Height) ->
    <<?aux_height_md_prefix, (integer_to_binary(Height))/binary>>.

aux_height_diff(Height) ->
    <<?aux_height_diff_prefix, (integer_to_binary(Height))/binary>>.

aux_height_diffsum(Height) ->
    <<?aux_height_diffsum_prefix, (integer_to_binary(Height))/binary>>.

%% ==================================================================
%% API
%% ==================================================================

new(Ledger) ->
    case application:get_env(blockchain, aux_ledger_dir, undefined) of
        undefined ->
            Ledger;
        Path ->
            new(Path, Ledger)
    end.

new(Path, Ledger) ->
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    {ok, DB, CFs} = blockchain_ledger_v1:open_db(aux, Path, false, false, GlobalOpts),
    [
        DefaultCF,
        AGwsCF,
        EntriesCF,
        DCEntriesCF,
        HTLCsCF,
        PoCsCF,
        SecuritiesCF,
        RoutingCF,
        SubnetsCF,
        SCsCF,
        H3DexCF,
        GwDenormCF,
        ValidatorsCF,
        AuxHeightsCF,
        AuxHeightsMDCF,
        AuxHeightsDiffCF,
        AuxHeightsDiffSumCF
    ] = CFs,
    Ledger#ledger_v1{
        aux = #aux_ledger_v1{
            dir = Path,
            db = DB,
            aux_heights = AuxHeightsCF,
            aux_heights_md = AuxHeightsMDCF,
            aux_heights_diff = AuxHeightsDiffCF,
            aux_heights_diffsum = AuxHeightsDiffSumCF,
            aux = #sub_ledger_v1{
                default = DefaultCF,
                active_gateways = AGwsCF,
                gw_denorm = GwDenormCF,
                entries = EntriesCF,
                dc_entries = DCEntriesCF,
                htlcs = HTLCsCF,
                pocs = PoCsCF,
                securities = SecuritiesCF,
                routing = RoutingCF,
                subnets = SubnetsCF,
                state_channels = SCsCF,
                h3dex = H3DexCF,
                validators = ValidatorsCF
            }
        }
    }.

-spec bootstrap(Path :: file:filename_all(), Ledger :: ledger()) -> ledger().
bootstrap(Path, Ledger) ->
    Exists = filelib:is_dir(Path),
    NewLedger = new(Path, Ledger),
    case Exists of
        true ->
            %% assume no need to bootstrap
            lager:info("aux_ledger already exists in path: ~p", [Path]),
            NewLedger;
        false ->
            case blockchain_ledger_v1:current_height(Ledger) of
                {ok, Height} when Height > 0 ->
                    %% bootstrap from active ledger
                    lager:info("bootstrapping aux_ledger from active ledger in path: ~p", [Path]),
                    {ok, Snap} = blockchain_ledger_snapshot_v1:snapshot(Ledger, [], [], active),
                    blockchain_ledger_snapshot_v1:load_into_ledger(Snap, NewLedger, aux),
                    NewLedger;
                _ ->
                    NewLedger
            end
    end.

-spec set_vars(AuxVars :: map(), AuxLedger :: ledger()) -> ok.
set_vars(AuxVars, #ledger_v1{mode = aux} = AuxLedger) ->
    ok = blockchain_utils:teardown_var_cache(),
    Ctx = blockchain_ledger_v1:new_context(AuxLedger),
    ok = blockchain_ledger_v1:vars(AuxVars, [], Ctx),
    ok = blockchain_txn_vars_v1:process_hooks(AuxVars, [], Ctx),
    ok = blockchain_ledger_v1:commit_context(Ctx),
    ok;
set_vars(_ExtraVars, _Ledger) ->
    error(cannot_set_vars_not_aux_ledger).

-spec get_rewards_at(Height :: non_neg_integer(), Ledger :: ledger()) ->
    {ok, blockchain_txn_reward_v1:rewards() | blockchain_txn_rewards_v2:rewards(),
        blockchain_txn_reward_v1:rewards() | blockchain_txn_rewards_v2:rewards()}
    | {error, any()}.
get_rewards_at(Height, Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false ->
            {error, no_aux_ledger};
        true ->
            AuxDB = blockchain_ledger_v1:aux_db(Ledger),
            AuxHeightsCF = blockchain_ledger_v1:aux_heights_cf(Ledger),
            Key = aux_height(Height),
            case rocksdb:get(AuxDB, AuxHeightsCF, Key, []) of
                {ok, BinRes} -> {ok, binary_to_term(BinRes)};
                not_found -> {error, not_found};
                Error -> Error
            end
    end.

-spec get_rewards(Ledger :: ledger()) -> aux_rewards().
get_rewards(Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false -> #{};
        true -> get_rewards_(Ledger)
    end.

-spec set_rewards(
    Height :: non_neg_integer(),
    Rewards :: blockchain_txn_reward_v1:rewards() | blockchain_txn_rewards_v2:rewards(),
    AuxRewards :: blockchain_txn_reward_v1:rewards() | blockchain_txn_rewards_v2:rewards(),
    Ledger :: ledger()
) -> ok | {error, any()}.
set_rewards(Height, Rewards, AuxRewards, Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false ->
            {error, no_aux_ledger};
        true ->
            AuxDB = blockchain_ledger_v1:aux_db(Ledger),
            AuxHeightsCF = blockchain_ledger_v1:aux_heights_cf(Ledger),
            Key = aux_height(Height),
            case rocksdb:get(AuxDB, AuxHeightsCF, Key, []) of
                {ok, _} ->
                    %% already exists, don't do anything
                    ok;
                not_found ->
                    Value = term_to_binary({Rewards, AuxRewards}),
                    rocksdb:put(AuxDB, AuxHeightsCF, Key, Value, []);
                Error ->
                    Error
            end
    end.

%% @doc Get aux reward diff for Account | Gateway (Key)
-spec diff_rewards_for(
    Key :: libp2p_crypto:pubkey_bin(),
    Ledger :: ledger()
) -> map().
diff_rewards_for(Key, Ledger) ->
    Diff = diff_rewards(Ledger),
    maps:fold(
        fun(Height, Res, Acc) ->
            maps:put(Height, maps:get(Key, Res, undefined), Acc)
        end,
        #{},
        Diff
    ).

-spec diff_rewards(Ledger :: ledger()) -> reward_diff_map().
diff_rewards(Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false ->
            #{};
        true ->
            OverallAuxRewards = get_rewards(Ledger),
            TallyFun =
                case blockchain:config(?rewards_txn_version, Ledger) of
                    {ok, 2} ->
                        tally_fun_v2();
                    _ ->
                        tally_fun_v1()
                end,

            DiffFun = fun(Height, {ActualRewards, AuxRewards}, Acc) ->
                ActualAccountBalances = lists:foldl(TallyFun, #{}, ActualRewards),
                AuxAccountBalances = lists:foldl(TallyFun, #{}, AuxRewards),
                Combined = maps:merge(AuxAccountBalances, ActualAccountBalances),
                Res = maps:fold(
                    fun(K, V, Acc2) ->
                        V2 = maps:get(K, AuxAccountBalances, #{amount => 0}),
                        case V == V2 of
                            true ->
                                %% check this is not missing in actual balances
                                case maps:is_key(K, ActualAccountBalances) of
                                    false ->
                                        maps:put(K, {#{amount => 0}, V}, Acc2);
                                    true ->
                                        %% no difference
                                        Acc2
                                end;
                            false ->
                                maps:put(K, {V, V2}, Acc2)
                        end
                    end,
                    #{},
                    Combined
                ),
                maps:put(Height, Res, Acc)
            end,

            maps:fold(DiffFun, #{}, OverallAuxRewards)
    end.

-spec diff_reward_sums(Ledger :: ledger()) -> reward_diff_sum().
diff_reward_sums(Ledger) ->
    Diff = diff_rewards(Ledger),
    acc_diff_reward_sums_(maps:values(Diff)).

-spec set_rewards_md(
    Height :: non_neg_integer(),
    OrigMD :: blockchain_txn_rewards_v2:rewards_metadata(),
    AuxMD :: blockchain_txn_rewards_v2:rewards_metadata(),
    Ledger :: ledger()
) -> ok | {error, any()}.
set_rewards_md(Height, OrigMD, AuxMD, Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false ->
            {error, no_aux_ledger};
        true ->
            AuxDB = blockchain_ledger_v1:aux_db(Ledger),
            AuxHeightsMDCF = blockchain_ledger_v1:aux_heights_md_cf(Ledger),
            AuxHeightsDiffCF = blockchain_ledger_v1:aux_heights_diff_cf(Ledger),
            AuxHeightsDiffSumCF = blockchain_ledger_v1:aux_heights_diffsum_cf(Ledger),
            MDKey = aux_height_md(Height),
            DiffKey = aux_height_diff(Height),
            DiffSumKey = aux_height_diffsum(Height),
            case rocksdb:get(AuxDB, AuxHeightsMDCF, MDKey, []) of
                {ok, _} ->
                    %% already exists, don't do anything
                    ok;
                not_found ->
                    MDValue = term_to_binary({OrigMD, AuxMD}),
                    %% Doing calculate_reward_diff_map with a single k:v input
                    %% would yield a single element list as value, so we just use head here
                    WDiffMap = calculate_reward_diff_map(witnesses, #{Height => {OrigMD, AuxMD}}),
                    CDiffMap = calculate_reward_diff_map(challengees, #{Height => {OrigMD, AuxMD}}),
                    WDiffVal = hd(maps:values(WDiffMap)),
                    CDiffVal = hd(maps:values(CDiffMap)),
                    DiffValue = term_to_binary(#{witnesses => WDiffVal, challengees => CDiffVal}),

                    %% Also calculate overall MDSum
                    WSum = acc_diff_reward_sums_(maps:values(WDiffMap)),
                    CSum = acc_diff_reward_sums_(maps:values(CDiffMap)),
                    MDSum = overall_diff_aux_rewards_md_sums(WSum, CSum),

                    %% Also keep track of ALL tallied MD sums
                    OverallMDSum = overall_diff_rewards_md_sums(Ledger),
                    NewOverallMDSum = acc_diff_reward_sums_([MDSum, OverallMDSum]),
                    MDSumValue = term_to_binary(#{mdsum => MDSum, overall => NewOverallMDSum}),

                    {ok, Batch} = rocksdb:batch(),
                    ok = rocksdb:batch_put(Batch, AuxHeightsMDCF, MDKey, MDValue),
                    ok = rocksdb:batch_put(Batch, AuxHeightsDiffCF, DiffKey, DiffValue),
                    ok = rocksdb:batch_put(Batch, AuxHeightsDiffSumCF, DiffSumKey, MDSumValue),
                    rocksdb:write_batch(AuxDB, Batch, []);
                Error ->
                    Error
            end
    end.

-spec get_rewards_md(Ledger :: ledger()) -> aux_rewards_md().
get_rewards_md(Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false -> #{};
        true -> get_rewards_md_(Ledger)
    end.

-spec get_rewards_md_at(
    Height :: non_neg_integer(),
    Ledger :: ledger()
) ->
    {ok, reward_md_diff()} | {error, any()}.
get_rewards_md_at(Height, Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false ->
            {error, no_aux_ledger};
        true ->
            AuxDB = blockchain_ledger_v1:aux_db(Ledger),
            AuxHeightsMDCF = blockchain_ledger_v1:aux_heights_md_cf(Ledger),
            Key = aux_height_md(Height),
            case rocksdb:get(AuxDB, AuxHeightsMDCF, Key, []) of
                {ok, BinRes} -> {ok, binary_to_term(BinRes)};
                not_found -> {error, not_found};
                Error -> Error
            end
    end.

-spec get_rewards_md_sums(Ledger :: ledger()) -> reward_diff_map().
get_rewards_md_sums(Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false -> #{};
        true -> get_rewards_md_sums_(Ledger)
    end.

-spec get_rewards_md_sums_at(
    Height :: pos_integer(),
    Ledger :: ledger()
) -> reward_diff_sum().
get_rewards_md_sums_at(Height, Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false ->
            {error, no_aux_ledger};
        true ->
            AuxDB = blockchain_ledger_v1:aux_db(Ledger),
            AuxHeightsMDDiffSumCF = blockchain_ledger_v1:aux_heights_diffsum_cf(Ledger),
            Key = aux_height_diffsum(Height),
            case rocksdb:get(AuxDB, AuxHeightsMDDiffSumCF, Key, []) of
                {ok, BinRes} -> {ok, binary_to_term(BinRes)};
                not_found -> {error, not_found};
                Error -> Error
            end
    end.

-spec get_rewards_md_diff(Ledger :: ledger()) -> #{Height :: pos_integer() => reward_diff_map()}.
get_rewards_md_diff(Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false -> #{};
        true -> get_rewards_md_diff_(Ledger)
    end.

-spec get_rewards_md_diff_at(
    Height :: pos_integer(),
    Ledger :: ledger()
) -> reward_diff_map().
get_rewards_md_diff_at(Height, Ledger) ->
    case blockchain_ledger_v1:has_aux(Ledger) of
        false ->
            {error, no_aux_ledger};
        true ->
            AuxDB = blockchain_ledger_v1:aux_db(Ledger),
            AuxHeightsMDDiffCF = blockchain_ledger_v1:aux_heights_diff_cf(Ledger),
            Key = aux_height_diff(Height),
            case rocksdb:get(AuxDB, AuxHeightsMDDiffCF, Key, []) of
                {ok, BinRes} ->
                    %% NOTE: This should return #{mdsum : MDSum, overall : OverallMDSum}
                    {ok, binary_to_term(BinRes)};
                not_found -> {error, not_found};
                Error -> Error
            end
    end.

-spec get_rewards_md_for(
    Type :: witness | challengee,
    GwPubkeyBin :: libp2p_crypto:pubkey_bin(),
    Ledger :: ledger()
) -> gw_rewards_md().
get_rewards_md_for(witness, GwPubkeyBin, Ledger) ->
    MD = get_rewards_md(Ledger),
    get_rewards_md_for_(poc_witness, GwPubkeyBin, MD);
get_rewards_md_for(challengee, GwPubkeyBin, Ledger) ->
    MD = get_rewards_md(Ledger),
    get_rewards_md_for_(poc_challengee, GwPubkeyBin, MD).

-spec get_rewards_md_for_(
    Type :: poc_witness | poc_challengee,
    GwPubkeyBin :: libp2p_crypto:pubkey_bin(),
    MD :: aux_rewards_md()
) -> gw_rewards_md().
get_rewards_md_for_(Type, GwPubkeyBin, MD) ->
    maps:fold(
        fun(Ht, {OrigMD, AuxMD}, Acc) ->
            OrigWitMD = maps:get(Type, OrigMD),
            AuxWitMD = maps:get(Type, AuxMD),

            Key =
                case Type of
                    poc_challengee -> {gateway, poc_challengees, GwPubkeyBin};
                    poc_witness -> {gateway, poc_witnesses, GwPubkeyBin}
                end,

            maps:put(
                Ht,
                {maps:get(Key, OrigWitMD, 0), maps:get(Key, AuxWitMD, 0)},
                Acc
            )
        end,
        #{},
        MD
    ).

-spec diff_rewards_md_sums(Ledger :: blockchain_ledger_v1:ledger()) -> reward_diff_sum().
diff_rewards_md_sums(Ledger) ->
    DiffSums = get_rewards_md_sums(Ledger),
    acc_diff_reward_sums_(maps:values(DiffSums)).

-spec overall_diff_rewards_md_sums(Ledger :: blockchain_ledger_v1:ledger()) -> reward_diff_sum().
overall_diff_rewards_md_sums(Ledger) ->
    {ok, Itr} = rocksdb:iterator(
        blockchain_ledger_v1:aux_db(Ledger),
        blockchain_ledger_v1:aux_heights_diffsum_cf(Ledger),
        []
    ),
    Res = overall_diff_rewards_md_sums_(rocksdb:iterator_move(Itr, last), #{}),
    catch rocksdb:iterator_close(Itr),
    Res.

%% ==================================================================
%% Helper Functions
%% ==================================================================

get_rewards_md_(Ledger) ->
    {ok, Itr} = rocksdb:iterator(
        blockchain_ledger_v1:aux_db(Ledger),
        blockchain_ledger_v1:aux_heights_md_cf(Ledger),
        []
    ),
    Res = get_aux_rewards_md_(Itr, rocksdb:iterator_move(Itr, first), #{}),
    catch rocksdb:iterator_close(Itr),
    Res.

get_aux_rewards_md_(_Itr, {error, _}, Acc) ->
    Acc;
get_aux_rewards_md_(Itr, {ok, Key, BinRes}, Acc) ->
    NewAcc =
        try binary_to_term(BinRes) of
            {OrigMD, AuxMD} ->
                <<"aux_height_md_", Height/binary>> = Key,
                maps:put(binary_to_integer(Height), {OrigMD, AuxMD}, Acc)
        catch
            What:Why ->
                lager:warning("error when deserializing plausible block at key ~p: ~p ~p", [
                    Key,
                    What,
                    Why
                ]),
                Acc
        end,
    get_aux_rewards_md_(Itr, rocksdb:iterator_move(Itr, next), NewAcc).

get_rewards_md_sums_(Ledger) ->
    {ok, Itr} = rocksdb:iterator(
        blockchain_ledger_v1:aux_db(Ledger),
        blockchain_ledger_v1:aux_heights_diffsum_cf(Ledger),
        []
    ),
    Res = get_aux_rewards_md_sums_(Itr, rocksdb:iterator_move(Itr, first), #{}),
    catch rocksdb:iterator_close(Itr),
    Res.

get_aux_rewards_md_sums_(_Itr, {error, _}, Acc) ->
    Acc;
get_aux_rewards_md_sums_(Itr, {ok, Key, BinRes}, Acc) ->
    NewAcc =
        try binary_to_term(BinRes) of
            #{mdsum := MDSum} ->
                <<"aux_height_diffsum_", Height/binary>> = Key,
                maps:put(binary_to_integer(Height), MDSum, Acc)
        catch
            What:Why ->
                lager:warning("error when deserializing plausible block at key ~p: ~p ~p", [
                    Key,
                    What,
                    Why
                ]),
                Acc
        end,
    get_aux_rewards_md_sums_(Itr, rocksdb:iterator_move(Itr, next), NewAcc).

get_rewards_md_diff_(Ledger) ->
    {ok, Itr} = rocksdb:iterator(
        blockchain_ledger_v1:aux_db(Ledger),
        blockchain_ledger_v1:aux_heights_diff_cf(Ledger),
        []
    ),
    Res = get_aux_rewards_md_diff_(Itr, rocksdb:iterator_move(Itr, first), #{}),
    catch rocksdb:iterator_close(Itr),
    Res.

get_aux_rewards_md_diff_(_Itr, {error, _}, Acc) ->
    Acc;
get_aux_rewards_md_diff_(Itr, {ok, Key, BinRes}, Acc) ->
    NewAcc =
        try binary_to_term(BinRes) of
            MDSum ->
                <<"aux_height_diff_", Height/binary>> = Key,
                maps:put(binary_to_integer(Height), MDSum, Acc)
        catch
            What:Why ->
                lager:warning("error when deserializing plausible block at key ~p: ~p ~p", [
                    Key,
                    What,
                    Why
                ]),
                Acc
        end,
    get_aux_rewards_md_diff_(Itr, rocksdb:iterator_move(Itr, next), NewAcc).

-spec tally_fun_v1() -> fun().
tally_fun_v1() ->
    fun(Reward, Acc0) ->
        Account = blockchain_txn_reward_v1:account(Reward),
        Amount = blockchain_txn_reward_v1:amount(Reward),
        Type = blockchain_txn_reward_v1:type(Reward),
        Acc =
            case blockchain_txn_reward_v1:gateway(Reward) of
                undefined ->
                    Acc0;
                Gateway ->
                    maps:update_with(
                        Gateway,
                        fun(V) ->
                            V#{
                                amount => maps:get(amount, V, 0) + Amount,
                                Type => maps:get(Type, V, 0) + 1
                            }
                        end,
                        #{amount => Amount, Type => 1},
                        Acc0
                    )
            end,
        maps:update_with(
            Account,
            fun(V) ->
                V#{amount => maps:get(amount, V, 0) + Amount, Type => maps:get(Type, V, 0) + 1}
            end,
            #{amount => Amount, Type => 1},
            Acc
        )
    end.

-spec tally_fun_v2() -> fun().
tally_fun_v2() ->
    fun(#blockchain_txn_reward_v2_pb{amount = Amount, account = Account}, Acc) ->
        maps:update_with(
            Account,
            fun(V) -> V#{amount => maps:get(amount, V, 0) + Amount} end,
            #{amount => Amount},
            Acc
        )
    end.

maps_sum(A, B) ->
    Keys = lists:usort(maps:keys(A) ++ maps:keys(B)),
    lists:foldl(
        fun(K, Acc) ->
            Acc#{K => maps:get(K, A, 0) + maps:get(K, B, 0)}
        end,
        #{},
        Keys
    ).

-spec calculate_reward_diff_map(
    Type :: witnesses | challengees,
    MD :: aux_rewards_md()
) -> reward_diff_map().
calculate_reward_diff_map(Type, MD) ->
    MDRewardKey =
        case Type of
            witnesses ->
                poc_witness;
            challengees ->
                poc_challengee
        end,

    Heights = maps:keys(MD),

    %% Temporary result only accumulating specific key type rewards
    Res1 = lists:foldl(
        fun(Ht, Acc) ->
            {Orig, Aux} = maps:get(Ht, MD),

            OrigRewards = maps:get(MDRewardKey, Orig),
            AuxRewards = maps:get(MDRewardKey, Aux),

            maps:put(Ht, {OrigRewards, AuxRewards}, Acc)
        end,
        #{},
        Heights
    ),

    maps:fold(
        fun(Ht, {Orig, Aux}, Acc) ->
            Orig1 = maps:fold(
                fun({gateway, _, Addr}, Value, Acc1) ->
                    maps:put(libp2p_crypto:bin_to_b58(Addr), #{amount => Value}, Acc1)
                end,
                #{},
                Orig
            ),
            Aux1 = maps:fold(
                fun({gateway, _, Addr}, Value, Acc1) ->
                    maps:put(libp2p_crypto:bin_to_b58(Addr), #{amount => Value}, Acc1)
                end,
                #{},
                Aux
            ),

            Val = lists:foldl(
                fun(Key, Acc2) ->
                    maps:put(
                        Key,
                        {maps:get(Key, Orig1, #{amount => 0}), maps:get(Key, Aux1, #{amount => 0})},
                        Acc2
                    )
                end,
                #{},
                lists:usort(maps:keys(Orig1) ++ maps:keys(Aux1))
            ),

            maps:put(Ht, Val, Acc)
        end,
        #{},
        Res1
    ).

-spec overall_diff_aux_rewards_md_sums(
    WSum :: reward_diff_sum(),
    CSum :: reward_diff_sum()
) -> reward_diff_sum().
overall_diff_aux_rewards_md_sums(WSum, CSum) ->
    lists:foldl(
        fun(Key, Acc) ->
            {OrigWVal, AuxWVal} = maps:get(Key, WSum, {#{amount => 0}, #{amount => 0}}),
            {OrigCVal, AuxCVal} = maps:get(Key, CSum, {#{amount => 0}, #{amount => 0}}),
            Tot = {
                #{amount => maps:get(amount, OrigWVal, 0) + maps:get(amount, OrigCVal, 0)},
                #{amount => maps:get(amount, AuxWVal, 0) + maps:get(amount, AuxCVal, 0)}
            },
            maps:put(Key, Tot, Acc)
        end,
        #{},
        lists:usort(maps:keys(WSum) ++ maps:keys(CSum))
    ).

get_rewards_(Ledger) ->
    {ok, Itr} = rocksdb:iterator(
        blockchain_ledger_v1:aux_db(Ledger),
        blockchain_ledger_v1:aux_heights_cf(Ledger),
        []
    ),
    Res = get_rewards_(Itr, rocksdb:iterator_move(Itr, first), #{}),
    catch rocksdb:iterator_close(Itr),
    Res.

get_rewards_(_Itr, {error, _}, Acc) ->
    Acc;
get_rewards_(Itr, {ok, Key, BinRes}, Acc) ->
    NewAcc =
        try binary_to_term(BinRes) of
            {R1, R2} ->
                <<"aux_height_", Height/binary>> = Key,
                maps:put(binary_to_integer(Height), {R1, R2}, Acc)
        catch
            What:Why ->
                lager:warning("error when deserializing plausible block at key ~p: ~p ~p", [
                    Key,
                    What,
                    Why
                ]),
                Acc
        end,
    get_rewards_(Itr, rocksdb:iterator_move(Itr, next), NewAcc).

-spec acc_diff_reward_sums_(DiffSumList :: [reward_diff_sum()]) -> reward_diff_sum().
acc_diff_reward_sums_(DiffSumList) ->
    lists:foldl(
        fun(Value, Acc) ->
            maps:fold(
                fun(PubkeyBin, {AmountBefore, AmountAfter}, Acc2) ->
                    {AB, AF} = maps:get(PubkeyBin, Acc, {#{}, #{}}),
                    maps:put(
                        PubkeyBin,
                        {maps_sum(AB, AmountBefore), maps_sum(AF, AmountAfter)},
                        Acc2
                    )
                end,
                Acc,
                Value
            )
        end,
        #{},
        DiffSumList
    ).

overall_diff_rewards_md_sums_({error, _}, Default) ->
    Default;
overall_diff_rewards_md_sums_({ok, Key, BinRes}, Default) ->
    try binary_to_term(BinRes) of
        #{overall := OverallMDSum} ->
            OverallMDSum
    catch
        What:Why ->
            lager:warning("error when deserializing plausible block at key ~p: ~p ~p", [
                                                                                        Key,
                                                                                        What,
                                                                                        Why
                                                                                       ]),
            Default
    end.
