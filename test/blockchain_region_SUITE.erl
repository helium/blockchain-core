%%--------------------------------------------------------------------
%% There are two test groups here: with_data and without_data.
%%
%% - with_data: test cases here do h3 data fetch for every regulatory region
%% - without_data: these don't
%%
%% However, data is fetched and stored once in group_init and passed along
%%--------------------------------------------------------------------

-module(blockchain_region_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("blockchain_region_test.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_ct_utils.hrl").

-export([
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    all_regions_test/1,
    as923_1_test/1,
    au915_test/1,
    cn470_test/1,
    us915_test/1,
    ru864_test/1,
    eu868_test/1,
    region_not_found_test/1,
    us915_region_param_test/1,
    eu868_region_param_test/1,
    au915_region_param_test/1,
    as923_1_region_param_test/1,
    as923_2_region_param_test/1,
    as923_3_region_param_test/1,
    ru864_region_param_test/1,
    cn470_region_param_test/1,
    in865_region_param_test/1,
    kr920_region_param_test/1,
    eu433_region_param_test/1
]).

all() ->
    [
        {group, without_h3_data},
        {group, with_h3_data}
    ].

with_h3_data_test_cases() ->
    [
        as923_1_test,
        au915_test,
        cn470_test,
        us915_test,
        ru864_test,
        eu868_test,
        region_not_found_test
    ].

without_h3_data_test_cases() ->
    [
        all_regions_test,
        us915_region_param_test,
        eu868_region_param_test,
        au915_region_param_test,
        as923_1_region_param_test,
        as923_2_region_param_test,
        as923_3_region_param_test,
        ru864_region_param_test,
        cn470_region_param_test,
        in865_region_param_test,
        kr920_region_param_test,
        eu433_region_param_test
    ].

groups() ->
    [
        {without_h3_data, [], without_h3_data_test_cases()},
        {with_h3_data, [], with_h3_data_test_cases()}
    ].

%%--------------------------------------------------------------------
%% group setup
%%--------------------------------------------------------------------
init_per_group(Group, Config) ->
    [{extra_vars, extra_vars(Group)} | Config].

%%--------------------------------------------------------------------
%% group teardown
%%--------------------------------------------------------------------
end_per_group(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% test case setup
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    BaseDir = ?config(base_dir, Config0),
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),

    ExtraVars = ?config(extra_vars, Config0),

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    _ = lists:foreach(
        fun(Entry) ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            0 = blockchain_ledger_entry_v1:nonce(Entry)
        end,
        maps:values(Entries)
    ),

    [
        {balance, Balance},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {ledger, Ledger},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        {base_dir, BaseDir},
        Keys
        | Config0
    ].

%%--------------------------------------------------------------------
%% test cases
%%--------------------------------------------------------------------
all_regions_test(Config) ->
    Ledger = ?config(ledger, Config),
    {ok, Regions} = blockchain_region_v1:get_all_regions(Ledger),
    [] = Regions -- [list_to_atom(R) || R <- ?SUPPORTED_REGIONS],
    ok.

as923_1_test(Config) ->
    Ledger = ?config(ledger, Config),
    JH3 = 631319855840474623,
    {ok, true} = blockchain_region_v1:h3_in_region(JH3, ?region_as923_1, Ledger),
    {ok, false} = blockchain_region_v1:h3_in_region(JH3, ?region_us915, Ledger),
    ok.

%% as923_2_test(Config) ->
%%     Ledger = ?config(ledger, Config),
%%     USH3 = 631183727389488639,
%%     case blockchain:config(?region_us915, Ledger) of
%%         {ok, Bin} ->
%%             {true, _Parent} = h3:contains(USH3, Bin),
%%             ok;
%%         _ ->
%%             ct:fail("broken")
%%     end.

%% as923_3_test(Config) ->
%%     Ledger = ?config(ledger, Config),
%%     USH3 = 631183727389488639,
%%     case blockchain:config(?region_us915, Ledger) of
%%         {ok, Bin} ->
%%             {true, _Parent} = h3:contains(USH3, Bin),
%%             ok;
%%         _ ->
%%             ct:fail("broken")
%%     end.

au915_test(Config) ->
    Ledger = ?config(ledger, Config),
    AUH3 = 633862093138897919,
    case blockchain:config(?region_au915, Ledger) of
        {ok, Bin} ->
            {true, _Parent} = h3:contains(AUH3, Bin),
            {ok, ?region_au915} = blockchain_region_v1:h3_to_region(AUH3, Ledger),
            ok;
        _ ->
            ct:fail("broken")
    end.

cn470_test(Config) ->
    Ledger = ?config(ledger, Config),
    CNH3 = 631645363084543487,
    {ok, true} = blockchain_region_v1:h3_in_region(CNH3, ?region_cn470, Ledger),
    {ok, false} = blockchain_region_v1:h3_in_region(CNH3, ?region_us915, Ledger),
    ok.

%% eu433_test(Config) ->
%%     Ledger = ?config(ledger, Config),
%%     CAH3 = 631222943758197247,
%%     case blockchain:config(?region_cn470, Ledger) of
%%         {ok, Bin} ->
%%             {true, _Parent} = h3:contains(CAH3, Bin),
%%             ok;
%%         _ ->
%%             ct:fail("broken")
%%     end.

eu868_test(Config) ->
    Ledger = ?config(ledger, Config),
    EUH3 = 631051317836014591,
    {ok, true} = blockchain_region_v1:h3_in_region(EUH3, ?region_eu868, Ledger),
    {ok, false} = blockchain_region_v1:h3_in_region(EUH3, ?region_us915, Ledger),
    ok.

%% in865_test(Config) ->
%%     Ledger = ?config(ledger, Config),
%%     CAH3 = 631222943758197247,
%%     case blockchain:config(?region_cn470, Ledger) of
%%         {ok, Bin} ->
%%             {true, _Parent} = h3:contains(CAH3, Bin),
%%             ok;
%%         _ ->
%%             ct:fail("broken")
%%     end.

%% kr920_test(Config) ->
%%     Ledger = ?config(ledger, Config),
%%     CAH3 = 631222943758197247,
%%     case blockchain:config(?region_cn470, Ledger) of
%%         {ok, Bin} ->
%%             {true, _Parent} = h3:contains(CAH3, Bin),
%%             ok;
%%         _ ->
%%             ct:fail("broken")
%%     end.

ru864_test(Config) ->
    Ledger = ?config(ledger, Config),
    %% massive-crimson-cat
    RUH3 = 630812791472857599,
    {ok, true} = blockchain_region_v1:h3_in_region(RUH3, ?region_ru864, Ledger),
    {ok, false} = blockchain_region_v1:h3_in_region(RUH3, ?region_us915, Ledger),
    ok.

us915_test(Config) ->
    Ledger = ?config(ledger, Config),
    USH3 = 631183727389488639,
    {ok, true} = blockchain_region_v1:h3_in_region(USH3, ?region_us915, Ledger),
    {ok, false} = blockchain_region_v1:h3_in_region(USH3, ?region_in865, Ledger),
    ok.

region_not_found_test(Config) ->
    Ledger = ?config(ledger, Config),
    InvalidH3 = 11111111111111111111,
    {error, {h3_contains_failed, _}} = blockchain_region_v1:h3_to_region(InvalidH3, Ledger),
    ok.

us915_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_us915, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(us915),
            Ser = blockchain_region_suite_helper:serialized_us915(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

eu868_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_eu868, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(eu868),
            Ser = blockchain_region_suite_helper:serialized_eu868(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

au915_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_au915, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(au915),
            Ser = blockchain_region_suite_helper:serialized_au915(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

as923_1_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_as923_1, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(as923_1),
            Ser = blockchain_region_suite_helper:serialized_as923_1(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

as923_2_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_as923_2, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(as923_2),
            Ser = blockchain_region_suite_helper:serialized_as923_2(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

as923_3_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_as923_3, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(as923_3),
            Ser = blockchain_region_suite_helper:serialized_as923_3(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

ru864_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_ru864, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(ru864),
            Ser = blockchain_region_suite_helper:serialized_ru864(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

cn470_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_cn470, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(cn470),
            Ser = blockchain_region_suite_helper:serialized_cn470(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

in865_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_in865, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(in865),
            Ser = blockchain_region_suite_helper:serialized_in865(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

kr920_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_kr920, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(kr920),
            Ser = blockchain_region_suite_helper:serialized_kr920(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

eu433_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_eu433, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_suite_helper:fetch(eu433),
            Ser = blockchain_region_suite_helper:serialized_eu433(),
            true = do_param_checks(Bin, Ser, KnownParams),
            ok;
        _ ->
            ct:fail("boom")
    end.

%%--------------------------------------------------------------------
%% test case teardown
%%--------------------------------------------------------------------

end_per_testcase(_, Config) ->
    Sup = ?config(sup, Config),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------
do_param_checks(ParamVarBin, KnownBin, KnownParams) ->
    Deser = blockchain_region_params_v1:deserialize(KnownBin),
    DeserFromVar = blockchain_region_params_v1:deserialize(ParamVarBin),
    %% check that the chain var matches our known binary
    C1 = ParamVarBin == KnownBin,
    %% check that we can properly deserialize
    C2 = Deser == KnownParams,
    %% check that the deserialization from chain var also matches our known value
    C3 = DeserFromVar == KnownParams,
    %% check that we can go back n forth between the chain var
    C4 =
        blockchain_region_params_v1:serialize(blockchain_region_params_v1:deserialize(ParamVarBin)) ==
            ParamVarBin,
    C1 andalso C2 andalso C3 andalso C4.

extra_vars(with_h3_data) ->
    RegionURLs = region_urls(),
    Regions = download_regions(RegionURLs),
    maps:put(regulatory_regions, ?regulatory_region_bin_str, maps:from_list(Regions));
extra_vars(without_h3_data) ->
    #{
        regulatory_regions => ?regulatory_region_bin_str,
        region_params_us915 => region_params_us915(),
        region_params_eu868 => region_params_eu868(),
        region_params_au915 => region_params_au915(),
        region_params_as923_1 => region_params_as923_1(),
        region_params_as923_2 => region_params_as923_2(),
        region_params_as923_3 => region_params_as923_3(),
        region_params_ru864 => region_params_ru864(),
        region_params_cn470 => region_params_cn470(),
        region_params_in865 => region_params_in865(),
        region_params_kr920 => region_params_kr920(),
        region_params_eu433 => region_params_eu433()
    };
extra_vars(_) ->
    #{}.

region_urls() ->
    [
        {region_as923_1, ?region_as923_1_url},
        {region_as923_2, ?region_as923_2_url},
        {region_as923_3, ?region_as923_3_url},
        {region_au915, ?region_au915_url},
        {region_cn470, ?region_cn470_url},
        {region_eu433, ?region_eu433_url},
        {region_eu868, ?region_eu868_url},
        {region_in865, ?region_in865_url},
        {region_kr920, ?region_kr920_url},
        {region_ru864, ?region_ru864_url},
        {region_us915, ?region_us915_url}
    ].

download_regions(RegionURLs) ->
    blockchain_ct_utils:pmap(
        fun({Region, URL}) ->
            Ser = blockchain_ct_utils:download_serialized_region(URL),
            {Region, Ser}
        end,
        RegionURLs
    ).

region_params_us915() ->
    <<10, 41, 8, 160, 144, 215, 175, 3, 16, 200, 208, 7, 24, 232, 2, 34, 26, 10, 4, 8, 4, 16, 25,
        10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 41, 8, 224, 245,
        202, 175, 3, 16, 200, 208, 7, 24, 232, 2, 34, 26, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67,
        10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 41, 8, 160, 219, 190, 175, 3, 16, 200,
        208, 7, 24, 232, 2, 34, 26, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139,
        1, 10, 5, 8, 1, 16, 128, 2, 10, 41, 8, 224, 192, 178, 175, 3, 16, 200, 208, 7, 24, 232, 2,
        34, 26, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16,
        128, 2, 10, 41, 8, 160, 166, 166, 175, 3, 16, 200, 208, 7, 24, 232, 2, 34, 26, 10, 4, 8, 4,
        16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 41, 8,
        224, 139, 154, 175, 3, 16, 200, 208, 7, 24, 232, 2, 34, 26, 10, 4, 8, 4, 16, 25, 10, 4, 8,
        3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 41, 8, 160, 241, 141, 175,
        3, 16, 200, 208, 7, 24, 232, 2, 34, 26, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8,
        2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 41, 8, 224, 214, 129, 175, 3, 16, 200, 208, 7,
        24, 232, 2, 34, 26, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2>>.

region_params_eu868() ->
    <<10, 35, 8, 224, 180, 236, 157, 3, 16, 200, 208, 7, 24, 140, 1, 34, 20, 10, 4, 8, 6, 16, 65,
        10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 154, 224, 157, 3, 16, 200,
        208, 7, 24, 140, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16,
        238, 1, 10, 35, 8, 224, 255, 211, 157, 3, 16, 200, 208, 7, 24, 140, 1, 34, 20, 10, 4, 8, 6,
        16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 229, 199, 157, 3,
        16, 200, 208, 7, 24, 140, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8,
        2, 16, 238, 1, 10, 35, 8, 224, 202, 187, 157, 3, 16, 200, 208, 7, 24, 140, 1, 34, 20, 10, 4,
        8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 132, 145,
        158, 3, 16, 200, 208, 7, 24, 140, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1,
        10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 132, 145, 158, 3, 16, 200, 208, 7, 24, 140, 1, 34,
        20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 224,
        233, 132, 158, 3, 16, 200, 208, 7, 24, 140, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16,
        129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 207, 248, 157, 3, 16, 200, 208, 7, 24, 140,
        1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1>>.

region_params_au915() ->
    <<10, 53, 8, 192, 189, 234, 181, 3, 16, 200, 208, 7, 24, 172, 2, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 163, 222, 181, 3, 16, 200, 208, 7, 24, 172, 2, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 136, 210, 181, 3, 16, 200, 208,
        7, 24, 172, 2, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4,
        8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 238, 197,
        181, 3, 16, 200, 208, 7, 24, 172, 2, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10,
        4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10,
        53, 8, 192, 211, 185, 181, 3, 16, 200, 208, 7, 24, 172, 2, 34, 38, 10, 4, 8, 6, 16, 25, 10,
        4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5,
        8, 1, 16, 128, 2, 10, 53, 8, 128, 185, 173, 181, 3, 16, 200, 208, 7, 24, 172, 2, 34, 38, 10,
        4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2,
        16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 158, 161, 181, 3, 16, 200, 208, 7, 24,
        172, 2, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3,
        16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 132, 149, 181, 3,
        16, 200, 208, 7, 24, 172, 2, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4,
        16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_as923_1() ->
    <<10, 53, 8, 128, 181, 210, 183, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 185, 143, 184, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 159, 131, 184, 3, 16, 200, 208,
        7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4,
        8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 132, 247,
        183, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10,
        4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10,
        53, 8, 128, 234, 234, 183, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10,
        4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5,
        8, 1, 16, 128, 2, 10, 53, 8, 192, 207, 222, 183, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10,
        4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2,
        16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 238, 167, 184, 3, 16, 200, 208, 7, 24,
        160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3,
        16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 212, 155, 184, 3,
        16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4,
        16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_as923_2() ->
    <<10, 53, 8, 192, 141, 241, 184, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 243, 228, 184, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 216, 216, 184, 3, 16, 200, 208,
        7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4,
        8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 190, 204,
        184, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10,
        4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10,
        53, 8, 192, 163, 192, 184, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10,
        4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5,
        8, 1, 16, 128, 2, 10, 53, 8, 128, 137, 180, 184, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10,
        4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2,
        16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 238, 167, 184, 3, 16, 200, 208, 7, 24,
        160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3,
        16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 128, 212, 155, 184, 3,
        16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4,
        16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_as923_3() ->
    <<10, 53, 8, 128, 132, 149, 181, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 192, 233, 136, 181, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_ru864() ->
    <<10, 53, 8, 224, 211, 181, 158, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 160, 185, 169, 158, 3, 16, 200, 208, 7, 24, 160, 1, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_cn470() ->
    <<10, 53, 8, 160, 236, 198, 232, 1, 16, 200, 208, 7, 24, 191, 1, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 224, 209, 186, 232, 1, 16, 200, 208, 7, 24, 191, 1, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 160, 183, 174, 232, 1, 16, 200, 208,
        7, 24, 191, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4,
        8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 224, 156, 162,
        232, 1, 16, 200, 208, 7, 24, 191, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10,
        4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10,
        53, 8, 160, 130, 150, 232, 1, 16, 200, 208, 7, 24, 191, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10,
        4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5,
        8, 1, 16, 128, 2, 10, 53, 8, 224, 231, 137, 232, 1, 16, 200, 208, 7, 24, 191, 1, 34, 38, 10,
        4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2,
        16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 160, 205, 253, 231, 1, 16, 200, 208, 7, 24,
        191, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3,
        16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 224, 178, 241, 231, 1,
        16, 200, 208, 7, 24, 191, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4,
        16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_in865() ->
    <<10, 53, 8, 132, 253, 211, 156, 3, 16, 200, 208, 7, 24, 172, 2, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 232, 195, 247, 156, 3, 16, 200, 208, 7, 24, 172, 2, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 228, 156, 191, 156, 3, 16, 200, 208,
        7, 24, 172, 2, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4,
        8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_kr920() ->
    <<10, 53, 8, 160, 225, 161, 184, 3, 16, 200, 208, 7, 24, 140, 1, 34, 38, 10, 4, 8, 6, 16, 25,
        10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10,
        5, 8, 1, 16, 128, 2, 10, 53, 8, 224, 198, 149, 184, 3, 16, 200, 208, 7, 24, 140, 1, 34, 38,
        10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5,
        8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 160, 172, 137, 184, 3, 16, 200, 208,
        7, 24, 140, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4,
        8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 224, 145, 253,
        183, 3, 16, 200, 208, 7, 24, 140, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10,
        4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10,
        53, 8, 160, 247, 240, 183, 3, 16, 200, 208, 7, 24, 140, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10,
        4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5,
        8, 1, 16, 128, 2, 10, 53, 8, 224, 220, 228, 183, 3, 16, 200, 208, 7, 24, 140, 1, 34, 38, 10,
        4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2,
        16, 139, 1, 10, 5, 8, 1, 16, 128, 2, 10, 53, 8, 160, 194, 216, 183, 3, 16, 200, 208, 7, 24,
        140, 1, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3,
        16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.

region_params_eu433() ->
    <<10, 51, 8, 188, 170, 214, 20, 16, 200, 208, 7, 24, 121, 34, 38, 10, 4, 8, 6, 16, 25, 10, 4, 8,
        5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1, 10, 5, 8, 1,
        16, 128, 2, 10, 51, 8, 156, 142, 213, 20, 16, 200, 208, 7, 24, 121, 34, 38, 10, 4, 8, 6, 16,
        25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2, 16, 139, 1,
        10, 5, 8, 1, 16, 128, 2, 10, 51, 8, 252, 241, 211, 20, 16, 200, 208, 7, 24, 121, 34, 38, 10,
        4, 8, 6, 16, 25, 10, 4, 8, 5, 16, 25, 10, 4, 8, 4, 16, 25, 10, 4, 8, 3, 16, 67, 10, 5, 8, 2,
        16, 139, 1, 10, 5, 8, 1, 16, 128, 2>>.
