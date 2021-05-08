%%--------------------------------------------------------------------
%% There are two test groups here: with_data and without_data.
%%
%% - with_data: test cases here do h3 data fetch for every regulatory region
%% - without_data: these don't
%%
%% However, data is fetched and stored once in group_init and passed along
%%
%%
%%
%%--------------------------------------------------------------------

-module(blockchain_region_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain.hrl").
-include("blockchain_region.hrl").
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
    eu868_region_param_test/1
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
        eu868_region_param_test
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
    case blockchain:config(?region_as923_1, Ledger) of
        {ok, Bin} ->
            {true, _Parent} = h3:contains(JH3, Bin),
            ok;
        _ ->
            ct:fail("broken")
    end.

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
            {ok, Region} = blockchain_region_v1:region(AUH3, Ledger),
            %% TODO: Fix me and do proper region_param checks
            true = au915 == Region,
            ok;
        _ ->
            ct:fail("broken")
    end.

cn470_test(Config) ->
    Ledger = ?config(ledger, Config),
    CNH3 = 631645363084543487,
    case blockchain:config(?region_cn470, Ledger) of
        {ok, Bin} ->
            {true, _Parent} = h3:contains(CNH3, Bin),
            {ok, Region} = blockchain_region_v1:region(CNH3, Ledger),
            %% TODO: Fix me and do proper region_param checks
            true = cn470 == Region,
            ok;
        _ ->
            ct:fail("broken")
    end.

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
    case blockchain:config(?region_eu868, Ledger) of
        {ok, Bin} ->
            {true, _Parent} = h3:contains(EUH3, Bin),
            ok;
        _ ->
            ct:fail("broken")
    end.

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
    case blockchain:config(?region_ru864, Ledger) of
        {ok, Bin} ->
            {true, _Parent} = h3:contains(RUH3, Bin),
            ok;
        _ ->
            ct:fail("broken")
    end.

us915_test(Config) ->
    Ledger = ?config(ledger, Config),
    USH3 = 631183727389488639,
    case blockchain:config(?region_us915, Ledger) of
        {ok, Bin} ->
            {true, _Parent} = h3:contains(USH3, Bin),
            {ok, Region} = blockchain_region_v1:region(USH3, Ledger),
            %% TODO: Fix me and do proper region_param checks
            true = us915 == Region,
            ok;
        _ ->
            ct:fail("broken")
    end.

region_not_found_test(Config) ->
    Ledger = ?config(ledger, Config),
    InvalidH3 = 11111111111111111111,
    {error, {h3_contains_failed, _}} = blockchain_region_v1:region(InvalidH3, Ledger),
    Ser = blockchain_region_params_v1:serialized_eu868(),
    ct:pal("Ser: ~p", [Ser]),
    ok.

us915_region_param_test(Config) ->

    %% US915 channel parameter specification deserialized:
    %%
    %% {blockchain_region_params_v1_pb,
    %%  [{blockchain_region_param_v1_pb,905300000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}},
    %%   {blockchain_region_param_v1_pb,905100000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}},
    %%   {blockchain_region_param_v1_pb,904900000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}},
    %%   {blockchain_region_param_v1_pb,904700000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}},
    %%   {blockchain_region_param_v1_pb,904500000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}},
    %%   {blockchain_region_param_v1_pb,904300000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}},
    %%   {blockchain_region_param_v1_pb,904100000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}},
    %%   {blockchain_region_param_v1_pb,903900000,125000,360,
    %%    {blockchain_region_spreading_v1_pb,
    %%     ['SF10','SF9','SF8','SF7']}}]}

    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_us915, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_params_v1:fetch(us915),
            Ser = blockchain_region_params_v1:serialized_us915(),
            Deser = blockchain_region_params_v1:deserialize(Ser),
            DeserFromVar = blockchain_region_params_v1:deserialize(Bin),
            %% check that the chain var matches our known binary
            true = Bin == Ser,
            %% check that we can properly deserialize
            true = Deser == KnownParams,
            %% check that the deserialization from chain var also matches our known value
            true = DeserFromVar == KnownParams,
            ct:pal("DeserFromVar: ~p", [DeserFromVar]),
            ok;
        _ ->
            ct:fail("boom")
    end.

eu868_region_param_test(Config) ->
    Ledger = ?config(ledger, Config),
    case blockchain:config(?region_params_eu868, Ledger) of
        {ok, Bin} ->
            KnownParams = blockchain_region_params_v1:fetch(eu868),
            Ser = blockchain_region_params_v1:serialized_eu868(),
            Deser = blockchain_region_params_v1:deserialize(Ser),
            DeserFromVar = blockchain_region_params_v1:deserialize(Bin),
            %% check that the chain var matches our known binary
            true = Bin == Ser,
            %% check that we can properly deserialize
            true = Deser == KnownParams,
            %% check that the deserialization from chain var also matches our known value
            true = DeserFromVar == KnownParams,
            ct:pal("DeserFromVar: ~p", [DeserFromVar]),
            ok;
        _ ->
            ct:fail("boom")
    end.

%% Ledger = ?config(ledger, Config),
%% case blockchain:config(?region_params_us915, Ledger) of
%%     {ok, V} ->
%%         ct:pal("V: ~p", [V]),
%%         ok;
%%     _ ->
%%         ct:fail("region param not defined")
%% end.

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

extra_vars(with_h3_data) ->
    RegionURLs = region_urls(),
    Regions = download_regions(RegionURLs),
    maps:put(regulatory_regions, ?regulatory_region_bin_str, maps:from_list(Regions));
extra_vars(without_h3_data) ->
    #{
        regulatory_regions => ?regulatory_region_bin_str,
        region_params_us915 => region_params_us915(),
        region_params_eu868 => region_params_eu868()
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
    <<10,21,8,160,144,215,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1,
      10,21,8,224,245,202,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1,
      10,21,8,160,219,190,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1,
      10,21,8,224,192,178,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1,
      10,21,8,160,166,166,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1,
      10,21,8,224,139,154,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1,
      10,21,8,160,241,141,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1,
      10,21,8,224,214,129,175,3,16,200,208,7,24,232,2,34,6,10,4,4,3,2,1>>.

region_params_eu868() ->
    <<10,23,8,160,132,145,158,3,16,200,208,7,24,140,1,34,8,10,6,6,5,4,3,2,1,
      10,23,8,224,233,132,158,3,16,200,208,7,24,140,1,34,8,10,6,6,5,4,3,2,1,
      10,23,8,160,207,248,157,3,16,200,208,7,24,140,1,34,8,10,6,6,5,4,3,2,1>>.
