-module(blockchain_region_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([get_all_regions_test/1]).

all() ->
    [
        get_all_regions_test
    ].

%%--------------------------------------------------------------------
%% test case setup
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    BaseDir = ?config(base_dir, Config0),
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),

    ExtraVars = extra_vars(TestCase),

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
get_all_regions_test(Config) ->
    Ledger = ?config(ledger, Config),
    {ok, Regions} = blockchain_region:get_all_regions(Ledger),
    [] = Regions -- ?SUPPORTED_REGIONS,
    ok.

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

extra_vars(get_all_regions_test) ->
    #{
        regulatory_regions =>
            <<"as923_1,as923_2,as923_3,au915,cn779,eu433,eu868,in865,kr920,ru864,us915">>
    };
extra_vars(_) ->
    #{}.
