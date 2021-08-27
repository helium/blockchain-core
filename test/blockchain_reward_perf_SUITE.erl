%%%-------------------------------------------------------------------
%%% @author Evan Vigil-McClanahan <mcclanhan@gmail.com>
%%% @copyright (C) 2020, Evan Vigil-McClanahan
%%% @doc
%%%
%%% @end
%%% Created :  1 Dec 2020 by Evan Vigil-McClanahan <mcclanhan@gmail.com>
%%%-------------------------------------------------------------------
-module(blockchain_reward_perf_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("blockchain_vars.hrl").


-export([
         suite/0,
         all/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2
        ]).

-export([
         reward_perf_test/1,
         hip15_vars/0,
         hip17_vars/0
        ]).

%%--------------------------------------------------------------------
%% @spec suite() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,200}}].

%%--------------------------------------------------------------------
%% @spec init_per_suite(Config0) ->
%%     Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, _} = application:ensure_all_started(lager),

    {ok, Dir} = file:get_cwd(),
    PrivDir = filename:join([Dir, "priv"]),
    NewDir = PrivDir ++ "/ledger/",
    ok = filelib:ensure_dir(NewDir),

    Filename = Dir ++ "/snap-933121",

    {ok, BinSnap} =
        try
            {ok, BinSnap1} = file:read_file(Filename),
            {ok, BinSnap1}
        catch _:_ ->
                os:cmd("wget https://snapshots.helium.wtf/mainnet/snap-933121"),
                {ok, BinSnap2} = file:read_file(Filename),
                {ok, BinSnap2}
        end,

    {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
    SHA = blockchain_ledger_snapshot_v1:hash(Snapshot),

    {ok, _GWCache} = blockchain_gateway_cache:start_link(),
    {ok, _Pid} = blockchain_score_cache:start_link(),

    {ok, BinGen} = file:read_file("../../../../test/genesis"),
    GenesisBlock = blockchain_block:deserialize(BinGen),
    {ok, Chain} = blockchain:new(NewDir, GenesisBlock, blessed_snapshot, undefined),

    Ledger1 = blockchain_ledger_snapshot_v1:import(Chain, SHA, Snapshot),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger1),

    CLedger = blockchain_ledger_v1:new_context(Ledger1),
    blockchain_ledger_v1:cf_fold(
      active_gateways,
      fun({Addr, BG}, _) ->
              G = blockchain_ledger_gateway_v2:deserialize(BG),
              blockchain_ledger_v1:update_gateway(G, Addr, CLedger)
      end, foo, CLedger),

    _ = blockchain_ledger_v1:commit_context(CLedger),

    ct:pal("loaded ledger at height ~p", [Height]),

    Chain1 = blockchain:ledger(CLedger, Chain),

    [{chain, Chain1} | Config].

end_per_testcase(_TestCase, Config) ->
    blockchain_score_cache:stop(),
    blockchain:clean(proplists:get_value(chain, Config)),
    ok.

all() ->
    [reward_perf_test].

reward_perf_test(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger1),

    Chain1 = blockchain:ledger(Ledger1, Chain),

    {Time, R} =
        timer:tc(
          fun() ->
                  {ok, Rewards} = blockchain_txn_rewards_v2:calculate_rewards(Height - 15, Height, Chain1),
                  Rewards
          end),

    %% don't think that this has issues, but keep it around just in case
    %% {Time3, _} =
    %%     timer:tc(
    %%       fun() ->
    %%               Txn = blockchain_txn_rewards_v2:new(Height - 15, Height, R2),
    %%               _ = blockchain_txn_rewards_v2:to_json(Txn, [{chain, Chain}])
    %%       end),

    %% hash will no longer match after we un-fix v1
    ct:pal("basic calc took: ~p ms hash ~p ct ~p", [Time div 1000, erlang:phash2(lists:sort(R)), length(R)]),
    %% ct:pal("json calc took: ~p ms", [Time3 div 1000]),

    %% error(print),
    ok.


hip15_vars() ->
    #{
        %% configured on chain
        ?poc_version => 10,
        ?reward_version => 5,
        %% new hip15 vars for testing
        ?poc_reward_decay_rate => 0.8,
        ?witness_redundancy => 4
    }.

hip17_vars() ->
    #{
        ?hip17_res_0 => <<"2,100000,100000">>,
        ?hip17_res_1 => <<"2,100000,100000">>,
        ?hip17_res_2 => <<"2,100000,100000">>,
        ?hip17_res_3 => <<"2,100000,100000">>,
        ?hip17_res_4 => <<"1,250,800">>,
        ?hip17_res_5 => <<"1,100,400">>,
        ?hip17_res_6 => <<"1,25,100">>,
        ?hip17_res_7 => <<"2,5,20">>,
        ?hip17_res_8 => <<"2,1,4">>,
        ?hip17_res_9 => <<"2,1,2">>,
        ?hip17_res_10 => <<"2,1,1">>,
        ?hip17_res_11 => <<"2,100000,100000">>,
        ?hip17_res_12 => <<"2,100000,100000">>,
        ?density_tgt_res => 4
    }.
