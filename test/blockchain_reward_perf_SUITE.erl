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

    os:cmd("wget https://blockchain-core.s3-us-west-1.amazonaws.com/snap-591841"),

    Filename = Dir ++ "/snap-591841",

    {ok, BinSnap} = file:read_file(Filename),

    {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
    SHA = blockchain_ledger_snapshot_v1:hash(Snapshot),

    {ok, _GWCache} = blockchain_gateway_cache:start_link(),
    {ok, _Pid} = blockchain_score_cache:start_link(),

    {ok, BinGen} = file:read_file("../../../../test/genesis"),
    GenesisBlock = blockchain_block:deserialize(BinGen),
    {ok, Chain} = blockchain:new(NewDir, GenesisBlock, blessed_snapshot, undefined),

    {ok, Ledger1} = blockchain_ledger_snapshot_v1:import(Chain, SHA, Snapshot),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger1),

    ct:pal("loaded ledger at height ~p", [Height]),

    [{chain, Chain} | Config].

end_per_testcase(_TestCase, _Config) ->
    blockchain_score_cache:stop(),
    ok.

all() ->
    [reward_perf_test].

reward_perf_test(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {Time, _} =
        timer:tc(
          fun() ->
                  {ok, _} = blockchain_txn_rewards_v1:calculate_rewards(Height - 15, Height, Chain)
          end),
    ct:pal("basic calc took: ~p ms", [Time div 1000]),

    Vars = maps:merge(blockchain_reward_perf_SUITE:hip15_vars(), blockchain_reward_perf_SUITE:hip17_vars()),
    Ledger2 = blockchain_ledger_v1:new_context(Ledger),
    ok = blockchain_ledger_v1:vars(Vars, [], Ledger2),
    blockchain:bootstrap_h3dex(Ledger2),
    blockchain_ledger_v1:commit_context(Ledger2),
     
    [erlang:garbage_collect(P) || P <- processes()],
    timer:sleep(3000),
    
    {Time2, _} =
        timer:tc(
          fun() ->
                  blockchain_txn_rewards_v1:calculate_rewards(Height - 15, Height, Chain)
          end),
    ct:pal("basic calc 2 took: ~p ms", [Time2 div 1000]),

    [erlang:garbage_collect(P) || P <- processes()],
    timer:sleep(3000),

    {Time3, _} =
        timer:tc(
          fun() ->
                  blockchain_txn_rewards_v1:calculate_rewards(Height - 15, Height, Chain)
          end),
    ct:pal("hip 17 calc took: ~p ms", [Time3 div 1000]),

    Vars = maps:merge(blockchain_reward_perf_SUITE:hip15_vars(), blockchain_reward_perf_SUITE:hip17_vars()),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    blockchain_ledger_v1:bootstrap_gw_denorm(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),

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
