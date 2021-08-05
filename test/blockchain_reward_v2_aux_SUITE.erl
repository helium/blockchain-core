-module(blockchain_reward_v2_aux_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("blockchain_vars.hrl").

-export([
         suite/0,
         all/0,
         init_per_suite/1,
         end_per_suite/1
        ]).

-export([
         calculate_rewards/1
        ]).

suite() ->
    [{timetrap,{seconds,200}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(lager),

    {ok, Dir} = file:get_cwd(),
    PrivDir = filename:join([Dir, "priv"]),
    NewDir = PrivDir ++ "/ledger/",
    ok = filelib:ensure_dir(NewDir),

    application:set_env(blockchain, aux_ledger_dir, NewDir),

    os:cmd("wget https://snapshots.helium.wtf/mainnet/snap-948961"),

    Filename = Dir ++ "/snap-948961",

    {ok, BinSnap} = file:read_file(Filename),

    {ok, Snapshot} = blockchain_ledger_snapshot_v1:deserialize(BinSnap),
    SHA = blockchain_ledger_snapshot_v1:hash(Snapshot),

    {ok, _GWCache} = blockchain_gateway_cache:start_link(),
    {ok, _Pid} = blockchain_score_cache:start_link(),

    {ok, BinGen} = file:read_file("../../../../test/genesis"),
    GenesisBlock = blockchain_block:deserialize(BinGen),
    {ok, Chain} = blockchain:new(NewDir, GenesisBlock, blessed_snapshot, undefined),

    Ledger1 = blockchain_ledger_snapshot_v1:import(Chain, SHA, Snapshot),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger1),

    ct:pal("loaded ledger at height ~p", [Height]),

    [{chain, Chain} | Config].

end_per_suite(Config) ->
    blockchain:clean(proplists:get_value(chain, Config)),
    ok.

all() ->
    [
     calculate_rewards
    ].

calculate_rewards(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% make sure we have an aux ledger
    true = blockchain_ledger_v1:has_aux(Ledger),

    %% set up vars for aux ledger
    AuxVars = #{ hip17_resolution_limit => 8 },

    ok = blockchain_ledger_v1:set_aux_vars(AuxVars, blockchain_ledger_v1:mode(aux, Ledger)),

    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {Time, RewardsMain} =
        timer:tc(
          fun() ->
                  {ok, R1} = blockchain_txn_rewards_v2:calculate_rewards(Height - 15, Height, Chain),
                  R1
          end),
    ct:pal("main rewards: ~p ms", [Time div 1000]),

    {Time2, RewardsAux} =
        timer:tc(
          fun() ->
                  {ok, R2} = blockchain_txn_rewards_v2:calculate_rewards(Height - 15, Height,
                                                                         blockchain:ledger(
                                                                           blockchain_ledger_v1:mode(aux, Ledger),
                                                                           Chain)),
                  R2
          end),
    ct:pal("aux rewards: ~p ms", [Time2 div 1000]),

    case RewardsMain == RewardsAux of
        true -> ok;
        false ->
            ct:fail("rewards differ"),
            ct:pal("diff: ~p", [blockchain_ledger_v1:diff_aux_rewards(Ledger)])
    end.
