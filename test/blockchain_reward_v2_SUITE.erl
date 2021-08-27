-module(blockchain_reward_v2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("blockchain_vars.hrl").

-export([
         suite/0,
         all/0,
         init_per_suite/1,
         end_per_suite/1
        ]).

-export([
         calc_v1_and_v2_test/1,
         calc_v2_and_compare_to_chain/1
        ]).

suite() ->
    [{timetrap,{seconds,200}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(lager),

    Dir = ?config(priv_dir, Config),
    PrivDir = filename:join(Dir, "priv"),
    NewDir = filename:join(PrivDir, "ledger"),
    ok = filelib:ensure_dir(NewDir),

    SnapFileName = "snap-723601",
    SnapFilePath = filename:join(Dir, SnapFileName),
    SnapURI =
        "https://blockchain-core.s3-us-west-1.amazonaws.com/" ++ SnapFileName,
    os:cmd("cd " ++ Dir ++ "  && wget " ++ SnapURI),

    {ok, BinSnap} = file:read_file(SnapFilePath),

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

    [{chain, blockchain:ledger(Ledger1, Chain)} | Config].

end_per_suite(Config) ->
    blockchain:clean(proplists:get_value(chain, Config)),
    ok.

all() ->
    [
     calc_v1_and_v2_test%,
     %calc_v2_and_compare_to_chain - do not run this for now, need a better snapshot
    ].

%% In this test we will start a chain from a snapshot and compare the results
%% from calculating rewards in v1 and v2 for the previous 15 blocks and make
%% sure they generate equivalent results.
calc_v1_and_v2_test(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    {Time, RewardsV1} =
        timer:tc(
          fun() ->
                  {ok, R1} = blockchain_txn_rewards_v1:calculate_rewards(Height - 15, Height, Chain),
                  R1
          end),
    ct:pal("rewards v1: ~p ms", [Time div 1000]),

    {Time2, RewardsV2} =
        timer:tc(
          fun() ->
                  {ok, R2} = blockchain_txn_rewards_v2:calculate_rewards(Height - 15, Height, Chain),
                  R2
          end),
    ct:pal("rewards v2: ~p ms", [Time2 div 1000]),

    V1 = blockchain_txn_rewards_v2:v1_to_v2(RewardsV1),

    case V1 == RewardsV2 of
        true -> ok;
        false ->
            ct:pal("length v1: ~p, v2: ~p", [length(V1), length(RewardsV2)]),
            {I0, I1, D} = diff_v1_v2(RewardsV1, V1, RewardsV2),
            ct:pal("reward types: ~p~n", [D]),
            ct:pal("~n~p~n~p", [I0, I1]),
            ct:fail("txns differ")
    end.


%% In this test we are going to find an existing on-chain rewards_v1
%% transaction, feed the parameters into rewards_v2 and make sure they generate
%% equivalent results
calc_v2_and_compare_to_chain(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    {ok, Block} = blockchain:get_block(Height, Chain),

    [Reward] = blockchain:fold_chain(fun(Blk, [] = Acc) ->
                                           Txns = blockchain_block:transactions(Blk),
                                           R = [ T || T <- Txns, blockchain_txn:type(T) == blockchain_txn_rewards_v1 ],
                                           case R of
                                               [] -> Acc;
                                               [H] -> [ H | Acc ];
                                               [H|_T] -> [ H | Acc ]
                                           end;
                                      %% stop after first reward
                                      (_Blk, _Acc) -> return
                                   end,
                                   [], Block, Chain),

    Start = blockchain_txn_rewards_v1:start_epoch(Reward),
    End = blockchain_txn_rewards_v1:end_epoch(Reward),
    V1Rewards = blockchain_txn_rewards_v1:rewards(Reward),

    V1 = blockchain_txn_rewards_v2:v1_to_v2(V1Rewards),

    RewardsV2 = blockchain_txn_rewards_v2:calculate_rewards(Start, End, Chain),

    case V1 == RewardsV2 of
        true -> ok;
        false ->
            ct:pal("length v1: ~p, v2: ~p", [length(V1), length(RewardsV2)]),
            {I0, I1, D} = diff_v1_v2(V1Rewards, V1, RewardsV2),
            ct:pal("reward types: ~p~n", [D]),
            ct:pal("~n~p~n~p", [I0, I1]),
            ct:fail("txns differ")
    end.

%% Internal functions
diff_v1_v2(RewardsV1, V1, RewardsV2) ->
    %% compare V1 to V2
    I0 = lists:foldl(fun({_RType, Owner, Amount}, Acc) ->
                             case lists:keyfind(Owner, 2, RewardsV2) of
                                 false ->
                                     maps:update_with(not_in_v2,
                                                      fun(E) -> [ {Owner, Amount} | E ] end,
                                                      [{Owner, Amount}], Acc);
                                 {_RType, Owner, Amount} ->
                                     maps:update_with(same, fun(C) -> C + 1 end, 1, Acc);
                                 {_RType, Owner, V2Amount} ->
                                     maps:update_with(amt_in_v2_different,
                                                     fun(E) -> [ {Owner, Amount, V2Amount} | E ] end,
                                                     [ {Owner, Amount, V2Amount} ], Acc)
                             end
                     end,
                     #{},
                     V1),
    %% now we are going to compare V2 to V1
    DifferAmts = maps:get(amt_in_v2_different, I0),
    D = find_reward_types(DifferAmts, RewardsV1),

    I1 = lists:foldl(fun({_RType, Owner, Amount}, Acc) ->
                             case lists:keyfind(Owner, 2, V1) of
                                 false ->
                                     maps:update_with(not_in_v1,
                                                      fun(E) -> [ {Owner, Amount} | E ] end,
                                                      [{Owner, Amount}], Acc);
                                 {_RType, Owner, Amount} ->
                                     maps:update_with(same, fun(C) -> C + 1 end, 1, Acc);
                                 {_RType, Owner, V1Amount} ->
                                     %% only insert if the owner is not already in the list from v1
                                     case lists:keyfind(Owner, 1, DifferAmts) of
                                         false ->
                                             maps:update_with(amt_in_v1_different,
                                                              fun(E) ->
                                                                      [ {Owner, Amount, V1Amount} | E ]
                                                              end,
                                                              [ {Owner, Amount, V1Amount} ], Acc);
                                         _ -> Acc
                                     end
                             end
                     end,
                     #{},
                     RewardsV2),
    {I0, I1, D}.

find_reward_types(DifferAmts, V1) ->
    lists:foldl(fun({Owner, _Total, _V2Total}, Acc) ->
                        Matches = lists:filter(fun({_Rec, Acct, _GW, _Amt, _Type}) when Acct == Owner -> true;
                                                  (_) -> false
                                               end,
                                               V1),
                        OwnerMap = maps:get(Owner, Acc, #{}),
                        NewOwnerMap = lists:foldl(fun({_Rec, _Acct, _GW, Amt, RewardType}, A) ->
                                            maps:update_with(RewardType, fun({Balance, Cnt}) -> {Balance + Amt, Cnt + 1} end, {Amt, 1}, A)
                                    end,
                                    OwnerMap,
                                    Matches),
                        Acc#{ Owner => NewOwnerMap }
                end,
                #{},
                DifferAmts).

