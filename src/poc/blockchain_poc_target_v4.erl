%%%-----------------------------------------------------------------------------
%%% @doc blockchain_poc_target_v4 implementation.
%%%
%%% Refer HIP-X: TBD
%%%
%%%-----------------------------------------------------------------------------
-module(blockchain_poc_target_v4).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([poc_interval/1]).

-spec poc_interval(Ledger :: blockchain_ledger_v1:ledger()) -> {ok, pos_integer()} |
                                                               {error, any()}.
poc_interval(Ledger) ->
    N = maps:size(blockchain_ledger_v1:active_gateways(Ledger)),
    %% TODO: maybe filter depending on
    %% - location exists for hospot
    %% - last_poc_challenge is not extremely stale (controlled by poc_consider_inactive)
    %% - has not poc-ed in X number of blocks
    Y = poc_challenges_per_block(Ledger),
    floor(N div Y).

-spec poc_challenges_per_block(Ledger :: blockchain_ledger_v1:ledger()) ->
    undefined | pos_integer().
poc_challenges_per_block(Ledger) ->
    case blockchain:config(?poc_challenges_per_block, Ledger) of
        {ok, K} -> K;
        _ -> undefined
    end.

%% -spec block_time(Ledger :: blockchain_ledger_v1:ledger()) -> pos_integer().
%% block_time(Ledger) ->
%%     case blockchain:config(?block_time, Ledger) of
%%         {ok, K} -> K;
%%         _ -> undefined
%%     end.

%% -spec poc_consider_inactive(Ledger :: blockchain_ledger_v1:ledger()) -> undefined | pos_integer().
%% poc_consider_inactive(Ledger) ->
%%     case blockchain:config(?poc_consider_inactive, Ledger) of
%%         {ok, K} -> K;
%%         _ -> undefined
%%     end.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    %% poc_challenges_per_block
    POCChallengesPerBlockRange = lists:reverse(lists:seq(70, 150, 20)),
    NetworkSizeRange = lists:seq(10000, 110000, 1000),

    Res = lists:foldl(fun(Size, Acc) ->
                              Ans = lists:foldl(fun(Y, Acc2) ->
                                                      maps:put(Y, floor(Size div Y), Acc2)
                                              end, #{}, POCChallengesPerBlockRange),
                              maps:put(Size, Ans, Acc)
                      end, #{}, NetworkSizeRange),

    io:format("Res: ~p~n", [Res]),

    %% Check that when we hit 20K, 50K, 75K, 100K hotspots, the poc_interval goes up

    POCInterval1 = maps:get(70, maps:get(20000, Res)),
    POCInterval2 = maps:get(70, maps:get(20000, Res)),
    POCInterval3 = maps:get(70, maps:get(20000, Res)),

    ?assert(POCInterval3 > POCInterval2 > POCInterval1),

    ok.

-endif.
