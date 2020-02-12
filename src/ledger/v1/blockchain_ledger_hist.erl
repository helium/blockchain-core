%%%-------------------------------------------------------------------
%% @doc
%% == Helper module for histogram related functions ==
%% @end
%%%-------------------------------------------------------------------

-module(blockchain_ledger_hist).

-export([merge/1]).

-type hist() :: #{any() => non_neg_integer()}.
-type hist_stack() :: [hist()].

-export_type([hist/0, hist_stack/0]).

-spec merge(Stack :: hist_stack()) -> hist().
merge(Stack) ->
    %% Sum all the values in stack to give a single histogram

    %% TODO: Optimize later if possible
    lists:foldl(fun(H, Acc) ->
                        maps:fold(fun(K, V, Map) ->
                                          maps:update_with(K, fun(X) -> X + V end, V, Map)
                                  end,
                                  H,
                                  Acc)
                end,
                #{},
                Stack).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_merge_test() ->
    A = #{-10 => 2, -30 => 5},
    B = #{-10 => 1, -20 => 6, -30 => 3},
    C = #{-40 => 10, -20 => 4, -30 => 8},
    S = [C, B, A],

    Expected = #{-40 => 10, -20 => 10, -30 => 16, -10 => 3},

    ?assertEqual(Expected, merge(S)).

empty_merge_test() ->
    A = #{},
    B = #{},
    C = #{},
    S = [C, B, A],

    Expected = #{},

    ?assertEqual(Expected, merge(S)).

-endif.
