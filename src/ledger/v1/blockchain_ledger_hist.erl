%%%-------------------------------------------------------------------
%% @doc
%% == Helper module for histogram related functions ==
%% @end
%%%-------------------------------------------------------------------

-module(blockchain_ledger_hist).

-include("blockchain_vars.hrl").

-export([merge/1, update/3]).

-type hist() :: #{any() => non_neg_integer()}.
-type hist_stack() :: [hist()].

-export_type([hist/0, hist_stack/0]).

%%--------------------------------------------------------------------
%% @doc
%% Sum all the values in stack to give a single histogram
%% TODO: Optimize later if possible
%% @end
%%--------------------------------------------------------------------
-spec merge(Stack :: hist_stack()) -> hist().
merge(Stack) ->
    lists:foldl(fun(H, Acc) ->
                        maps:fold(fun(K, V, Map) ->
                                          maps:update_with(K, fun(X) -> X + V end, V, Map)
                                  end,
                                  H,
                                  Acc)
                end,
                #{},
                Stack).

%%--------------------------------------------------------------------
%% @doc
%% Update histogram stack depending on the limit set via hist_limit
%% chain var.
%%
%% If the stack is at hist_limit, pop from tail, insert at head of the stack
%% If the stack is not at hist_limit, insert at head
%% If the hist_limit is not set, error
%% @end
%%--------------------------------------------------------------------
-spec update(Hist :: hist(),
             Stack :: hist_stack(),
             Ledger :: blockchain_ledger_v1:ledger()) -> {ok, hist_stack()} |
                                                         {error, hist_limit_not_set}.
update(Hist, Stack, Ledger) ->
    case blockchain_ledger_v1:config(?hist_limit, Ledger) of
        {ok, HistLimit} when is_integer(HistLimit) ->
            case length(Stack) < HistLimit of
                true ->
                    %% Insert at head of the stack directly
                    io:format("Insert"),
                    NewStack = [Hist | Stack],
                    {ok, NewStack};
                false ->
                    io:format("Cycle"),
                    %% Remove from tail, insert at head
                    [_Popped | Rest] = lists:reverse(Stack),
                    NewStack = [Hist | lists:reverse(Rest)],
                    {ok, NewStack}
            end;
        _ ->
            io:format("Error"),
            {error, hist_limit_not_set}
    end.

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

hist_update_test() ->
    BaseDir = test_utils:tmp_dir("hist_update_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    ok = blockchain_ledger_v1:commit_context(Ledger1),
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, config, fun(hist_limit, _) ->
        {ok, 7}
    end),
    ?assertEqual({ok, 7}, blockchain_ledger_v1:config(?hist_limit, Ledger)),

    %% Some yolo histograms
    A = #{-10 => 2, -30 => 5},
    B = #{-10 => 1, -20 => 6, -30 => 3},
    C = #{-40 => 10, -20 => 4, -30 => 8},
    D = #{-10 => 2, -30 => 5},
    E = #{-10 => 1, -20 => 6, -30 => 3},
    F = #{-40 => 10, -20 => 4, -30 => 8},
    G = #{-40 => 10, -20 => 4, -30 => 8},

    %% Assume that the current hist stack only contains 3 histograms
    CurrentStack = [A, B, C],

    %% Add this new one
    NewHist = #{-10 => 1, -20 => 3},

    %% Should just get added at the head
    Expected = [NewHist | CurrentStack],
    ?assertEqual({ok, Expected}, update(NewHist, CurrentStack, Ledger)),

    %% Assume that the new current hist stack contains 7 histograms
    NewCurrentStack = [A, B, C, D, E, F, G],

    %% "G" should get popped
    NewExpected = [NewHist | [A, B, C, D, E, F]],
    ?assertEqual({ok, NewExpected}, update(NewHist, NewCurrentStack, Ledger)),

    meck:unload(blockchain_ledger_v1),
    ok.


-endif.
