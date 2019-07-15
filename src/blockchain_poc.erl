%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc).

-export([
    challenge_interval/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(CHALLENGE_INTERVAL, poc_challenge_interval).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenge_interval(blockchain_ledger_v1:ledger()) -> non_neg_integer().
challenge_interval(Ledger) ->
    {ok, Interval} = blockchain:config(?CHALLENGE_INTERVAL, Ledger),
    Interval.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.
