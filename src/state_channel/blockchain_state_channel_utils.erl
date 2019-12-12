%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_utils).

-export([calculate_dc_amount/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% TODO: maybe make this a chain var
-spec calculate_dc_amount(non_neg_integer()) -> pos_integer().
calculate_dc_amount(PayloadSize) when PayloadSize =< 24 ->
    1;
calculate_dc_amount(PayloadSize) ->
    Float = PayloadSize/24,
    Truncated = erlang:trunc(Float),
    case Float > Truncated of
        true -> Truncated+1;
        false -> Truncated
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

calculate_dc_amount_test() ->
    ?assertEqual(1, calculate_dc_amount(1)),
    ?assertEqual(1, calculate_dc_amount(23)),
    ?assertEqual(1, calculate_dc_amount(24)),
    ?assertEqual(2, calculate_dc_amount(25)),
    ?assertEqual(2, calculate_dc_amount(47)),
    ?assertEqual(2, calculate_dc_amount(48)),
    ?assertEqual(3, calculate_dc_amount(49)).

-endif.
