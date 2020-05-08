%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_utils).

-include("blockchain_vars.hrl").

-export([calculate_dc_amount/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec calculate_dc_amount(Ledger :: blockchain_ledger_v1:ledg(),
                          PayloadSize :: non_neg_integer()) -> pos_integer().
calculate_dc_amount(Ledger, PayloadSize) ->
    case blockchain_ledger_v1:config(?dc_payload_size, Ledger) of
        {ok, DCPayloadSize} ->
            case PayloadSize =< DCPayloadSize of
                true ->
                    1;
                false ->
                    erlang:ceil(PayloadSize/DCPayloadSize)
            end;
        _ ->
            {error, dc_payload_size_not_set}
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

calculate_dc_amount_test() ->
    BaseDir = test_utils:tmp_dir("calculate_dc_amount_test"),
    Ledger = blockchain_ledger_v1:new(BaseDir),

    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, config, fun(_, _) ->
        {ok, 24}
    end),

    ?assertEqual(1, calculate_dc_amount(Ledger, 1)),
    ?assertEqual(1, calculate_dc_amount(Ledger, 23)),
    ?assertEqual(1, calculate_dc_amount(Ledger, 24)),
    ?assertEqual(2, calculate_dc_amount(Ledger, 25)),
    ?assertEqual(2, calculate_dc_amount(Ledger, 47)),
    ?assertEqual(2, calculate_dc_amount(Ledger, 48)),
    ?assertEqual(3, calculate_dc_amount(Ledger, 49)),

    meck:unload(blockchain_ledger_v1),
    test_utils:cleanup_tmp_dir(BaseDir).

-endif.
