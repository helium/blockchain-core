-module(blockchain_state_channel_client_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("blockchain_vars.hrl").

-export([
         %% init_per_suite/1,
         %% end_per_suite/1,
         %% init_per_testcase/2,
         all/0
        ]).

-export([
         without_chainvar/1,
         accept_routers_struct/1,
         reject_malformed_routers_struct/1
        ]).

all() ->
    [
     without_chainvar,
     accept_routers_struct,
     reject_malformed_routers_struct
    ].

%% init_per_suite(Config) ->
%%     {ok, _} = application:ensure_all_started(throttle),
%%     {ok, _} = application:ensure_all_started(lager),
%%     %% blockchain_ct_utils:init_per_suite(Config).
%%     Config.

%% end_per_suite(Config) ->
%%     blockchain:clean(proplists:get_value(chain, Config)).

%% init_per_testcase(TestCase, Config) ->
%%    blockchain_ct_utils:init_per_testcase(TestCase, Config).

without_chainvar(_Config) ->
    %% TODO ensure stable behavior without the chain var existing
    ok.

accept_routers_struct(_Config) ->
    Vars = #{
               routers_by_netid_to_oui => term_to_binary([{16#000009, 115},
                                                          {16#600025, 120},
                                                          {16#000037, 116},
                                                          {16#60003A, 55}])
              },
    {InitialVars, _} = blockchain_ct_utils:create_vars(Vars),
    ?assertEqual(ok, maps:map(fun blockchain_txn_vars_v1:validate_var/2, InitialVars)),
    ok.

reject_malformed_routers_struct(Config) ->
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    %% {ok, Block} = blockchain:get_block(Height, Chain),

    ValidRoutes = [
              {111001, 101},
              {222002, 202},
              {333003, 303}
             ],
    BadRoutes = ValidRoutes ++ [{1, 2, 3, 4, 5, 6}],
    Vars1 = #{?routers_by_netid_to_oui => term_to_binary({erlang:length(BadRoutes), BadRoutes})},
    {[Txn], _} = blockchain_ct_utils:create_vars(Vars1),
    ?assertException(throw, {error, {invalid_routers_by_netid_to_oui, _, _}},
                     blockchain_txn_vars_v1:is_valid(Txn, Ledger)),
    ok.
