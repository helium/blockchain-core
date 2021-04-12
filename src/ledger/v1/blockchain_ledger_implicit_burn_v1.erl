%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Implicit Burn Record ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_implicit_burn_v1).

-export([
    new/0, new/1,
    fee/1, fee/2,
    serialize/1, deserialize/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(implicit_burn_v1, {
    fee = 0 :: non_neg_integer()
}).

-type implicit_burn() :: #implicit_burn_v1{}.

-export_type([implicit_burn/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new() -> implicit_burn().
new() ->
    #implicit_burn_v1{}.

-spec new(non_neg_integer()) -> implicit_burn().
new(Fee) when Fee /= undefined ->
    #implicit_burn_v1{fee=Fee}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(implicit_burn()) -> non_neg_integer().
fee(#implicit_burn_v1{fee=Fee}) ->
    Fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(non_neg_integer(), implicit_burn()) -> implicit_burn().
fee(Fee, ImplicitBurn) ->
    ImplicitBurn#implicit_burn_v1{fee=Fee}.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(implicit_burn()) -> binary().
serialize(ImplicitBurn) ->
    BinEntry = erlang:term_to_binary(ImplicitBurn, [compressed]),
    <<1, BinEntry/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> implicit_burn().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------

-spec to_json(implicit_burn(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(ImplicitBurn, _Opts) ->
    #{
      fee => fee(ImplicitBurn)
    }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    ImplicitBurn0 = #implicit_burn_v1{
        fee = 0
    },
    ?assertEqual(ImplicitBurn0, new()),
    ImplicitBurn1 = #implicit_burn_v1{
        fee = 1
    },
    ?assertEqual(ImplicitBurn1, new(1)).

fee_test() ->
    ImplictiBurn = new(),
    ?assertEqual(0, fee(ImplictiBurn)),
    ?assertEqual(1, fee(fee(1, ImplicitBurn))).

-endif.
