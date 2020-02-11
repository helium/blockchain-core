%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Gateway Witness ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_gateway_witness_v1).

-export([
         new/0, new/2,
         nonce/1, nonce/2,
         count/1, count/2,
         first_time/1, first_time/2,
         recent_time/1, recent_time/2,
         active_hist/1, active_hist/2,
         old_hist/1, old_hist/2,
         serialize/1, deserialize/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(witness_v1, {
          nonce = 0 :: non_neg_integer(),
          count = 0 :: non_neg_integer(),
          active_hist = #{} :: witness_hist(), %% currently active RSSI histogram
          old_hist = #{} :: witness_hist(), %% stale RSSI histogram
          first_time :: undefined | non_neg_integer(), %% first time a hotspot witnessed this one
          recent_time :: undefined | non_neg_integer(), %% most recent a hotspots witnessed this one
          tof_hist = #{} :: tof_hist() %% TODO: add time of flight histogram
         }).

-type witness() :: #witness_v1{}.
-type witness_hist() :: #{integer() => integer()}.
-type tof_hist() :: #{integer() => integer()}.

-export_type([witness/0]).

-spec new() -> witness().
new() ->
    #witness_v1{}.

-spec new(Nonce :: non_neg_integer(), Count :: non_neg_integer()) -> witness().
new(Nonce, Count) ->
    #witness_v1{nonce=Nonce, count=Count}.

-spec nonce(witness()) -> non_neg_integer().
nonce(#witness_v1{nonce=Nonce}) ->
    Nonce.

-spec nonce(non_neg_integer(), witness()) -> witness().
nonce(Nonce, Witness) ->
    Witness#witness_v1{nonce=Nonce}.

-spec count(witness()) -> non_neg_integer().
count(#witness_v1{count=Count}) ->
    Count.

-spec count(non_neg_integer(), witness()) -> witness().
count(Count, Witness) ->
    Witness#witness_v1{count=Count}.

-spec first_time(witness()) -> non_neg_integer().
first_time(#witness_v1{first_time=FirstTime}) ->
    FirstTime.

-spec first_time(non_neg_integer(), witness()) -> witness().
first_time(FirstTime, Witness) ->
    Witness#witness_v1{first_time=FirstTime}.

-spec recent_time(witness()) -> non_neg_integer().
recent_time(#witness_v1{recent_time=RecentTime}) ->
    RecentTime.

-spec recent_time(non_neg_integer(), witness()) -> witness().
recent_time(RecentTime, Witness) ->
    Witness#witness_v1{recent_time=RecentTime}.

-spec active_hist(witness()) -> witness_hist().
active_hist(#witness_v1{active_hist=ActiveHist}) ->
    ActiveHist.

-spec active_hist(witness_hist(), witness()) -> witness().
active_hist(ActiveHist, Witness) ->
    Witness#witness_v1{active_hist=ActiveHist}.

-spec old_hist(witness()) -> witness_hist().
old_hist(#witness_v1{old_hist=OldHist}) ->
    OldHist.

-spec old_hist(witness_hist(), witness()) -> witness().
old_hist(OldHist, Witness) ->
    Witness#witness_v1{old_hist=OldHist}.

-spec serialize(witness()) -> binary().
serialize(Witness) ->
    BinWitness = erlang:term_to_binary(Witness),
    <<1, BinWitness/binary>>.

-spec deserialize(binary()) -> witness().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Witness0 = #witness_v1{},
    ?assertEqual(Witness0, new()),
    Witness1 = #witness_v1{
                nonce = 1,
                count = 1
               },
    ?assertEqual(Witness1, new(1, 1)).

-endif.
