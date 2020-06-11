%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Gateway Witness ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_gateway_witness_v2).

-include("blockchain_vars.hrl").

-export([
         new/0, new/2,
         nonce/1, nonce/2,
         count/1, count/2,

         first_time/1, first_time/2,
         recent_time/1, recent_time/2,

         %% get overview of each histogram
         rssi_hist/1,
         tof_hist/1,
         snr_hist/1,

         %% get current histograms for each category
         current_rssi_hist/1,
         current_tof_hist/1,
         current_snr_hist/1,

         %% get histogram history
         rssi_hist_stack/1,
         tof_hist_stack/1,
         snr_hist_stack/1,

         serialize/1, deserialize/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(witness_v2, {
          nonce = 0 :: non_neg_integer(),
          count = 0 :: non_neg_integer(),

          rssi_hist_stack = [] :: blockchain_ledger_hist:hist_stack(),
          tof_hist_stack = [] :: blockchain_ledger_hist:hist_stack(),
          snr_hist_stack = [] :: blockchain_ledger_hist:hist_stack(),

          first_time :: undefined | non_neg_integer(),
          recent_time :: undefined | non_neg_integer()
         }).

-type witness() :: #witness_v2{}.

-export_type([witness/0]).

-spec new() -> witness().
new() ->
    #witness_v2{}.

-spec new(Nonce :: non_neg_integer(), Count :: non_neg_integer()) -> witness().
new(Nonce, Count) ->
    #witness_v2{nonce=Nonce, count=Count}.

-spec nonce(witness()) -> non_neg_integer().
nonce(#witness_v2{nonce=Nonce}) ->
    Nonce.

-spec nonce(Witness :: witness(),
            Nonce :: non_neg_integer()) -> witness().
nonce(Witness, Nonce) ->
    Witness#witness_v2{nonce=Nonce}.

-spec count(Witness :: witness()) -> non_neg_integer().
count(#witness_v2{count=Count}) ->
    Count.

-spec count(Witness :: witness(),
            Count :: non_neg_integer()) -> witness().
count(Witness, Count) ->
    Witness#witness_v2{count=Count}.

-spec first_time(Witness :: witness()) -> non_neg_integer().
first_time(#witness_v2{first_time=FirstTime}) ->
    FirstTime.

-spec first_time(Witness :: witness(),
                 FirstTime :: non_neg_integer()) -> witness().
first_time(Witness, FirstTime) ->
    Witness#witness_v2{first_time=FirstTime}.

-spec recent_time(Witness :: witness()) -> non_neg_integer().
recent_time(#witness_v2{recent_time=RecentTime}) ->
    RecentTime.

-spec recent_time(Witness :: witness(),
                  RecentTime :: non_neg_integer()) -> witness().
recent_time(Witness, RecentTime) ->
    Witness#witness_v2{recent_time=RecentTime}.

-spec rssi_hist(Witness :: witness()) -> blockchain_ledger_hist:hist().
rssi_hist(#witness_v2{rssi_hist_stack=Stack}) ->
    blockchain_ledger_hist:merge(Stack).

-spec tof_hist(Witness :: witness()) -> blockchain_ledger_hist:hist().
tof_hist(#witness_v2{tof_hist_stack=Stack}) ->
    blockchain_ledger_hist:merge(Stack).

-spec snr_hist(Witness :: witness()) -> blockchain_ledger_hist:hist().
snr_hist(#witness_v2{tof_hist_stack=Stack}) ->
    blockchain_ledger_hist:merge(Stack).

-spec current_rssi_hist(Witness :: witness()) -> blockchain_ledger_hist:hist().
current_rssi_hist(#witness_v2{rssi_hist_stack=[Head | _]}) ->
    Head.

-spec current_tof_hist(Witness :: witness()) -> blockchain_ledger_hist:hist().
current_tof_hist(#witness_v2{tof_hist_stack=[Head | _]}) ->
    Head.

-spec current_snr_hist(Witness :: witness()) -> blockchain_ledger_hist:hist().
current_snr_hist(#witness_v2{snr_hist_stack=[Head | _]}) ->
    Head.

-spec rssi_hist_stack(Witness :: witness()) -> blockchain_ledger_hist:hist_stack().
rssi_hist_stack(Witness) ->
    Witness#witness_v2.rssi_hist_stack.

-spec tof_hist_stack(Witness :: witness()) -> blockchain_ledger_hist:hist_stack().
tof_hist_stack(Witness) ->
    Witness#witness_v2.tof_hist_stack.

-spec snr_hist_stack(Witness :: witness()) -> blockchain_ledger_hist:hist_stack().
snr_hist_stack(Witness) ->
    Witness#witness_v2.snr_hist_stack.

-spec serialize(Witness :: witness()) -> binary().
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
    Witness0 = #witness_v2{},
    ?assertEqual(Witness0, new()),
    Witness1 = #witness_v2{
                  nonce = 1,
                  count = 1
                 },
    ?assertEqual(Witness1, new(1, 1)).

-endif.
