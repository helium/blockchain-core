-module(blockchain_state_channel_merge_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    lower_limit/1,
    higher_limit/1,
    at_limit/1
]).

all() ->
    [
        lower_limit,
        higher_limit,
        at_limit
    ].

init_per_suite(Config) ->
    blockchain_ct_utils:init_per_suite(Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    %% TODO: Remove duplication and do configuration here...
    Config.

end_per_testcase(_, _Config) ->
    ok.

lower_limit(_Config) ->
    {SC1, SC2} = gen_sc(),

    {NewTime, Merged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:new_merge(SC1, SC2, 2)
                       end),
    ct:pal("NewTime: ~p", [NewTime]),
    {OldTime, OldMerged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:merge(SC1, SC2, 2)
                       end),
    ct:pal("OldTime: ~p", [OldTime]),

    case lists:sort(blockchain_state_channel_v1:summaries(Merged)) == lists:sort(blockchain_state_channel_v1:summaries(OldMerged)) of
        false ->
            ct:pal("~p ~p", [blockchain_state_channel_v1:summaries(Merged), blockchain_state_channel_v1:summaries(OldMerged)]),
            ?assert(false);
        true ->
            ok
    end,

    ok.

higher_limit(_Config) ->
    {SC1, SC2} = gen_sc(),

    {NewTime, Merged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:new_merge(SC1, SC2, 8)
                       end),
    ct:pal("NewTime: ~p", [NewTime]),
    {OldTime, OldMerged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:merge(SC1, SC2, 8)
                       end),
    ct:pal("OldTime: ~p", [OldTime]),

    case lists:sort(blockchain_state_channel_v1:summaries(Merged)) == lists:sort(blockchain_state_channel_v1:summaries(OldMerged)) of
        false ->
            ct:pal("~p ~p", [blockchain_state_channel_v1:summaries(Merged), blockchain_state_channel_v1:summaries(OldMerged)]),
            ?assert(false);
        true ->
            ok
    end,

    ok.

at_limit(_Config) ->
    {SC1, SC2} = gen_sc(),

    {NewTime, Merged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:new_merge(SC1, SC2, 6)
                       end),
    ct:pal("NewTime: ~p", [NewTime]),
    {OldTime, OldMerged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:merge(SC1, SC2, 6)
                       end),
    ct:pal("OldTime: ~p", [OldTime]),

    case lists:sort(blockchain_state_channel_v1:summaries(Merged)) == lists:sort(blockchain_state_channel_v1:summaries(OldMerged)) of
        false ->
            ct:pal("~p ~p", [blockchain_state_channel_v1:summaries(Merged), blockchain_state_channel_v1:summaries(OldMerged)]),
            ?assert(false);
        true ->
            ok
    end,

    ok.

gen_sc() ->

    Summary1 = blockchain_state_channel_summary_v1:num_packets(2, blockchain_state_channel_summary_v1:num_dcs(2, blockchain_state_channel_summary_v1:new(<<"p1">>))),
    Summary2 = blockchain_state_channel_summary_v1:num_packets(4, blockchain_state_channel_summary_v1:num_dcs(4, blockchain_state_channel_summary_v1:new(<<"p1">>))),
    Summary3 = blockchain_state_channel_summary_v1:num_packets(1, blockchain_state_channel_summary_v1:num_dcs(1, blockchain_state_channel_summary_v1:new(<<"p2">>))),
    Summary4 = blockchain_state_channel_summary_v1:num_packets(1, blockchain_state_channel_summary_v1:num_dcs(1, blockchain_state_channel_summary_v1:new(<<"p3">>))),
    Summary5 = blockchain_state_channel_summary_v1:num_packets(1, blockchain_state_channel_summary_v1:num_dcs(1, blockchain_state_channel_summary_v1:new(<<"p4">>))),
    Summary6 = blockchain_state_channel_summary_v1:num_packets(1, blockchain_state_channel_summary_v1:num_dcs(1, blockchain_state_channel_summary_v1:new(<<"p4">>))),

    BaseSC1 = blockchain_state_channel_v1:new(<<"1">>, crypto:strong_rand_bytes(4), 1),
    BaseSC2 = blockchain_state_channel_v1:new(<<"2">>, crypto:strong_rand_bytes(4), 1),

    {blockchain_state_channel_v1:summaries([Summary1, Summary3, Summary5], BaseSC1),
     blockchain_state_channel_v1:summaries([Summary2, Summary4, Summary6], BaseSC2)}.
