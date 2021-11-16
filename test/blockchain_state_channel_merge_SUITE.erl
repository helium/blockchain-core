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
    basic/1
]).

all() ->
    [
        basic
    ].

init_per_suite(Config) ->
    blockchain_ct_utils:init_per_suite(Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

basic(_Config) ->
    {ok, Dir0} = file:get_cwd(),
    Dir = filename:join(lists:reverse(lists:nthtail(4, lists:reverse(filename:split(Dir0))))),
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    {ok, [SC1]} = file:consult(filename:join([PrivDir, "sc1.erl"])),
    {ok, [SC2]} = file:consult(filename:join([PrivDir, "sc2.erl"])),
    {NewTime, Merged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:merge(SC1, SC2, 2000)
                       end),
    ct:pal("NewTime: ~p", [NewTime]),
    {OldTime, OldMerged} = timer:tc(
                       fun() ->
                               blockchain_state_channel_v1:old_merge(SC1, SC2, 2000)
                       end),
    ct:pal("OldTime: ~p", [OldTime]),

    true = lists:sort(blockchain_state_channel_v1:summaries(Merged)) == lists:sort(blockchain_state_channel_v1:summaries(OldMerged)),

    ok.
