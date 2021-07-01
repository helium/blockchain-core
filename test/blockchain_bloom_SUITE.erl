-module(blockchain_bloom_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("blockchain_vars.hrl").

-export([
    all/0
]).

-export([
   bloom_test/1
]).


all() ->
    [
        bloom_test
    ].

%% Right now we put about 1100 actors per SC with the current 15000000 size it seems to be causing issues
%% I have here an exemple of a better tailored bloom. If you repeat this test it will pass 99% of the time
%% if you decide to increase m (Number of bits in the filter)  it will start having issue and taking too long
%% https://hur.st/bloomfilter/?n=1100&p=1.0E-6&m=&k=20
bloom_test(_Config) ->
    Max = 1100,
    {ok, ClientBloom} = bloom:new(31631, Max),
    Runs = lists:foldl(
        fun(I, Acc) ->
            Key = crypto:strong_rand_bytes(32),
            {Time1, _Result1} = timer:tc(bloom, check, [ClientBloom, Key]),
            {Time2, _Result2} = timer:tc(bloom, set, [ClientBloom, Key]),
            {Time3, _Result3} = timer:tc(bloom, check, [ClientBloom, Key]),
            case Time1 > 10000 orelse Time2 > 10000 orelse Time3 > 10000 of
                false -> Acc;
                true -> [{I, Time1, Time2, Time3}|Acc]
                
            end
        end,
        [],
        lists:seq(1, Max)
    ),
    ct:pal("~p", [Runs]),
    ?assertEqual(0, erlang:length(Runs)),
    ok.
