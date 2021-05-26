%%%-----------------------------------------------------------------------------
%%% Whatever determine_sizes_math_v2(Size, OldLen, Delay, ReplacementFactor, ReplacementSlope, Interval) ->
%%%-----------------------------------------------------------------------------

-module(election_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_determine_sizes_v2/0]).

prop_determine_sizes_v2() ->
    ?FORALL({Size, OldLen, Delay, ReplacementFactor, ReplacementSlope, Interval},
            {gen_size(), gen_old_len(), gen_delay(), gen_replacement_factor(), gen_replacement_slope(), gen_interval()},
            begin

                {Remove, Replace} = blockchain_election:determine_sizes_v2_math(
                                      Size,
                                      OldLen,
                                      Delay,
                                      ReplacementFactor,
                                      ReplacementSlope,
                                      Interval),

                %% io:format("Remove: ~p~n", [Remove]),
                %% io:format("Replace: ~p~n", [Replace]),

                ?WHENFAIL(begin
                              io:format("Size: ~p~n", [Size]),
                              io:format("OldLen: ~p~n", [OldLen]),
                              io:format("Delay: ~p~n", [Delay]),
                              io:format("ReplacementFactor: ~p~n", [ReplacementFactor]),
                              io:format("ReplacementSlope: ~p~n", [ReplacementSlope]),
                              io:format("Interval: ~p~n", [Interval]),
                              io:format("Remove: ~p~n", [Remove]),
                              io:format("Replace: ~p~n", [Replace])
                          end,
                          noshrink(conjunction(
                                     [{verify_remove,
                                       Remove > 0 orelse Size >= OldLen},
                                      {non_negative, Remove >= 0 andalso Replace >= 0},
                                      {relative_size,
                                       (Remove == Replace andalso Size == OldLen)
                                       orelse
                                         (Remove > Replace andalso Size < OldLen)
                                       orelse
                                         (Remove < Replace andalso Size > OldLen)},
                                      {totals,
                                       (OldLen - Remove + Replace) == Size}
                                     ]
                                    )
                                  )
                         )
            end).

gen_size() ->
    ?SUCHTHAT(N, int(), N >= 16 andalso N =< 100).

gen_old_len() ->
    ?SUCHTHAT(N, int(), N >= 16 andalso N =< 100).

gen_delay() ->
    ?SUCHTHAT(N, int(), N >= 20 andalso N =< 1000).

gen_replacement_factor() ->
    ?SUCHTHAT(N, int(), N >= 2 andalso N =< 100).

gen_replacement_slope() ->
    ?SUCHTHAT(N, int(), N >= 1 andalso N =< 100).

gen_interval() ->
    ?SUCHTHAT(N, int(), N >= 5 andalso N =< 100).
