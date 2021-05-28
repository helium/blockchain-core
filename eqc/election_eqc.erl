%%%-----------------------------------------------------------------------------
%%% Whatever determine_sizes_math_v2(Size, OldLen, Delay, ReplacementFactor, ReplacementSlope, Interval) ->
%%%-----------------------------------------------------------------------------

-module(election_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
         prop_determine_sizes_v2/0,
         prop_removal/0
        ]).

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


prop_removal() ->
    ?FORALL({Size, OldLen, Delay, OffCt, Pool},
            gen_removal(),
            begin

                {Remove, Replace} = blockchain_election:determine_sizes_v2_math(
                                      Size,
                                      OldLen,
                                      Delay,
                                      %% set a few of these statically to cut down on the state
                                      %% space size
                                      4, 20, 10),

                {OldGroup, _ValidatorsPool} = lists:split(OldLen, Pool),

                OldGroupAddrs = [blockchain_election:val_addr(V)
                                 || V <- OldGroup],

                meck:new(blockchain_election, [passthrough]),
                meck:expect(blockchain_election, adjust_old_group_v2,
                            fun(Group, _Ledger) ->
                                    lists:map(
                                      fun(G) ->
                                              {blockchain_election:val_addr(G), 1.0}
                                      end,
                                      Group)
                            end),

                meck:new(blockchain_ledger_v1, [passthrough]),
                meck:expect(blockchain_ledger_v1, current_height,
                            fun(_Ledger) ->
                                    {ok, 10}
                            end),
                meck:expect(blockchain_ledger_v1, consensus_members,
                            fun(_Ledger) ->
                                    {ok, OldGroupAddrs}
                            end),

                meck:expect(blockchain_ledger_v1, config,
                            fun(validator_liveness_interval, _Ledger) ->
                                    {ok, 1};
                               (validator_liveness_grace_period, _Ledger) ->
                                    {ok, 2};
                               (validator_penalty_filter, _Ledger) ->
                                    {ok, 5.0};
                               (_Var, _Ledger) ->
                                    {error, _Var}
                            end),

                %% we expect a hb within the last 3 blocks
                ExpectedOffline = [blockchain_election:val_addr(V)
                                   || V <- Pool, blockchain_election:val_hb(V) < 7,
                                      lists:member(blockchain_election:val_addr(V), OldGroupAddrs)],

                Validators = maps:from_list([{blockchain_election:val_addr(V), V}
                                             || V <- Pool]),

                {OldGroupDeduped0, Offline, _Validators} =
                    blockchain_election:val_dedup(OldGroupAddrs, Validators, ledger),

                OfflineAddrs = [blockchain_election:val_addr(V)
                                || V <- Offline],

                ToRemove = blockchain_election:select_removals(Remove,
                                                               OldLen,
                                                               Size,
                                                               Offline,
                                                               OldGroupDeduped0,
                                                               ledger),

                io:format("~p ~p~n", [OffCt, Offline]),

                meck:unload(blockchain_election),
                meck:unload(blockchain_ledger_v1),
                ?WHENFAIL(begin
                              io:format("Size: ~p~n", [Size]),
                              io:format("OldLen: ~p~n", [OldLen]),
                              io:format("Delay: ~p~n", [Delay]),
                              io:format("OldGroupAddrs: ~p~n", [OldGroupAddrs]),
                              io:format("ToRemove: ~p~n", [ToRemove]),
                              io:format("Offline: ~p~n", [Offline]),
                              io:format("ExpectedOffline: ~p~n", [ExpectedOffline]),
                              io:format("OffCt: ~p~n", [OffCt]),
                              io:format("Remove: ~p~n", [Remove]),
                              io:format("Replace: ~p~n", [Replace])
                          end,
                          conjunction(
                            [
                             {size, length(ToRemove) == Remove},
                             {offline,
                              (OfflineAddrs -- ToRemove) == [] orelse
                              length(ToRemove) < OffCt
                             },
                             {offline_size,
                              length(ExpectedOffline) == length(Offline)}
                            ]
                           )
                         )
            end).

%%%%%%%%%%%%%%%%
%% generators %%
%%%%%%%%%%%%%%%%

gen_removal() ->
    ?LET(Size, gen_size(),
         begin
             OffCt = noshrink(no_faults(choose(0, ceil(Size/5)))),
             {Size, gen_old_len(), gen_delay(), OffCt, noshrink(gen_pool(OffCt))}
         end).

gen_pool(OffCt) ->
    %% do we need to shuffle this?
    ?LET(OfflineCt,
         OffCt,
         begin
             blockchain_utils:shuffle(
               [begin
                    F = ?SUCHTHAT(R, real(), R > 0.5 andalso R < 10.0),
                    %% this is an absolute height and the simulated election is happening at height 10, so it
                    %% needs to be on the chain, but lower than the current height
                    blockchain_election:val(F, no_faults(choose(7, 9)), <<N>>)
                end
                || N <- lists:seq(1, 110 - OfflineCt)] ++
                   [begin
                        F = ?SUCHTHAT(R, real(), R > 0.5 andalso R < 10.0),
                        %% this is an absolute height and the simulated election is happening at height 10, so it
                        %% needs to be on the chain, but lower than the current height
                        blockchain_election:val(F, no_faults(choose(2, 6)), <<N>>)
                    end
                    || N <- lists:seq(111 - OfflineCt, 110)])
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
