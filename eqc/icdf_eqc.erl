%%%-----------------------------------------------------------------------------
%% Evaluate probabilistic accuracy of calculating inverse cumulative distritbution
%% function.
%%
%% - Generate a random list: [{Weight: float, Node: binary}, ...]
%%
%% - Check that the number of times a node gets picked is roughly equal to it's
%% weight of getting picked in the list. Statistically, those count and weights
%% should line up, probably. Maybe.
%%
%% Inverse Cumulative Distribution Function gives the value associated with a
%% _cumulative_ proabability. It ONLY works with cumulative probabilities and that's
%% what makes it magical.
%%%-----------------------------------------------------------------------------

-module(icdf_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_icdf_check/0]).

prop_icdf_check() ->
    ?FORALL({Population, Iterations, Hash}, {gen_population(), gen_iterations(), binary(32)},
            begin
                %% Switch the position to make it easier to work with the existing function.
                %% Also Node names are unique cuz map and also each hotspot has a unique addr.
                PopulationList = [{Weight, Node} || {Node, Weight} <- maps:to_list(Population)],

                %% Convert this to a cumulative probability distribution, the sum of
                %% probabilities must add to 1 for ICDF to work.
                %% We already do this for targeting when we assign the probabilities.
                CumulativePopulationList = blockchain_utils:cdf(maps:to_list(Population)),

                %% Sigh, need it back to find the cumulative probability to verify against
                CumulativePopulationMap = maps:from_list(CumulativePopulationList),

                Entropy = blockchain_utils:rand_state(Hash),

                %% Intiial acc for the counter, each node starts with a 0 count
                InitAcc = maps:map(fun(_, _) -> 0 end, CumulativePopulationMap),

                %% Track all counts a node gets picked
                {Counter, _} = lists:foldl(fun(_I, {Acc, AccEntropy}) ->
                                              {RandVal, NewEntropy} = rand:uniform_s(AccEntropy),
                                              {ok, Node} = blockchain_utils:icdf_select(CumulativePopulationList, RandVal),
                                              {maps:update_with(Node, fun(X) -> X + 1 end, 1, Acc), NewEntropy}
                                      end,
                                      {InitAcc, Entropy},
                                      lists:seq(1, Iterations)),

                %% Check that it's roughly equal or more appropriately within some threshold (0.1 is good enough, probably).
                %% Fucking probabilities.
                CheckCounterLinesUp = lists:all(fun({Node, Count}) ->
                                                        abs(Count/Iterations - maps:get(Node, CumulativePopulationMap)) < 0.1
                                                end,
                                                maps:to_list(Counter)),

                ?WHENFAIL(begin
                              io:format("PopulationList: ~p~n", [PopulationList]),
                              io:format("CDF: ~p~n", [CumulativePopulationList]),
                              io:format("Counter: ~p~n", [Counter])
                          end,
                          noshrink(conjunction(
                                     [{verify_unique_nodes, length(PopulationList) == length(lists:usort(PopulationList))},
                                      {verify_cdf, lists:sum([W || {_, W} <- CumulativePopulationList]) >= 0.99}, %% it's pretty much 1.0 but damn floats
                                      {verify_population_exists, length(PopulationList) > 0},
                                      {verify_counts_line_up, CheckCounterLinesUp}]
                                    )
                                  )
                         )
            end).

gen_iterations() ->
    %% Count these many times, lower counts don't quite "work" in the sense that the
    %% error threshold maybe too high for eqc to work with. But given enough iterations
    %% counts _should_ line up with the weights.
    elements([1000, 10000, 100000]).

gen_population() ->
    %% We need unique node names to mimic unique hotspot addresses
    %% Also keeps things simple.
    ?SUCHTHAT(M, map(gen_node(), gen_weight()), map_size(M) >= 3).

gen_node() ->
    %% Some random node name.
    binary(32).

gen_weight() ->
    %% A viable weight between (0, 1), open intervals.
    gen_prob().

gen_prob() ->
    %% Some probability
    ?SUCHTHAT(W, real(), W > 0 andalso W < 1).
