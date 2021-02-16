%%%-----------------------------------------------------------------------------
%%% Make a short list of electors (select 4 of 10, perhaps),
%%% generate some random probs for each elector, run N random selects and make
%%% sure that the electors ended up in the group roughly Prob percentage of the time
%%%-----------------------------------------------------------------------------

-module(election_prob_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_election_prob_check/0]).

prop_election_prob_check() ->
    ?FORALL({Iterations, Electors}, {gen_iterations(), gen_electors()},
            begin
                {ToSelect, Candidates} = Electors,

                Counter = lists:foldl(fun(_Iteration, Acc) ->
                                              Selected = blockchain_election:icdf_select(Candidates, ToSelect, []),
                                              lists:foldl(fun(Selectee, Acc2) ->
                                                                  maps:update_with(Selectee, fun(V) -> V+1 end, 0, Acc2)
                                                          end, Acc, Selected)
                                      end,
                                      #{},
                                      lists:seq(1, Iterations)),

                Expectations = lists:foldl(fun({Candidate, Prob}, Acc) ->
                                                   maps:put(Candidate, round(Prob * Iterations), Acc)
                                           end, #{}, Candidates),

                CheckCounter = Counter == Expectations,

                ?WHENFAIL(begin
                              io:format("Iterations: ~p~n", [Iterations]),
                              io:format("Electors: ~p~n", [Electors]),
                              io:format("Counter: ~p~n", [Counter]),
                              io:format("Expectations: ~p~n", [Expectations])
                          end,
                          noshrink(conjunction(
                                     [{verify_population_exists, length(Candidates) > 0},
                                      {verify_counter, CheckCounter}]
                                    )
                                  )
                         )
            end).

gen_iterations() ->
    %% number of iterations to run
    elements([10000, 20000, 100000]).

gen_candidates() ->
    %% Generate some candidates with associated probabilities
    ?SUCHTHAT(L, list({gen_node(), gen_prob()}), length(L) >= 3 andalso length(L) =< 15).

gen_electors() ->
    %% Generate electors, select ToSelect out of Candidates
    ?SUCHTHAT({ToSelect, Candidates}, {int(), gen_candidates()}, ToSelect > 0 andalso ToSelect =< length(Candidates)).

gen_node() ->
    %% Some random node name.
    non_empty(bitstring(32)).

gen_prob() ->
    %% Some probability
    ?SUCHTHAT(W, real(), W > 0 andalso W < 1).
