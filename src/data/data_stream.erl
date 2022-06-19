-module(data_stream).

-export_type([
    next/1,
    t/1
]).

-export([
    next/1,
    from_fun/1,
    from_list/1,
    to_list/1,
    foreach/2,
    fold/3,
    map/2,     % Alias for lazy_map.
    filter/2,  % Alias for lazy_filter.
    lazy_map/2,
    lazy_filter/2,
    pmap_to_bag/2,
    pmap_to_bag/3,
    sample/2
]).

-define(T, ?MODULE).

-type reservoir(A) :: #{pos_integer() => A}.

-type filter(A, B)
    :: {map, fun((A) -> B)}
    |  {test, fun((A) -> boolean())}
    .

-type next(A) :: fun(() -> none | {some, {A, next(A)}}).

-record(?T, {
    next :: next(any()),
    filters :: [filter(any(), any())]
}).

-opaque t(A) ::
    %% XXX Records to do not support type parameters.
    %% XXX Ensure the field order is the same as in the corresponding record.
    {
        ?T,
        next(A),
        [filter(A, any())]
    }.

-record(sched, {
    id             :: reference(),
    producers      :: [{pid(), reference()}],
    consumers      :: [{pid(), reference()}],
    consumers_free :: [pid()],  % available to work.
    work           :: [any()],  % received from producers.
    results        :: [any()]   % received from consumers.
}).

%% API ========================================================================

-spec from_fun(next(A)) -> t(A).
from_fun(Next) ->
    #?T{
        next = Next,
        filters = []
    }.

-spec next(t(A)) -> none | {some, {A, t(A)}}.
next(#?T{next=Next0, filters=Filters}=T0) when is_function(Next0) ->
    case Next0() of
        none ->
            none;
        {some, {X, Next1}} when is_function(Next1) ->
            T1 = T0#?T{next=Next1},
            case filters_apply(X, Filters) of
                none ->
                    next(T1);
                {some, Y} ->
                    {some, {Y, T1}}
            end
    end.

map(T, F) ->
    lazy_map(T, F).

filter(T, F) ->
    lazy_filter(T, F).

-spec lazy_map(t(A), fun((A) -> B)) -> t(B).
lazy_map(#?T{filters=Filters}=T, F) ->
    T#?T{filters=Filters ++ [{map, F}]}.

-spec lazy_filter(t(A), fun((A) -> boolean())) -> t(A).
lazy_filter(#?T{filters=Filters}=T, F) ->
    T#?T{filters=Filters ++ [{test, F}]}.

-spec fold(t(A), B, fun((A, B) -> B)) -> B.
fold(T0, Acc, F) ->
    case next(T0) of
        none ->
            Acc;
        {some, {X, T1}} ->
            fold(T1, F(X, Acc), F)
    end.

-spec foreach(t(A), fun((A) -> ok)) -> ok.
foreach(T0, F) ->
    case next(T0) of
        none ->
            ok;
        {some, {X, T1}} ->
            F(X),
            foreach(T1, F)
    end.

-spec from_list([A]) -> t(A).
from_list(Xs) ->
    from_fun(from_list_(Xs)).

-spec from_list_([A]) -> next(A).
from_list_([]) ->
    fun () -> none end;
from_list_([X | Xs]) ->
    fun () -> {some, {X, from_list_(Xs)}} end.

-spec to_list(t(A)) -> [A].
to_list(T0) ->
    case next(T0) of
        none ->
            [];
        {some, {X, T1}} ->
            [X | to_list(T1)]
    end.

%% A pmap which doesn't preserve order.
-spec pmap_to_bag(t(A), fun((A) -> B)) -> [B].
pmap_to_bag(Xs, F) when is_function(F) ->
    pmap_to_bag(Xs, F, blockchain_utils:cpus()).

-spec pmap_to_bag(t(A), fun((A) -> B), non_neg_integer()) -> [B].
pmap_to_bag(T, F, J) when is_function(F), is_integer(J), J > 0 ->
    CallerPid = self(),
    SchedID = make_ref(),
    Scheduler =
        fun () ->
            SchedPid = self(),
            Consumer =
                fun Consume () ->
                    ConsumerPid = self(),
                    SchedPid ! {SchedID, consumer_ready, ConsumerPid},
                    receive
                        {SchedID, job, X} ->
                            Y = F(X),
                            SchedPid ! {SchedID, consumer_output, Y},
                            Consume();
                        {SchedID, done} ->
                            ok
                    end
                end,
            Producer =
                fun () ->
                    %% XXX Producer is racing against consumers.
                    %%
                    %% This hasn't (yet) caused a problem, but in theory it is
                    %% bad: producer is pouring into the scheduler's queue as
                    %% fast as possible, potentially faster than consumers can
                    %% pull from it, so heap usage could explode.
                    %%
                    %% Solution ideas:
                    %% A. have the scheduler call the producer whenever more
                    %%    work is asked for, but ... that can block the
                    %%    scheduler, starving consumers;
                    %% B. produce in (configurable size) batches, pausing
                    %%    production when batch is full and resuming when not
                    %%    (this is probably the way to go).
                    ok = foreach(T, fun (X) -> SchedPid ! {SchedID, producer_output, X} end)
                end,
            Ys =
                sched(#sched{
                    id             = SchedID,
                    producers      = [spawn_monitor(Producer)],
                    consumers      = [spawn_monitor(Consumer) || _ <- lists:duplicate(J, {})],
                    consumers_free = [],
                    work           = [],
                    results        = []
                }),
            CallerPid ! {SchedID, Ys}
        end,
    %% XXX Scheduling from a dedicated process to avoid conflating our 'DOWN'
    %%     messages (from producers and consumers) with those of the caller
    %%     process.
    {SchedPid, SchedMonRef} = spawn_monitor(Scheduler),
    %% TODO timeout?
    receive
        {SchedID, Ys} ->
            receive
                {'DOWN', SchedMonRef, process, SchedPid, normal} ->
                    Ys
            end;
        {'DOWN', SchedMonRef, process, SchedPid, Reason} ->
            error({data_stream_scheduler_crashed_before_sending_results, Reason})
    end.

-spec sample(t(A), non_neg_integer()) -> [A].
sample(_, 0) -> [];
sample(T, K) when K > 0 ->
    {_N, Reservoir} = reservoir_sample(T, #{}, K),
    [X || {_, X} <- maps:to_list(Reservoir)].

%% Internal ===================================================================

%% @doc
%% The optimal reservoir sampling algorithm. Known as "Algorithm L" in:
%% https://dl.acm.org/doi/pdf/10.1145/198429.198435
%% https://en.wikipedia.org/wiki/Reservoir_sampling#An_optimal_algorithm
%% @end
-spec reservoir_sample(t(A), reservoir(A), pos_integer()) ->
    {pos_integer(), reservoir(A)}.
reservoir_sample(T0, R0, K) ->
    case reservoir_sample_init(T0, R0, 1, K) of
        {none, R1, I} ->
            {I, R1};
        {{some, T1}, R1, I} ->
            W = random_weight_init(K),
            J = random_index_next(I, W),
            reservoir_sample_update(T1, R1, W, I, J, K)
    end.

-spec reservoir_sample_init(t(A), reservoir(A), pos_integer(), pos_integer()) ->
    {none | {some, A}, reservoir(A), pos_integer()}.
reservoir_sample_init(T0, R, I, K) ->
    case I > K of
        true ->
            {{some, T0}, R, I - 1};
        false ->
            case next(T0) of
                {some, {X, T1}} ->
                    reservoir_sample_init(T1, R#{I => X}, I + 1, K);
                none ->
                    {none, R, I - 1}
            end
    end.

-spec random_weight_init(pos_integer()) -> float().
random_weight_init(K) ->
    math:exp(math:log(rand:uniform()) / K).

-spec random_weight_next(float(), pos_integer()) -> float().
random_weight_next(W, K) ->
    W * random_weight_init(K).

-spec random_index_next(pos_integer(), float()) -> pos_integer().
random_index_next(I, W) ->
    I + floor(math:log(rand:uniform()) / math:log(1 - W)) + 1.

-spec reservoir_sample_update(
    t(A),
    reservoir(A),
    float(),
    pos_integer(),
    pos_integer(),
    pos_integer()
) ->
    {pos_integer(), reservoir(A)}.
reservoir_sample_update(T0, R0, W0, I0, J0, K) ->
    case next(T0) of
        none ->
            {I0, R0};
        {some, {X, T1}} ->
            I1 = I0 + 1,
            case I0 =:= J0 of
                true ->
                    R1 = R0#{rand:uniform(K) => X},
                    W1 = random_weight_next(W0, K),
                    J1 = random_index_next(J0, W0),
                    reservoir_sample_update(T1, R1, W1, I1, J1, K);
                false ->
                    % Here is where the big win takes place over the simple
                    % Algorithm R. We skip computing random numbers for an
                    % element that will not be picked.
                    reservoir_sample_update(T1, R0, W0, I1, J0, K)
            end
    end.

-spec sched(#sched{}) -> [any()].
sched(#sched{id=_, producers=[], consumers=[], consumers_free=[], work=[], results=Ys}) ->
    Ys;
sched(#sched{id=ID, producers=[], consumers=[_|_], consumers_free=[_|_]=CsFree, work=[]}=S0) ->
    _ = [C ! {ID, done} || C <- CsFree],
    sched(S0#sched{consumers_free=[]});
sched(#sched{id=_, producers=_, consumers=[_|_], consumers_free=[_|_], work=[_|_]}=S0) ->
    S1 = sched_assign(S0),
    sched(S1);
sched(#sched{id=ID, producers=Ps, consumers=_, consumers_free=CsFree, work=Xs, results=Ys }=S) ->
    receive
        {ID, producer_output, X} -> sched(S#sched{work=[X | Xs]});
        {ID, consumer_output, Y} -> sched(S#sched{results=[Y | Ys]});
        {ID, consumer_ready, C}  -> sched(S#sched{consumers_free=[C | CsFree]});
        {'DOWN', MonRef, process, Pid, normal} ->
            S1 = sched_remove_worker(S, {Pid, MonRef}),
            sched(S1);
        {'DOWN', MonRef, process, Pid, Reason} ->
            case lists:member({Pid, MonRef}, Ps) of
                true  -> error({?MODULE, pmap_to_bag, producer_crash, Reason});
                false -> error({?MODULE, pmap_to_bag, consumer_crash, Reason})
            end
    end.

-spec sched_remove_worker(#sched{}, {pid(), reference()}) -> #sched{}.
sched_remove_worker(#sched{producers=Ps, consumers=Cs, consumers_free=CsFree}=S, {Pid, _}=PidRef) ->
    case lists:member(PidRef, Ps) of
        true ->
            S#sched{producers = Ps -- [PidRef]};
        false ->
            S#sched{
                consumers = Cs -- [PidRef],
                consumers_free = CsFree -- [Pid]
            }
    end.

-spec sched_assign(#sched{}) -> #sched{}.
sched_assign(#sched{consumers_free=[], work=Xs}=S) -> S#sched{consumers_free=[], work=Xs};
sched_assign(#sched{consumers_free=Cs, work=[]}=S) -> S#sched{consumers_free=Cs, work=[]};
sched_assign(#sched{consumers_free=[C | Cs], work=[X | Xs], id=ID}=S) ->
    C ! {ID, job, X},
    sched_assign(S#sched{consumers_free=Cs, work=Xs}).

-spec filters_apply(A, [filter(A, B)]) -> none | {some, B}.
filters_apply(X, Filters) ->
    lists:foldl(
        fun (_, none) ->
                none;
            (F, {some, Y}) ->
                case F of
                    {map, Map} ->
                        {some, Map(Y)};
                    {test, Test} ->
                        case Test(Y) of
                            true ->
                                {some, Y};
                            false ->
                                none
                        end
                end
        end,
        {some, X},
        Filters
    ).

%% Tests ======================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

pmap_to_bag_test_() ->
    NonDeterminism = fun (N) -> timer:sleep(rand:uniform(N)) end,
    FromListWithNonDeterminism =
        fun (N) ->
            fun (Xs) ->
                lazy_map(from_list(Xs), fun (X) -> NonDeterminism(N), X end)
            end
        end,
    Tests =
        [
            begin
                G = fun (X) -> NonDeterminism(ConsumerDelay), F(X) end,
                Test =
                    ?_assertEqual(
                        lists:sort(lists:map(G, Xs)),
                        lists:sort(pmap_to_bag(
                            (FromListWithNonDeterminism(ProducerDelay))(Xs),
                            G,
                            J
                        ))
                    ),
                Timeout = 1000 + ProducerDelay + (ConsumerDelay * J),
                Name = lists:flatten(io_lib:format(
                    "#Xs: ~p, J: ~p, ProducerDelay: ~p, ConsumerDelay: ~p, Timeout: ~p",
                    [length(Xs), J, ProducerDelay, ConsumerDelay, Timeout]
                )),
                {Name, {timeout, Timeout, Test}}
            end
        ||
            J <- lists:seq(1, 16),
            F <- [
                fun (X) -> {X, X} end,
                fun (X) -> X * 2 end
            ],
            Xs <- [
                lists:seq(1, 100)
            ],
            {ProducerDelay, ConsumerDelay} <-
                begin
                    Lo = 1,
                    Hi = 10,
                    [
                        {Hi, Lo}, % slow producer, fast consumer
                        {Lo, Hi}, % fast producer, slow consumer
                        {Lo, Lo}, % both fast
                        {Hi, Hi}  % both slow
                    ]
                end
        ],
    {inparallel, Tests}.

round_trip_test_() ->
    [
        ?_assertEqual(Xs, to_list(from_list(Xs)))
    ||
        Xs <- [
            [1, 2, 3],
            [a, b, c],
            [<<>>, <<"foo">>, <<"bar">>, <<"baz">>, <<"qux">>]
        ]
    ].

lazy_map_test_() ->
    Double = fun (X) -> X * 2 end,
    [
        ?_assertEqual(
            lists:map(Double, Xs),
            to_list(lazy_map(from_list(Xs), Double))
        )
    ||
        Xs <- [
            [1, 2, 3, 4, 5]
        ]
    ].

lazy_filter_test_() ->
    IsEven = fun (X) -> 0 =:= X rem 2 end,
    [
        ?_assertEqual(
            lists:filter(IsEven, Xs),
            to_list(lazy_filter(from_list(Xs), IsEven))
        )
    ||
        Xs <- [
            [1, 2, 3, 4, 5]
        ]
    ].

lazy_filters_compose_test_() ->
    IsMultOf = fun (M) -> fun (N) -> 0 =:= N rem M end end,
    Double = fun (N) -> N * 2 end,
    [
        ?_assertEqual(
            begin
                L0 = Xs,
                L1 = lists:filter(IsMultOf(2), L0),
                L2 = lists:map(Double, L1),
                L3 = lists:filter(IsMultOf(3), L2),
                L3
            end,
            to_list(
                begin
                    S0 = from_list(Xs),
                    S1 = lazy_filter(S0, IsMultOf(2)),
                    S2 = lazy_map(S1, Double),
                    S3 = lazy_filter(S2, IsMultOf(3)),
                    S3
                end
            )
        )
    ||
        Xs <- [
            lists:seq(1, 10),
            lists:seq(1, 100),
            lists:seq(1, 100, 3)
        ]
    ].

fold_test_() ->
    [
        ?_assertEqual(
            lists:foldl(F, Acc, Xs),
            fold(from_list(Xs), Acc, F)
        )
    ||
        {Acc, F} <- [
            {0, fun erlang:'+'/2},
            {[], fun (X, Xs) -> [X | Xs] end}
        ],
        Xs <- [
            [1, 2, 3, 4, 5]
        ]
    ].

random_elements_test_() ->
    TestCases =
        [
            ?_assertMatch([a], sample(from_list([a]), 1)),
            ?_assertEqual(0, length(sample(from_list([]), 1))),
            ?_assertEqual(0, length(sample(from_list([]), 10))),
            ?_assertEqual(0, length(sample(from_list([]), 100))),
            ?_assertEqual(1, length(sample(from_list(lists:seq(1, 100)), 1))),
            ?_assertEqual(2, length(sample(from_list(lists:seq(1, 100)), 2))),
            ?_assertEqual(3, length(sample(from_list(lists:seq(1, 100)), 3))),
            ?_assertEqual(5, length(sample(from_list(lists:seq(1, 100)), 5)))
        |
            [
                (fun () ->
                    Trials = 10,
                    K = floor(N * KF),
                    L = lists:seq(1, N),
                    S = from_list(L),
                    Rands =
                        [
                            sample(S, K)
                        ||
                            _ <- lists:duplicate(Trials, {})
                        ],
                    Head = lists:sublist(L, K),
                    Unique = lists:usort(Rands) -- [Head],
                    Name =
                        lists:flatten(io_lib:format(
                            "At least 1/~p of trials makes a new sequence. "
                            "N:~p K:~p KF:~p length(Unique):~p",
                            [Trials, N, K, KF, length(Unique)]
                        )),
                    {Name, ?_assertMatch([_|_], Unique)}
                end)()
            ||
                N <- lists:seq(10, 100),
                KF <- [
                    0.25,
                    0.50,
                    0.75
                ]
            ]
        ],
    {inparallel, TestCases}.

-endif.
