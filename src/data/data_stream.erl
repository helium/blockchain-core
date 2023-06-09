-module(data_stream).

-export_type([
    t/1
]).

-export([
    next/1,
    from_list/1,
    to_list/1,
    iter/2,
    pmap_to_bag/2,
    pmap_to_bag/3
    %% TODO map
    %% TODO fold
]).

-record(sched, {
    id             :: reference(),
    producers      :: [{pid(), reference()}],
    consumers      :: [{pid(), reference()}],
    consumers_free :: [pid()],  % available to work.
    work           :: [any()],  % received from producers.
    results        :: [any()]   % received from consumers.
}).

%% API ========================================================================

-type t(A) :: fun(() -> none | {some, {A, t(A)}}).

-spec next(t(A)) -> none | {some, {A, t(A)}}.
next(T) when is_function(T) ->
    T().

-spec iter(fun((A) -> ok), t(A)) -> ok.
iter(F, T0) ->
    case next(T0) of
        none ->
            ok;
        {some, {X, T1}} ->
            F(X),
            iter(F, T1)
    end.

-spec from_list([A]) -> t(A).
from_list([]) ->
    fun () -> none end;
from_list([X | Xs]) ->
    fun () -> {some, {X, from_list(Xs)}} end.

-spec to_list(t(A)) -> [A].
to_list(T0) when is_function(T0) ->
    case next(T0) of
        none ->
            [];
        {some, {X, T1}} ->
            [X | to_list(T1)]
    end.

%% A pmap which doesn't preserve order.
-spec pmap_to_bag(t(A), fun((A) -> B)) -> [B].
pmap_to_bag(Xs, F) when is_function(Xs), is_function(F) ->
    pmap_to_bag(Xs, F, blockchain_utils:cpus()).

-spec pmap_to_bag(t(A), fun((A) -> B), non_neg_integer()) -> [B].
pmap_to_bag(T, F, J) when is_function(T), is_function(F), is_integer(J), J > 0 ->
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
                    ok = iter(fun (X) -> SchedPid ! {SchedID, producer_output, X} end, T)
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

%% Internal ===================================================================

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

%% Tests ======================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

pmap_to_bag_test_() ->
    NonDeterminism = fun (N) -> timer:sleep(rand:uniform(N)) end,
    FromListWithNonDeterminism =
        fun (N) ->
            fun Stream (Xs) ->
                fun () ->
                    case Xs of
                        [] ->
                            none;
                        [X | Xs1] ->
                            NonDeterminism(N),
                            {some, {X, Stream(Xs1)}}
                    end
                end
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

-endif.
