-module(blockchain_lock).

%% @doc a simple process based mutex.
%% TODO replace this with atomics:compare_exchange once we are on 21.2 or later

-behaviour(gen_server).

-export([acquire/0, release/0, force_release/0, check/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(holding_lock, holding_lock).
-define(lock_ref, lock_ref).

acquire() ->
    case get(?holding_lock) of
        undefined ->
            Ref = make_ref(),
            put(?lock_ref, Ref),
            put(?holding_lock, 1),
            gen_server:call(?MODULE, {acquire, Ref}, infinity);
        N ->
            put(?holding_lock, N + 1)
    end.

release() ->
    case get(?holding_lock) of
        undefined ->
            lager:warning("calling release on unheld lock"),
            ok;
        1 ->
            erase(?holding_lock),
            Ref = get(?lock_ref),
            erase(?lock_ref),
            ?MODULE ! {Ref, release};
        N ->
            put(?holding_lock, N - 1)
    end.

force_release()  ->
    erase(?holding_lock),
    ?MODULE ! release.

check() ->
    get(?holding_lock) /= undefined.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, {}}.

handle_call({acquire, LRef}, {Client, _MRef} = From, State) ->
    Ref = erlang:monitor(process, Client),
    gen_server:reply(From, ok),
    receive
        {LRef, release} ->
            erlang:demonitor(Ref, [flush]),
            ok;
        {'DOWN', Ref, process, Client, _} ->
            ok
    end,
    {noreply, State};
handle_call(_, _, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.
