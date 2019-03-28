-module(blockchain_lock).

%% @doc a simple process based mutex.
%% TODO replace this with atomics:compare_exchange once we are on 21.2 or later

-behaviour(gen_server).

-export([acquire/0, release/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2]).

acquire() ->
    gen_server:call(?MODULE, acquire, infinity).

release() ->
    ?MODULE ! release.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, {}}.

handle_call(acquire, {Client, _MRef} = From, State) ->
    Ref = erlang:monitor(process, Client),
    gen_server:reply(From, ok),
    receive
        release ->
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
