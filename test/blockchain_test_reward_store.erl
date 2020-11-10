-module(blockchain_test_reward_store).

-behaviour(gen_server).

-export([start/0]).
-export([fetch/1, insert/2, state/0, stop/0]).

% our handlers
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

insert(Key, Value) ->
    gen_server:call(?MODULE, {insert, {Key, Value}}).

fetch(Key) ->
    gen_server:call(?MODULE, {fetch, Key}).

state() ->
    gen_server:call(?MODULE, state).

stop() ->
    gen_server:stop(?MODULE).

init([]) ->
    {ok, #{}}.

handle_call({fetch, Key}, _From, State) ->
    {reply, maps:get(Key, State, undefined), State};
handle_call(state, _From, State) ->
    {reply, State, State};
handle_call({insert, {Key, Value}}, _From, State) ->
    NewState = maps:put(Key, Value, State),
    {reply, ok, NewState};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
