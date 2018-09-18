%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Event ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_event).

-behaviour(gen_event).

-export([add_handler/1]).

%% ------------------------------------------------------------------
%% gen_event Function Exports
%% ------------------------------------------------------------------
-export([start_link/1
         ,init/1
         ,handle_event/2
         ,handle_call/2
         ,handle_info/2
         ,code_change/3
         ,terminate/2]).

start_link(Args) ->
    gen_event:start_link({local, ?MODULE}, Args).

-spec add_handler(pid()) -> ok | any().
add_handler(Pid) ->
    gen_event:add_handler(?MODULE, {?MODULE, make_ref()}, [Pid]).

init([Pid]) ->
    {ok, Pid}.

handle_event(Event, Pid) ->
    Pid ! {blockchain_event, Event},
    {ok, Pid}.

handle_call(_, Pid) ->
    {ok, ok, Pid}.

handle_info(_, Pid) ->
    {ok, Pid}.

code_change(_OldVsn, Pid, _Extra) ->
    {ok, Pid}.

terminate(_Reason, _Pid) ->
    ok.
