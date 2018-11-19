%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Event ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_event).

-behaviour(gen_event).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    add_handler/1
]).

%% ------------------------------------------------------------------
%% gen_event Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_event/2,
    handle_call/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_event:start_link({local, ?EVT_MGR}, Args).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_handler(pid()) -> ok | any().
add_handler(Pid) ->
    gen_event:add_handler(?EVT_MGR, {?MODULE, make_ref()}, [Pid]).

%% ------------------------------------------------------------------
%% gen_event Function Definitions
%% ------------------------------------------------------------------
init([Pid]) ->
    {ok, Pid}.

handle_event(Event, Pid) ->
    Pid ! {?MODULE, Event},
    {ok, Pid}.

handle_call(_Msg, Pid) ->
    lager:debug("rcv unhandled msg ~p", [_Msg]),
    {'ok', 'ok', Pid}.

handle_info(_Msg, Pid) ->
    lager:debug("rcv unhandled msg ~p", [_Msg]),
    {'ok', Pid}.

code_change(_OldVsn, Pid, _Extra) ->
    {'ok', Pid}.

terminate(_Reason, _Pid) ->
    lager:warning("terminating ~p", [_Reason]),
    'ok'.
