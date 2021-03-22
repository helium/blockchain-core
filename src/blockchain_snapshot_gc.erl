-module(blockchain_snapshot_gc).

-behaviour(gen_server).

%% API
-export(
    [
        start_link/1
    ]
).

%% gen_server
-export(
    [
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
    ]
).

%% ----------------------------------------------------------------------------
%% Data (internal)
%% ----------------------------------------------------------------------------

-record(state, {}).

-type state() :: #state{}.

%% ----------------------------------------------------------------------------
%%  External
%% ----------------------------------------------------------------------------

%% API ------------------------------------------------------------------------
start_link(X) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, X, []).

%% gen_server (unused) --------------------------------------------------------

handle_cast(_Msg, #state{} = S) ->
    {noreply, S}.

handle_call(_Msg, _From, #state{} = S) ->
    {reply, {}, S}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% gen_server -----------------------------------------------------------------

init(Args) ->
    lager:info("~p:init(%p)", [?MODULE, Args]),
    ok = blockchain_event:add_handler(self()),
    {ok, #state{}}.

-spec handle_info({blockchain_event, term()}, state()) -> {noreply, state()}.
handle_info({blockchain_event, {maybe_do_snapshots_gc, BC}}, #state{} = S) ->
    ok = blockchain:maybe_do_snapshots_gc(BC),
    {noreply, S};
handle_info(_, #state{} = S) ->
    {noreply, S}.
