%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("blockchain.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle(),
    credits = 0 :: non_neg_integer(),
    nonce = 0 :: non_neg_integer(),
    packets :: merkerl:merkle()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?SERVER, Args, []).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    % TODO: Fetch credits from chain and DB
    {ok, DB} = blockchain_state_channel_db:get(),
    Credits = 0,
    {ok, #state{
        db=DB,
        credits=Credits,
        nonce=0,
        % TODO: build or rebuild tree from DB
        packets=merkerl:new([], fun merkerl:hash_value/1)
    }}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------