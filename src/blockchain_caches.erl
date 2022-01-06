-module(blockchain_caches).

-behaviour(gen_server).

%% API
-export([
         start_link/0,
         lookup_loc/2,
         cache_loc/3,
         lookup_gain/2,
         cache_gain/3,

         clear_all/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(loc, bc_loc_cache).
-define(gain, bc_gain_cache).

-record(state,
        {
         location :: ets:tab()
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

lookup_loc(Addr, Height) ->
    case ets:lookup(?loc, Addr) of
        [] -> not_found;
        [{_Addr, StoreHeight, Location}] ->
            case StoreHeight > Height of
                true -> height_mismatch;
                _ -> {ok, Location}
            end
        end.

cache_loc(Addr, Height, Location) ->
    ets:insert(?loc, {Addr, Height, Location}).

%% possible to fold these together?
lookup_gain(Addr, Height) ->
    case ets:lookup(?gain, Addr) of
        [] -> not_found;
        [{_Addr, StoreHeight, Gain}] ->
            case StoreHeight > Height of
                true -> height_mismatch;
                _ -> {ok, Gain}
            end
        end.

cache_gain(Addr, Height, Gain) ->
    ets:insert(?gain, {Addr, Height, Gain}).

clear_all() ->
    ets:delete(?gain),
    ets:delete(?loc),
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% todo add proper cache stats
    Loc = ets:new(?loc, [named_table, public,
                         {read_concurrency, true}]),
    {ok, #state{location = Loc}}.

handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    lager:warning("unexpected message ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
