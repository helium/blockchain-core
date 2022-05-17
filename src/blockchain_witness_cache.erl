-module(blockchain_witness_cache).

-behaviour(gen_server).

%% API
-export([
         start_link/0,
         cache_witness/3,
         lookup_witness/2,
         clear_address/1,
         clear_all/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(tab, bcwc).

-record(state,
        {
         tab :: ets:tab()
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

cache_witness(Addr, OnionKey, Result) ->
    ets:insert(?tab, {Addr, OnionKey, Result}).

lookup_witness(Addr, OnionKey) ->
    case ets:lookup(?tab, Addr) of
        Results = [_|_] ->
            case lists:keyfind(OnionKey, 2, Results) of
                false -> not_found;
                {_Addr, _OnionKey, Result} ->
                    {ok, Result}
            end;
        [] -> not_found
    end.

clear_address(Addr) ->
    ets:delete(?tab, Addr).

clear_all() ->
    ets:delete_all_objects(?tab).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Tab = ets:new(?tab, [
                         named_table, public, bag,
                         {write_concurrency, true},
                         {read_concurrency, true}
                        ]),
    {ok, #state{tab = Tab}}.

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
