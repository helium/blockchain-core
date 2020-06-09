-module(blockchain_gateway_cache).

-behaviour(gen_server).

%% API
-export([
         start_link/0,
         get/2, get/3,
         bulk_put/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
                height = 1 :: pos_integer(),
                cache :: undefined | ets:tid()
               }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get(GwAddr :: libp2p_crypto:pubkey_bin(),
          Ledger :: blockchain_ledger_v1:ledger()) ->
                 ok | {error, _}.
get(Addr, Ledger) ->
    get(Addr, Ledger, true).

-spec get(GwAddr :: libp2p_crypto:pubkey_bin(),
          Ledger :: blockchain_ledger_v1:ledger(),
          CacheRead :: boolean()) ->
                 ok | {error, _}.
get(Addr, Ledger, false) ->
    blockchain_ledger_v1:find_gateway_info(Addr, Ledger);
get(Addr, Ledger, true) ->
    try
        case cache_get(Addr, Ledger) of
            {ok, _} = Result ->
                Result;
            _ ->
                case blockchain_ledger_v1:find_gateway_info(Addr, Ledger) of
                    {ok, Gw} = Result2 ->
                        cache_put(Addr, Gw, Ledger),
                        Result2;
                    Else ->
                        Else
                end
        end
    catch _:_ ->
            blockchain_ledger_v1:find_gateway_info(Addr, Ledger)
    end.

bulk_put(Height, List) ->
    gen_server:call(?MODULE, {bulk_put, Height, List}).

%% -spec put(GwAddr :: libp2p_crypto:pubkey_bin(),
%%           Gw :: blockchain_ledger_gateway_v2:gateway(),
%%           Ledger :: blockchain_ledger_v1:ledger()) ->
%%                  ok | {error, _}.
%% put(Addr, Gw, Ledger) ->
%%     put(Addr, Gw, Ledger, true).

%% -spec put(GwAddr :: libp2p_crypto:pubkey_bin(),
%%           Gw :: blockchain_ledger_gateway_v2:gateway(),
%%           Ledger :: blockchain_ledger_v1:ledger(),
%%           CacheWrite :: boolean()) ->
%%                  ok | {error, _}.
%% put(Addr, Gw, Ledger, false) ->
%%     blockchain_ledger_v1:update_gateway(Gw, Addr, Ledger);
%% put(Addr, Gw, Ledger, true) ->
%%     case blockchain_ledger_v1:update_gateway(Gw, Addr, Ledger) of
%%         ok ->
%%             try
%%                 cache_put(Addr, Gw, Ledger)
%%             catch _:_ ->
%%                     ok
%%             end,
%%             ok;
%%         Else ->
%%             Else
%%     end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% register for blocks here
    %% start the cache
    Cache = ets:new(?MODULE,
                    [named_table,
                     public,
                     {write_concurrency, true},
                     {read_concurrency, true}]),
    case application:get_env(blockchain, disable_gateway_cache, false) of
        false ->
            try blockchain_worker:blockchain() of
                undefined ->
                    erlang:send_after(500, self(), chain_init),
                    {ok, #state{cache = Cache}};
                Chain ->
                    ok = blockchain_event:add_handler(self()),
                    {ok, Height} = blockchain:height(Chain),
                    {ok, #state{height = Height, cache = Cache}}
            catch _:_ ->
                      erlang:send_after(500, self(), chain_init),
                      {ok, #state{cache = Cache}}
            end;
        _ ->
            {ok, #state{cache=Cache}}
    end.

handle_call({bulk_put, Height, List}, _From, State) ->
    lists:foreach(
      fun({Addr, SerGw}) ->
              Gw = blockchain_ledger_gateway_v1:deserialize(SerGw),
              ets:insert(?MODULE, {{Addr, Height}, Gw})
      end, List),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast({update, {{Addr, Height}, Gw}}, #state{height = CurrHeight} = State) ->
    case Height == CurrHeight of
        %% because of speculative absorbs, we cannot accept these, as
        %% they may be affected by transactions that will never land
        true ->
            {noreply, State};
        false ->
            ets:insert(?MODULE, {{Addr, Height}, Gw}),
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info({blockchain_event, {add_block, _Hash, _Sync, Ledger}}, State) ->
    %% sweep here
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, Height} ->
            ets:select_delete(?MODULE, [{{{'_','$1'},'_'},[{'<','$1', Height - 51}],[true]}]),
            ok;
        {error, _Err} ->
            ok
    end,
    {noreply, State};
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    ets:delete_all_objects(?MODULE),
    {ok, Height} = blockchain:height(NC),
    {noreply, State#state{height = Height}};
handle_info(chain_init, State) ->
    try blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), chain_init),
            {noreply, State};
        Chain ->
            ok = blockchain_event:add_handler(self()),
            {ok, Height} = blockchain:height(Chain),
            {noreply, State#state{height = Height}}
    catch _:_ ->
              erlang:send_after(500, self(), chain_init),
              {noreply, State}
    end;
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

cache_get(Addr, Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case ets:lookup(?MODULE, {Addr, Height}) of
        [] ->
            {error, not_found};
        [{_, Res}] ->
            {ok, Res}
    end.

cache_put(Addr, Gw, Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    gen_server:cast(?MODULE, {update, {{Addr, Height}, Gw}}),
    ok.
