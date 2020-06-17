-module(blockchain_gateway_cache).

-behaviour(gen_server).

%% API
-export([
         start_link/0,
         get/2, get/3,
         %% invalidate/2,
         bulk_put/2,
         stats/0
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
    {ok, blockchain_ledger_gateway_v2:gateway()} | {error, _}.
get(Addr, Ledger) ->
    get(Addr, Ledger, true).

%% make sure that during absorb you're always skipping the cache, just
%% in case.
-spec get(GwAddr :: libp2p_crypto:pubkey_bin(),
          Ledger :: blockchain_ledger_v1:ledger(),
          CacheRead :: boolean()) ->
    {ok, blockchain_ledger_gateway_v2:gateway()} | {error, _}.
get(Addr, Ledger, false) ->
    ets:update_counter(?MODULE, total, 1, {total, 0}),
    blockchain_ledger_v1:find_gateway_info(Addr, Ledger);
get(Addr, Ledger, true) ->
    ets:update_counter(?MODULE, total, 1, {total, 0}),
    try
        {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
        %% first try the context cache
        case blockchain_ledger_v1:gateway_cache_get(Addr, Ledger) of
            spillover ->
                blockchain_ledger_v1:find_gateway_info(Addr, Ledger);
            {ok, Gw} ->
                lager:debug("new cache hit ~p ~p", [Addr, Height]),
                ets:update_counter(?MODULE, hit, 1, {hit, 0}),
                {ok, Gw};
            _ ->
                %% then try our cache
                case cache_get(Addr, Ledger) of
                    {ok, _} = Result ->
                        ets:update_counter(?MODULE, hit, 1, {hit, 0}),
                        lager:debug("get ~p at ~p hit", [Addr, Height]),
                        Result;
                    _ ->
                        %% then back off finally to the disk
                        case blockchain_ledger_v1:find_gateway_info(Addr, Ledger) of
                            {ok, DiskGw} = Result2 ->
                                lager:debug("get ~p at ~p miss writeback", [Addr, Height]),
                                cache_put(Addr, DiskGw, Ledger),
                                Result2;
                            Else ->
                                lager:debug("get ~p at ~p miss err", [Addr, Height]),
                                Else
                        end
                end
        end
    catch _:_ ->
            ets:update_counter(?MODULE, error, 1, {error, 0}),
            blockchain_ledger_v1:find_gateway_info(Addr, Ledger)
    end.

stats() ->
    case ets:lookup(?MODULE, total) of
        [] -> 0;
        [{_, Tot}] ->
            HitRate =
                case ets:lookup(?MODULE, hit) of
                    [] -> no_hits;
                    [{_, Hits}] ->
                        Hits / Tot
                end,
            Err = case ets:lookup(?MODULE, error) of
                      [] -> 0;
                      [{_, E}] -> E
                  end,
            {Tot, HitRate, Err}
    end.

bulk_put(Height, List) ->
    gen_server:call(?MODULE, {bulk_put, Height, List}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% register for blocks here
    %% start the cache
    Cache = ets:new(?MODULE,
                    [named_table,
                     public,
                     {read_concurrency, true}]),
    case application:get_env(blockchain, disable_gateway_cache, false) of
        false ->
            try blockchain_worker:blockchain() of
                undefined ->
                    erlang:send_after(500, self(), chain_init),
                    {ok, #state{cache = Cache}};
                Chain ->
                    ok = blockchain_event:add_handler(self()),
                    {ok, Height} = blockchain_ledger_v1:current_height(blockchain:ledger(Chain)),
                    ets:insert(?MODULE, {start_height, Height}),
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
      fun({Addr, Gw}) ->
              lager:debug("bulk ~p", [Addr]),
              add_height(Addr, Height),
              ets:insert(?MODULE, {{Addr, Height}, Gw})
      end, List),
    ets:insert(?MODULE, {curr_height, Height}),
    lager:debug("bulk_add at ~p ~p", [Height, length(List)]),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info({blockchain_event, {add_block, _Hash, _Sync, Ledger}}, State) ->
    %% sweep here
    RetentionLimit = application:get_env(blockchain, gw_cache_retention_limit, 76),
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, Height} ->
            lager:debug("sweeping at ~p", [Height]),
            %% sweep later so we get some use out of the tail on
            %% rarely updated spots
            ets:select_delete(?MODULE, [{{{'_','$1'},'_'},
                                         [{'<','$1', Height - RetentionLimit}],
                                         [true]}]),
            {noreply, State#state{height = Height}};
        {error, _Err} ->
            {noreply, State}
    end;
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    ets:delete_all_objects(?MODULE),
    {ok, Height} = blockchain_ledger_v1:current_height(blockchain:ledger(NC)),
    {noreply, State#state{height = Height}};
handle_info(chain_init, State) ->
    try blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), chain_init),
            {noreply, State};
        Chain ->
            ok = blockchain_event:add_handler(self()),
            {ok, Height} = blockchain_ledger_v1:current_height(blockchain:ledger(Chain)),
            ets:insert(?MODULE, {start_height, Height}),
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
    [{_, StartHeight}] = ets:lookup(?MODULE, start_height),
    case ets:lookup(?MODULE, curr_height) of
        [{_, CurrHeight}] ->
            case ets:lookup(?MODULE, Addr) of
                [] -> {error, not_found};
                [{_, Updates}] ->
                    case get_update(Height, CurrHeight, StartHeight, Updates) of
                        none -> {error, not_found};
                        Update ->
                            case ets:lookup(?MODULE, {Addr, Update}) of
                                [] ->
                                    {error, not_found};
                                [{_, Res}] ->
                                    lager:debug("lastupdate ~p ~p ~p", [Addr, Update, Height]),
                                    {ok, Res}
                            end
                    end
            end;
        _ -> {error, not_found}
    end.

cache_put(Addr, Gw, Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    [{_, StartHeight}] = ets:lookup(?MODULE, start_height),
    case Height =< StartHeight of
        true ->
            ok;
        false ->
            case ets:lookup(?MODULE, curr_height) of
                [{_, CurrHeight}] ->
                    case Height == CurrHeight of
                        %% because of speculative absorbs, we cannot accept these, as
                        %% they may be affected by transactions that will
                        %% never land. They should mostly be covered by the
                        %% context cache
                        true ->
                            lager:debug("get ~p at ~p miss *no* writeback", [Addr, Height]),
                            ok;
                        false ->
                            case ets:lookup(?MODULE, Addr) of
                                [] ->
                                    add_height(Addr, Height);
                                [{_, LastUpdate}] ->
                                    case Height > LastUpdate of
                                        true ->
                                            add_height(Addr, Height);
                                        false ->
                                            ok
                                    end
                            end,
                            lager:debug("get ~p at ~p miss writeback", [Addr, Height]),
                            ets:insert(?MODULE, {{Addr, Height}, Gw}),
                            ok
                    end;
                [] ->
                    %% not sure that it's safe to do anything here
                    ok
            end
    end.

add_height(Addr, Height) ->
    case ets:lookup(?MODULE, Addr) of
        [] ->
            New = {Addr, [Height]},
            %% CAS insert is different than CAS
            case ets:insert_new(?MODULE, New) of
                true ->
                    ok;
                _ ->
                    add_height(Addr, Height)
            end;
        [{_, Heights} = Old] ->
            Heights1 = add_in_order(Height, Heights),
            case Heights == Heights1 of
                %% don't bother with the expensive CAS if our insert
                %% wouldn't change anything
                true ->
                    ok;
                false ->
                    New = {Addr, Heights1},
                    %% CAS the index
                    case ets:select_replace(?MODULE, [{Old, [], [{const, New}]}]) of
                        1 ->
                            ok;
                        _ ->
                            add_height(Addr, Height)
                    end
            end
    end.

add_in_order(New, Heights) ->
    Heights1 =
        %% high to low
        lists:reverse(
          %% no dups
          lists:usort([New | Heights])),
    Top = hd(Heights1),
    %% drop old indices, as they'll be unreachable shortly anyway due
    %% to sweeping
    RetentionLimit = application:get_env(blockchain, gw_cache_retention_limit, 76),
    lists:filter(fun(X) -> X > Top - RetentionLimit end, Heights1).

get_update(_Height, _CurrHeight, _StartHeight, []) ->
    none;
get_update(Height, CurrHeight, StartHeight, [H | T]) ->
    case Height >= H of
        true when H > (StartHeight + 1) ->
            H;
        %% it's not safe to use anything earlier than the starting
        %% height, because the index may be incomplete, because it's
        %% too expensive to rematerialize the whole index on startup.
        %% so we start slow/nonoptimal, but safely.
        true ->
            none;
        false ->
            get_update(Height, CurrHeight, StartHeight, T)
    end.
