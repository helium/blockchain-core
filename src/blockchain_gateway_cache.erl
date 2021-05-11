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
    catch ets:update_counter(?MODULE, total, 1, {total, 0}),
    blockchain_ledger_v1:find_gateway_info(Addr, Ledger);
get(Addr, Ledger, true) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    get_at(Addr, Ledger, Height).

get_at(Addr, Ledger, _Height) ->
    blockchain_ledger_v1:find_gateway_info(Addr, Ledger).

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
                                         [{'<','$1', max(1, Height - RetentionLimit)}],
                                         [true]}]),
            {noreply, State#state{height = Height}};
        {error, _Err} ->
            {noreply, State}
    end;
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    ets:delete_all_objects(?MODULE),
    {ok, Height} = blockchain_ledger_v1:current_height(blockchain:ledger(NC)),
    ets:insert(?MODULE, {start_height, Height}),
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

