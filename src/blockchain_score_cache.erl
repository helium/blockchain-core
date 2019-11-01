%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Score Cache ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_score_cache).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
         start_link/0,
         fetch/2,
         find/1,
         set/2,
         stop/0
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

-define(SERVER, ?MODULE).

-record(state, {
          chain :: undefined | blockchain:blockchain(),
          cache :: undefined | ets:tid()
         }).

-type score() :: {float(), float(), float()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, [], []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fetch({Address :: libp2p_crypto:pubkey_bin(), float(), float(),
             Delta :: non_neg_integer(),
             Height :: pos_integer()},
            ScoreFun :: fun()) -> score().
fetch({Address, Alpha, Beta, Delta, Height}, ScoreFun) ->
    %% Try to find in cache, if not found, set in cache and return the value
    case find({Address, Alpha, Beta, Delta, Height}) of
        {error, not_found} ->
            set({Address, Alpha, Beta, Delta, Height}, ScoreFun());
        {ok, Score} ->
            Score
    end.

-spec find({Address :: libp2p_crypto:pubkey_bin(), float(), float(),
            Delta :: non_neg_integer(),
            Height :: pos_integer()}) -> {error, not_found} | {ok, score()}.
find({Address, Alpha, Beta, Delta, Height}) ->
    case ets:lookup(score_cache, {Address, Alpha, Beta, Delta, Height}) of
        [] ->
            {error, not_found};
        [{_, Res}] ->
            {ok, Res}
    end.

-spec set({Address :: libp2p_crypto:pubkey_bin(), float(), float(),
           Delta :: non_neg_integer(),
           Height :: pos_integer()},
          Score :: score()) -> score().
set({Address, Alpha, Beta, Delta, Height}, Score) ->
    true = ets:insert(score_cache, {{Address, Alpha, Beta, Delta, Height}, Score}),
    Score.

stop() ->
    gen_server:stop(?SERVER, normal, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    ok = blockchain_event:add_handler(self()),
    Cache = ets:new(score_cache,
                    [named_table,
                     public,
                     {write_concurrency, true},
                     {read_concurrency, true}]),
    case  blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), chain_init),
            {ok, #state{cache=Cache}};
        Chain ->
            ok = blockchain_event:add_handler(self()),
            {ok, #state{chain=Chain, cache=Cache}}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({blockchain_event, {add_block, _Hash, _Sync, _Ledger}}, #state{chain = undefined} = State) ->
    {noreply, State};
handle_info({blockchain_event, {add_block, Hash, _Sync, _Ledger}}, State) ->
    case blockchain:get_block(Hash, State#state.chain) of
        {ok, Block} ->
            Height = blockchain_block:height(Block),
            ets:select_delete(score_cache, [{{{'_','_','_','_','$1'},'_'},[{'<','$1', Height - 51}],[true]}]),
            ok;
        {error, _Err} ->
            ok
    end,
    {noreply, State};
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    {noreply, State#state{chain = NC}};
handle_info(chain_init, State) ->
    case  blockchain_worker:blockchain() of
        undefined ->
            erlang:send_after(500, self(), chain_init),
            {noreply, State};
        Chain ->
            ok = blockchain_event:add_handler(self()),
            {noreply, State#state{chain=Chain}}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
