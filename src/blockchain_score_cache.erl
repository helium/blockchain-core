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
    start_link/1,
    fetch/2,
    find/1,
    set/2
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
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fetch({Address :: libp2p_crypto:pubkey_bin(),
             Height :: pos_integer()},
            ScoreFun :: fun()) -> score().
fetch({Address, Height}, ScoreFun) ->
    %% Try to find in cache, if not found, set in cache and return the value
    case find({Address, Height}) of
        {error, not_found} ->
            set({Address, Height}, ScoreFun());
        {ok, Score} ->
            Score
    end.

-spec find({Address :: libp2p_crypto:pubkey_bin(),
            Height :: pos_integer()}) -> {error, not_found} | {ok, score()}.
find({Address, Height}) ->
    gen_server:call(?MODULE, {find, {Address, Height}}, infinity).

-spec set({Address :: libp2p_crypto:pubkey_bin(),
           Height :: pos_integer()},
          Score :: score()) -> score().
set({Address, Height}, Score) ->
    gen_server:call(?MODULE, {set, {{Address, Height}, Score}}, infinity).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    ok = blockchain_event:add_handler(self()),
    Cache = ets:new(score_cache,
                    [named_table,
                     {write_concurrency, true},
                     {read_concurrency, true}]),
    case  blockchain_worker:blockchain() of
        undefined ->
            %% XXX: need to handle clauses when there is no chain
            {ok, #state{}};
        Chain ->
            {ok, #state{chain=Chain, cache=Cache}}
    end.

handle_call({find, {_Address, _Height}}, _From, #state{chain=undefined}=State) ->
    {reply, {error, no_chain}, State};
handle_call({find, {Address, Height}}, _From, #state{cache=Cache}=State) ->
    Reply = case ets:lookup(Cache, {Address, Height}) of
                [] ->
                    {error, not_found};
                [Res] ->
                    {ok, Res}
            end,
    {reply, Reply, State};
handle_call({set, {{Address, Height}, Score}}, _From, #state{cache=Cache}=State) ->
    true = ets:insert(Cache, {{Address, Height}, Score}),
    {reply, Score, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({blockchain_event, {add_block, _Hash, _Sync, _Ledger}}, State) ->
    %% Do cache invalidation here?
    {noreply, State};
handle_info({blockchain_event, {new_chain, NC}}, State) ->
    {noreply, State#state{chain = NC}};
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
