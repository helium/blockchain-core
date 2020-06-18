-module(blockchain_follower).

-callback requires_sync() -> boolean().
-callback requires_ledger() -> boolean().
-callback follower_height(State::any()) -> pos_integer().
-callback init(Args::any()) -> {ok, State::any()} | {error, term()}.
-callback load_chain(blockchain:blockchain(), State::any()) -> {ok, NewState::any()}.
-callback load_block(Hash::binary(),
                     blockchain:block(),
                     Sync::boolean(),
                     blockchain_ledger_v1:ledger() | undefined,
                     State::any()) -> {ok, NewState::any()}.
-callback terminate(State::any()) -> ok.

-optional_callbacks([terminate/1]).

-behaviour(gen_server).

%% API
-export([start_link/1, start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state,
        {
         chain=undefined :: undefined | blockchain:blockchain(),
         follower_mod :: atom(),
         follower_state :: any()
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ServerName, Args) ->
    gen_server:start_link(ServerName, ?MODULE, Args, []).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Args) ->
    %% Init the follower module
    {FollowerMod, FollowerArgs} = proplists:get_value(follower_module, Args),
    {ok, FollowerState} = FollowerMod:init(FollowerArgs),
    %% Register a sync/async blockchain_event handler
    case FollowerMod:requires_sync() of
        true -> blockchain_event:add_sync_handler(self());
        false -> blockchain_event:add_handler(self())
    end,
    %% Now load the chain
    BaseDir = application:get_env(blockchain, base_dir, "data"),
    blockchain_worker:load(BaseDir, "update"),
    {ok, #state{follower_mod=FollowerMod, follower_state=FollowerState}}.

handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info({blockchain_event, From, Event}, State=#state{}) ->
    %% Received a sync blockchain_event. Dispatch as if async and then
    %% acknowledge once it returns
    Result = handle_info({blockchain_event, Event}, State),
    blockchain_event:acknowledge(From),
    Result;

handle_info({blockchain_event, {new_chain, Chain}}, State=#state{follower_mod=FollowerMod}) ->
    {ok, FollowerState} = FollowerMod:load_chain(Chain, State#state.follower_state),
    {noreply, State#state{chain=Chain, follower_state=FollowerState}};

handle_info({blockchain_event, _ChainEvent}=Msg, State=#state{ chain=undefined}) ->
    handle_info(Msg, State#state{chain=blockchain_worker:blockchain()});
handle_info({blockchain_event, {add_block, Hash, Sync, Ledger}},
            State=#state{chain=Chain, follower_mod=FollowerMod}) ->
    {ok, Block} = blockchain:get_block(Hash, Chain),
    MaybePlaybackBlocks =
        fun() ->
           Height = FollowerMod:follower_height(State#state.follower_state),
           BlockHeight = blockchain_block:height(Block),
           case BlockHeight of
               X when X == Height + 1 ->
                   %% as expected, just continue below
                   {ok, State#state.follower_state};
               X when X =< Height ->
                   lager:info("ignoring block ~p", [BlockHeight]),
                   %% already have this block
                   {error, already_loaded};
               X when X > Height + 1 ->
                   %% missing some blocks, try to obtain them
                   BlockHeights = lists:seq(Height + 1, BlockHeight - 1),
                   RequiresLedger = FollowerMod:requires_ledger(),
                   lager:info("trying to absorb missing blocks [~p..~p]", [hd(BlockHeights), lists:last(BlockHeights)]),
                   lists:foldl(fun(MissingHeight, {ok, FS}) ->
                                       {ok, MissingBlock} = blockchain:get_block(MissingHeight, Chain),
                                       MissingHash = blockchain_block:hash_block(MissingBlock),
                                       {ok, MissingLedger} = case RequiresLedger of
                                                                 true ->
                                                                     blockchain:ledger_at(MissingHeight, Chain);
                                                                 false ->
                                                                     {ok, undefined}
                                                             end,
                                       FollowerMod:load_block(MissingHash, MissingBlock, true, MissingLedger, FS)
                               end, {ok, State#state.follower_state}, BlockHeights)
           end
        end,

    case MaybePlaybackBlocks() of
        {error, already_loaded} ->
            {noreply, State};
        {ok, FollowerState} ->
            {ok, NewFollowerState} = FollowerMod:load_block(Hash, Block, Sync, Ledger, FollowerState),
            {noreply, State#state{follower_state=NewFollowerState}}
    end;

handle_info({blockchain_event, Other}, State=#state{}) ->
    lager:info("Ignoring blockchain event: ~p", [Other]),
    {noreply, State};

handle_info(_Info, State) ->
    lager:warning("unexpected message ~p", [_Info]),
    {noreply, State}.

terminate(Reason, State=#state{follower_mod=FollowerMod}) ->
    case erlang:function_exported(FollowerMod, terminate, 2) of
        true->
            FollowerMod:terminate(Reason, State#state.follower_state);
        false ->
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
