%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Cache ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_cache).

-behavior(gen_server).

-include("blockchain_vars.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    lookup_hotspot/1,
    insert_hotspot/2,
    delete_pids/1,
    lookup_actives/0,
    insert_actives/1,
    delete_actives/1,
    overwrite_actives/1
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
-define(ETS, blockchain_state_channels_cache_ets).
%% ets:fun2ms(fun({_, Pid}) when Pid == Self -> true end).
-define(SELECT_DELETE_PID(Pid), [{{'_', '$1'}, [{'==', '$1', {const, Pid}}], [true]}]).
-define(ACTIVES_KEY, active_scs).

-record(state, {}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec lookup_hotspot(HotspotID :: libp2p_crypto:pubkey_bin()) -> pid() | undefined.
lookup_hotspot(HotspotID) ->
    case ets:lookup(?ETS, HotspotID) of
        [] ->
            undefined;
        [{HotspotID, Pid}] ->
            case erlang:is_process_alive(Pid) of
                false ->
                    _ = erlang:spawn(?MODULE, delete_pids, [Pid]),
                    undefined;
                true ->
                    Pid
            end
    end.

-spec insert_hotspot(HotspotID :: libp2p_crypto:pubkey_bin(), Pid :: pid()) -> ok.
insert_hotspot(HotspotID, Pid) ->
    true = ets:insert(?ETS, {HotspotID, Pid}),
    ok.

-spec delete_pids(Pid :: pid()) -> integer().
delete_pids(Pid) ->
    ets:select_delete(?ETS, ?SELECT_DELETE_PID(Pid)).

-spec lookup_actives() -> [pid()].
lookup_actives() ->
    case ets:lookup(?ETS, ?ACTIVES_KEY) of
        [] -> [];
        [{?ACTIVES_KEY, Pids}] -> Pids
    end.

-spec insert_actives(Pid :: pid()) -> ok.
insert_actives(Pid) ->
    Actives = lookup_actives(),
    case lists:member(Pid, Actives) of
        true ->
            ok;
        false ->
            true = ets:insert(?ETS, {?ACTIVES_KEY, [Pid|Actives]}),
            ok
    end.

-spec delete_actives(Pid :: pid()) -> ok.
delete_actives(Pid) ->
    Actives = lookup_actives(),
    case lists:member(Pid, Actives) of
        false ->
            ok;
        true ->
            true = ets:insert(?ETS, {?ACTIVES_KEY, lists:delete(Pid, Actives)}),
            ok
    end.

-spec overwrite_actives(Pids :: [pid()]) -> ok.
overwrite_actives(Pids) ->
    true = ets:insert(?ETS, {?ACTIVES_KEY, Pids}),
    ok.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Opts = [
        public,
        named_table,
        set,
        {write_concurrency, true},
        {read_concurrency, true}
    ],
    _ = ets:new(?ETS, Opts),
    {ok, #state{}}.

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
