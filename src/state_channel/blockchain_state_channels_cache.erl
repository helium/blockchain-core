%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Cache ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_cache).

-include("blockchain_vars.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    init/0,
    lookup_hotspot/1,
    insert_hotspot/2,
    delete_hotspot/1,
    delete_pids/1,
    lookup_actives/0,
    insert_actives/1,
    delete_actives/1,
    overwrite_actives/1
]).

-define(ETS, blockchain_state_channels_cache_ets).
-define(DIFF_ETS, blockchain_state_channels_diff_ets).
%% ets:fun2ms(fun({_, Pid}) when Pid == self() -> true end).
-define(SELECT_DELETE_PID(Pid), [{{'_', '$1'}, [{'==', '$1', {const, Pid}}], [true]}]).
-define(ACTIVES_KEY, active_scs).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
init() ->
    Opts = [
        public,
        named_table,
        set,
        {write_concurrency, true},
        {read_concurrency, true}
    ],
    _ = ets:new(?ETS, Opts),
    lager:info("init cache"),
    ok.

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

-spec delete_hotspot(HotspotID :: libp2p_crypto:pubkey_bin()) -> ok.
delete_hotspot(HotspotID)->
    true = ets:delete(?ETS, HotspotID),
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
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

hotspot_pid_test() ->
    ok = ?MODULE:init(),

    Self = self(),
    HotspotID0 = crypto:strong_rand_bytes(32),

    % Test simple insert/lookup
    ?assertEqual(ok, ?MODULE:insert_hotspot(HotspotID0, Self)),
    ?assertEqual(Self, ?MODULE:lookup_hotspot(HotspotID0)),
    ?assertEqual(undefined, ?MODULE:lookup_hotspot(crypto:strong_rand_bytes(32))),

    % Test insert/delete
    lists:foreach(
        fun(_) ->
            HotspotID = crypto:strong_rand_bytes(32),
            ?assertEqual(ok, ?MODULE:insert_hotspot(HotspotID, Self)),
            ?assertEqual(Self, ?MODULE:lookup_hotspot(HotspotID0))
        end,
        lists:seq(1, 2000) % We use 2k here as it is the number of actors per state channel at the moment
    ),
    ?assertEqual(2001, ?MODULE:delete_pids(Self)),
    ?assertEqual([], ets:tab2list(?ETS)),

    % Test lookup with is_process_alive=false
    Pid = erlang:spawn(
        fun() ->
            receive _ -> ok end
        end
    ),
    HotspotID1 = crypto:strong_rand_bytes(32),
    ?assertEqual(ok, ?MODULE:insert_hotspot(HotspotID1, Pid)),
    ?assertEqual(Pid, ?MODULE:lookup_hotspot(HotspotID1)),
    HotspotID2 = crypto:strong_rand_bytes(32),
    ?assertEqual(ok, ?MODULE:insert_hotspot(HotspotID2, Pid)),
    ?assertEqual(Pid, ?MODULE:lookup_hotspot(HotspotID2)),
    Pid ! stop,
    ok = timer:sleep(10),
    ?assertEqual(false, erlang:is_process_alive(Pid)),
    ?assertEqual(undefined, ?MODULE:lookup_hotspot(HotspotID1)),
    ok = timer:sleep(10),
    ?assertEqual([], ets:tab2list(?ETS)),

    _ = ets:delete(?ETS),
    ok.

actives_test() ->
    ok = ?MODULE:init(),

    Self = self(),

    ?assertEqual([], ?MODULE:lookup_actives()),
    ?assertEqual(ok, ?MODULE:insert_actives(Self)),
    ?assertEqual([Self], ?MODULE:lookup_actives()),
    ?assertEqual(ok, ?MODULE:delete_actives(Self)),
    ?assertEqual([], ?MODULE:lookup_actives()),
    ?assertEqual(ok, ?MODULE:overwrite_actives([Self])),
    ?assertEqual([Self], ?MODULE:lookup_actives()),

    _ = ets:delete(?ETS),
    ok.

-endif.
