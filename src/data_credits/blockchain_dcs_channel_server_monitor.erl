%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Server Monitor ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_channel_server_monitor).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    channel_server/1, channel_server/2
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

-include("blockchain.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle(),
    swarm :: undefined | pid(),
    monitored = #{} :: #{pid() | libp2p_crypto:pubkey_bin() => libp2p_crypto:pubkey_bin() | pid()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec channel_server(libp2p_crypto:pubkey_bin()) -> {ok, pid()} | {error, any()}.
channel_server(PubKeyBin) ->
    gen_statem:call(?SERVER, {channel_server, PubKeyBin}).

-spec channel_server(map(), non_neg_integer()) -> ok.
channel_server(Keys, Amount) ->
    gen_statem:cast(?SERVER, {channel_server, Keys, Amount}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    erlang:process_flag(trap_exit, true),
    lager:info("~p init with ~p", [?SERVER, _Args]),
    {ok, DB} = blockchain_dcs_db:get_db(),
    Swarm = blockchain_swarm:swarm(),
    {ok, CFs} = blockchain_dcs_db:get_cfs(),
    FilteredCFs = maps:filter(
        fun(CFName, _CF) -> is_prefixed(CFName) end,
        CFs
    ),
    Monitored = maps:fold(
        fun(CFName, CF, Acc) ->
            PubKeyBin = remove_prefix(CFName),
            case get_keys(DB, PubKeyBin) of
                {ok, Keys} ->
                    start_channel_server(DB, CF, Keys, 0, PubKeyBin, Acc);
                _Error ->
                    Acc
            end
        end,
        #{},
        FilteredCFs
    ),
    {ok, #state{
        db=DB,
        swarm=Swarm,
        monitored=Monitored
    }}.

handle_call({channel_server, PubKeyBin}, _From, #state{monitored=Monitored}=State) ->
    case maps:get(PubKeyBin, Monitored, not_found) of
        not_found ->
            {reply, {error, not_found}, State};
        Pid ->
            {reply, {ok, Pid}, State}
    end;
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({channel_server, #{public := PubKey}=Keys, Amount}, #state{db=DB, monitored=Monitored0}=State) ->
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, CF} = blockchain_dcs_db:get_cf(add_prefix(PubKeyBin)),
    Monitored1 = start_channel_server(DB, CF, Keys, Amount, PubKeyBin, Monitored0),
    ok = store_keys(DB, Keys),
    {noreply, State#state{monitored=Monitored1}};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({'EXIT', Pid, normal}, #state{monitored=Monitored0}=State) ->
    case maps:get(Pid, Monitored0, undefined) of
        undefined ->
            lager:warning("unknown pid ~p died", [Pid]),
            {noreply, State};
        PubKeyBin ->
            lager:info("~p ~p settled", [Pid, PubKeyBin]),
            ok = blockchain_dcs_db:destroy_cf(add_prefix(PubKeyBin)),
            Monitored1 = maps:remove(Pid, maps:remove(PubKeyBin, Monitored0)),
            {noreply, State#state{monitored=Monitored1}}
    end;
handle_info({'EXIT', Pid, _Reason}, #state{db=DB, monitored=Monitored0}=State) ->
    lager:warning("~p went down ~p, trying to restart", [Pid, _Reason]),
    case maps:get(Pid, Monitored0, undefined) of
        undefined ->
            lager:error("could not restart ~p", [Pid]),
            {noreply, State};
        PubKeyBin->
            Monitored1 = maps:remove(Pid, maps:remove(PubKeyBin, Monitored0)),
            case get_keys(DB, PubKeyBin) of
                {ok, Keys} ->
                    {ok, CF} = blockchain_dcs_db:get_cf(add_prefix(PubKeyBin)),
                    Monitored2 = start_channel_server(DB, CF, Keys, 0, PubKeyBin, Monitored1),
                    {noreply, State#state{monitored=Monitored2}};
                _Error ->
                    lager:error("failed to get keys for ~p: ~p", [PubKeyBin, _Error]),
                    {noreply, State#state{monitored=Monitored1}}
            end
    end;
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

-spec store_keys(rocksdb:db_handle(), map()) -> ok.
store_keys(DB, #{public := PubKey}=Keys) ->
    KeysBin = libp2p_crypto:keys_to_bin(Keys),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    ok = rocksdb:put(DB, PubKeyBin, KeysBin, []).

-spec get_keys(rocksdb:db_handle(), libp2p_crypto:pubkey_bin()) -> {ok, map()} | {error, any()}.
get_keys(DB, PubKeyBin) ->
   case rocksdb:get(DB, PubKeyBin, [{sync, true}]) of
        {ok, KeysBin} ->
            Keys = libp2p_crypto:keys_from_bin(KeysBin),
            {ok, Keys};
        _Error ->
            _Error
    end.

-spec start_channel_server(rocksdb:db_handle(), rocksdb:cf_handle(), map(),
                           non_neg_integer(), libp2p_crypto:pubkey_bin(), map()) -> map().
start_channel_server(DB, CF, Keys, Amount, PubKeyBin, Monitored) ->
    {ok, Pid} = blockchain_dcs_channel_server:start_link([DB, CF, Keys, Amount]),
    maps:put(Pid, PubKeyBin, maps:put(PubKeyBin, Pid, Monitored)).

-spec add_prefix(libp2p_crypto:pubkey_bin()) -> binary().
add_prefix(PubKeyBin) ->
    <<"DCS_SERVER_CHANNEL_", PubKeyBin/binary>>.

-spec remove_prefix(binary() | string()) -> libp2p_crypto:pubkey_bin().
remove_prefix(CFName) when is_list(CFName) ->
    remove_prefix(erlang:list_to_binary(CFName));
remove_prefix(<<"DCS_SERVER_CHANNEL_", PubKeyBin/binary>>) ->
    PubKeyBin.

-spec is_prefixed(binary() | string()) -> boolean().
is_prefixed(CFName) when is_list(CFName) -> 
    is_prefixed(erlang:list_to_binary(CFName));
is_prefixed(<<"DCS_SERVER_CHANNEL_", _/binary>>) -> 
    true;
is_prefixed(_) -> 
    false.