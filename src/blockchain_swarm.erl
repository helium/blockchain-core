%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Core Swarm ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_swarm).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    tid/0,
    pubkey_bin/0,
    swarm/0,
    keys/0,
    gossip_peers/0
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
%% define the name of the blockchain swarm, NOTE: libp2p will create and name the ets table based on this name
-define(SWARM_NAME, blockchain_swarm).

-record(state, {
    swarm :: undefined | pid()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

%%--------------------------------------------------------------------
%% @doc returns the ets TID for the blockchain swarm
%% @end
%%--------------------------------------------------------------------
-spec tid() -> ets:tab().
tid() ->
    ?SWARM_NAME.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec pubkey_bin() -> libp2p_crypto:pubkey_bin().
pubkey_bin() ->
    libp2p_swarm:pubkey_bin(?SWARM_NAME).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec swarm() -> pid().
swarm() ->
    whereis(libp2p_swarm:reg_name_from_tid(?SWARM_NAME, libp2p_swarm_sup)).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec keys() -> {ok, libp2p_crypto:public_key(), libp2p_crypto:sig_fun(), libp2p_crypto:ecdh_fun()} | {error, term()}.
keys() ->
    libp2p_swarm:keys(?SWARM_NAME).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gossip_peers() -> [string()].
gossip_peers() ->
    libp2p_group_gossip:connected_addrs(libp2p_swarm:gossip_group(?SWARM_NAME), all).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, Pid} = libp2p_swarm:start(?SWARM_NAME, Args),
    true = erlang:link(Pid),
    {ok, #state{swarm=Pid}}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({'EXIT', Swarm, Reason} , #state{swarm=Swarm}=State) ->
    lager:error("swarm ~p exited: ~p", [Swarm, Reason]),
    {stop, swarm_exit, State};
handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{swarm=Swarm}) when is_pid(Swarm) ->
    _ = libp2p_swarm:stop(Swarm),
    ok;
terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
