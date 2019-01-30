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

-record(state, {
    swarm :: undefined | pid()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec pubkey_bin() -> libp2p_crypto:pubkey_bin().
pubkey_bin() ->
    gen_server:call(?MODULE, pubkey_bin).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec swarm() -> pid().
swarm() ->
    gen_server:call(?MODULE, swarm).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec keys() -> {ok, libp2p_crypto:public_key(), libp2p_crypto:sig_fun()} | {error, term()}.
keys() ->
    gen_server:call(?MODULE, key).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gossip_peers() -> [{string(), pid()}].
gossip_peers() ->
    gen_server:call(?MODULE, gossip_peers).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    erlang:process_flag(trap_exit, true),
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, Pid} = libp2p_swarm:start(?SERVER, Args),
    true = erlang:link(Pid),
    {ok, #state{swarm=Pid}}.

handle_call(pubkey_bin, _From, #state{swarm=Swarm}=State) ->
    {reply, libp2p_swarm:pubkey_bin(Swarm), State};
handle_call(swarm, _From, #state{swarm=Swarm}=State) ->
    {reply, Swarm, State};
handle_call(key, _From, #state{swarm=Swarm}=State)  ->
    {reply, libp2p_swarm:keys(Swarm), State};
handle_call(gossip_peers, _From, #state{swarm=Swarm}=State) ->
    {reply, libp2p_group_gossip:connected_addrs(libp2p_swarm:gossip_group(Swarm), all), State};
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
