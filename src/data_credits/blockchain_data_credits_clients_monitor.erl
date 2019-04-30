%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Clients Monitor ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_clients_monitor).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    payment_req/2
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
-define(DB_FILE, "data_credits.db").

-record(state, {
    db :: rocksdb:db_handle(),
    monitored = #{} :: #{pid() => libp2p_crypto:pubkey_bin()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

payment_req(Payer, Amount) ->
    gen_statem:cast(?SERVER, {payment_req, Payer, Amount}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = blockchain_swarm:swarm(),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?DATA_CREDITS_CHANNEL_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_data_credits_channel_stream]}
    ),
    {ok, #state{
        db=DB
    }}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({payment_req, Payer, Amount}, #state{db=DB, monitored=Pids}=State) ->
    CFName = erlang:binary_to_list(Payer),
    {ok, CF} = rocksdb:create_column_family(DB, CFName, []),
    {ok, Pid} = blockchain_data_credits_channel_client:start([DB, CF, Payer, Amount]),
    _Ref = erlang:monitor(process, Pid),
    {noreply, State#state{monitored=maps:put(Pid, {Payer, Amount}, Pids)}};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    % TODO
    {noreply, State};
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