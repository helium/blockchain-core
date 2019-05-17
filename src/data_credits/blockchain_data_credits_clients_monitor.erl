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
    payment_req/2,
    channel_client/1
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
    monitored = #{} :: #{pid() | libp2p_crypto:pubkey_bin() => libp2p_crypto:pubkey_bin() | pid()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

payment_req(Payer, Amount) ->
    gen_statem:cast(?SERVER, {payment_req, Payer, Amount}).

channel_client(Payer) ->
    gen_statem:call(?SERVER, {channel_client, Payer}).

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

handle_call({channel_client, Payer}, _From, #state{monitored=Monitored}=State) ->
    lager:info("got channel_client request for ~p from ~p", [Payer, _From]),
    case maps:get(Payer, Monitored, undefined) of
        undefined ->
            lager:warning("could not find pid for ~p", [Payer]),
            {reply, {error, not_found}, State};
        Pid ->
            {reply, {ok, Pid}, State}
    end;
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({payment_req, Payer, Amount}, #state{db=DB, monitored=Monitored0}=State) ->
    case maps:get(Payer, Monitored0, undefined) of
        undefined ->
            CFName = erlang:binary_to_list(Payer),
            {ok, CF} = rocksdb:create_column_family(DB, CFName, []),
            {ok, Pid} = blockchain_data_credits_channel_client:start([DB, CF, Payer, Amount]),
            _Ref = erlang:monitor(process, Pid),
            Monitored1 = maps:put(Pid, Payer, maps:put(Payer, Pid, Monitored0)),
            {noreply, State#state{monitored=Monitored1}};
        Pid ->
            ok = blockchain_data_credits_channel_client:payment_req(Pid, Amount),
            {noreply, State}
    end;
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