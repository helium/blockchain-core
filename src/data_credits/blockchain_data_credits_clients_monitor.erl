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
-include("pb/blockchain_data_credits_handler_pb.hrl").

-define(SERVER, ?MODULE).
-define(DB_FILE, "data_credits.db").

-record(state, {
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    cf :: rocksdb:cf_handle(),
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
init([DB, DefaultCF, ClientCF]=Args) ->
    erlang:process_flag(trap_exit, true),
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, #state{
        db=DB,
        default=DefaultCF,
        cf=ClientCF
    }}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({payment_req, Payer, Amount}, #state{db=DB, cf=CF, monitored=Pids}=State) ->
    {ok, Pid} = blockchain_data_credits_channel_server:start([DB, CF, Payer, Amount]),
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