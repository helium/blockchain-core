%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Servers Monitor ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_servers_monitor).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    channel_server/1, channel_server/2,
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

-record(state, {
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    cf :: rocksdb:cf_handle(),
    swarm :: undefined | pid(),
    monitored = #{} :: #{pid() => libp2p_crypto:pubkey_bin()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

channel_server(PubKeyBin) ->
    gen_statem:call(?SERVER, {channel_server, PubKeyBin}).

channel_server(Keys, Amount) ->
    gen_statem:cast(?SERVER, {channel_server, Keys, Amount}).

payment_req(Payee, Amount) ->
    gen_statem:cast(?SERVER, {payment_req, Payee, Amount}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB, DefaultCF, ServerCF]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Swarm = blockchain_swarm:swarm(),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?DATA_CREDITS_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_data_credits_handler]}
    ),
    {ok, #state{
        db=DB,
        default=DefaultCF,
        cf=ServerCF,
        swarm=Swarm
    }}.

handle_call({channel_server, PubKeyBin}, _From, #state{monitored=Pids0}=State) ->
    % TODO: Maybe return error also here?
    [Pid] = maps:keys(maps:filter(
        fun(_K, V) -> V =:= PubKeyBin end,
        Pids0
    )),
    {reply, {ok, Pid}, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({channel_server, #{public := PubKey}=Keys, Amount}, #state{db=DB, default=DefaultCF,
                                                                       cf=ServerCF, monitored=Pids}=State) ->
    KeysBin = libp2p_crypto:keys_to_bin(Keys),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, Pid} = blockchain_data_credits_channel_server:start([DB, ServerCF, Keys, Amount]),
    _Ref = erlang:monitor(process, Pid),
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, DefaultCF, PubKeyBin, KeysBin),
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
    {noreply, State#state{monitored=maps:put(Pid, PubKeyBin, Pids)}};
handle_cast({payment_req, Payee, Amount}, #state{monitored=Pids}=State) ->
    ShuffledPids = blockchain_utils:shuffle_from_hash(Payee, maps:keys(Pids)),
    ok = try_payment_req(ShuffledPids, Payee, Amount),
    {noreply, State};
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec try_payment_req([pid()], libp2p_crypto:pubkey_bin(), non_neg_integer()) -> ok.
try_payment_req([], _Payee, _Amount)->
    ok;
try_payment_req([Pid|ShuffledPids], Payee, Amount) ->
    case blockchain_data_credits_channel_server:credits(Pid) of
        {ok, Credits} when Credits >= Amount ->
            blockchain_data_credits_channel_server:payment_req(Pid, Payee, Amount);
        _ ->
            try_payment_req(ShuffledPids, Payee, Amount)
    end.