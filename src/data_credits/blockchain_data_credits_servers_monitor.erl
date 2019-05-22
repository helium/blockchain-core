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
    payment_req/1
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
-include("../pb/blockchain_data_credits_pb.hrl").

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

channel_server(PubKeyBin) ->
    gen_statem:call(?SERVER, {channel_server, PubKeyBin}).

channel_server(Keys, Amount) ->
    gen_statem:cast(?SERVER, {channel_server, Keys, Amount}).

payment_req(PaymentReq) ->
    gen_statem:cast(?SERVER, {payment_req, PaymentReq}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    erlang:process_flag(trap_exit, true),
    lager:info("~p init with ~p", [?SERVER, _Args]),
    {ok, DB} = blockchain_data_credits_db:get_db(),
    Swarm = blockchain_swarm:swarm(),
    ok = libp2p_swarm:add_stream_handler(
        Swarm,
        ?DATA_CREDITS_PAYMENT_PROTOCOL,
        {libp2p_framed_stream, server, [blockchain_data_credits_payment_stream]}
    ),
    {ok, CFs} = blockchain_data_credits_db:get_cfs(),
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
    {ok, CF} = blockchain_data_credits_db:get_cf(add_prefix(PubKeyBin)),
    Monitored1 = start_channel_server(DB, CF, Keys, Amount, PubKeyBin, Monitored0),
    ok = store_keys(DB, Keys),
    {noreply, State#state{monitored=Monitored1}};
handle_cast({payment_req, PaymentReq}, #state{monitored=Monitored}=State) ->
    ID = PaymentReq#blockchain_data_credits_payment_req_pb.id,
    Pids = lists:filter(fun erlang:is_pid/1, maps:keys(Monitored)),
    ShuffledPids = blockchain_utils:shuffle_from_hash(ID, Pids),
    lager:info("got payment request ~p", [PaymentReq]),
    ok = try_payment_req(ShuffledPids, PaymentReq),
    {noreply, State};
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({'EXIT', Pid, normal}, #state{monitored=Monitored0}=State) ->
    case maps:get(Pid, Monitored0, undefined) of
        undefined ->
            {noreply, State};
        PubKeyBin ->
            ok = blockchain_data_credits_db:destroy_cf(PubKeyBin),
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
                    {ok, CF} = blockchain_data_credits_db:get_cf(add_prefix(PubKeyBin)),
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
store_keys(DB, #{public := PubKey}=Keys) ->
    KeysBin = libp2p_crypto:keys_to_bin(Keys),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    ok = rocksdb:put(DB, PubKeyBin, KeysBin, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_keys(DB, PubKeyBin) ->
   case rocksdb:get(DB, PubKeyBin, [{sync, true}]) of
        {ok, KeysBin} ->
            Keys = libp2p_crypto:keys_from_bin(KeysBin),
            {ok, Keys};
        _Error ->
            _Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
start_channel_server(DB, CF, Keys, Amount, PubKeyBin, Monitored) ->
    {ok, Pid} = blockchain_data_credits_channel_server:start_link([DB, CF, Keys, Amount]),
    maps:put(Pid, PubKeyBin, maps:put(PubKeyBin, Pid, Monitored)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
try_payment_req([], _Payee)->
    ok;
try_payment_req([Pid|ShuffledPids], PaymentReq) ->
    Amount = PaymentReq#blockchain_data_credits_payment_req_pb.amount,
    case blockchain_data_credits_channel_server:credits(Pid) of
        {ok, Credits} when Credits >= Amount ->
            lager:info("transfering payment request to ~p", [Pid]),
            blockchain_data_credits_channel_server:payment_req(Pid, PaymentReq);
        _ ->
            try_payment_req(ShuffledPids, PaymentReq)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
add_prefix(PubKeyBin) ->
    <<"S_", PubKeyBin/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
remove_prefix(CFName) when is_list(CFName) ->
    remove_prefix(erlang:list_to_binary(CFName));
remove_prefix(<<"S_", PubKeyBin/binary>>) ->
    PubKeyBin.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
is_prefixed(CFName) when is_list(CFName) -> 
    is_prefixed(erlang:list_to_binary(CFName));
is_prefixed(<<"S_", _/binary>>) -> 
    true;
is_prefixed(_) -> 
    false.