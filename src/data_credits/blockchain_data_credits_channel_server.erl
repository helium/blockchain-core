%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_channel_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start/1,
    credits/1,
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
-include("../pb/blockchain_data_credits_pb.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    keys :: libp2p_crypto:key_map(),
    credits = 0 :: non_neg_integer(),
    height = 0 :: non_neg_integer(),
    channel_clients = #{} :: #{libp2p_crypto:pubkey_bin() => any()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start(Args) ->
    gen_server:start(?SERVER, Args, []).

credits(Pid) ->
    gen_statem:call(Pid, credits).

payment_req(Pid, PaymentReq) ->
    gen_statem:cast(Pid, {payment_req, PaymentReq}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB, CF, Keys, Credits]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Payment = blockchain_data_credits_utils:new_payment(
        <<>>,
        Keys,
        0,
        blockchain_swarm:pubkey_bin(),
        blockchain_swarm:pubkey_bin(),
        Credits
    ),
    ok = blockchain_data_credits_utils:store_payment(DB, CF, Payment),
    {ok, #state{
        db=DB,
        cf=CF,
        keys=Keys,
        credits=Credits
    }}.

handle_call(credits, _From, #state{credits=Credits}=State) ->
    {reply, {ok, Credits}, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({payment_req, PaymentReq}, #state{db=DB, cf=CF, keys=Keys,
                                                 credits=Credits, height=Height0,
                                                 channel_clients=Clients0}=State) ->
    Height1 = Height0+1,
    ID = PaymentReq#blockchain_data_credits_payment_req_pb.id,
    Payee = PaymentReq#blockchain_data_credits_payment_req_pb.payee,
    Amount = PaymentReq#blockchain_data_credits_payment_req_pb.amount,
    Payment = blockchain_data_credits_utils:new_payment(
        ID,
        Keys,
        Height1,
        blockchain_swarm:pubkey_bin(),
        Payee,
        Amount
    ),
    ok = blockchain_data_credits_utils:store_payment(DB, CF, Payment),
    lager:info("got payment request ~p (leftover: ~p)", [Payee, Amount, Credits-Amount]),
    EncodedPayment = blockchain_data_credits_utils:encode_payment(Payment),
    case maps:is_key(Payee, Clients0) of
        true ->
            ok = broacast_payment(maps:keys(Clients0), EncodedPayment),
            {noreply, State#state{credits=Credits-Amount, height=Height1}};
        false ->
            ok = update_client(DB, CF, Payee, Height1),
            ok = broacast_payment(maps:keys(Clients0), EncodedPayment),
            Clients1 = maps:put(Payee, <<>>, Clients0),
            {noreply, State#state{credits=Credits-Amount, height=Height1, channel_clients=Clients1}}
    end;
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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
broacast_payment([], _EncodedPayment) ->
    ok;
broacast_payment([PubKeyBin|Clients], EncodedPayment) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddr,
                                         ?DATA_CREDITS_CHANNEL_PROTOCOL,
                                         blockchain_data_credits_channel_stream,
                                         [])
    of
        {ok, Stream} ->
            lager:info("broadcasting payment update to ~p", [PubKeyBin]),
            blockchain_data_credits_channel_stream:send_update(Stream, EncodedPayment),
            _ = erlang:send_after(2000, Stream, stop),
            broacast_payment(Clients, EncodedPayment);
        _Error ->
            broacast_payment(Clients, EncodedPayment)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
update_client(DB, CF, PubKeyBin, Height) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddr,
                                         ?DATA_CREDITS_CHANNEL_PROTOCOL,
                                         blockchain_data_credits_channel_stream,
                                         [])
    of
        {ok, Stream} ->
            lists:foreach(
                fun(EncodedPayment) ->
                    lager:info("sending payment update to client: ~p", [PubKeyBin]),
                    blockchain_data_credits_channel_stream:send_update(Stream, EncodedPayment)
                end,
                get_all_payments(DB, CF, Height)
            ),
            _ = erlang:send_after(2000, Stream, stop),
            ok;
        _Error ->
            lager:error("failed to dial ~p (~p): ~p", [P2PAddr, PubKeyBin, _Error])
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_all_payments(DB, CF, Height) ->
    get_all_payments(DB, CF, Height, 0, []).

get_all_payments(_DB, _CF, Height, I, Payments) when Height < I ->
    lists:reverse(Payments);
get_all_payments(DB, CF, Height, I, Payments) ->
    case rocksdb:get(DB, CF, <<I>>, [{sync, true}]) of
        {ok, Payment} ->
            get_all_payments(DB, CF, Height, I+1, [Payment|Payments]);
        _Error ->
            lager:error("failed to get ~p: ~p", [<<Height>>, _Error]),
            get_all_payments(DB, CF, Height, I+1, Payments)
    end.
    