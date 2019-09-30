%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_channel_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    credits/1
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
-define(CLIENTS, <<"channel_clients">>).

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
start_link(Args) ->
    gen_server:start_link(?SERVER, Args, []).

-spec credits(pid()) -> {ok, non_neg_integer()}.
credits(Pid) ->
    gen_statem:call(Pid, credits).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB, CF, Keys, 0]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, Height} = blockchain_dcs_utils:get_height(DB, CF),
    {ok, Credits} = blockchain_dcs_utils:get_credits(DB, CF),
    {ok, Clients} = channel_clients(DB, CF),
    {ok, #state{
        db=DB,
        cf=CF,
        keys=Keys,
        credits=Credits,
        height=Height,
        channel_clients=Clients
    }};
init([DB, CF, Keys, Credits]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Payment = blockchain_dcs_payment:new(
        <<>>,
        Keys,
        0,
        blockchain_swarm:pubkey_bin(),
        blockchain_swarm:pubkey_bin(),
        Credits
    ),
    ok = blockchain_dcs_payment:store(DB, CF, Payment),
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
                                              channel_clients=Clients0}=State0) ->
    % TODO: Check that we can pay for it
    Height1 = Height0+1,
    ID = blockchain_dcs_payment_req:id(PaymentReq),
    Payee = blockchain_dcs_payment_req:payee(PaymentReq),
    Amount = blockchain_dcs_payment_req:amount(PaymentReq),
    Payment = blockchain_dcs_payment:new(
        ID,
        Keys,
        Height1,
        blockchain_swarm:pubkey_bin(),
        Payee,
        Amount
    ),
    ok = blockchain_dcs_payment:store(DB, CF, Payment),
    lager:info("got payment request ~p (leftover: ~p)", [PaymentReq, Credits-Amount]),
    EncodedPayment = blockchain_dcs_payment:encode(Payment),
    State1 = 
        case maps:is_key(Payee, Clients0) of
            true ->
                ok = broacast_payment(maps:keys(Clients0), EncodedPayment),
                State0#state{credits=Credits-Amount, height=Height1};
            false ->
                ok = update_client(DB, CF, Payee, Height1),
                ok = broacast_payment(maps:keys(Clients0), EncodedPayment),
                Clients1 = maps:put(Payee, <<>>, Clients0),
                ok = channel_clients(DB, CF, Payee),
                State0#state{credits=Credits-Amount, height=Height1, channel_clients=Clients1}
        end,
    case Credits-Amount == 0 of
        false ->
            {noreply, State1};
        true ->
            % TODO: Should settle right her
            {stop, normal, State1}
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
-spec channel_clients(rocksdb:db_handle(), rocksdb:cf_handle()) -> {ok, map()} | {error, any()}.
channel_clients(DB, CF) ->
    case rocksdb:get(DB, CF, ?CLIENTS, [{sync, true}]) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {ok, #{}};
        _Error ->
            lager:error("failed to get ~p: ~p", [?CLIENTS, _Error]),
            _Error
    end.

-spec channel_clients(rocksdb:db_handle(), rocksdb:cf_handle(),
                      libp2p_crypto:pubkey_bin()) -> ok | {error, any()}.
channel_clients(DB, CF, PubKeyBin) ->
    case channel_clients(DB, CF) of
        {ok, Map0} ->
            Map1 = maps:put(PubKeyBin, <<>>, Map0),
            rocksdb:put(DB, CF, ?CLIENTS, erlang:term_to_binary(Map1), []);
        _Error ->
            _Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec broacast_payment([libp2p_crypto:pubkey_bin()], binary()) -> ok.
broacast_payment([], _EncodedPayment) ->
    ok;
broacast_payment([PubKeyBin|Clients], EncodedPayment) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddr,
                                         ?DATA_CREDITS_CHANNEL_PROTOCOL,
                                         blockchain_dcs_channel_stream,
                                         [])
    of
        {ok, Stream} ->
            lager:info("broadcasting payment update to ~p", [PubKeyBin]),
            blockchain_dcs_channel_stream:send_update(Stream, EncodedPayment),
            _ = erlang:send_after(2000, Stream, stop),
            broacast_payment(Clients, EncodedPayment);
        _Error ->
            broacast_payment(Clients, EncodedPayment)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec update_client(rocksdb:db_handle(), rocksdb:cf_handle(),
                    libp2p_crypto:pubkey_bin(), non_neg_integer()) -> ok.
update_client(DB, CF, PubKeyBin, Height) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddr,
                                         ?DATA_CREDITS_CHANNEL_PROTOCOL,
                                         blockchain_dcs_channel_stream,
                                         [])
    of
        {ok, Stream} ->
            lists:foreach(
                fun(EncodedPayment) ->
                    lager:info("sending payment update to client: ~p", [PubKeyBin]),
                    blockchain_dcs_channel_stream:send_update(Stream, EncodedPayment)
                end,
                blockchain_dcs_payment:get_all(DB, CF, Height)
            ),
            _ = erlang:send_after(2000, Stream, stop),
            ok;
        _Error ->
            lager:error("failed to dial ~p (~p): ~p", [P2PAddr, PubKeyBin, _Error])
    end.