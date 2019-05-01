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
    payment_req/3
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
-include("pb/blockchain_data_credits_pb.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    keys :: libp2p_crypto:key_map(),
    credits = 0 :: non_neg_integer(),
    channel_clients = #{} :: #{libp2p_crypto:pubkey_bin() => any()}
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start(Args) ->
    gen_server:start(?SERVER, Args, []).

credits(Pid) ->
    gen_statem:call(Pid, credits).

payment_req(Pid, Payee, Amount) ->
    gen_statem:cast(Pid, {payment_req, Payee, Amount}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB, CF, Keys, Credits]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
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

handle_cast({payment_req, Payee, Amount}, #state{db=DB, cf=CF, keys=Keys,
                                                 credits=Credits, channel_clients=Clients0}=State) ->
    % TODO: Broadcast this
    {Signature, Payment} = create_payment(Keys, Payee, Amount),
    EncodedPayment = blockchain_data_credits_pb:encode_msg(Payment),
    ok = rocksdb:put(DB, CF, Signature, EncodedPayment, []),
    lager:info("got payment request from ~p for ~p (leftover: ~p)", [Payee, Amount, Credits-Amount]),
    Clients1 = maps:put(Payee, <<>>, Clients0),
    ok = broacast_payment(maps:keys(Clients1), EncodedPayment),
    {noreply, State#state{credits=Credits-Amount, channel_clients=Clients1}};
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

create_payment(#{secret := PrivKey, public := PubKey}, Payee, Amount) -> 
    Payment = #blockchain_data_credits_payment_pb{
        key=libp2p_crypto:pubkey_to_bin(PubKey),
        payer=blockchain_swarm:pubkey_bin(),
        payee=Payee,
        amount=Amount
    },
    EncodedPayment = blockchain_data_credits_pb:encode_msg(Payment),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Signature = SigFun(EncodedPayment),
    {Signature, Payment#blockchain_data_credits_payment_pb{signature=Signature}}.

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
        {ok, _Stream} ->
            _Stream ! {update, EncodedPayment},
            broacast_payment(Clients, EncodedPayment);
        _Error ->
            broacast_payment(Clients, EncodedPayment)
    end.