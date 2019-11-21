%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_client).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    credits/1,
    packet/1,
    state_channel/1
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
-include_lib("helium_proto/src/pb/helium_longfi_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    swarm :: pid(),
    state_channels = #{} :: #{libp2p_crypto:pubkey_bin() => blockchain_state_channel_v1:state_channel()},
    pending = #{} :: pending()
}).

-type pending() :: #{blockchain_state_channel_payment_req_v1:id() => {blockchain_state_channel_payment_req_v1:payment_req(), any()}}.

-define(STATE_CHANNELS, <<"blockchain_state_channels_client.STATE_CHANNELS">>).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
credits(ID) ->
    gen_server:call(?SERVER, {credits, ID}).

packet(Packet) ->
    gen_server:cast(?SERVER, {packet, Packet}).

state_channel(SC) ->
    gen_server:cast(?SERVER, {state_channel, SC}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Swarm]=_Args) ->
    lager:info("~p init with ~p", [?SERVER, _Args]),
    {ok, DB} = blockchain_state_channel_db:get(),
    {ok, #state{db=DB, swarm=Swarm}}.

handle_call({credits, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel_v1:credits(SC)}
    end,
    {reply, Reply, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({packet, #helium_LongFiRxPacket_pb{oui=OUI,
                                               fingerprint=Fingerprint}=Packet},
            #state{swarm=Swarm, pending=Pending}=State) ->
    lager:debug("got packet ~p", [Packet]),
    case find_routing(OUI) of
        {error, _Reason} ->
             lager:warning("failed to find router for oui ~p:~p", [OUI, _Reason]),
             {noreply, State};
        {ok, Peer} ->
            case blockchain_state_channel_handler:dial(Swarm, Peer, []) of
                {error, _Reason} ->
                    lager:warning("failed to dial ~p:~p", [Peer, _Reason]),
                    {noreply, State};
                {ok, Pid} ->
                    {PubKeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                    % TODO: Get amount from somewhere?
                    ReqID = crypto:strong_rand_bytes(32),
                    Req = blockchain_state_channel_payment_req_v1:new(ReqID, PubKeyBin, 1, Fingerprint),
                    SignedReq = blockchain_state_channel_payment_req_v1:sign(Req, SigFun),
                    lager:info("sending payment req ~p to ~p", [Req, Peer]),
                    blockchain_state_channel_handler:send_payment_req(Pid, SignedReq),
                    {noreply, State#state{pending=maps:put(ReqID, {SignedReq, Packet}, Pending)}}
            end
    end;
handle_cast({state_channel, SC}, #state{db=DB, state_channels=SCs, pending=Pending}=State) ->
    lager:debug("received state channel update for ~p", [blockchain_state_channel_v1:id(SC)]),
    case check_pending_requests(SC, Pending) of
        {error, _Reason} ->
            lager:warning("state channel ~p is invalid ~p", [SC, _Reason]),
            {noreply, State};
        {ok, Found} ->
            % TODO: Send found packets
            ok = blockchain_state_channel_v1:save(DB, SC),
            ID = blockchain_state_channel_v1:id(SC),
            {noreply, State#state{state_channels=maps:put(ID, SC, SCs),
                                  pending=maps:without(Found, Pending)}}
    end;
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason,  #state{db=DB}) ->
    ok = rocksdb:close(DB).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec check_pending_requests(blockchain_state_channel_v1:state_channel(), pending()) ->
    {ok, [blockchain_state_channel_payment_req_v1:id()]} | {error, any()}.
check_pending_requests(SC, Pending) ->
    case blockchain_state_channel_v1:validate(SC) of
        {error, _}=Error -> 
            Error;
        true ->
            Payments = blockchain_state_channel_v1:payments(SC),
            Found = lists:filter(
                fun(ReqID) ->
                    case proplists:get_value(ReqID, Payments, undefined) of
                        undefined -> false;
                        Payment ->
                            {Req, _} = maps:get(ReqID, Pending),
                            check_request(Req, Payment)
                    end
                end,
                maps:keys(Pending)
            ),
            {ok, Found}
    end.

-spec check_request(blockchain_state_channel_payment_req_v1:payment_req(),
                    blockchain_state_channel_payment_v1:payment()) -> boolean().
check_request(Req, Payment) ->
    ReqAmount = blockchain_state_channel_payment_req_v1:amount(Req),
    ReqPayee = blockchain_state_channel_payment_req_v1:payee(Req),
    PaymentAmount = blockchain_state_channel_payment_v1:amount(Payment),
    PaymentPayee = blockchain_state_channel_payment_v1:payee(Payment),
    ReqAmount == PaymentAmount andalso ReqPayee == PaymentPayee.


-spec find_routing(non_neg_integer()) -> {ok, string()} | {error, any()}.
find_routing(OUI) ->
    Chain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Routing} ->
            [Address|_] = blockchain_ledger_routing_v1:addresses(Routing),
            {ok, erlang:binary_to_list(Address)}
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-endif.