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
    packet/1
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
    state_channels = #{} :: #{libp2p_crypto:pubkey_bin() => blockchain_state_channel:state_channel()}
}).

% -type state() :: #state{}.
-define(STATE_CHANNELS, <<"blockchain_state_channels_client.STATE_CHANNELS">>).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

packet(Packet) ->
    gen_server:cast(?SERVER, {packet, Packet}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Swarm]=_Args) ->
    lager:info("~p init with ~p", [?SERVER, _Args]),
    {ok, DB} = blockchain_state_channel_db:get(),
    {ok, #state{db=DB, swarm=Swarm}}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({packet, #helium_LongFiRxPacket_pb{oui=OUI,
                                               fingerprint=Fingerprint}},
            #state{swarm=Swarm}=State) ->
    case find_routing(OUI) of
        {error, _Reason} ->
             lager:warning("failed to find router for oui ~p:~p", [OUI, _Reason]);
        {ok, Peer} ->
            case blockchain_state_channel_handler:dial(Swarm, Peer, []) of
                {error, _Reason} ->
                    lager:warning("failed to dial ~p:~p", [Peer, _Reason]);
                {ok, Pid} ->
                    {PubKeyBin, SigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                    % TODO: Get amount from somewhere?
                    Req = blockchain_state_channel_payment_req:new(PubKeyBin, 1, Fingerprint),
                    SignedReq = blockchain_state_channel_payment_req:sign(Req, SigFun),
                    blockchain_state_channel_handler:send_payment_req(Pid, SignedReq)
            end
    end,
    {noreply, State};
% handle_cast({payment, Payment}, #state{db=DB, state_channels=SCs}=State) ->
%     case validate_payment(Payment, State) of
%         {error, _Reason} ->
%             lager:warning("ignored unvalid payment ~p ~p", [_Reason, Payment]),
%             {noreply, State};
%         ok ->
%             Owner = blockchain_dcs_payment:payer(Payment),
%             case maps:get(Owner, SCs, undefined) of
%                 undefined ->
%                     lager:warning("ignored unknown payment ~p", [Payment]),
%                     {noreply, State};
%                 SC0 ->
%                     SC1 = blockchain_state_channel:add_payment(Payment, SC0),
%                     ok = blockchain_state_channel:save(DB, SC1),
%                     {noreply, State#state{state_channels=maps:put(Owner, SC1, SCs)}}
%             end
%     end;
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

% -spec validate_payment(blockchain_dcs_payment:dcs_payment(), state()) -> ok | {error, any()}.
% validate_payment(Payment, #state{state_channels=SCs}) ->
%     Owner = blockchain_dcs_payment:payer(Payment),
%     case maps:get(Owner, SCs, undefined) of
%         undefined -> {error, unknown_payment};
%         SC -> blockchain_state_channel:validate_payment(Payment, SC)
%     end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-endif.