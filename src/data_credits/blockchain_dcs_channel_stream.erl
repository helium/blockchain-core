%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Stream ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_channel_stream).

-behavior(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2,
    send_update/2
]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/3,
    handle_info/3,
    handle_data/3
]).

-record(state, {}).

client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, _Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, Args).

send_update(Pid, Payment) ->
    Pid ! {update, Payment}.

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _Args) ->
    {ok, #state{}};
init(server, _Conn, _Args) ->
    {ok, #state{}}.

handle_data(server, Data, State) ->	
    Payment = blockchain_dcs_payment:decode(Data),	
    lager:debug("got payment update ~p", [Payment]),
    _Payer = blockchain_dcs_payment:payer(Payment),
    % case blockchain_data_credits_clients_monitor:channel_client(Payer) of
    %     {error, Reason} ->
    %         lager:error("got unkown payment update ~p", [Payment]),
    %         {stop, Reason, State};
    %     {ok, Pid} ->
    %         lager:debug("transfering to ~p", [Pid]),
    %         Pid ! {update, Payment},
    %         {noreply, State}
    % end;
    {noreply, State};
handle_data(_Type, _Data, State) ->
    lager:warning("unknown ~p data message ~p", [_Type, _Data]),
    {noreply, State}.

handle_info(client, {update, EncodedPayment}, State) ->
    lager:debug("sending payment update ~p", [EncodedPayment]),
    {noreply, State, EncodedPayment};
handle_info(_Type, stop, State) ->
    {stop, normal, State};
handle_info(_Type, _Msg, State) ->
    lager:warning("unknown ~p info message ~p", [_Type, _Msg]),
    {noreply, State}.
