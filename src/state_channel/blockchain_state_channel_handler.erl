%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_handler).

-behavior(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2,
    dial/3,
    send_payment_req/2,
    broadcast/2
]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/3,
    handle_data/3,
    handle_info/3
]).

-include("blockchain.hrl").

-record(state, {}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

dial(Swarm, Peer, Opts) ->
    libp2p_swarm:dial_framed_stream(Swarm,
                                    Peer,
                                    ?STATE_CHANNEL_PROTOCOL,
                                    ?MODULE,
                                    Opts).

send_payment_req(Pid, Req) ->
    Pid ! {send_payment_req, Req}.

broadcast(Pid, SC) ->
    Pid ! {broadcast, SC}.

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _) ->
    {ok, #state{}};
init(server, _Conn, _) ->
    {ok, #state{}}.

handle_data(client, Data, State) ->
    case blockchain_state_channel_message:decode(Data) of
        {state_channel, SC} ->
           blockchain_state_channels_client:state_channel(SC);
        _ ->
            ingore
    end,
    {noreply, State};
handle_data(server, Data, State) ->
    case blockchain_state_channel_message:decode(Data) of
        {payment_req, Req} ->
            blockchain_state_channels_server:payment_req(Req);
        {state_channel, SC} ->
           blockchain_state_channels_client:state_channel(SC)
    end,
    {noreply, State}.

handle_info(client, {send_payment_req, Req}, State) ->
    Data = blockchain_state_channel_message:encode(Req),
    {noreply, State, Data};
handle_info(client, {broadcast, SC}, State) ->
    Data = blockchain_state_channel_message:encode(SC),
    {noreply, State, Data};
handle_info(server, {broadcast, SC}, State) ->
    Data = blockchain_state_channel_message:encode(SC),
    {noreply, State, Data};
handle_info(_Type, _Msg, State) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, State}.
