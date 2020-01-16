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
    send_request/2,
    send_packet/2,
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

-spec send_request(pid(), blockchain_state_channel_request_v1:request()) -> ok.
send_request(Pid, Req) ->
    Pid ! {send_request, Req},
    ok.

-spec send_packet(pid(), blockchain_state_channel_packet_v1:packet()) -> ok.
send_packet(Pid, Packet) ->
    Pid ! {send_packet, Packet},
    ok.

-spec broadcast(pid(), blockchain_state_channel_update_v1:state_channel_update()) -> ok.
broadcast(Pid, SCUpdate) ->
    Pid ! {broadcast, SCUpdate},
    ok.

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _) ->
    {ok, #state{}};
init(server, _Conn, _) ->
    {ok, #state{}}.

handle_data(client, Data, State) ->
    case blockchain_state_channel_message_v1:decode(Data) of
        {state_channel_update, SCUpdate} ->
           blockchain_state_channels_client:state_channel_update(SCUpdate);
        _ ->
            ingore
    end,
    {noreply, State};
handle_data(server, Data, State) ->
    case blockchain_state_channel_message_v1:decode(Data) of
        {request, Req} ->
            blockchain_state_channels_server:request(Req);
        {packet, Packet} ->
            blockchain_state_channels_server:packet(Packet);
        {state_channel_update, SCUpdate} ->
           blockchain_state_channels_client:state_channel_update(SCUpdate)
    end,
    {noreply, State}.

handle_info(client, {send_request, Req}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Req),
    {noreply, State, Data};
handle_info(client, {send_packet, Packet}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Packet),
    {noreply, State, Data};
handle_info(client, {broadcast, SCUpdate}, State) ->
    Data = blockchain_state_channel_message_v1:encode(SCUpdate),
    {noreply, State, Data};
handle_info(server, {broadcast, SCUpdate}, State) ->
    Data = blockchain_state_channel_message_v1:encode(SCUpdate),
    {noreply, State, Data};
handle_info(_Type, _Msg, State) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, State}.
