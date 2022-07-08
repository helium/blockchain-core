%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Packet Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_packet_handler).

-behavior(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2,
    dial/3,
    close/1
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

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

dial(SwarmTID, Peer, Opts) ->
    libp2p_swarm:dial_framed_stream(
        SwarmTID,
        Peer,
        ?STATE_CHANNEL_PROTOCOL_V1,
        ?MODULE,
        Opts
    ).

close(HandlerPid) ->
    _ = libp2p_framed_stream:close(HandlerPid).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _) ->
    lager:info("starting new client", []),
    {ok, blockchain_state_channel_common:new_handler_state()};
init(server, _Conn, _) ->
    lager:info("starting new server", []),
    HandlerMod = application:get_env(blockchain, sc_packet_handler, undefined),
    OfferLimit = application:get_env(blockchain, sc_pending_offer_limit, 5),
    HandlerState = blockchain_state_channel_common:new_handler_state(
        #{}, [], HandlerMod, OfferLimit, true
    ),
    {ok, HandlerState}.

-spec handle_data(
    Kind :: libp2p_framed_stream:kind(),
    Data :: any(),
    HandlerState :: any()
) -> libp2p_framed_stream:handle_data_result().
handle_data(client, Data, HandlerState) ->
    Decoded = blockchain_state_channel_message_v1:decode(Data),
    lager:debug("sc_handler client got response: ~p", [Decoded]),
    case Decoded of
        {response, Resp} ->
            blockchain_packet_client:response(Resp);
        _Other ->
            %% TODO: handle gracefully?
            throw({client_only_accepting_responses, _Other})
    end,
    {noreply, HandlerState};
handle_data(server, Data, HandlerState) ->
    Decoded = blockchain_state_channel_message_v1:decode(Data),
    lager:info("server handling data of type : ~p", [element(1, Decoded)]),

    PacketTime = erlang:system_time(millisecond),

    case Decoded of
        {packet, SCPacket} ->
            SCPacketHandler = blockchain_state_channel_common:handler_mod(HandlerState),
            SCPacketHandler:handle_packet(SCPacket, PacketTime, self());
        _Other ->
            %% TODO: handle gracefully?
            throw({server_only_accepting_packets, _Other})
    end,
    {noreply, HandlerState}.

handle_info(client, {send_packet, Packet}, HandlerState) ->
    lager:debug("sc_handler client sending packet: ~p", [Packet]),
    Data = blockchain_state_channel_message_v1:encode(Packet),
    {noreply, HandlerState, Data};
handle_info(server, {send_response, Resp}, HandlerState) ->
    lager:debug("sc_handler server sending resp: ~p", [Resp]),
    Data = blockchain_state_channel_message_v1:encode(Resp),
    {noreply, HandlerState, Data};
handle_info(_Type, _Msg, HandlerState) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, HandlerState}.
