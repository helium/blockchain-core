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
    send_packet/2,
    send_offer/2,
    send_purchase/2,
    send_response/2,
    send_banner/2
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
                                    ?STATE_CHANNEL_PROTOCOL_V1,
                                    ?MODULE,
                                    Opts).


-spec send_packet(pid(), blockchain_state_channel_packet_v1:packet()) -> ok.
send_packet(Pid, Packet) ->
    Pid ! {send_packet, Packet},
    ok.

-spec send_offer(pid(), blockchain_state_channel_packet_offer_v1:offer()) -> ok.
send_offer(Pid, Offer) ->
    lager:info("sending offer: ~p, pid: ~p", [Offer, Pid]),
    Pid ! {send_offer, Offer},
    ok.

-spec send_purchase(pid(), blockchain_state_channel_purchase_v1:purchase()) -> ok.
send_purchase(Pid, Purchase) ->
    lager:info("sending purchase: ~p, pid: ~p", [Purchase, Pid]),
    Pid ! {send_purchase, Purchase},
    ok.

-spec send_banner(pid(), blockchain_state_channel_banner_v1:banner()) -> ok.
send_banner(Pid, Banner) ->
    lager:info("sending banner: ~p, pid: ~p", [Banner, Pid]),
    Pid ! {send_banner, Banner},
    ok.

-spec send_response(pid(), blockchain_state_channel_response_v1:response()) -> ok.
send_response(Pid, Resp) ->
    Pid ! {send_response, Resp},
    ok.

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _) ->
    {ok, #state{}};
init(server, _Conn, _) ->
    case blockchain_state_channels_server:active_sc() of
        undefined ->
            %% Send empty banner
            SCBanner = blockchain_state_channel_banner_v1:new(),
            lager:info("sc_handler, empty banner: ~p", [SCBanner]),
            {ok, #state{}, blockchain_state_channel_message_v1:encode(SCBanner)};
        ActiveSC ->
            lager:info("sc_handler, active_sc: ~p", [ActiveSC]),
            SCBanner = blockchain_state_channel_banner_v1:new(ActiveSC),
            lager:info("sc_handler, banner: ~p", [SCBanner]),
            {ok, #state{}, blockchain_state_channel_message_v1:encode(SCBanner)}
    end.

handle_data(client, Data, State) ->
    case blockchain_state_channel_message_v1:decode(Data) of
        {banner, Banner} ->
            lager:info("sc_handler client got banner: ~p", [Banner]),
            blockchain_state_channels_client:banner(Banner, self());
        {purchase, Purchase} ->
            lager:info("sc_handler client got purchase: ~p", [Purchase]),
            blockchain_state_channels_client:purchase(Purchase, self());
        {response, Resp} ->
            lager:debug("sc_handler client got response: ~p", [Resp]),
            blockchain_state_channels_client:response(Resp)
    end,
    {noreply, State};
handle_data(server, Data, State) ->
    case blockchain_state_channel_message_v1:decode(Data) of
        {offer, Offer} ->
            lager:info("sc_handler server got offer: ~p", [Offer]),
            blockchain_state_channels_server:offer(Offer, self());
        {packet, Packet} ->
            lager:debug("sc_handler server got packet: ~p", [Packet]),
            blockchain_state_channels_server:packet(Packet, self())
    end,
    {noreply, State}.

handle_info(client, {send_offer, Offer}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Offer),
    {noreply, State, Data};
handle_info(client, {send_packet, Packet}, State) ->
    lager:debug("sc_handler client sending packet: ~p", [Packet]),
    Data = blockchain_state_channel_message_v1:encode(Packet),
    {noreply, State, Data};
handle_info(server, {send_banner, Banner}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Banner),
    {noreply, State, Data};
handle_info(server, {send_purchase, Purchase}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Purchase),
    {noreply, State, Data};
handle_info(server, {send_response, Resp}, State) ->
    lager:debug("sc_handler server sending resp: ~p", [Resp]),
    Data = blockchain_state_channel_message_v1:encode(Resp),
    {noreply, State, Data};
handle_info(_Type, _Msg, State) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, State}.
