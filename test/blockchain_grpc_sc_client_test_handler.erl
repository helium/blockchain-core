%%
%% test specific grpc client handler for state channels
%% some additional work required before this can be used in non test scenarios
%%
-module(blockchain_grpc_sc_client_test_handler).

-include("../src/grpc/autogen/client/state_channel_client_pb.hrl").
-include("../src/grpc/autogen/server/state_channel_pb.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

%% ------------------------------------------------------------------
%% Stream Exports
%% ------------------------------------------------------------------
-export([
    init/0,
    handle_msg/2,
    handle_info/2
]).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    dial/3
]).

init()->
    Blockchain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Blockchain),
    HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], undefined, undefined, false),
    HandlerState.

dial(_SwarmTID, Peer, _Opts) ->
    try
        %% get the test specific grpc port for the peer
        %% ( which is going to be the libp2p port + 1000 )
        %% see blockchain_ct_utils for more info
        {ok, PeerGrpcPort} = p2p_port_to_grpc_port(Peer),
        lager:info("connecting over grpc to peer ~p on port ~p", [Peer, PeerGrpcPort]),
        {ok, Connection} = grpc_client:connect(tcp, "127.0.0.1", PeerGrpcPort),
        grpc_client_stream_test:new(
            Connection,
            'helium.state_channel',
            msg,
            state_channel_client_pb,
            [],
            ?MODULE
        )
     catch _Error:_Reason:_Stack ->
        lager:warning("*** failed to connect over grpc to peer ~p.  Reason ~p Stack ~p", [Peer, _Reason, _Stack]),
        {error, failed_to_dial_peer}
     end.

%% as this client is only used currently in tests, we are just ignoring headers
%% if the client is to be used in real world scenarios this will need revisited
handle_msg({headers, _Headers}, StreamState) ->
    lager:debug("*** grpc client ignoring headers ~p", [_Headers]),
    StreamState;
handle_msg(eof, StreamState) ->
    lager:debug("*** grpc client received eof", []),
    StreamState;
handle_msg({data, #blockchain_state_channel_message_v1_pb{msg = Msg}}, StreamState) ->
    lager:debug("grpc client received msg ~p", [Msg]),
    #{handler_state := HandlerState}  = StreamState,
    NewHandlerState = blockchain_state_channel_common:handle_client_msg(Msg, HandlerState),
    StreamState#{handler_state => NewHandlerState}.

handle_info({send_offer, Offer}, StreamState) ->
    lager:info("sending offer over grpc: ~p", [Offer]),
    Msg = blockchain_state_channel_message_v1:wrap_msg(Offer),
    NewStreamState = grpc_client_stream_test:send_msg(StreamState, Msg, false),
    NewStreamState;
handle_info({send_packet, Packet}, StreamState) ->
    lager:info("sending packet over grpc: ~p", [Packet]),
    Msg = blockchain_state_channel_message_v1:wrap_msg(Packet),
    NewStreamState = grpc_client_stream_test:send_msg(StreamState, Msg, false),
    NewStreamState;
handle_info(_Msg, StreamState) ->
    lager:warning("grpc client unhandled msg: ~p", [_Msg]),
    StreamState.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------
p2p_port_to_grpc_port(PeerAddr)->
    SwarmTID = blockchain_swarm:tid(),
    Peerbook = libp2p_swarm:peerbook(SwarmTID),
    {ok, _ConnAddr, {Transport, _TransportPid}} = libp2p_transport:for_addr(SwarmTID, PeerAddr),
    {ok, PeerPubKeyBin} = Transport:p2p_addr(PeerAddr),
    {ok, PeerInfo} = libp2p_peerbook:get(Peerbook, PeerPubKeyBin),
    ListenAddrs = libp2p_peer:listen_addrs(PeerInfo),
    [H | _ ] = libp2p_transport:sort_addrs(SwarmTID, ListenAddrs),
    [_, _, _IP,_, Port] = _Full = re:split(H, "/"),
    lager:info("*** peer p2p port ~p", [Port]),
    {ok, list_to_integer(binary_to_list(Port)) + 1000}.