-module(blockchain_grpc_sc_server_handler).

-behavior(helium_state_channel_bhvr).

-include("autogen/server/state_channel_pb.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

%% ------------------------------------------------------------------
%% helium_state_channel_bhvr Exports
%% ------------------------------------------------------------------
-export([
    msg/2,
    init/2,
    handle_info/2
]).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    close/1
]).

close(_HandlerPid)->
    %% TODO - implement close in grpc stream
    ok.

-spec init(atom(), grpcbox_stream:t()) -> grpcbox_stream:t().
init(_RPC, StreamState)->
    lager:debug("initiating grpc state channel server handler with state ~p", [StreamState]),
    HandlerMod = application:get_env(blockchain, sc_packet_handler, undefined),
    OfferLimit = application:get_env(blockchain, sc_pending_offer_limit, 5),
    Blockchain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Blockchain),
    HandlerState = blockchain_state_channel_common:new_handler_state(Blockchain, Ledger, #{}, [], HandlerMod,OfferLimit, false),
    %% TODO - libp2p SC handler sends a banner upon init - need to work out how to handle that under grpc conns
    grpcbox_stream:stream_handler_state(
        StreamState,
        HandlerState
    ).

-spec msg(blockchain_state_channel_v1:message(), grpcbox_stream:t()) -> grpcbox_stream:t().
msg(#blockchain_state_channel_message_v1_pb{msg = Msg}, StreamState) ->
    lager:debug("grpc msg called with  ~p and state ~p", [Msg, StreamState]),
    HandlerState = grpcbox_stream:stream_handler_state(StreamState),
    Chain =  blockchain_state_channel_common:chain(HandlerState),

    %% get our chain and only handle the request if the chain is up
    %% if chain not up we have no way to return routing data so just return a 14/503
    case is_chain_ready(Chain) of
        false ->
            {grpc_error,
                {grpcbox_stream:code_to_status(14), <<"temporarily unavavailable">>}};
        true ->
            case blockchain_state_channel_common:handle_server_msg(Msg, HandlerState) of
                {ok, NewHandlerState, ResponseData} ->
                    NewStreamState = grpcbox_stream:stream_handler_state(StreamState, NewHandlerState),
                    {continue, NewStreamState, ResponseData};
                {ok, NewHandlerState}->
                    NewStreamState = grpcbox_stream:stream_handler_state(StreamState, NewHandlerState),
                    {continue, NewStreamState};
                stop->
                    ok
            end
    end;
msg(_Other, _StreamState)->
    lager:warning("unhandled server msg ~p", [_Other]),
    ok.

-spec handle_info(any(), grpcbox_stream:t()) -> grpcbox_stream:t().
handle_info({send_banner, Banner}, StreamState) ->
    Msg = blockchain_state_channel_message_v1:wrap_msg(Banner),
    NewStreamState = grpcbox_stream:send(false, Msg, StreamState),
    NewStreamState;
handle_info({send_rejection, Rejection}, StreamState) ->
    Msg = blockchain_state_channel_message_v1:wrap_msg(Rejection),
    NewStreamState = grpcbox_stream:send(false, Msg, StreamState),
    NewStreamState;
handle_info({send_purchase, SignedPurchaseSC, Hotspot, PacketHash, Region}, StreamState) ->
    %% NOTE: We're constructing the purchase with the hotspot obtained from offer here
    PurchaseMsg = blockchain_state_channel_purchase_v1:new(SignedPurchaseSC, Hotspot, PacketHash, Region),
    Msg = blockchain_state_channel_message_v1:wrap_msg(PurchaseMsg),
    NewStreamState = grpcbox_stream:send(false, Msg, StreamState),
    NewStreamState;
handle_info({send_response, Resp}, StreamState) ->
    lager:debug("grpc sc handler server sending resp: ~p", [Resp]),
    Msg = blockchain_state_channel_message_v1:wrap_msg(Resp),
    NewStreamState = grpcbox_stream:send(false, Msg, StreamState),
    NewStreamState;
handle_info(_Msg, StreamState) ->
    lager:warning("got unhandled msg: ~p", [_Msg]),
    StreamState.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------
-spec is_chain_ready(undefined | blockchain:blockchain()) -> boolean().
is_chain_ready(undefined) ->
    false;
is_chain_ready(_Chain) ->
    true.
