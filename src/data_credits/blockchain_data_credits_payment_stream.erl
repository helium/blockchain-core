%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Payment Stream ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_payment_stream).

-behavior(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2
]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/3,
    handle_info/3,
    handle_data/3
]).

-include("pb/blockchain_data_credits_pb.hrl").

-record(state, {}).

client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, _Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, Args).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _Args) ->
    {ok, #state{}};
init(server, _Conn, _Args) ->
    {ok, #state{}}.

handle_data(server, Data, State) ->	
    Msg = blockchain_data_credits_pb:decode_msg(Data, blockchain_data_credits_payment_req_pb),	
    lager:info("got payment request ~p", [Msg]),
    Payee = Msg#blockchain_data_credits_payment_req_pb.payee,	
    Amount = Msg#blockchain_data_credits_payment_req_pb.amount,	
    blockchain_data_credits_servers_monitor:payment_req(Payee, Amount),	
    {stop, normal, State};
handle_data(_Type, _Data, State) ->
    lager:warning("unknown ~p data message ~p", [_Type, _Data]),
    {noreply, State}.

handle_info(client, {payment_req, Amount}, State) ->	
    PubKeyBin = blockchain_swarm:pubkey_bin(),	
    Msg = #blockchain_data_credits_payment_req_pb{	
        payee=PubKeyBin,	
        amount=Amount	
    },	
    Encoded = blockchain_data_credits_pb:encode_msg(Msg),
    lager:info("sending payment request ~p", [Msg]),
    {stop, normal, State, Encoded};
handle_info(_Type, _Msg, State) ->
    lager:warning("unknown ~p info message ~p", [_Type, _Msg]),
    {noreply, State}.

