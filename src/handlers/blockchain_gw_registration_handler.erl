%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain GW Registration Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_gw_registration_handler).

-behavior(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4
    ,client/2
]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/3
    ,handle_data/3
]).

-record(state, {}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, _Args) ->
    lager:info("started gw_registration_handler client"),
    {ok, #state{}};
init(server, _Conn, _Args) ->
    lager:info("started gw_registration_handler server"),
    {ok, #state{}}.

handle_data(client, Data, State) ->
    lager:info("client got data: ~p", [Data]),
    {stop, normal, State};
handle_data(server, Data, State) ->
    lager:info("server got data: ~p", [Data]),
    {stop, normal, State, term_to_binary(Data)}.
