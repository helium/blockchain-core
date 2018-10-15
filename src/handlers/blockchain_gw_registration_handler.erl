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
init(client, _Conn, [Txn, Token]) ->
    lager:info("started gw_registration_handler client, txn: ~p, token: ~p", [Txn, Token]),
    {stop, normal, term_to_binary([{txn, Txn}, {token, Token}])};
init(server, _Conn, _Args) ->
    lager:info("started gw_registration_handler server"),
    {ok, #state{}}.

handle_data(server, Data, State) ->
    [{txn, Txn}, {token, Token}] = binary_to_term(Data),
    lager:info("gw_registration_handler server got txn: ~p, token: ~p", [Txn, Token]),
    ok = blockchain_worker:notify({gw_registration_request, Txn, Token}),
    {stop, normal, State}.
