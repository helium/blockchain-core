%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Loc Assertion Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_loc_assertion_handler).

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
    handle_data/3
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
init(client, _Conn, [Txn]) ->
    lager:info("started loc_assertion_handler client, txn: ~p", [Txn]),
    {stop, normal, blockchain_txn:serialize(Txn)};
init(server, _Conn, _Args) ->
    lager:info("started loc_assertion_handler server"),
    {ok, #state{}}.

handle_data(server, Data, State) ->
    Txn = blockchain_txn:deserialize(Data),
    lager:info("loc_assertion_handler server got txn: ~p", [Txn]),
    ok = blockchain_worker:notify({loc_assertion_request, Txn}),
    {stop, normal, State}.
