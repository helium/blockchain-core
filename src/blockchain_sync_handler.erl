%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Sybc Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_sync_handler).

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

-record(state, {
    parent :: pid()
    ,multiaddr :: undefined | string()
}).

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
init(client, Conn, [Parent]) ->
    {_, MultiAddr} = libp2p_connection:addr_info(Conn),
    {ok, #state{parent=Parent, multiaddr=MultiAddr}};
init(server, _Conn, [Path, _Parent]) ->
    [Height, Hash] = string:tokens(Path, "/"),
    lager:info("sync_handler server accepted connection"),
    lager:info("syncing blocks with peer at height ~p and hash ~p", [Height, Hash]),
    ToSend =
        case blockchain_worker:blocks(Hash) of
            {ok, Blocks} -> {sync, Blocks};
            {error, Reason} -> {error, Reason};
            ok -> {error, no_blockchain}
        end,
    {stop, normal, term_to_binary(ToSend)}.

handle_data(client, Data, State=#state{parent=_Parent}) ->
    blockchain_worker:sync_blocks(erlang:binary_to_term(Data)),
    {stop, normal, State};
handle_data(server, _Data, State) ->
    {stop, normal, State}.
