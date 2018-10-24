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
    ,handle_info/3
]).

-record(state, {
    dir :: undefined | file:filename_all()
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
init(client, _Conn, _Args) ->
    lager:info("started sync_handler client"),
    {ok, #state{}};
init(server, _Conn, [_Path, _, Dir]) ->
    lager:info("started sync_handler server"),
    {ok, #state{dir=Dir}}.

handle_data(client, Data, State) ->
    lager:info("client got data: ~p", [Data]),
    blockchain_worker:sync_blocks(erlang:binary_to_term(Data)),
    {stop, normal, State};
handle_data(server, Data, #state{dir=BaseDir}=State) ->
    lager:info("server got data: ~p", [Data]),
    {hash, Hash} = erlang:binary_to_term(Data),
    lager:info("syncing blocks with peer hash ~p", [Hash]),
    StartingBlock =
        case blockchain:get_block(Hash, BaseDir) of
            {ok, Block} -> Block;
            {error, _Reason} -> blockchain_worker:genesis_block()
        end,
    Blocks = blockchain:build(StartingBlock, BaseDir),
    {stop, normal, State, erlang:term_to_binary(Blocks)}.

handle_info(client, {hash, Hash}, State) ->
    {noreply, State, erlang:term_to_binary({hash, Hash})};
handle_info(_Type, _Msg, State) ->
    lager:info("rcvd unknown type: ~p unknown msg: ~p", [_Type, _Msg]),
    {noreply, State}.
