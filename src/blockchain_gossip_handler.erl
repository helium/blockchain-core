%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Gossip Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_gossip_handler).

-behaviour(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4
    ,client/2
    ,send/2, send/3
]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/3
    ,handle_data/3
    ,handle_call/4
]).

-record(state, {
    path :: binary() | undefined
    ,parent :: pid() | undefined
    ,connection :: libp2p_connection:connection()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
send(Pid, Bin) ->
    send(Pid, Bin, 1000).

send(Pid, Bin, Timeout) ->
    gen_server:call(Pid, {send, Bin, Timeout}).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(server, Connection, [Path, Parent]) ->
    Height = blockchain_worker:height(),
    Head = blockchain_worker:head_hash(),
    {ok, #state{connection=Connection, path=Path, parent=Parent}, erlang:term_to_binary({height, Height, Head})};
init(client, Connection, []) ->
    {ok, #state{connection=Connection}}.

handle_data(client, Data, #state{connection=Conn}=State) ->
    try erlang:binary_to_term(Data) of
        {height, Height, Head} ->
            {ok, Session} = libp2p_connection:session(Conn),
            ok = blockchain_worker:peer_height(Height, Head, Session);
        Other ->
            lager:info("unhandled message ~p", [Other])
    catch
        What:Why ->
            lager:info("unhandled message error ~p:~p", [What, Why])
    end,
    {noreply, State};
handle_data(server, Data, #state{connection=Conn}=State) ->
    case erlang:binary_to_term(Data) of
        {block, Block} ->
            {ok, Session} = libp2p_connection:session(Conn),
            blockchain_worker:add_block(Block, Session);
        _ ->
            lager:notice("gossip handler got unknown data")
    end,
    {noreply, State}.

handle_call(_, {send, Bin, Timeout}, _From, #state{connection=Conn}=State) ->
    Result = libp2p_connection:send(Conn, Bin, Timeout),
    {reply, Result, State}.
