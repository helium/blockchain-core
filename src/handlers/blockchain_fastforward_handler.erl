%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain fastforward Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_fastforward_handler).

-behavior(libp2p_framed_stream).


-include_lib("helium_proto/include/blockchain_sync_handler_pb.hrl").

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
    handle_data/3,
    handle_info/3
]).

-record(state, {
    blockchain :: blockchain:blochain()
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
init(client, _Conn, [Blockchain]) ->
    lager:debug("started fastforward_handler client"),
    {ok, #state{blockchain=Blockchain}};
init(server, _Conn, [_Path, _, Blockchain]) ->
    lager:debug("started fastforward_handler server"),
    %% use the process registry as a poor man's singleton
    %% registering will fail if another fastforward is running
    try erlang:register(?MODULE, self()) of
        true ->
            %% die if we haven't started to add blocks in 15s
            %% in case we have a slow peer
            erlang:send_after(15000, self(), timeout),
            {ok, Hash} = blockchain:head_hash(Blockchain),
            Msg = #blockchain_sync_hash_pb{hash=Hash},
            {ok, #state{blockchain=Blockchain}, blockchain_sync_handler_pb:encode_msg(Msg)}
    catch _:_ ->
              {stop, normal}
    end.

handle_data(client, Data, #state{blockchain=Blockchain}=State) ->
    #blockchain_sync_hash_pb{hash=Hash} =
    blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_hash_pb),
    case blockchain:get_block(Hash, Blockchain) of
        {ok, Block} ->
            Blocks = blockchain:build(Block, Blockchain, 200),
            Msg = #blockchain_sync_blocks_pb{blocks=[blockchain_block:serialize(B) || B <- Blocks]},
            {stop, normal, State, blockchain_sync_handler_pb:encode_msg(Msg)};
        {error, _Reason} ->
            %% peer is ahead of us
            {stop, normal, State}
    end;
handle_data(server, Data, #state{blockchain=Blockchain}=State) ->
    #blockchain_sync_blocks_pb{blocks=BinBlocks} =
        blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_blocks_pb),
    Blocks = [blockchain_block:deserialize(B) || B <- BinBlocks],
    lager:info("adding blocks ~p", [[blockchain_block:height(B) || B <- Blocks]]),
    case blockchain:add_blocks(Blocks, Blockchain) of
        ok ->
            ok;
        Error ->
            %% TODO: maybe dial for sync again?
            lager:error("Couldn't sync blocks, error: ~p", [Error])
    end,
    {stop, normal, State}.

handle_info(server, timeout, State) ->
    {stop, normal, State};
handle_info(_Type, _Msg, State) ->
    lager:debug("rcvd unknown type: ~p unknown msg: ~p", [_Type, _Msg]),
    {noreply, State}.
