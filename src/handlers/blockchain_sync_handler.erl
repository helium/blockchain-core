%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Sybc Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_sync_handler).

-behavior(libp2p_framed_stream).


-include("pb/blockchain_sync_handler_pb.hrl").

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
    case blockchain_worker:sync_paused() of
        true ->
            {stop, sync_paused};
        false ->
            lager:debug("started sync_handler client"),
            {ok, #state{blockchain=Blockchain}}
    end;
init(server, _Conn, [_Path, _, Blockchain]) ->
    lager:debug("started sync_handler server"),
    {ok, #state{blockchain=Blockchain}}.

handle_data(client, Data, #state{blockchain=Chain}=State) ->
    lager:debug("client got data: ~p", [Data]),
    #blockchain_sync_blocks_pb{blocks=BinBlocks} =
        blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_blocks_pb),
    Blocks = [blockchain_block:deserialize(B) || B <- BinBlocks],
    case blockchain:add_blocks(Blocks, Chain) of
        ok ->
            ok;
        Error ->
            %% TODO: maybe dial for sync again?
            lager:error("Couldn't sync blocks, error: ~p", [Error])
    end,
    {stop, normal, State};
handle_data(server, Data, #state{blockchain=Blockchain}=State) ->
    lager:debug("server got data: ~p", [Data]),
    #blockchain_sync_hash_pb{hash=Hash} =
        blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_hash_pb),
    lager:debug("syncing blocks with peer hash ~p", [Hash]),
    StartingBlock =
        case blockchain:get_block(Hash, Blockchain) of
            {ok, Block} ->
                Block;
            {error, _Reason} ->
                {ok, B} = blockchain:genesis_block(Blockchain),
                B
        end,
    Blocks = blockchain:build(StartingBlock, Blockchain, 200),
    Msg = #blockchain_sync_blocks_pb{blocks=[blockchain_block:serialize(B) || B <- Blocks]},
    {stop, normal, State, blockchain_sync_handler_pb:encode_msg(Msg)
}.

handle_info(client, {hash, Hash}, State) ->
    Msg = #blockchain_sync_hash_pb{hash=Hash},
    {noreply, State, blockchain_sync_handler_pb:encode_msg(Msg)};
handle_info(_Type, _Msg, State) ->
    lager:debug("rcvd unknown type: ~p unknown msg: ~p", [_Type, _Msg]),
    {noreply, State}.
