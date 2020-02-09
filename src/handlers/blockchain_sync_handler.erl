%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Sybc Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_sync_handler).

-behavior(libp2p_framed_stream).


-include_lib("helium_proto/src/pb/blockchain_sync_handler_pb.hrl").

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
    blockchain :: blockchain:blochain(),
    block :: undefined | blockchain_block:block(),
    batch_size :: pos_integer(),
    batch_limit :: pos_integer(),
    batches_sent = 0 :: non_neg_integer()
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
    erlang:process_flag(trap_exit, true),
    lager:info("starting sync handler client",[]),
    BatchSize = application:get_env(blockchain, block_sync_batch_size, 5),
    BatchLimit = application:get_env(blockchain, block_sync_batch_limit, 40),
    {ok, #state{blockchain=Blockchain, batch_size=BatchSize, batch_limit=BatchLimit}};

init(server, _Conn, [_Path, _, Blockchain]) ->
    erlang:process_flag(trap_exit, true),
    lager:info("starting sync handler server",[]),
    BatchSize = application:get_env(blockchain, block_sync_batch_size, 5),
    BatchLimit = application:get_env(blockchain, block_sync_batch_limit, 40),
    {ok, #state{blockchain=Blockchain, batch_size=BatchSize, batch_limit=BatchLimit}}.

handle_data(client, Data, #state{blockchain=Chain}=State) ->
    #blockchain_sync_blocks_pb{blocks=BinBlocks} =
        blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_blocks_pb),
    Blocks = [blockchain_block:deserialize(B) || B <- BinBlocks],
    case blockchain:add_blocks(Blocks, Chain) of
        ok ->
            {noreply, State, blockchain_sync_handler_pb:encode_msg(#blockchain_sync_req_pb{msg={response, true}})};
        _Error ->
            %% TODO: maybe dial for sync again?
            {stop, normal, State, blockchain_sync_handler_pb:encode_msg(#blockchain_sync_req_pb{msg={response, false}})}
    end;
handle_data(server, Data, #state{blockchain=Blockchain, batch_size=BatchSize, batches_sent=Sent, batch_limit=Limit}=State) ->
    case blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_req_pb) of
        #blockchain_sync_req_pb{msg={hash, #blockchain_sync_hash_pb{hash=Hash}}} ->
            case blockchain:get_block(Hash, Blockchain) of
                {ok, StartingBlock} ->
                    case blockchain:build(StartingBlock, Blockchain, BatchSize) of
                        [] ->
                            {stop, normal, State};
                        Blocks ->
                            Msg = #blockchain_sync_blocks_pb{blocks=[blockchain_block:serialize(B) || B <- Blocks]},
                            {noreply, State#state{batches_sent=Sent+1, block=lists:last(Blocks)}, blockchain_sync_handler_pb:encode_msg(Msg)}
                    end;
                {error, _Reason} ->
                    {stop, normal, State}
            end;
        #blockchain_sync_req_pb{msg={response, true}} when Sent < Limit, State#state.block /= undefined ->
            StartingBlock = State#state.block,
            case blockchain:build(StartingBlock, Blockchain, BatchSize) of
                [] ->
                    {stop, normal, State};
                Blocks ->
                    Msg = #blockchain_sync_blocks_pb{blocks=[blockchain_block:serialize(B) || B <- Blocks]},
                    {noreply, State#state{batches_sent=Sent+1, block=lists:last(Blocks)}, blockchain_sync_handler_pb:encode_msg(Msg)}
            end;
        _ ->
            %% ack was false, block was undefined, limit was hit or the message was not understood
            {stop, normal, State}
    end.

handle_info(client, {hash, Hash}, State) ->
    Msg = #blockchain_sync_req_pb{msg={hash, #blockchain_sync_hash_pb{hash=Hash}}},
    {noreply, State, blockchain_sync_handler_pb:encode_msg(Msg)};
handle_info(_Type, _Msg, State) ->
    {noreply, State}.
