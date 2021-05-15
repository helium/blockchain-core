%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Sybc Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_sync_handler).

-behavior(libp2p_framed_stream).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_sync_handler_pb.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2,
    dial/3,
    dial/4
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
    batches_sent = 0 :: non_neg_integer(),
    path :: string()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, _Path, _TID, Args) ->
    %% NOTE: server/4 in the handler is never called.
    %% When spawning a server its handled only in libp2p_framed_stream
    libp2p_framed_stream:server(?MODULE, Connection, [Args]).

-spec dial(Swarm::pid(), Chain::blockchain:blockchain(), Peer::libp2p_crypto:pubkey_bin())->
        {ok, pid()} | {error, any()}.
dial(Swarm, Chain, Peer) ->
    DialFun =
        fun
            Dial([])->
                lager:debug("dialing Sync stream failed, no compatible protocol versions",[]),
                {error, no_supported_protocols};
            Dial([ProtocolVersion | Rest]) ->
                case blockchain_sync_handler:dial(Swarm, Chain, Peer, ProtocolVersion) of
                        {ok, Stream} ->
                            lager:debug("dialing Sync stream successful, stream pid: ~p, protocol version: ~p", [Stream, ProtocolVersion]),
                            {ok, Stream};
                        {error, protocol_unsupported} ->
                            lager:debug("dialing Sync stream failed with protocol version: ~p, trying next supported protocol version",[ProtocolVersion]),
                            Dial(Rest);
                        {error, Reason} ->
                            lager:debug("dialing Sync stream failed: ~p",[Reason]),
                            {error, Reason}
                end
        end,
    DialFun(?SUPPORTED_SYNC_PROTOCOLS).

-spec dial(Swarm::pid(), Chain::blockchain:blockchain(), Peer::libp2p_crypto:pubkey_bin(), ProtocolVersino::string())->
            {ok, pid()} | {error, any()}.
dial(Swarm, Chain, Peer, ProtocolVersion)->
    libp2p_swarm:dial_framed_stream(Swarm,
                                    Peer,
                                    ProtocolVersion,
                                    ?MODULE,
                                    [ProtocolVersion, Chain]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, [Path, Blockchain]) ->
    case blockchain_worker:sync_paused() of
        true ->
            {stop, normal};
        false ->
            BatchSize = application:get_env(blockchain, block_sync_batch_size, 5),
            BatchLimit = application:get_env(blockchain, block_sync_batch_limit, 40),
            {ok, #state{blockchain=Blockchain, batch_size=BatchSize, batch_limit=BatchLimit,
                        path=Path}}
    end;
init(server, _Conn, [_, _HandlerModule, [Path, Blockchain]] = _Args) ->
    BatchSize = application:get_env(blockchain, block_sync_batch_size, 5),
    BatchLimit = application:get_env(blockchain, block_sync_batch_limit, 40),
    {ok, #state{blockchain=Blockchain, batch_size=BatchSize, batch_limit=BatchLimit,
                path=Path}}.

handle_data(client, Data0, #state{blockchain=Chain, path=Path}=State) ->
    Data =
        case Path of
            ?SYNC_PROTOCOL_V1 -> Data0;
            ?SYNC_PROTOCOL_V2 -> zlib:uncompress(Data0)
        end,
    #blockchain_sync_blocks_pb{blocks=BinBlocks} =
        blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_blocks_pb),
    Blocks = [blockchain_block:deserialize(B) || B <- BinBlocks],
    lager:info("adding sync blocks ~p", [[blockchain_block:height(B) || B <- Blocks]]),
    %% do this in a spawn so that the connection dying does not stop adding blocks
    {Pid, Ref} = spawn_monitor(fun() ->
                          case blockchain:add_blocks(Blocks, Chain) of
                              ok ->
                                  ok;
                              exists ->
                                  ok;
                              Error ->
                                  lager:info("Error adding blocks ~p", [Error]),
                                  erlang:error(Error)
                          end
                  end),
    receive
        {'DOWN', Ref, process, Pid, normal} ->
            {noreply, State, blockchain_sync_handler_pb:encode_msg(#blockchain_sync_req_pb{msg={response, true}})};
        {'DOWN', Ref, process, Pid, _Error} ->
            %% TODO: maybe dial for sync again?
            {stop, normal, State, blockchain_sync_handler_pb:encode_msg(#blockchain_sync_req_pb{msg={response, false}})}
    end;
handle_data(server, Data, #state{blockchain=Blockchain, batch_size=BatchSize,
                                 batches_sent=Sent, batch_limit=Limit,
                                 path=Path}=State) ->
    case blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_req_pb) of
        #blockchain_sync_req_pb{msg={hash, #blockchain_sync_hash_pb{hash=Hash}}} ->
            case blockchain:get_block(Hash, Blockchain) of
                {ok, StartingBlock} ->
                    case blockchain:build(StartingBlock, Blockchain, BatchSize) of
                        [] ->
                            {stop, normal, State};
                        Blocks ->
                            Msg1 = #blockchain_sync_blocks_pb{blocks=[blockchain_block:serialize(B) || B <- Blocks]},
                            Msg0 = blockchain_sync_handler_pb:encode_msg(Msg1),
                            Msg = case Path of
                                      ?SYNC_PROTOCOL_V1 -> Msg0;
                                      ?SYNC_PROTOCOL_V2 -> zlib:compress(Msg0)
                                  end,
                            {noreply, State#state{batches_sent=Sent+1, block=lists:last(Blocks)}, Msg}
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
                    Msg1 = #blockchain_sync_blocks_pb{blocks=[blockchain_block:serialize(B) || B <- Blocks]},
                    Msg0 = blockchain_sync_handler_pb:encode_msg(Msg1),
                    Msg = case Path of
                              ?SYNC_PROTOCOL_V1 -> Msg0;
                              ?SYNC_PROTOCOL_V2 -> zlib:compress(Msg0)
                          end,
                    {noreply, State#state{batches_sent=Sent+1, block=lists:last(Blocks)}, Msg}
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
