%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain fastforward Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_fastforward_handler).

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
    libp2p_framed_stream:server(?MODULE, Connection, Args).

-spec dial(Swarm::pid(), Chain::blockchain:blockchain(), Peer::libp2p_crypto:pubkey_bin())->
        {ok, pid()} | {error, any()}.
dial(Swarm, Chain, Peer) ->
    DialFun =
        fun
            Dial([])->
                lager:debug("dialing FF stream failed, no compatible protocol versions",[]),
                {error, no_supported_protocols};
            Dial([ProtocolVersion | Rest]) ->
                case blockchain_fastforward_handler:dial(Swarm, Chain, Peer, ProtocolVersion) of
                        {ok, Stream} ->
                            lager:info("dialing FF stream successful, stream pid: ~p, protocol version: ~p", [Stream, ProtocolVersion]),
                            {ok, Stream};
                        {error, protocol_unsupported} ->
                            lager:debug("dialing FF stream failed with protocol version: ~p, trying next supported protocol version",[ProtocolVersion]),
                            Dial(Rest);
                        {error, Reason} ->
                            lager:debug("dialing FF stream failed: ~p",[Reason]),
                            {error, Reason}
                end
        end,
    DialFun(?SUPPORTED_FASTFORWARD_PROTOCOLS).

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
    lager:debug("started fastforward_handler client"),
    {ok, #state{blockchain=Blockchain, path=Path}};
init(server, _Conn, [_, _HandlerModule, [Path, Blockchain]] = _Args) ->
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
            {ok, #state{blockchain=Blockchain, path=Path}, blockchain_sync_handler_pb:encode_msg(Msg)}
    catch _:_ ->
              {stop, normal}
    end.

handle_data(client, Data, #state{blockchain=Blockchain}=State) ->
    #blockchain_sync_hash_pb{hash=Hash} =
    blockchain_sync_handler_pb:decode_msg(Data, blockchain_sync_hash_pb),
    case blockchain:get_block(Hash, Blockchain) of
        {ok, Block} ->
            Blocks = blockchain:build(Block, Blockchain, 200),
            Msg0 = #blockchain_sync_blocks_pb{blocks=[blockchain_block:serialize(B) || B <- Blocks]},
            Msg = blockchain_sync_handler_pb:encode_msg(Msg0),

            case State#state.path of
                ?FASTFORWARD_PROTOCOL_V1 ->
                    {stop, normal, State, Msg};
                ?FASTFORWARD_PROTOCOL_V2 ->
                    {stop, normal, State, zlib:compress(Msg)}
            end;
        {error, _Reason} ->
            %% peer is ahead of us
            {stop, normal, State}
    end;
handle_data(server, Data0, #state{blockchain=Blockchain, path=Path}=State) ->
    Data =
        case Path of
            ?FASTFORWARD_PROTOCOL_V1 -> Data0;
            ?FASTFORWARD_PROTOCOL_V2 -> zlib:uncompress(Data0)
        end,

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
