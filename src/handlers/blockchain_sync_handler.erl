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
          n :: pos_integer(),
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
init(client, _Conn, [N, Blockchain]) ->
    lager:info("started sync_handler client"),
    {ok, #state{n=N, blockchain=Blockchain}};
init(server, _Conn, [_Path, _, N, Blockchain]) ->
    lager:info("started sync_handler server"),
    {ok, #state{n=N, blockchain=Blockchain}}.

handle_data(client, Data, #state{blockchain=Chain}=State) ->
    lager:info("client got data: ~p", [Data]),
    Blocks = erlang:binary_to_term(Data),
    case blockchain:add_blocks(Blocks, Chain) of
        ok ->
            {ok, HeadHash} = blockchain:head_hash(Chain),
            ok = blockchain_worker:notify({add_block, HeadHash, false}),
            blockchain_worker:synced_blocks();
        Error ->
            %% TODO: maybe dial for sync again?
            lager:error("Couldn't sync blocks, error: ~p", [Error])
    end,
    {stop, normal, State};
handle_data(server, Data, #state{blockchain=Blockchain}=State) ->
    lager:info("server got data: ~p", [Data]),
    {hash, Hash} = erlang:binary_to_term(Data),
    lager:info("syncing blocks with peer hash ~p", [Hash]),
    case blockchain:get_block(Hash, Blockchain) of
        {ok, Block} ->
            case blockchain_block:is_genesis(Block) of
                true ->
                    lager:warning("Will not send the genesis block"),
                    {stop, normal, State};
                false ->
                    Blocks = blockchain:build(Block, Blockchain, 200),
                    {stop, normal, State, erlang:term_to_binary(Blocks)}
            end;
        {error, _Reason}=Error ->
            lager:error("Unable to get_block, error: ~p, hash: ~p", [Error, Hash]),
            {stop, normal, State}
    end.

handle_info(client, {hash, Hash}, State) ->
    {noreply, State, erlang:term_to_binary({hash, Hash})};
handle_info(_Type, _Msg, State) ->
    lager:info("rcvd unknown type: ~p unknown msg: ~p", [_Type, _Msg]),
    {noreply, State}.
