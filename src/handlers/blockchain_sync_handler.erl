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

handle_data(client, Data, #state{blockchain=Chain, n=N}=State) ->
    lager:info("client got data: ~p", [Data]),
    Blocks = erlang:binary_to_term(Data),
    F = ((N-1) div 3),
    % TODO: Too much nesting
    lists:foreach(
      fun(Block) ->
              case blockchain:head_hash(Chain) of
                  {error, _Reason} ->
                      lager:error("could not get head hash ~p", [_Reason]),
                      ok;
                  {ok, Head} ->
                      case blockchain_block:prev_hash(Block) == Head of
                          false ->
                              ok;
                          true ->
                              lager:info("prev hash matches the gossiped block"),
                              Ledger = blockchain:ledger(Chain),
                              case blockchain_ledger_v1:consensus_members(Ledger) of
                                  {error, _Reason} ->
                                      lager:error("could not get consensus_members ~p", [_Reason]);
                                  {ok, ConsensusAddrs} ->
                                      case blockchain_block:verify_signature(Block,
                                                                             ConsensusAddrs,
                                                                             blockchain_block:signature(Block),
                                                                             N-F)
                                      of
                                          false ->
                                              ok;
                                          {true, _} ->
                                              case blockchain:add_block(Block, Chain) of
                                                  {error, _} ->
                                                      ok;
                                                  ok ->
                                                      ok = blockchain_worker:notify({add_block, blockchain_block:hash_block(Block), false})
                                              end
                                      end
                              end
                      end
              end
      end,
        Blocks
    ),
    blockchain_worker:synced_blocks(),
    {stop, normal, State};
handle_data(server, Data, #state{blockchain=Blockchain}=State) ->
    lager:info("server got data: ~p", [Data]),
    {hash, Hash} = erlang:binary_to_term(Data),
    lager:info("syncing blocks with peer hash ~p", [Hash]),
    StartingBlock =
        case blockchain:get_block(Hash, Blockchain) of
            {ok, Block} ->
                Block;
            {error, _Reason} ->
                {ok, B} = blockchain:genesis_block(Blockchain),
                B
        end,
    Blocks = blockchain:build(StartingBlock, Blockchain, 200),
    {stop, normal, State, erlang:term_to_binary(Blocks)}.

handle_info(client, {hash, Hash}, State) ->
    {noreply, State, erlang:term_to_binary({hash, Hash})};
handle_info(_Type, _Msg, State) ->
    lager:info("rcvd unknown type: ~p unknown msg: ~p", [_Type, _Msg]),
    {noreply, State}.
