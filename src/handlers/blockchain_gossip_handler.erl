%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Gossip Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_gossip_handler).

-behavior(libp2p_group_gossip_handler).

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
         init_gossip_data/1,
         handle_gossip_data/2,
         add_block/5
        ]).

init_gossip_data([Swarm, _N, Blockchain]) ->
    lager:info("gossiping init"),
    {ok, Block} = blockchain:head_block(Blockchain),
    lager:info("gossiping block to peers on init"),
    Address = libp2p_swarm:address(Swarm),
    {send, term_to_binary({block, Address, Block})};
init_gossip_data(WAT) ->
    lager:info("WAT ~p", [WAT]),
    {send, <<>>}.

handle_gossip_data(Data, [Swarm, N, Blockchain]) ->
    case erlang:binary_to_term(Data) of
        {block, From, Block} ->
            case blockchain_block:is_block(Block) of
                true ->
                    lager:info("Got block: ~p from: ~p", [Block, From]),
                    add_block(Swarm, Block, Blockchain, N, From);
                _ ->
                    lager:notice("gossip_handler received invalid data: ~p", [Block])
            end;
        Other ->
            lager:notice("gossip handler got unknown data ~p", [Other])
    end,
    ok.


add_block(Swarm, Block, Chain, N, Sender) ->
    lager:info("Sender: ~p, MyAddress: ~p", [Sender, blockchain_swarm:address()]),
    Hash = blockchain_block:hash_block(Block),
    F = ((N-1) div 3),
    case blockchain:head_hash(Chain) of
        {error, _Reason} ->
            lager:error("could not get head hash ~p", [_Reason]);
        {ok, Head} ->
            case blockchain_block:prev_hash(Block) =:= Head of
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
                                {true, _} ->
                                    case blockchain:add_block(Block, Chain) of
                                        {error, _Reason} ->
                                            lager:error("failed to add block ~p", [_Reason]);
                                        ok ->
                                            lager:info("sending the gossipped block to other workers"),
                                            Address = libp2p_swarm:address(Swarm),
                                            libp2p_group_gossip:send(
                                              libp2p_swarm:gossip_group(Swarm),
                                              ?GOSSIP_PROTOCOL,
                                              term_to_binary({block, Address, Block})
                                             ),
                                            ok = blockchain_worker:notify({add_block, Hash, true})
                                    end;
                                false ->
                                    lager:warning("signature on block ~p is invalid", [Block])
                            end
                    end;
                false when Hash == Head ->
                    lager:info("already have this block");
                false ->
                    lager:warning("gossipped block doesn't fit with our chain"),
                    P2PAddress = libp2p_crypto:address_to_p2p(Sender),
                    lager:info("syncing with the sender ~p", [P2PAddress]),
                    case libp2p_swarm:dial_framed_stream(Swarm,
                                                         P2PAddress,
                                                         ?SYNC_PROTOCOL,
                                                         blockchain_sync_handler,
                                                         [N, Chain]) of
                        {ok, Stream} ->
                            unlink(Stream),
                            {ok, HeadHash} = blockchain:head_hash(Chain),
                            Stream ! {hash, HeadHash};
                        _Error ->
                            lager:warning("Failed to dial sync service on: ~p ~p", [P2PAddress, _Error])
                    end
            end
    end.

