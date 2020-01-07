%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Gossip Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_gossip_handler).

-behavior(libp2p_group_gossip_handler).

-include("blockchain.hrl").
-include("pb/blockchain_gossip_handler_pb.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
         init_gossip_data/1,
         handle_gossip_data/3,
         add_block/4,
         gossip_data/2
        ]).

init_gossip_data([Swarm, Blockchain]) ->
    lager:debug("gossiping init"),
    {ok, Block} = blockchain:head_block(Blockchain),
    lager:debug("gossiping block to peers on init"),
    {send, gossip_data(Swarm, Block)};
init_gossip_data(WAT) ->
    lager:info("WAT ~p", [WAT]),
    {send, <<>>}.

handle_gossip_data(_StreamPid, Data, [Swarm, Blockchain]) ->
    try
        #blockchain_gossip_block_pb{from=From, block=BinBlock} =
            blockchain_gossip_handler_pb:decode_msg(Data, blockchain_gossip_block_pb),
        Block = blockchain_block:deserialize(BinBlock),
        case blockchain_block:type(Block) of
            undefined ->
                lager:notice("gossip_handler unknown block: ~p", [Block]);
            _ ->
                lager:debug("Got block: ~p from: ~p", [Block, From]),
                add_block(Swarm, Block, Blockchain, From)
        end
    catch
        _What:Why ->
            lager:notice("gossip handler got bad data: ~p", [Why])
    end,
    noreply.

add_block(Swarm, Block, Chain, Sender) ->
    lager:debug("Sender: ~p, MyAddress: ~p", [Sender, blockchain_swarm:pubkey_bin()]),
    case blockchain:add_block(Block, Chain) of
        ok ->
            ok;
        {error, disjoint_chain} ->
            lager:warning("gossipped block doesn't fit with our chain"),
            P2PPubkeyBin = libp2p_crypto:pubkey_bin_to_p2p(Sender),
            lager:info("syncing with the sender ~p", [P2PPubkeyBin]),
            case libp2p_swarm:dial_framed_stream(Swarm,
                                                 P2PPubkeyBin,
                                                 ?SYNC_PROTOCOL,
                                                 blockchain_sync_handler,
                                                 [Chain])
            of
                {ok, Stream} ->
                    erlang:unlink(Stream),
                    {ok, HeadHash} = blockchain:head_hash(Chain),
                    Stream ! {hash, HeadHash};
                _Error ->
                    lager:warning("Failed to dial sync service on: ~p ~p", [P2PPubkeyBin, _Error])
            end;
        Error ->
            %% Uhm what is this?
            lager:error("Something bad happened: ~p", [Error])
    end.

-spec gossip_data(libp2p_swarm:swarm(), blockchain_block:block()) -> binary().
gossip_data(Swarm, Block) ->
    PubKeyBin = libp2p_swarm:pubkey_bin(Swarm),
    BinBlock = blockchain_block:serialize(Block),
    Msg= #blockchain_gossip_block_pb{from=PubKeyBin, block=BinBlock},
    blockchain_gossip_handler_pb:encode_msg(Msg).
