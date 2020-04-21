%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Gossip Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_gossip_handler).

-behavior(libp2p_group_gossip_handler).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_gossip_handler_pb.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
         init_gossip_data/1,
         handle_gossip_data/3,
         add_block/3,
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
                case blockchain:is_block_plausible(Block, Blockchain) of
                    true ->
                        lager:debug("Got block: ~p from: ~p", [Block, From]),
                        %% don't block the gossip server
                        spawn(fun() -> add_block(Block, Blockchain, From) end),
                        %% pass it along
                        libp2p_group_gossip:send(
                          libp2p_swarm:gossip_group(Swarm),
                          ?GOSSIP_PROTOCOL,
                          blockchain_gossip_handler:gossip_data(Swarm, Block)
                         );
                    false ->
                        ok
                end
        end
    catch
        _What:Why ->
            lager:notice("gossip handler got bad data: ~p", [Why])
    end,
    noreply.

add_block(Block, Chain, Sender) ->
    lager:debug("Sender: ~p, MyAddress: ~p", [Sender, blockchain_swarm:pubkey_bin()]),
    case blockchain:add_block(Block, Chain) of
        ok ->
            lager:info("got gossipped block ~p", [blockchain_block:height(Block)]),
            ok;
        plausible ->
            lager:warning("plausuble gossipped block doesn't fit with our chain, will start sync if not already active"),
            blockchain_worker:maybe_sync(),
            ok;
        {error, disjoint_chain} ->
            lager:warning("gossipped block ~p doesn't fit with our chain,"
                          " will start sync if not already active", [blockchain_block:height(Block)]),
            blockchain_worker:maybe_sync(),
            ok;
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
